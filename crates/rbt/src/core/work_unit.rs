//! # Work-unit IR — partition-aware execution (RBT-C)
//!
//! This module is the **pure planning IR** for concurrent / partition-local lake runs.
//! It has **no** DataFusion session, no I/O, and no side effects: given a
//! [`ModelDag`], a [`RunScope`], and [`ConcurrencyConfig`], it produces an
//! [`ExecutionPlan`] of [`WorkUnit`]s that the engine (or an external host) executes.
//!
//! ## Why this exists
//!
//! rbt already had the *ingredients* for scale (topo tiers, multi-value scope,
//! `scoped_replace` parts, stream writers) but historically treated multi-value
//! scope as **one filtered SQL plan** and ran independent tier models **serially**.
//!
//! For medallion lakes where grain is **partition-local** (e.g. OHLCV + indicators
//! per `symbol`), wall-clock is dominated by serial I/O and full-table rewrite—not
//! by Parquet encoding. The fix is not a cost-based SQL optimizer; it is:
//!
//! 1. Treat **partition layout** as an execution contract (parts + manifest).
//! 2. Expand multi-value scope into **WorkUnits** when safe.
//! 3. Optionally run units with **bounded workers** and **private sessions**.
//!
//! ## How hosts use this
//!
//! ### In-process (library / CLI)
//!
//! ```yaml
//! # rbt_project.yml
//! execution:
//!   concurrency:
//!     enabled: true
//!     max_workers: 8
//!     strategy: partition   # serial | model_tier | partition | auto
//!     multi_value_fanout_threshold: 4
//!     dirty_part_skip: true
//! ```
//!
//! ```bash
//! rbt run -p proj --jobs 8 --execution-strategy partition \
//!   --var-file symbol=symbols.txt
//! rbt explain -p proj --plan --jobs 8 --var-file symbol=symbols.txt
//! ```
//!
//! ### External protocol (orchestrator / dual-track T2)
//!
//! ```bash
//! rbt work-units -p proj --json --jobs 8 --var-file symbol=symbols.txt
//! # host spawns: rbt run -p proj --select stg_x --var symbol=AAPL
//! # then merges manifests / receipts
//! ```
//!
//! ## Fan-out eligibility (v1 rules)
//!
//! A model is expanded into N partition units only when **all** hold:
//!
//! | Condition | Meaning |
//! |-----------|---------|
//! | Concurrency enabled + strategy `partition` or `auto` | Opt-in |
//! | [`ParallelContract`] allows fan-out | Not `Global` / not forced serial |
//! | `materialization: scoped_replace` | Physical unit of isolation = part file |
//! | Multi-value size ≥ `multi_value_fanout_threshold` | Avoid tiny fan-outs |
//! | Part keys present in scope | `part_key` / `partition_by` |
//!
//! Otherwise the engine keeps the **mega plan** (multi-value as SQL `IN` filter).
//!
//! ## Design patterns
//!
//! - **IR-first dual frontend** — same `ExecutionPlan` for CLI, library, JSON export
//! - **Fail-closed classification** — [`ParallelContract::Unknown`] → mega plan
//! - **No N YAML models** — one model × N bindings = N units
//! - **Layout before threads** — fan-out is correct even with `max_workers: 1`
//!
//! ## Related
//!
//! - Config: [`ConcurrencyConfig`], [`ExecutionStrategy`] in `project.rs`
//! - Design B partition API (Phase 2): [`crate::RustModel::execute_partition`]
//! - Analysis: `docs/analysis/partition-concurrent-execution-user-feedback.md`
//! - Plan: `docs/plans/partition-work-units-and-concurrent-scheduler.md`

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::dag::{Materialization, ModelDag, ModelKind, ModelNode};
use super::project::{ConcurrencyConfig, ExecutionStrategy};
use super::run_scope::{RunScope, ScopeValue};

/// Declares how safely a model may be split across partitions for scheduling.
///
/// Used by:
/// - the **planner** ([`classify_parallel_contract`] / frontmatter heuristics)
/// - **Design B** hosts via [`crate::RustModel::parallel_contract`] (Phase 2)
/// - JSON work-unit export for external orchestrators
///
/// # Semantics
///
/// | Variant | Scheduler behavior |
/// |---------|-------------------|
/// | [`Unknown`](Self::Unknown) | Mega plan (IN filter). Safe default. |
/// | [`Global`](Self::Global) | Mega plan. Cross-partition grain (global window, join). |
/// | [`PartitionLocal`](Self::PartitionLocal) | Eligible for multi-value → N WorkUnits |
/// | [`MapOnly`](Self::MapOnly) | Row-local; may fan-out when keys are known |
///
/// # Frontmatter override
///
/// ```yaml
/// parallel_safe: true    # → PartitionLocal { keys from part_key/partition_by }
/// parallel_safe: false   # → Global (force mega)
/// ```
///
/// # Design B
///
/// ```rust,ignore
/// fn parallel_contract(&self) -> ParallelContract {
///     ParallelContract::PartitionLocal { keys: vec!["symbol".into()] }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ParallelContract {
    /// Conservative default — single mega plan (IN-filter path). Prefer this when unsure.
    Unknown,
    /// Needs full table: global windows, cross-symbol joins, full-table sorts, etc.
    Global,
    /// Safe to fan-out by these keys (should align with `partition_by` / `part_key`).
    PartitionLocal {
        /// Partition key column names, e.g. `["symbol"]` or `["entity", "report_date"]`.
        keys: Vec<String>,
    },
    /// Row-local map; may split any way. Treated like partition-local when keys exist.
    MapOnly,
}

impl ParallelContract {
    /// Stable wire / log name (`unknown`, `global`, `partition_local`, `map_only`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Global => "global",
            Self::PartitionLocal { .. } => "partition_local",
            Self::MapOnly => "map_only",
        }
    }

    /// Whether the planner may expand multi-value scope into partition WorkUnits.
    pub fn allows_partition_fanout(&self) -> bool {
        matches!(self, Self::PartitionLocal { .. } | Self::MapOnly)
    }

    /// Partition keys when [`PartitionLocal`](Self::PartitionLocal); empty otherwise.
    pub fn partition_keys(&self) -> &[String] {
        match self {
            Self::PartitionLocal { keys } => keys.as_slice(),
            _ => &[],
        }
    }
}

/// Reference to a physical part file (for plans, receipts, external protocols).
///
/// Not required for v1 execution (the engine derives paths from `scope_id`), but
/// useful in JSON exports and future bytes-aware scheduling (Phase 3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartRef {
    /// Absolute or project-relative path to a parquet part (or hive data file).
    pub path: String,
    /// Partition key → value bindings for this part (e.g. `symbol=AAPL`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub keys: BTreeMap<String, String>,
    /// Content fingerprint of the part bytes (`fnv1a64:…` / `blake3:…`) when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_fp: Option<String>,
    /// Row count when known from manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
}

/// One schedulable unit of lake work (model × optional partition bindings).
///
/// # Identity
///
/// - Mega plan: `id == model` (e.g. `"stg_bars"`)
/// - Partition slice: `id == "{model}#p{i}"` (e.g. `"stg_bars#p0"`)
///
/// # Execution
///
/// The engine narrows [`RunScope`] via [`scope_for_unit`] so bronze filters and
/// `scoped_replace` `scope_id` see **scalar** partition values for that unit.
///
/// Design B Phase 2: when `is_partition_slice` and the host implements
/// [`crate::RustModel::execute_partition`], the engine feeds **one part’s** batches
/// only (see [`crate::PartitionInput`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkUnit {
    /// Unique unit id within the plan (`model` or `model#pN`).
    pub id: String,
    /// DAG model name (registry key for Design B).
    pub model: String,
    /// Empty ⇒ whole-model / mega plan under the parent run scope.
    /// Non-empty ⇒ scalar binds for this slice (e.g. `symbol → AAPL`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub partition_bindings: BTreeMap<String, String>,
    /// Reserved for future inter-unit deps (v1 uses tier barriers only).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deps: Vec<String>,
    /// Optional size hint for future bytes-aware scheduling (Phase 3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_rows: Option<u64>,
    /// Classification used when this unit was planned.
    pub parallel_contract: ParallelContract,
    /// True when this unit is a partition slice (not the multi-value mega plan).
    #[serde(default)]
    pub is_partition_slice: bool,
    /// When true, engine may skip if part `source_fp` matches current bronze fp.
    #[serde(default)]
    pub skip_if_clean: bool,
}

/// Full plan for a DAG run under a scope + concurrency config.
///
/// Produced by [`plan_execution`]. Consumed by:
/// - `TransformationEngine::execute_dag_with_scope` (in-process)
/// - `rbt explain --plan` / `rbt work-units --json` (export)
///
/// # Fields of interest for hosts
///
/// - `units` — flat list in tier order (fan-out expands one model into many)
/// - `tiers` — unit ids per topo tier (L1 concurrent models share a tier)
/// - `notes` — human-readable why fan-out or mega was chosen
/// - `max_workers` / `strategy` — effective scheduling policy
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    /// Effective strategy after applying `enabled` (disabled ⇒ always serial).
    pub strategy: ExecutionStrategy,
    /// Max in-flight workers (models or partition units).
    pub max_workers: usize,
    /// Whether concurrency was enabled for this plan.
    pub concurrency_enabled: bool,
    /// Flattened units in dependency-respecting order (tier order preserved).
    pub units: Vec<WorkUnit>,
    /// Tier index → unit ids in that tier (for L1 concurrent models).
    pub tiers: Vec<Vec<String>>,
    /// Human notes (why mega vs fan-out). Safe to show in CLI.
    #[serde(default)]
    pub notes: Vec<String>,
}

impl ExecutionPlan {
    /// Number of units that are partition slices (not mega plans).
    pub fn partition_slice_count(&self) -> usize {
        self.units.iter().filter(|u| u.is_partition_slice).count()
    }

    /// Distinct model names appearing in the plan (sorted).
    pub fn model_names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.units.iter().map(|u| u.model.clone()).collect();
        v.sort();
        v.dedup();
        v
    }
}

/// Classify a model for partition fan-out without a Design B registry (planner-time).
///
/// # Order of precedence
///
/// 1. Frontmatter `parallel_safe: false` → [`ParallelContract::Global`]
/// 2. Frontmatter `parallel_safe: true` → [`PartitionLocal`](ParallelContract::PartitionLocal)
///    (or [`MapOnly`](ParallelContract::MapOnly) if no keys)
/// 3. `materialization: scoped_replace` + part keys → [`PartitionLocal`](ParallelContract::PartitionLocal)
/// 4. SQL heuristic: window `OVER (` without `PARTITION BY` → [`Global`](ParallelContract::Global)
/// 5. Else [`Unknown`](ParallelContract::Unknown)
///
/// Design B hosts should **also** implement [`crate::RustModel::parallel_contract`];
/// at execute time the engine prefers the trait method for `execute_partition` routing.
pub fn classify_parallel_contract(model: &ModelNode) -> ParallelContract {
    // Explicit frontmatter opt-out / opt-in
    if let Some(fm) = model.frontmatter.as_ref() {
        if fm.parallel_safe == Some(false) {
            return ParallelContract::Global;
        }
        if fm.parallel_safe == Some(true) {
            let keys = resolve_partition_keys(model);
            if keys.is_empty() {
                return ParallelContract::MapOnly;
            }
            return ParallelContract::PartitionLocal { keys };
        }
    }

    // scoped_replace + part keys → partition-local candidate (SQL or Design B)
    if matches!(model.materialization, Materialization::ScopedReplace) {
        let keys = resolve_partition_keys(model);
        if !keys.is_empty() {
            return ParallelContract::PartitionLocal { keys };
        }
    }

    // Design B nodes without scoped_replace still default Unknown (host trait at execute)
    if matches!(model.kind, ModelKind::Rust) {
        return ParallelContract::Unknown;
    }

    // Global window heuristic: OVER ( without PARTITION BY
    if looks_like_global_window(&model.compiled_sql) || looks_like_global_window(&model.raw_sql) {
        return ParallelContract::Global;
    }

    ParallelContract::Unknown
}

/// Keys used for part identity / fan-out (`part_key` else `partition_by`).
///
/// Empty when the model has no frontmatter partition contract — such models
/// cannot fan-out (there is no stable part identity).
pub fn resolve_partition_keys(model: &ModelNode) -> Vec<String> {
    let Some(fm) = model.frontmatter.as_ref() else {
        return Vec::new();
    };
    if let Some(pk) = fm.part_key.as_ref() {
        if !pk.is_empty() {
            return pk.clone();
        }
    }
    fm.partition_by.clone().unwrap_or_default()
}

fn looks_like_global_window(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("OVER") {
        return false;
    }
    if upper.contains("PARTITION BY") {
        return false;
    }
    upper.contains("OVER (") || upper.contains("OVER(")
}

/// Build an execution plan for the DAG under scope + concurrency config.
///
/// # Arguments
///
/// * `dag` — compiled model graph (file project or [`crate::DagBuilder`])
/// * `scope` — run vars (multi-value keys drive fan-out)
/// * `concurrency` — from `rbt_project.yml` `execution.concurrency` + CLI overrides
///
/// # Returns
///
/// An [`ExecutionPlan`] ready for the engine or JSON export. Never starts workers.
///
/// # Errors
///
/// - DAG tier computation failures (cycles should already be rejected at build)
/// - Empty partition axes when expanding bindings
pub fn plan_execution(
    dag: &ModelDag,
    scope: &RunScope,
    concurrency: &ConcurrencyConfig,
) -> anyhow::Result<ExecutionPlan> {
    let tiers = dag.execution_tiers()?;
    let strategy = concurrency.effective_strategy();
    let max_workers = concurrency.effective_max_workers();
    let enabled = concurrency.is_enabled();
    let threshold = concurrency.multi_value_fanout_threshold.max(1);

    let mut units: Vec<WorkUnit> = Vec::new();
    let mut tier_ids: Vec<Vec<String>> = Vec::new();
    let mut notes: Vec<String> = Vec::new();

    let fanout_allowed = enabled
        && matches!(
            strategy,
            ExecutionStrategy::Partition | ExecutionStrategy::Auto
        );

    for (tier_idx, tier) in tiers.iter().enumerate() {
        let mut ids = Vec::new();
        for model in tier {
            let contract = classify_parallel_contract(model);
            let part_keys = resolve_partition_keys(model);
            let multi_fanout_keys: Vec<String> = part_keys
                .iter()
                .filter(|k| {
                    scope
                        .vars
                        .get(k.as_str())
                        .map(|v| v.is_multi() && v.len() >= threshold)
                        .unwrap_or(false)
                })
                .cloned()
                .collect();

            let want_fanout = fanout_allowed
                && contract.allows_partition_fanout()
                && matches!(model.materialization, Materialization::ScopedReplace)
                && !multi_fanout_keys.is_empty()
                // all part_keys that are multi must be in fanout set; scalar part keys stay fixed
                && part_keys.iter().all(|k| {
                    scope
                        .vars
                        .get(k)
                        .map(|v| !v.is_multi() || multi_fanout_keys.contains(k))
                        .unwrap_or(true)
                });

            if want_fanout {
                let slices = expand_partition_bindings(&part_keys, scope)?;
                notes.push(format!(
                    "tier{tier_idx}/{}: fan-out {} partition unit(s) (contract={}, threshold={threshold})",
                    model.name,
                    slices.len(),
                    contract.as_str()
                ));
                for (i, bindings) in slices.into_iter().enumerate() {
                    let id = format!("{}#p{i}", model.name);
                    ids.push(id.clone());
                    units.push(WorkUnit {
                        id,
                        model: model.name.clone(),
                        partition_bindings: bindings,
                        deps: Vec::new(),
                        estimated_bytes: None,
                        estimated_rows: None,
                        parallel_contract: contract.clone(),
                        is_partition_slice: true,
                        skip_if_clean: concurrency.dirty_part_skip,
                    });
                }
            } else {
                if !multi_fanout_keys.is_empty() && !want_fanout {
                    notes.push(format!(
                        "tier{tier_idx}/{}: multi-value kept as mega/IN plan (strategy={:?}, contract={}, mat={})",
                        model.name,
                        strategy,
                        contract.as_str(),
                        model.materialization.as_str()
                    ));
                }
                let id = model.name.clone();
                ids.push(id.clone());
                units.push(WorkUnit {
                    id,
                    model: model.name.clone(),
                    partition_bindings: BTreeMap::new(),
                    deps: Vec::new(),
                    estimated_bytes: None,
                    estimated_rows: None,
                    parallel_contract: contract,
                    is_partition_slice: false,
                    skip_if_clean: false,
                });
            }
        }
        tier_ids.push(ids);
    }

    if !enabled {
        notes.push(
            "concurrency disabled: serial mega path (set execution.concurrency.enabled or --jobs)"
                .into(),
        );
    }

    let mut plan = ExecutionPlan {
        strategy,
        max_workers,
        concurrency_enabled: enabled,
        units,
        tiers: tier_ids,
        notes,
    };

    if concurrency.large_parts_first && enabled {
        plan.notes.push(
            "cost heuristic: large_parts_first=true (sort partition units by estimated_bytes desc when known)"
                .into(),
        );
    }
    if let Some(cap) = concurrency.max_inflight_bytes {
        plan.notes.push(format!(
            "cost heuristic: max_inflight_bytes={cap} (advisory; hard backpressure Phase 3+)"
        ));
    }

    Ok(plan)
}

/// Phase 3: attach estimated_bytes/rows from on-disk manifests and reorder partition
/// slices largest-first when `large_parts_first`.
///
/// Safe no-op when paths are missing (cold start). Call after [`plan_execution`].
pub fn enrich_plan_from_manifests(plan: &mut ExecutionPlan, dag: &ModelDag) {
    use crate::materializer::{
        load_manifest, resolve_parts_layout, scoped_part_rel_path, table_layout_root, PartsLayout,
    };

    // Model name → layout root path
    let mut roots: BTreeMap<String, (std::path::PathBuf, PartsLayout)> = BTreeMap::new();
    for (name, &idx) in &dag.node_map {
        let node = &dag.graph[idx];
        let Some(ref op) = node.output_path else {
            continue;
        };
        let dest = std::path::PathBuf::from(op);
        let layout = resolve_parts_layout(
            node.frontmatter
                .as_ref()
                .and_then(|f| f.parts_layout.as_deref()),
            None,
        );
        let root = table_layout_root(&dest, layout);
        if root.is_dir() {
            roots.insert(name.clone(), (root, layout));
        }
    }

    for unit in &mut plan.units {
        if !unit.is_partition_slice {
            continue;
        }
        let Some((root, layout)) = roots.get(&unit.model) else {
            continue;
        };
        let Ok(man) = load_manifest(root) else {
            continue;
        };
        // Match part by keys in part_meta
        let mut best: Option<(u64, u64)> = None; // bytes, rows
        for meta in man.part_meta.values() {
            if !unit.partition_bindings.is_empty()
                && unit
                    .partition_bindings
                    .iter()
                    .all(|(k, v)| meta.keys.get(k) == Some(v))
            {
                best = Some((
                    meta.bytes.unwrap_or(0),
                    meta.rows.unwrap_or(0),
                ));
                break;
            }
        }
        // Fallback: reconstruct rel path for parts layout with scope_id unknown — skip
        let _ = (layout, scoped_part_rel_path);
        if let Some((b, r)) = best {
            if b > 0 {
                unit.estimated_bytes = Some(b);
            }
            if r > 0 {
                unit.estimated_rows = Some(r);
            }
        }
    }

    // Reorder: within each contiguous model-name group of slices, sort large first
    // Rebuild units list preserving mega units and model order from tiers
    let mut new_units: Vec<WorkUnit> = Vec::with_capacity(plan.units.len());
    let mut i = 0;
    while i < plan.units.len() {
        let model = plan.units[i].model.clone();
        let mut j = i;
        while j < plan.units.len() && plan.units[j].model == model {
            j += 1;
        }
        let mut group: Vec<WorkUnit> = plan.units[i..j].to_vec();
        if group.iter().all(|u| u.is_partition_slice) && group.len() > 1 {
            group.sort_by(|a, b| {
                b.estimated_bytes
                    .unwrap_or(0)
                    .cmp(&a.estimated_bytes.unwrap_or(0))
                    .then_with(|| a.id.cmp(&b.id))
            });
        }
        new_units.extend(group);
        i = j;
    }
    // Rebuild tier id lists from new unit order (preserve tier membership by model)
    let unit_by_model: BTreeMap<String, Vec<String>> = {
        let mut m: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for u in &new_units {
            m.entry(u.model.clone()).or_default().push(u.id.clone());
        }
        m
    };
    for tier in &mut plan.tiers {
        let mut new_tier = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for id in tier.iter() {
            // map old id to model
            if let Some(u) = plan.units.iter().find(|u| &u.id == id) {
                if seen.insert(u.model.clone()) {
                    if let Some(ids) = unit_by_model.get(&u.model) {
                        new_tier.extend(ids.iter().cloned());
                    }
                }
            }
        }
        *tier = new_tier;
    }
    plan.units = new_units;
}

/// Cartesian product of partition key values from scope (scalar or multi).
///
/// # Example
///
/// Scope: `entity ∈ {a,b}`, `dt = 2026-01-01`  
/// Keys: `[entity, dt]`  
/// → `[{entity:a, dt:…}, {entity:b, dt:…}]`
///
/// # Errors
///
/// `E_RBT_WORK_UNIT` if a key is missing from scope or has no values.
pub fn expand_partition_bindings(
    part_keys: &[String],
    scope: &RunScope,
) -> anyhow::Result<Vec<BTreeMap<String, String>>> {
    if part_keys.is_empty() {
        return Ok(vec![BTreeMap::new()]);
    }
    let mut axes: Vec<Vec<(String, String)>> = Vec::new();
    for k in part_keys {
        let Some(sv) = scope.vars.get(k) else {
            anyhow::bail!(
                "E_RBT_WORK_UNIT: partition key '{k}' not in run scope for fan-out"
            );
        };
        let vals: Vec<(String, String)> = sv
            .values()
            .into_iter()
            .map(|v| (k.clone(), v.to_string()))
            .collect();
        if vals.is_empty() {
            anyhow::bail!("E_RBT_WORK_UNIT: empty values for partition key '{k}'");
        }
        axes.push(vals);
    }
    // Cartesian product
    let mut acc: Vec<BTreeMap<String, String>> = vec![BTreeMap::new()];
    for axis in axes {
        let mut next = Vec::new();
        for base in &acc {
            for (k, v) in &axis {
                let mut m = base.clone();
                m.insert(k.clone(), v.clone());
                next.push(m);
            }
        }
        acc = next;
    }
    Ok(acc)
}

/// Narrow a run scope to scalar partition bindings (keeps other vars unchanged).
///
/// Used by the engine so bronze `IN` filters and `scope_part_id` see one value
/// per partition key for this WorkUnit.
///
/// Mega units (`partition_bindings` empty) return `base` cloned as-is.
pub fn scope_for_unit(base: &RunScope, unit: &WorkUnit) -> RunScope {
    if unit.partition_bindings.is_empty() {
        return base.clone();
    }
    let mut s = base.clone();
    for (k, v) in &unit.partition_bindings {
        s.vars.insert(k.clone(), ScopeValue::single(v.clone()));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::dag::{Materialization, ModelKind, ModelLayer, OutputFormat};
    use crate::core::frontmatter::StagingFrontmatter;
    use crate::core::parser::DependencyRef;

    fn scoped_model(name: &str, part_by: &[&str]) -> ModelNode {
        ModelNode {
            name: name.into(),
            description: None,
            kind: ModelKind::Sql,
            raw_sql: "SELECT 1".into(),
            compiled_sql: "SELECT 1".into(),
            materialization: Materialization::ScopedReplace,
            output_format: OutputFormat::Parquet,
            output_path: Some(format!("{name}.parquet")),
            dependencies: Vec::<DependencyRef>::new(),
            layer: ModelLayer::Staging,
            frontmatter: Some(StagingFrontmatter {
                partition_by: Some(part_by.iter().map(|s| (*s).to_string()).collect()),
                part_key: Some(part_by.iter().map(|s| (*s).to_string()).collect()),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn classify_scoped_replace_is_partition_local() {
        let m = scoped_model("stg_x", &["entity"]);
        match classify_parallel_contract(&m) {
            ParallelContract::PartitionLocal { keys } => assert_eq!(keys, vec!["entity"]),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn classify_global_window() {
        let mut m = scoped_model("stg_x", &["entity"]);
        m.materialization = Materialization::Table;
        m.frontmatter = None;
        m.compiled_sql = "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM t".into();
        assert!(matches!(
            classify_parallel_contract(&m),
            ParallelContract::Global
        ));
    }

    #[test]
    fn expand_bindings_cartesian() {
        let scope = RunScope::new()
            .with_var("dt", "2026-01-01")
            .with_var_multi("entity", ["a", "b"])
            .unwrap();
        let slices = expand_partition_bindings(&["entity".into(), "dt".into()], &scope).unwrap();
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].get("entity").unwrap(), "a");
        assert_eq!(slices[0].get("dt").unwrap(), "2026-01-01");
    }

    #[test]
    fn plan_fanout_when_enabled() {
        let mut dag = ModelDag::new();
        let m = scoped_model("stg_x", &["entity"]);
        let idx = dag.graph.add_node(m.clone());
        dag.node_map.insert(m.name.clone(), idx);

        let scope = RunScope::new()
            .with_var_multi("entity", ["a", "b", "c", "d"])
            .unwrap();
        let mut conc = ConcurrencyConfig::default();
        conc.enabled = true;
        conc.strategy = ExecutionStrategy::Partition;
        conc.multi_value_fanout_threshold = 4;
        let plan = plan_execution(&dag, &scope, &conc).unwrap();
        assert_eq!(plan.partition_slice_count(), 4);
        assert_eq!(plan.units.len(), 4);
    }

    #[test]
    fn plan_no_fanout_when_disabled() {
        let mut dag = ModelDag::new();
        let m = scoped_model("stg_x", &["entity"]);
        let idx = dag.graph.add_node(m.clone());
        dag.node_map.insert(m.name.clone(), idx);

        let scope = RunScope::new()
            .with_var_multi("entity", ["a", "b", "c", "d"])
            .unwrap();
        let conc = ConcurrencyConfig::default(); // disabled
        let plan = plan_execution(&dag, &scope, &conc).unwrap();
        assert_eq!(plan.partition_slice_count(), 0);
        assert_eq!(plan.units.len(), 1);
        assert!(!plan.units[0].is_partition_slice);
    }
}

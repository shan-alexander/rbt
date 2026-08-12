//! Named pipeline stages for DAG execution (systems design — thin façade over a single entry).
//!
//! Public product surface remains [`crate::TransformationEngine::execute_dag_with_scope`].
//! Hosts that need re-entry (register once, re-run tiers, force one model without
//! re-fingerprint) call the stage methods on [`crate::TransformationEngine`] directly.
//!
//! ```text
//! execute_dag_with_scope
//!   ├─ Stage 1  Fingerprint + PlanSkip   (ops::plan_skip / stage_plan_skip)
//!   ├─ Stage 2  RegisterBronze           (stage_register_bronze)
//!   ├─ Stage 3  ExecuteTiers             (stage_execute_tiers)
//!   └─ Stage 4  WriteReceipt             (stage_write_receipt)
//! ```
//!
//! Future object-store / SQLite control-plane work plugs into stages, not a parallel engine.

use crate::core::dag::ModelDag;
use crate::core::project::RbtProjectConfig;
use crate::core::receipt::{ModelRunResult, RunReceipt, RunStatus};
use crate::core::run_scope::RunScope;
use crate::ops::{plan_skip, SkipPlan};
use anyhow::Result;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Logical stages of a scoped DAG run (documentation + host hooks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStage {
    /// Compute bronze fingerprint and compare to latest receipt.
    PlanSkip,
    /// Register bronze sources into the DataFusion session.
    RegisterBronze,
    /// Topological tiers: SQL → materialize (table / scoped_replace / upsert / …).
    ExecuteTiers,
    /// Persist run receipt under `.rbt/runs/`.
    WriteReceipt,
}

impl PipelineStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PlanSkip => "plan_skip",
            Self::RegisterBronze => "register_bronze",
            Self::ExecuteTiers => "execute_tiers",
            Self::WriteReceipt => "write_receipt",
        }
    }
}

/// Options for Stage 3 (execute tiers) without re-running Stage 1.
#[derive(Debug, Clone, Default)]
pub struct ExecuteTiersOptions {
    /// When set, only these model names run (plus ancestors if [`Self::include_ancestors`]).
    pub only_models: Option<HashSet<String>>,
    /// When `only_models` is set, also execute ancestors (default **true**, fail-safe).
    pub include_ancestors: bool,
    /// Fingerprint string for lineage stamps / receipt (host may pass prior Stage 1 result).
    pub bronze_fingerprint: Option<String>,
}

impl ExecuteTiersOptions {
    pub fn all() -> Self {
        Self {
            only_models: None,
            include_ancestors: true,
            bronze_fingerprint: None,
        }
    }

    pub fn only(models: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            only_models: Some(models.into_iter().map(|s| s.into()).collect()),
            include_ancestors: true,
            bronze_fingerprint: None,
        }
    }

    pub fn with_fingerprint(mut self, fp: impl Into<String>) -> Self {
        self.bronze_fingerprint = Some(fp.into());
        self
    }

    pub fn without_ancestors(mut self) -> Self {
        self.include_ancestors = false;
        self
    }
}

/// Result of Stage 3 alone (hosts compose Stage 4 themselves).
#[derive(Debug, Clone, Default)]
pub struct StageExecuteResult {
    pub models_executed: usize,
    pub total_rows_produced: usize,
    pub model_results: Vec<ModelRunResult>,
    pub bronze_fingerprint: Option<String>,
}

/// Inputs for Stage 4 receipt write after a successful materialize.
#[derive(Debug, Clone)]
pub struct ReceiptWriteArgs<'a> {
    pub project_dir: &'a Path,
    pub config: &'a RbtProjectConfig,
    pub scope: &'a RunScope,
    pub run_id: String,
    pub scope_key: String,
    pub contract_version: String,
    pub bronze_fingerprint: String,
    pub models_executed: usize,
    pub total_rows: usize,
    pub bronze_sources: usize,
    pub model_results: Vec<ModelRunResult>,
    pub started_unix_ms: u128,
    pub finished_unix_ms: u128,
    pub skipped: bool,
    pub skip_reason: Option<String>,
    pub error: Option<String>,
}

/// Stage 1: plan skip (same identity as CLI `--skip-if-match`).
///
/// Hosts that only need the decision call this (or [`crate::ops::plan_skip`]) without
/// running the rest of the pipeline.
pub fn stage_plan_skip(
    dag: &ModelDag,
    project_dir: &Path,
    config: &RbtProjectConfig,
    scope: &RunScope,
) -> Result<SkipPlan> {
    plan_skip(dag, project_dir, config, scope)
}

/// Write a run receipt (Stage 4). Returns path written.
pub fn stage_write_receipt(args: ReceiptWriteArgs<'_>) -> Result<PathBuf> {
    let status = if args.skipped {
        RunStatus::Skipped
    } else if args.error.is_some() {
        RunStatus::Error
    } else {
        RunStatus::Ok
    };
    let receipt = RunReceipt {
        schema_version: RunReceipt::SCHEMA_VERSION,
        run_id: args.run_id,
        project: args.config.name.clone(),
        package_version: crate::VERSION.into(),
        contract_version: args.contract_version,
        scope_key: args.scope_key,
        vars: args.scope.vars.clone(),
        status,
        skipped: args.skipped,
        skip_reason: args.skip_reason,
        bronze_fingerprint: args.bronze_fingerprint,
        models_executed: args.models_executed,
        total_rows: args.total_rows,
        bronze_sources: args.bronze_sources,
        model_results: args.model_results,
        started_unix_ms: args.started_unix_ms,
        finished_unix_ms: args.finished_unix_ms,
        wall_ms: args.finished_unix_ms.saturating_sub(args.started_unix_ms),
        error: args.error,
    };
    receipt.write(args.project_dir)
}

/// Expand `only_models` with ancestors when requested (topo-safe filter).
pub fn expand_model_selection(
    dag: &ModelDag,
    only: &HashSet<String>,
    include_ancestors: bool,
) -> Result<HashSet<String>> {
    if !include_ancestors {
        return Ok(only.clone());
    }
    let mut out = only.clone();
    for name in only {
        let Some(&idx) = dag.node_map.get(name) else {
            anyhow::bail!(
                "E_RBT_SELECT: unknown model '{name}' in only_models (not in DAG)"
            );
        };
        // Walk inbound edges (dependencies).
        let mut stack = vec![idx];
        let mut seen = HashSet::new();
        while let Some(n) = stack.pop() {
            if !seen.insert(n) {
                continue;
            }
            let node_name = dag.graph[n].name.clone();
            out.insert(node_name);
            for dep in dag.graph.neighbors_directed(n, petgraph::Direction::Incoming) {
                stack.push(dep);
            }
        }
    }
    Ok(out)
}

use super::frontmatter::{
    BronzeCheckMode, BronzeDiagnostic, BronzeValidationReport, DiagnosticSeverity,
    StagingFrontmatter,
};
use super::parser::{DependencyRef, SqlModelParser};
use anyhow::{bail, Result};
use petgraph::algo::{is_cyclic_directed, toposort};
use petgraph::graph::{DiGraph, NodeIndex};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Model materialization strategy in Apache Iceberg & data lakes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Materialization {
    View,
    Table,
    IncrementalAppend,
    /// RBT-A2: deterministic part file per scope key; re-run replaces that part only.
    ScopedReplace,
    IncrementalMerge,
    /// RBT-A7: Type-1 entity-grain upsert by `unique_key` (touch-only when attrs unchanged).
    KeyedUpsert,
    /// Identity / pass-through: no re-encode of upstream lake bytes (RBT-C Phase 0).
    ///
    /// Frontmatter: `materialization: alias` (aliases: `zero_copy_ref`, `zero_copy_clone`,
    /// `clone`). Optional `alias_of: upstream_model`. Publishes hardlink/symlink/pointer
    /// to the upstream path — see [`crate::materializer::alias`].
    ZeroCopyClone,
}

impl Materialization {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::View => "view",
            Self::Table => "table",
            Self::IncrementalAppend => "incremental_append",
            Self::ScopedReplace => "scoped_replace",
            Self::IncrementalMerge => "incremental_merge",
            Self::KeyedUpsert => "keyed_upsert",
            // Product name is `alias`; enum keeps ZeroCopyClone for stable serde/API.
            Self::ZeroCopyClone => "alias",
        }
    }

    /// True for identity / zero-copy materialization (no SQL rewrite of upstream bytes).
    pub fn is_alias(&self) -> bool {
        matches!(self, Self::ZeroCopyClone)
    }
}

/// Parse frontmatter `materialization:` string into [`Materialization`].
pub fn parse_materialization_hint(s: &str) -> Result<Materialization> {
    match s.trim().to_ascii_lowercase().as_str() {
        "table" | "full_refresh" | "full-refresh" => Ok(Materialization::Table),
        "view" => Ok(Materialization::View),
        "incremental_append" | "append" | "incremental" => Ok(Materialization::IncrementalAppend),
        "scoped_replace" | "incremental_replace" | "replace_scope" => {
            Ok(Materialization::ScopedReplace)
        }
        "incremental_merge" | "merge" => Ok(Materialization::IncrementalMerge),
        "keyed_upsert" | "upsert" | "scd1" | "type1" | "type_1" => {
            Ok(Materialization::KeyedUpsert)
        }
        "alias" | "zero_copy_ref" | "zero_copy_clone" | "zero_copy" | "clone" => {
            Ok(Materialization::ZeroCopyClone)
        }
        other => bail!(
            "E_RBT_MATERIALIZATION: unknown materialization '{other}' \
             (table | view | incremental_append | scoped_replace | keyed_upsert | \
             incremental_merge | alias)"
        ),
    }
}

/// Configurable model output format supported by the engine.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum OutputFormat {
    /// Output directly as Parquet file(s)
    #[default]
    Parquet,
    /// Output as JSONL (jshift zero-copy compatible)
    Jsonl,
    /// Output as CSV
    Csv,
    /// Output directly into Apache Iceberg catalog table
    Iceberg,
    /// Dual-write output: local Parquet files AND Apache Iceberg catalog registration
    ParquetAndIceberg,
    /// Native zero-copy metadata pointer clone
    ZeroCopyClone,
}

/// DAG Layer classification following project architecture conventions.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ModelLayer {
    Staging,
    Transform,
    Mart,
}

impl ModelLayer {
    pub fn from_name(name: &str) -> Self {
        if name.starts_with("stg_")
            || name.starts_with("seed_")
            || name.starts_with("ref_")
            || name.starts_with("lkp_")
        {
            Self::Staging
        } else if name.starts_with("tf_") || name.starts_with("int_") {
            Self::Transform
        } else if name.starts_with("dim_")
            || name.starts_with("fact_")
            || name.starts_with("obt_")
            || name.starts_with("fct_")
        {
            Self::Mart
        } else {
            Self::Transform
        }
    }
}

/// Silver/gold **role** vocabulary (RBT-A10) — orthogonal to medallion [`ModelLayer`].
///
/// Does not change materialization by itself; documents intent for humans and agents.
/// Prefer gold prefixes (`dim_`/`fact_`/`obt_`) only on mart models.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelRole {
    /// Silver stage endpoint (`stg_*`): grain-honest usable table from bronze.
    Stage,
    /// Lookup / ref table (`ref_*` / `lkp_*`) — not a Kimball dim until gold.
    Lookup,
    /// Static seed (`seed_*`) small reference file.
    Seed,
    /// Intermediate transform (`tf_*` / `int_*`).
    Transform,
    /// Gold mart (`dim_*` / `fact_*` / `obt_*`).
    Mart,
    /// Unrecognized prefix — treat as transform.
    Other,
}

impl ModelRole {
    pub fn from_name(name: &str) -> Self {
        if name.starts_with("stg_") {
            Self::Stage
        } else if name.starts_with("ref_") || name.starts_with("lkp_") {
            Self::Lookup
        } else if name.starts_with("seed_") {
            Self::Seed
        } else if name.starts_with("tf_") || name.starts_with("int_") {
            Self::Transform
        } else if name.starts_with("dim_")
            || name.starts_with("fact_")
            || name.starts_with("obt_")
            || name.starts_with("fct_")
        {
            Self::Mart
        } else {
            Self::Other
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stage => "stage",
            Self::Lookup => "lookup",
            Self::Seed => "seed",
            Self::Transform => "transform",
            Self::Mart => "mart",
            Self::Other => "other",
        }
    }
}

/// How a DAG node produces Arrow (ADR-003 Design A SQL vs Design B Rust).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ModelKind {
    /// SQL text → DataFusion (default file models).
    #[default]
    Sql,
    /// Host-registered [`crate::engine::rust_model::RustModel`] (Design B).
    Rust,
}

impl ModelKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::Rust => "rust",
        }
    }
}

/// Pipeline Model Node definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelNode {
    pub name: String,
    pub description: Option<String>,
    /// Model authoring kind (SQL default; Rust = Design B host node).
    #[serde(default)]
    pub kind: ModelKind,
    pub raw_sql: String,
    pub compiled_sql: String,
    pub materialization: Materialization,
    pub output_format: OutputFormat,
    pub output_path: Option<String>,
    pub dependencies: Vec<DependencyRef>,
    pub layer: ModelLayer,
    pub frontmatter: Option<StagingFrontmatter>,
}

/// Complete pipeline DAG with topological execution tiering and cycle detection.
#[derive(Debug, Default)]
pub struct ModelDag {
    pub graph: DiGraph<ModelNode, ()>,
    pub node_map: HashMap<String, NodeIndex>,
}

impl ModelDag {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a raw model SQL definition into the DAG with a default Parquet format.
    pub fn add_model(
        &mut self,
        name: impl Into<String>,
        raw_sql: &str,
        materialization: Materialization,
        catalog_prefix: &str,
    ) -> Result<NodeIndex> {
        self.add_model_with_format(
            name,
            raw_sql,
            materialization,
            OutputFormat::Parquet,
            None,
            catalog_prefix,
        )
    }

    /// Registers a raw model SQL definition with explicit `OutputFormat` and optional output path.
    pub fn add_model_with_format(
        &mut self,
        name: impl Into<String>,
        raw_sql: &str,
        materialization: Materialization,
        output_format: OutputFormat,
        output_path: Option<String>,
        catalog_prefix: &str,
    ) -> Result<NodeIndex> {
        let name = name.into();
        let (frontmatter, pure_sql) = SqlModelParser::parse_frontmatter(raw_sql)
            .map_err(|e| anyhow::anyhow!("model '{}': {}", name, e))?;
        let dependencies = SqlModelParser::extract_dependencies(&pure_sql)?;
        let mut compiled_sql = SqlModelParser::compile_sql(&pure_sql, catalog_prefix)?;
        // ADR-009: expand bare sk() / surrogate_key('algo') from frontmatter grain.
        let grain = frontmatter
            .as_ref()
            .and_then(|f| f.grain.as_ref())
            .map(|g| g.as_slice())
            .unwrap_or(&[]);
        compiled_sql = crate::engine::surrogate_key::expand_sk_shorthands(&compiled_sql, grain)
            .map_err(|e| anyhow::anyhow!("model '{}': {}", name, e))?;
        let layer = ModelLayer::from_name(&name);
        let description = frontmatter.as_ref().and_then(|f| f.description.clone());
        // Frontmatter materialization overrides the positional default when set.
        let materialization = frontmatter
            .as_ref()
            .and_then(|f| f.materialization.as_deref())
            .map(parse_materialization_hint)
            .transpose()?
            .unwrap_or(materialization);

        let node = ModelNode {
            name: name.clone(),
            description,
            kind: ModelKind::Sql,
            raw_sql: raw_sql.to_string(),
            compiled_sql,
            materialization,
            output_format,
            output_path,
            dependencies,
            layer,
            frontmatter,
        };

        let idx = self.graph.add_node(node);
        self.node_map.insert(name, idx);
        Ok(idx)
    }

    /// Resolves dependency edges between models and validates that there are no circular dependencies or layer boundary violations.
    pub fn build_graph(&mut self) -> Result<()> {
        let name_map = self.node_map.clone();
        let mut edges = Vec::new();

        for (idx, node) in self.node_map.values().map(|&i| (i, &self.graph[i])) {
            for dep in &node.dependencies {
                if let DependencyRef::Model(dep_name) = dep {
                    if let Some(&dep_idx) = name_map.get(dep_name) {
                        let dep_node = &self.graph[dep_idx];

                        // Transform models cannot depend on Mart models (facts/dims are terminal for transforms)
                        if node.layer == ModelLayer::Transform && dep_node.layer == ModelLayer::Mart
                        {
                            bail!(
                                "Illegal DAG Layer Boundary: Transform model '{}' (tf_) cannot depend on Mart model '{}' (dim_/fact_/obt_)",
                                node.name,
                                dep_node.name
                            );
                        }

                        // Staging is the silver endpoint: may ref silver prep transforms (tf_ before stg),
                        // but not marts or other staging models.
                        if node.layer == ModelLayer::Staging {
                            match dep_node.layer {
                                ModelLayer::Mart => {
                                    bail!(
                                        "Illegal DAG Layer Boundary: Staging model '{}' (stg_) cannot depend on Mart model '{}'",
                                        node.name,
                                        dep_node.name
                                    );
                                }
                                ModelLayer::Staging => {
                                    bail!(
                                        "Illegal DAG Layer Boundary: Staging model '{}' cannot depend on staging model '{}' \
                                         (stg_* are silver endpoints; chain prep in tf_* then land stg_*)",
                                        node.name,
                                        dep_node.name
                                    );
                                }
                                ModelLayer::Transform => {
                                    // OK: bronze → tf_base_* → stg_* (optional silver prep before stage endpoint)
                                }
                            }
                        }

                        edges.push((dep_idx, idx));
                    } else {
                        let available: Vec<String> = name_map.keys().cloned().collect();
                        return Err(crate::core::diagnostics::dep_missing_report(
                            &node.name,
                            dep_name,
                            &available,
                            None,
                        )
                        .into_error());
                    }
                }
            }
        }

        for (from, to) in edges {
            self.graph.add_edge(from, to, ());
        }

        if is_cyclic_directed(&self.graph) {
            bail!("Circular dependency detected in model pipeline DAG!");
        }

        // Medallion transform band: never mix silver-prep and gold-prep deps on one tf.
        // - Gold transforms: only ref stg_* (silver endpoints)
        // - Silver prep transforms: only ref other tf_* (or bronze via source()), never stg_*
        // - Never: stg_* → silver/tf_* (stg is endpoint; gold tf refs stg instead)
        for &idx in self.node_map.values() {
            let node = &self.graph[idx];
            if node.layer != ModelLayer::Transform {
                continue;
            }
            let mut refs_stg = false;
            let mut refs_tf = false;
            for dep in &node.dependencies {
                if let DependencyRef::Model(dep_name) = dep {
                    if let Some(&dep_idx) = self.node_map.get(dep_name) {
                        match self.graph[dep_idx].layer {
                            ModelLayer::Staging => refs_stg = true,
                            ModelLayer::Transform => refs_tf = true,
                            ModelLayer::Mart => {}
                        }
                    }
                }
            }
            if refs_stg && refs_tf {
                bail!(
                    "E_RBT_LAYER_TRANSFORM_BAND: transform '{}' refs both stg_* and tf_*. \
                     Gold transforms may only ref silver stage endpoints (stg_*). \
                     Silver prep transforms may only ref bronze sources or other silver tf_* \
                     (then land stg_*). Never stg_* → silver/tf_*.",
                    node.name
                );
            }
        }

        Ok(())
    }

    /// Computes a flat topologically sorted execution sequence.
    pub fn topological_sequence(&self) -> Result<Vec<ModelNode>> {
        let indices = toposort(&self.graph, None)
            .map_err(|_| anyhow::anyhow!("Circular dependency found during topological sort"))?;

        Ok(indices.into_iter().map(|i| self.graph[i].clone()).collect())
    }

    /// Computes parallel execution tiers where all models in a tier can be run concurrently.
    pub fn execution_tiers(&self) -> Result<Vec<Vec<ModelNode>>> {
        let _sorted = self.topological_sequence()?;
        let mut in_degrees: HashMap<NodeIndex, usize> = HashMap::new();

        for idx in self.graph.node_indices() {
            in_degrees.insert(
                idx,
                self.graph
                    .neighbors_directed(idx, petgraph::Direction::Incoming)
                    .count(),
            );
        }

        let mut current_tier: Vec<NodeIndex> = in_degrees
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&idx, _)| idx)
            .collect();

        let mut tiers = Vec::new();
        let mut visited = HashSet::new();

        while !current_tier.is_empty() {
            let tier_nodes: Vec<ModelNode> = current_tier
                .iter()
                .map(|&i| self.graph[i].clone())
                .collect();
            tiers.push(tier_nodes);

            for &node_idx in &current_tier {
                visited.insert(node_idx);
            }

            let mut next_tier = Vec::new();
            for idx in self.graph.node_indices() {
                if visited.contains(&idx) {
                    continue;
                }
                let incoming_unvisited = self
                    .graph
                    .neighbors_directed(idx, petgraph::Direction::Incoming)
                    .filter(|n| !visited.contains(n))
                    .count();

                if incoming_unvisited == 0 {
                    next_tier.push(idx);
                }
            }

            current_tier = next_tier;
        }

        Ok(tiers)
    }

    /// First `source('ns','table')` dependency, if any.
    pub fn primary_source(node: &ModelNode) -> Option<(&str, &str)> {
        node.dependencies.iter().find_map(|d| match d {
            DependencyRef::Source {
                source_name,
                table_name,
            } => Some((source_name.as_str(), table_name.as_str())),
            _ => None,
        })
    }

    /// Resolve registration identity for a bronze-backed model.
    pub fn bronze_source_ident(node: &ModelNode) -> Option<(String, String)> {
        if let Some(fm) = &node.frontmatter {
            if let (Some(s), Some(t)) = (&fm.source_name, &fm.source_table) {
                return Some((s.clone(), t.clone()));
            }
            if let Some((s, t)) = Self::primary_source(node) {
                let source = fm.source_name.clone().unwrap_or_else(|| s.to_string());
                let table = fm.source_table.clone().unwrap_or_else(|| t.to_string());
                return Some((source, table));
            }
            if fm.has_scan_contract() {
                // Fall back to model name under schema "bronze"
                let table = fm.source_table.clone().unwrap_or_else(|| node.name.clone());
                let source = fm
                    .source_name
                    .clone()
                    .unwrap_or_else(|| "bronze".to_string());
                return Some((source, table));
            }
        }
        Self::primary_source(node).map(|(s, t)| (s.to_string(), t.to_string()))
    }

    /// Compile-time bronze frontmatter / scan_path validation.
    ///
    /// * `Off` — no filesystem checks
    /// * `Warn` — missing paths become warnings
    /// * `Fail` — missing paths become errors (`report.has_errors()`)
    pub fn validate_bronze_sources(
        &self,
        project_dir: &Path,
        mode: BronzeCheckMode,
    ) -> Result<BronzeValidationReport> {
        self.validate_bronze_sources_with_roots(
            project_dir,
            mode,
            &std::collections::HashMap::new(),
        )
    }

    /// Compile-time bronze checks with project `roots:` for `$name` path templates.
    pub fn validate_bronze_sources_with_roots(
        &self,
        project_dir: &Path,
        mode: BronzeCheckMode,
        roots: &std::collections::HashMap<String, String>,
    ) -> Result<BronzeValidationReport> {
        if mode == BronzeCheckMode::Off {
            return Ok(BronzeValidationReport::default());
        }

        let severity = match mode {
            BronzeCheckMode::Fail => DiagnosticSeverity::Error,
            BronzeCheckMode::Warn => DiagnosticSeverity::Warning,
            BronzeCheckMode::Off => unreachable!(),
        };

        let mut report = BronzeValidationReport::default();

        for idx in self.graph.node_indices() {
            let node = &self.graph[idx];
            let has_source_dep = node
                .dependencies
                .iter()
                .any(|d| matches!(d, DependencyRef::Source { .. }));
            let fm = node.frontmatter.as_ref();

            // Staging models with source() should declare a scan contract.
            if has_source_dep && node.layer == ModelLayer::Staging {
                let missing_scan = fm.map(|f| !f.has_scan_contract()).unwrap_or(true);
                if missing_scan {
                    report.diagnostics.push(BronzeDiagnostic {
                        model: node.name.clone(),
                        severity,
                        code: "E_RBT_BRONZE_SCAN_PATH_MISSING",
                        message:
                            "staging model references source() but has no frontmatter scan_path; \
                             add YAML frontmatter with scan_path (and source_format)"
                                .to_string(),
                    });
                    continue;
                }
            }

            let Some(fm) = fm else {
                continue;
            };

            if !fm.has_scan_contract() {
                continue;
            }

            let scan_path = fm.scan_path.as_deref().unwrap();

            // Format resolvable?
            if let Err(e) = fm.resolve_format() {
                report.diagnostics.push(BronzeDiagnostic {
                    model: node.name.clone(),
                    severity,
                    code: "E_RBT_BRONZE_FORMAT_UNKNOWN",
                    message: e.to_string(),
                });
            }

            if !super::frontmatter::scan_path_exists_with_roots(project_dir, scan_path, roots) {
                // Optional artifact families may be absent for a partition (P5a).
                if fm.on_missing_policy() == super::run_scope::OnMissing::Empty {
                    if let Err(e) = fm.empty_frame_schema() {
                        report.diagnostics.push(BronzeDiagnostic {
                            model: node.name.clone(),
                            severity,
                            code: "E_RBT_EMPTY_SCHEMA",
                            message: format!(
                                "on_missing: empty but schema invalid while scan_path missing: {e}"
                            ),
                        });
                    }
                } else {
                    let resolved = super::paths::resolve_project_path(project_dir, scan_path, roots)
                        .unwrap_or_else(|_| project_dir.join(scan_path));
                    report.diagnostics.push(BronzeDiagnostic {
                        model: node.name.clone(),
                        severity,
                        code: "E_RBT_BRONZE_SCAN_PATH_NOT_FOUND",
                        message: format!(
                            "scan_path '{}' does not exist (resolved: {}). \
                             Hint: set on_missing: empty for optional artifact families.",
                            scan_path,
                            resolved.display()
                        ),
                    });
                }
            }

            // Validate path_glob patterns early (syntax only).
            if let Some(globs) = fm.path_glob.as_ref() {
                if let Err(e) = super::paths::validate_glob_patterns(globs) {
                    report.diagnostics.push(BronzeDiagnostic {
                        model: node.name.clone(),
                        severity,
                        code: "E_RBT_PATH_GLOB_INVALID",
                        message: e.to_string(),
                    });
                }
            }

            // source() identity vs frontmatter overrides — soft check
            if let (Some((dep_s, dep_t)), Some(ident)) =
                (Self::primary_source(node), Self::bronze_source_ident(node))
            {
                if let Some(fm_s) = &fm.source_name {
                    if fm_s != dep_s {
                        report.diagnostics.push(BronzeDiagnostic {
                            model: node.name.clone(),
                            severity: DiagnosticSeverity::Warning,
                            code: "W_RBT_BRONZE_SOURCE_NAME_MISMATCH",
                            message: format!(
                                "frontmatter source_name='{}' differs from source('{}', ...); \
                                 registration will use '{}'.{} ",
                                fm_s, dep_s, ident.0, ident.1
                            ),
                        });
                    }
                }
                if let Some(fm_t) = &fm.source_table {
                    if fm_t != dep_t {
                        report.diagnostics.push(BronzeDiagnostic {
                            model: node.name.clone(),
                            severity: DiagnosticSeverity::Warning,
                            code: "W_RBT_BRONZE_SOURCE_TABLE_MISMATCH",
                            message: format!(
                                "frontmatter source_table='{}' differs from source(..., '{}')",
                                fm_t, dep_t
                            ),
                        });
                    }
                }
            }
        }

        // Kimball / gold hygiene (warnings; does not fail bronze_check alone).
        self.append_modeling_hygiene_diagnostics(&mut report);
        Ok(report)
    }

    /// Star-schema and layer hygiene warnings (grain/unique, parts on marts, source(tf_*)).
    pub fn modeling_hygiene_diagnostics(&self) -> Vec<BronzeDiagnostic> {
        let mut report = BronzeValidationReport::default();
        self.append_modeling_hygiene_diagnostics(&mut report);
        report.diagnostics
    }

    fn append_modeling_hygiene_diagnostics(&self, report: &mut BronzeValidationReport) {
        for idx in self.graph.node_indices() {
            let node = &self.graph[idx];
            let fm = node.frontmatter.as_ref();

            // source() must not name an upstream transform-like table (private, unstable).
            for dep in &node.dependencies {
                if let DependencyRef::Source {
                    source_name: _,
                    table_name,
                } = dep
                {
                    let t = table_name.as_str();
                    if t.starts_with("tf_") || t.starts_with("int_") {
                        report.diagnostics.push(BronzeDiagnostic {
                            model: node.name.clone(),
                            severity: DiagnosticSeverity::Warning,
                            code: "W_RBT_SOURCE_UPSTREAM_TRANSFORM",
                            message: format!(
                                "source(..., '{table_name}') looks like a transform endpoint; \
                                 prefer published stage (stg_*) or dim/fact contracts, not private tf_*"
                            ),
                        });
                    }
                }
            }

            // Gold transforms (tf under mart naming is rare): if transform refs another transform,
            // that is fine for silver/tf chains. Flag marts that scan parts without ref-only.
            if matches!(node.layer, ModelLayer::Mart) {
                if let Some(fm) = fm {
                    if fm.wants_parts_source() || fm.has_scan_contract() {
                        report.diagnostics.push(BronzeDiagnostic {
                            model: node.name.clone(),
                            severity: DiagnosticSeverity::Warning,
                            code: "W_RBT_MART_SCAN_CONTRACT",
                            message: "mart (dim_/fact_/obt_) declares a bronze scan/parts contract; \
                                 prefer scan on stg_*, business prep on tf_*, thin marts via ref()"
                                .into(),
                        });
                    }
                }
            }

            let Some(fm) = fm else {
                continue;
            };

            // Grain without unique coverage
            if let Some(grain) = fm.grain.as_ref().filter(|g| !g.is_empty()) {
                let unique_cols: Option<Vec<String>> = fm
                    .tests
                    .as_ref()
                    .and_then(|t| t.unique.clone())
                    .or_else(|| fm.unique_key.clone());
                match unique_cols {
                    None => {
                        report.diagnostics.push(BronzeDiagnostic {
                            model: node.name.clone(),
                            severity: DiagnosticSeverity::Warning,
                            code: "W_RBT_GRAIN_NO_UNIQUE",
                            message: format!(
                                "grain {:?} declared but no tests.unique / unique_key; \
                                 grain should be testable for uniqueness",
                                grain
                            ),
                        });
                    }
                    Some(u) if u != *grain && u.len() != 1 => {
                        // Allow unique_key = [sk] while grain is natural keys
                        if !(u.len() == 1
                            && (u[0].ends_with("_sk") || u[0].ends_with("_key") || u[0] == "sk"))
                        {
                            report.diagnostics.push(BronzeDiagnostic {
                                model: node.name.clone(),
                                severity: DiagnosticSeverity::Warning,
                                code: "W_RBT_GRAIN_UNIQUE_MISMATCH",
                                message: format!(
                                    "grain {:?} differs from unique {:?}; ensure SK vs NK is intentional",
                                    grain, u
                                ),
                            });
                        }
                    }
                    _ => {}
                }
            }

            // Entity-grained table materialization: suggest keyed_upsert (peer retention).
            // keyed_upsert is a general merge primitive — Type-1 dims are a common consumer.
            let is_table = matches!(
                node.materialization,
                Materialization::Table | Materialization::View
            );
            let looks_entity_grain = fm
                .unique_key
                .as_ref()
                .map(|u| !u.is_empty())
                .unwrap_or(false)
                || fm
                    .grain
                    .as_ref()
                    .map(|g| !g.is_empty() && g.len() <= 3)
                    .unwrap_or(false);
            let looks_dim_or_mart = matches!(node.layer, ModelLayer::Mart)
                || node.name.starts_with("dim_")
                || node.name.starts_with("fct_")
                || node.name.contains("registry")
                || node.name.contains("_current");
            if is_table && looks_entity_grain && looks_dim_or_mart {
                report.diagnostics.push(BronzeDiagnostic {
                    model: node.name.clone(),
                    severity: DiagnosticSeverity::Warning,
                    code: "W_RBT_UPSERT_HINT",
                    message: format!(
                        "model looks entity-grained (grain/unique_key present) with \
                         materialization: {:?}. Full table refresh rewrites from this run's \
                         SQL only — peers absent from the candidate set are dropped. \
                         For durable registries / Type-1 dims, prefer \
                         materialization: keyed_upsert with unique_key (defaults to grain).",
                        node.materialization
                    ),
                });
            }

            // Fact-like models: recommend relationship tests when none
            if node.name.starts_with("fact_") || node.name.starts_with("fct_") {
                let has_rel = fm
                    .tests
                    .as_ref()
                    .and_then(|t| t.relationships.as_ref())
                    .map(|r| !r.is_empty())
                    .unwrap_or(false);
                if !has_rel {
                    report.diagnostics.push(BronzeDiagnostic {
                        model: node.name.clone(),
                        severity: DiagnosticSeverity::Warning,
                        code: "W_RBT_FACT_NO_RELATIONSHIP",
                        message: "fact model has no tests.relationships; \
                             prefer SK FK checks to dim_* (Unknown member -1)"
                            .into(),
                    });
                }
            }

            // Dim: soft note if no unknown convention in description (optional noise) — skip

            // Lineage columns in grain
            if let Some(grain) = &fm.grain {
                if grain.iter().any(|c| c.starts_with("_rbt_")) {
                    report.diagnostics.push(BronzeDiagnostic {
                        model: node.name.clone(),
                        severity: DiagnosticSeverity::Warning,
                        code: "W_RBT_LINEAGE_IN_GRAIN",
                        message: "grain includes _rbt_* lineage columns; keep lineage out of business grain"
                            .into(),
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dag_building_and_tiering() -> Result<()> {
        let mut dag = ModelDag::new();

        // Tier 0: Staging models (no dependencies)
        dag.add_model_with_format(
            "stg_users",
            "SELECT * FROM {{ source('raw', 'users') }}",
            Materialization::View,
            OutputFormat::Jsonl,
            None,
            "db",
        )?;
        dag.add_model_with_format(
            "stg_orders",
            "SELECT * FROM {{ source('raw', 'orders') }}",
            Materialization::View,
            OutputFormat::ParquetAndIceberg,
            None,
            "db",
        )?;

        // Tier 1: Intermediate model depending on stg_users and stg_orders
        dag.add_model(
            "int_user_orders",
            "SELECT * FROM {{ ref('stg_users') }} u JOIN {{ ref('stg_orders') }} o ON u.id = o.user_id",
            Materialization::Table,
            "db",
        )?;

        // Tier 2: Mart model depending on int_user_orders
        dag.add_model(
            "fct_revenue",
            "SELECT user_id, SUM(amount) FROM {{ ref('int_user_orders') }} GROUP BY user_id",
            Materialization::IncrementalAppend,
            "db",
        )?;

        dag.build_graph()?;

        let tiers = dag.execution_tiers()?;
        assert_eq!(tiers.len(), 3);

        // Tier 0 has 2 parallel models with formats Jsonl and ParquetAndIceberg
        assert_eq!(tiers[0].len(), 2);
        let tier0_formats: Vec<OutputFormat> =
            tiers[0].iter().map(|m| m.output_format.clone()).collect();
        assert!(tier0_formats.contains(&OutputFormat::Jsonl));
        assert!(tier0_formats.contains(&OutputFormat::ParquetAndIceberg));

        Ok(())
    }

    #[test]
    fn test_cycle_detection() -> Result<()> {
        let mut dag = ModelDag::new();

        dag.add_model(
            "model_a",
            "SELECT * FROM {{ ref('model_b') }}",
            Materialization::View,
            "db",
        )?;
        dag.add_model(
            "model_b",
            "SELECT * FROM {{ ref('model_a') }}",
            Materialization::View,
            "db",
        )?;

        let res = dag.build_graph();
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("Circular dependency"));

        Ok(())
    }

    #[test]
    fn modeling_hygiene_flags_source_tf_and_grain() -> Result<()> {
        let mut dag = ModelDag::new();
        dag.add_model_with_format(
            "stg_ok",
            "---\ngrain: [id]\n---\nSELECT 1 AS id FROM {{ source('raw', 'x') }}",
            Materialization::Table,
            OutputFormat::Parquet,
            None,
            "db",
        )?;
        dag.add_model_with_format(
            "stg_bad_source",
            "SELECT * FROM {{ source('upstream', 'tf_secret') }}",
            Materialization::Table,
            OutputFormat::Parquet,
            None,
            "db",
        )?;
        dag.add_model_with_format(
            "fact_sales",
            "---\ngrain: [id]\ntests:\n  unique: [id]\n---\nSELECT 1 AS id",
            Materialization::Table,
            OutputFormat::Parquet,
            None,
            "db",
        )?;
        dag.build_graph()?;
        let diags = dag.modeling_hygiene_diagnostics();
        assert!(
            diags.iter().any(|d| d.code == "W_RBT_GRAIN_NO_UNIQUE"),
            "expected grain warning, got {:?}",
            diags
        );
        assert!(
            diags
                .iter()
                .any(|d| d.code == "W_RBT_SOURCE_UPSTREAM_TRANSFORM"),
            "expected source(tf_*) warning, got {:?}",
            diags
        );
        assert!(
            diags.iter().any(|d| d.code == "W_RBT_FACT_NO_RELATIONSHIP"),
            "expected fact relationship warning, got {:?}",
            diags
        );
        Ok(())
    }

    #[test]
    fn test_layer_boundary_enforcement() -> Result<()> {
        let mut dag = ModelDag::new();

        // Mart model
        dag.add_model(
            "fact_orders",
            "SELECT 1 AS order_id",
            Materialization::Table,
            "db",
        )?;

        // Illegal transform model attempting to pull from a mart model!
        dag.add_model(
            "tf_illegal_transform",
            "SELECT * FROM {{ ref('fact_orders') }}",
            Materialization::Table,
            "db",
        )?;

        let res = dag.build_graph();
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Illegal DAG Layer Boundary"));

        Ok(())
    }

    #[test]
    fn transform_cannot_mix_stg_and_tf_deps() -> Result<()> {
        let mut dag = ModelDag::new();
        dag.add_model(
            "stg_a",
            "SELECT 1 AS id FROM {{ source('b', 'x') }}",
            Materialization::Table,
            "db",
        )?;
        dag.add_model(
            "tf_prep",
            "SELECT 1 AS id FROM {{ source('b', 'y') }}",
            Materialization::Table,
            "db",
        )?;
        dag.add_model(
            "tf_mixed",
            "SELECT * FROM {{ ref('stg_a') }} a JOIN {{ ref('tf_prep') }} t ON 1=1",
            Materialization::Table,
            "db",
        )?;
        let err = dag.build_graph().unwrap_err().to_string();
        assert!(
            err.contains("E_RBT_LAYER_TRANSFORM_BAND"),
            "got {err}"
        );
        Ok(())
    }

    #[test]
    fn staging_may_ref_silver_prep_transform() -> Result<()> {
        let mut dag = ModelDag::new();
        dag.add_model(
            "tf_base_events",
            "SELECT 1 AS id FROM {{ source('bronze', 'raw') }}",
            Materialization::Table,
            "db",
        )?;
        dag.add_model(
            "stg_events",
            "SELECT * FROM {{ ref('tf_base_events') }}",
            Materialization::Table,
            "db",
        )?;
        dag.build_graph()?;
        Ok(())
    }

    #[test]
    fn gold_transform_may_ref_only_stg() -> Result<()> {
        let mut dag = ModelDag::new();
        dag.add_model(
            "stg_a",
            "SELECT 1 AS id FROM {{ source('b', 'x') }}",
            Materialization::Table,
            "db",
        )?;
        dag.add_model(
            "tf_gold_prep",
            "SELECT * FROM {{ ref('stg_a') }}",
            Materialization::Table,
            "db",
        )?;
        dag.add_model(
            "fact_a",
            "SELECT * FROM {{ ref('tf_gold_prep') }}",
            Materialization::Table,
            "db",
        )?;
        dag.build_graph()?;
        Ok(())
    }

    #[test]
    fn test_bronze_scan_path_validation_warn_and_fail() -> Result<()> {
        let mut dag = ModelDag::new();
        dag.add_model_with_format(
            "stg_events",
            r#"---
source_format: jsonl
scan_path: "lake/bronze/missing.jsonl"
---
SELECT * FROM {{ source('bronze', 'events') }}
"#,
            Materialization::Table,
            OutputFormat::Parquet,
            None,
            "",
        )?;
        dag.build_graph()?;

        let project = Path::new("/tmp/rbt_nonexistent_project_root");
        let warn = dag.validate_bronze_sources(project, BronzeCheckMode::Warn)?;
        assert_eq!(warn.error_count(), 0);
        assert!(warn.warning_count() >= 1);
        assert!(warn
            .diagnostics
            .iter()
            .any(|d| d.code == "E_RBT_BRONZE_SCAN_PATH_NOT_FOUND"));

        let fail = dag.validate_bronze_sources(project, BronzeCheckMode::Fail)?;
        assert!(fail.has_errors());
        Ok(())
    }
}

use super::frontmatter::{
    scan_path_exists, BronzeCheckMode, BronzeDiagnostic, BronzeValidationReport,
    DiagnosticSeverity, StagingFrontmatter,
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
    IncrementalMerge,
    /// Native zero-copy metadata table clone (zero disk byte duplication)
    ZeroCopyClone,
}

/// Configurable model output format supported by the engine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OutputFormat {
    /// Output directly as Parquet file(s)
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

impl Default for OutputFormat {
    fn default() -> Self {
        Self::Parquet
    }
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
        if name.starts_with("stg_") {
            Self::Staging
        } else if name.starts_with("tf_") || name.starts_with("int_") {
            Self::Transform
        } else if name.starts_with("dim_") || name.starts_with("fact_") || name.starts_with("obt_") || name.starts_with("fct_") {
            Self::Mart
        } else {
            Self::Transform
        }
    }
}

/// Pipeline Model Node definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelNode {
    pub name: String,
    pub description: Option<String>,
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
        let compiled_sql = SqlModelParser::compile_sql(&pure_sql, catalog_prefix)?;
        let layer = ModelLayer::from_name(&name);
        let description = frontmatter
            .as_ref()
            .and_then(|f| f.description.clone());

        let node = ModelNode {
            name: name.clone(),
            description,
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

                        // Enforce Layer Boundary Rule: Transform models cannot depend on Mart models
                        if node.layer == ModelLayer::Transform && dep_node.layer == ModelLayer::Mart {
                            bail!(
                                "Illegal DAG Layer Boundary: Transform model '{}' (tf_) cannot depend on Mart model '{}' (dim_/fact_/obt_)",
                                node.name,
                                dep_node.name
                            );
                        }

                        // Enforce Layer Boundary Rule: Staging models cannot depend on downstream models
                        if node.layer == ModelLayer::Staging {
                            bail!(
                                "Illegal DAG Layer Boundary: Staging model '{}' (stg_) cannot depend on downstream model '{}'",
                                node.name,
                                dep_node.name
                            );
                        }

                        edges.push((dep_idx, idx));
                    } else {
                        bail!(
                            "Missing model dependency '{}' referenced by model '{}'",
                            dep_name,
                            node.name
                        );
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
            let tier_nodes: Vec<ModelNode> =
                current_tier.iter().map(|&i| self.graph[i].clone()).collect();
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
                let table = fm
                    .source_table
                    .clone()
                    .unwrap_or_else(|| node.name.clone());
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
                        message: format!(
                            "staging model references source() but has no frontmatter scan_path; \
                             add YAML frontmatter with scan_path (and source_format)"
                        ),
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

            if !scan_path_exists(project_dir, scan_path) {
                let resolved = super::frontmatter::resolve_scan_path(project_dir, scan_path);
                report.diagnostics.push(BronzeDiagnostic {
                    model: node.name.clone(),
                    severity,
                    code: "E_RBT_BRONZE_SCAN_PATH_NOT_FOUND",
                    message: format!(
                        "scan_path '{}' does not exist (resolved: {})",
                        scan_path,
                        resolved.display()
                    ),
                });
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

        Ok(report)
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
        let tier0_formats: Vec<OutputFormat> = tiers[0].iter().map(|m| m.output_format.clone()).collect();
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
        assert!(res.unwrap_err().to_string().contains("Illegal DAG Layer Boundary"));

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

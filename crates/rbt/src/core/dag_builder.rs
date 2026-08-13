//! Programmatic DAG construction (RBT-L1.3 / ADR-006).
//!
//! File-based projects (`RbtProjectConfig::build_dag`) and embedders share the same
//! execution IR: [`ModelDag`]. This module is the **Builder** frontend for hosts that
//! do not want a `models/` directory.
//!
//! # Example
//!
//! ```rust,no_run
//! use rbt::{DagBuilder, Materialization, ModelLayer, ModelSpec, OutputFormat};
//! # fn main() -> anyhow::Result<()> {
//! let dag = DagBuilder::new()
//!     .model(
//!         ModelSpec::sql("stg_x", "SELECT 1 AS id UNION ALL SELECT 2")
//!             .layer(ModelLayer::Staging)
//!             .materialization(Materialization::Table)
//!             .output_format(OutputFormat::Parquet)
//!             .output_path("/tmp/stg_x.parquet"),
//!     )
//!     .build()?;
//! assert!(dag.node_map.contains_key("stg_x"));
//! # Ok(())
//! # }
//! ```

use super::dag::{Materialization, ModelDag, ModelKind, ModelLayer, ModelNode, OutputFormat};
use super::frontmatter::StagingFrontmatter;
use super::parser::{DependencyRef, SqlModelParser};
use anyhow::{bail, Context, Result};

/// Owned specification for one model node before it joins a [`ModelDag`].
#[derive(Debug, Clone)]
pub struct ModelSpec {
    pub name: String,
    pub kind: ModelKind,
    pub raw_sql: String,
    pub materialization: Materialization,
    pub output_format: OutputFormat,
    pub output_path: Option<String>,
    pub layer: Option<ModelLayer>,
    pub frontmatter: Option<StagingFrontmatter>,
    pub description: Option<String>,
    /// When set, used as compiled SQL without Jinja compile (tests / fully-expanded SQL).
    pub compiled_sql_override: Option<String>,
    /// Catalog prefix for `{{ ref() }}` / `{{ source() }}` compile.
    ///
    /// Default **empty** (L1.10): `ref('x')` → bare `x`, matching engine table registration.
    pub catalog_prefix: String,
    /// Explicit model deps for Design B Rust nodes (and optional SQL override).
    pub explicit_refs: Vec<String>,
    /// Explicit bronze `source(schema, table)` deps for Rust nodes.
    pub explicit_sources: Vec<(String, String)>,
}

impl ModelSpec {
    /// SQL model with raw text (may contain `{{ ref() }}` / frontmatter).
    pub fn sql(name: impl Into<String>, raw_sql: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: ModelKind::Sql,
            raw_sql: raw_sql.into(),
            materialization: Materialization::Table,
            output_format: OutputFormat::Parquet,
            output_path: None,
            layer: None,
            frontmatter: None,
            description: None,
            compiled_sql_override: None,
            // L1.10: empty default so `ref('x')` matches bare DF table names after materialize.
            // Use `.catalog_prefix("rbt")` only if you dual-register / use a catalog schema.
            catalog_prefix: String::new(),
            explicit_refs: Vec::new(),
            explicit_sources: Vec::new(),
        }
    }

    /// Design B Rust model — host implements [`crate::RustModel`] with the same `name`.
    ///
    /// Declare upstream models with [`.refs`](Self::refs); bronze via [`.sources`](Self::sources).
    pub fn rust(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: ModelKind::Rust,
            raw_sql: String::new(),
            materialization: Materialization::Table,
            output_format: OutputFormat::Parquet,
            output_path: None,
            layer: None,
            frontmatter: None,
            description: None,
            compiled_sql_override: None,
            catalog_prefix: String::new(),
            explicit_refs: Vec::new(),
            explicit_sources: Vec::new(),
        }
    }

    /// Upstream model names this node depends on (`ref` edges).
    pub fn refs(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.explicit_refs = names.into_iter().map(|s| s.into()).collect();
        self
    }

    /// Bronze `source(schema, table)` dependencies for Rust models.
    pub fn sources(
        mut self,
        sources: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.explicit_sources = sources
            .into_iter()
            .map(|(a, b)| (a.into(), b.into()))
            .collect();
        self
    }

    pub fn materialization(mut self, m: Materialization) -> Self {
        self.materialization = m;
        self
    }

    pub fn output_format(mut self, f: OutputFormat) -> Self {
        self.output_format = f;
        self
    }

    pub fn output_path(mut self, p: impl Into<String>) -> Self {
        self.output_path = Some(p.into());
        self
    }

    pub fn layer(mut self, layer: ModelLayer) -> Self {
        self.layer = Some(layer);
        self
    }

    pub fn frontmatter(mut self, fm: StagingFrontmatter) -> Self {
        self.frontmatter = Some(fm);
        self
    }

    pub fn description(mut self, d: impl Into<String>) -> Self {
        self.description = Some(d.into());
        self
    }

    pub fn catalog_prefix(mut self, p: impl Into<String>) -> Self {
        self.catalog_prefix = p.into();
        self
    }

    /// Skip template compile; use this string as `compiled_sql` (already expanded).
    pub fn compiled_sql(mut self, sql: impl Into<String>) -> Self {
        self.compiled_sql_override = Some(sql.into());
        self
    }

    fn into_node(self) -> Result<ModelNode> {
        let name = self.name;
        if name.trim().is_empty() {
            bail!("E_RBT_DAG_BUILDER: model name must be non-empty");
        }

        let layer = self.layer.unwrap_or_else(|| ModelLayer::from_name(&name));

        if self.kind == ModelKind::Rust {
            let mut dependencies: Vec<DependencyRef> = self
                .explicit_refs
                .into_iter()
                .map(DependencyRef::Model)
                .collect();
            for (source_name, table_name) in self.explicit_sources {
                dependencies.push(DependencyRef::Source {
                    source_name,
                    table_name,
                });
            }
            let description = self
                .description
                .or_else(|| self.frontmatter.as_ref().and_then(|f| f.description.clone()));
            let materialization = self
                .frontmatter
                .as_ref()
                .and_then(|f| f.materialization.as_deref())
                .map(super::dag::parse_materialization_hint)
                .transpose()?
                .unwrap_or(self.materialization);
            return Ok(ModelNode {
                name,
                description,
                kind: ModelKind::Rust,
                raw_sql: String::new(),
                compiled_sql: String::new(),
                materialization,
                output_format: self.output_format,
                output_path: self.output_path,
                dependencies,
                layer,
                frontmatter: self.frontmatter,
            });
        }

        let (parsed_fm, pure_sql) = SqlModelParser::parse_frontmatter(&self.raw_sql)
            .map_err(|e| anyhow::anyhow!("E_RBT_DAG_BUILDER: model '{name}': {e}"))?;

        let frontmatter = self.frontmatter.or(parsed_fm);
        let materialization = frontmatter
            .as_ref()
            .and_then(|f| f.materialization.as_deref())
            .map(super::dag::parse_materialization_hint)
            .transpose()?
            .unwrap_or(self.materialization);

        let description = self
            .description
            .or_else(|| frontmatter.as_ref().and_then(|f| f.description.clone()));

        // Deps from SQL jinja + any explicit_refs (host may add extra edges).
        let mut dependencies = SqlModelParser::extract_dependencies(&pure_sql).unwrap_or_default();
        for r in self.explicit_refs {
            let d = DependencyRef::Model(r);
            if !dependencies.contains(&d) {
                dependencies.push(d);
            }
        }
        for (source_name, table_name) in self.explicit_sources {
            let d = DependencyRef::Source {
                source_name,
                table_name,
            };
            if !dependencies.contains(&d) {
                dependencies.push(d);
            }
        }

        let compiled_sql = match self.compiled_sql_override {
            Some(c) => c,
            None => SqlModelParser::compile_sql(&pure_sql, &self.catalog_prefix)
                .with_context(|| format!("E_RBT_DAG_BUILDER: compile SQL for model '{name}'"))?,
        };

        Ok(ModelNode {
            name,
            description,
            kind: ModelKind::Sql,
            raw_sql: self.raw_sql,
            compiled_sql,
            materialization,
            output_format: self.output_format,
            output_path: self.output_path,
            dependencies,
            layer,
            frontmatter,
        })
    }
}

/// Fluent builder for a [`ModelDag`] without an on-disk models directory.
#[derive(Debug, Default)]
pub struct DagBuilder {
    models: Vec<ModelSpec>,
}

impl DagBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a model (order does not need to be topological; graph edges come from `ref`).
    pub fn model(mut self, spec: ModelSpec) -> Self {
        self.models.push(spec);
        self
    }

    /// Build and validate the DAG (cycles, medallion layer bands).
    pub fn build(self) -> Result<ModelDag> {
        let mut dag = ModelDag::new();
        for spec in self.models {
            let name = spec.name.clone();
            let node = spec.into_node().with_context(|| {
                format!("E_RBT_DAG_BUILDER: failed to materialize ModelSpec '{name}'")
            })?;
            if dag.node_map.contains_key(&node.name) {
                bail!(
                    "E_RBT_DAG_BUILDER: duplicate model name '{}'",
                    node.name
                );
            }
            let idx = dag.graph.add_node(node.clone());
            dag.node_map.insert(node.name, idx);
        }
        dag.build_graph()
            .context("E_RBT_DAG_BUILDER: build_graph failed")?;
        Ok(dag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::dag::Materialization;

    #[test]
    fn builder_two_models_ref_edge() -> Result<()> {
        let dag = DagBuilder::new()
            .model(
                ModelSpec::sql("stg_a", "SELECT 1 AS id")
                    .materialization(Materialization::Table)
                    .output_path("/tmp/stg_a.parquet"),
            )
            .model(
                ModelSpec::sql("tf_b", "SELECT id FROM {{ ref('stg_a') }}")
                    .materialization(Materialization::Table)
                    .output_path("/tmp/tf_b.parquet"),
            )
            .build()?;
        assert_eq!(dag.node_map.len(), 2);
        let tiers = dag.execution_tiers()?;
        assert_eq!(tiers.len(), 2);
        assert_eq!(tiers[0][0].name, "stg_a");
        assert_eq!(tiers[1][0].name, "tf_b");
        Ok(())
    }

    #[test]
    fn builder_rust_model_refs() -> Result<()> {
        use super::super::dag::ModelKind;
        let dag = DagBuilder::new()
            .model(
                ModelSpec::sql("stg_a", "SELECT 1 AS id")
                    .materialization(Materialization::Table)
                    .output_path("/tmp/stg_a.parquet"),
            )
            .model(
                ModelSpec::rust("tf_rust")
                    .refs(["stg_a"])
                    .layer(ModelLayer::Transform)
                    .output_path("/tmp/tf_rust.parquet"),
            )
            .build()?;
        let idx = dag.node_map["tf_rust"];
        assert_eq!(dag.graph[idx].kind, ModelKind::Rust);
        let tiers = dag.execution_tiers()?;
        assert_eq!(tiers[0][0].name, "stg_a");
        assert_eq!(tiers[1][0].name, "tf_rust");
        Ok(())
    }

    #[test]
    fn duplicate_name_errors() {
        let err = DagBuilder::new()
            .model(ModelSpec::sql("stg_a", "SELECT 1"))
            .model(ModelSpec::sql("stg_a", "SELECT 2"))
            .build()
            .unwrap_err()
            .to_string();
        assert!(err.contains("duplicate"));
    }

    #[test]
    fn compiled_sql_override_skips_jinja() -> Result<()> {
        let dag = DagBuilder::new()
            .model(
                ModelSpec::sql("stg_x", "SELECT 1 AS id")
                    .compiled_sql("SELECT 99 AS id")
                    .output_path("/tmp/stg_x.parquet"),
            )
            .build()?;
        let idx = dag.node_map["stg_x"];
        assert_eq!(dag.graph[idx].compiled_sql, "SELECT 99 AS id");
        Ok(())
    }

    #[test]
    fn layer_from_name_when_unset() -> Result<()> {
        let dag = DagBuilder::new()
            .model(ModelSpec::sql("dim_entity", "SELECT 1 AS id"))
            .build()?;
        let idx = dag.node_map["dim_entity"];
        assert_eq!(dag.graph[idx].layer, ModelLayer::Mart);
        Ok(())
    }
}

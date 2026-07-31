//! Model selection (`--select`) similar to dbt node selectors (`name` / `+name` / `name+`).
//!
//! # Execute vs Exact
//!
//! * [`SelectMode::Exact`] — expand only explicit `+` modifiers (for `compile` listing).
//! * [`SelectMode::Execute`] — always include **ancestors** of every selected node so
//!   `ref()` dependencies exist when the subgraph is run.

use crate::dag::{ModelDag, ModelNode};
use crate::parser::DependencyRef;
use anyhow::{bail, Result};
use petgraph::Direction;
use std::collections::HashSet;

/// How selection expands relative to named seeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectMode {
    /// Expand `+` modifiers only (compile listing).
    Exact,
    /// Ensure a runnable subgraph: always include ancestors of every selected node.
    Execute,
}

/// One comma-separated token: `name`, `+name`, `name+`, `+name+`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectToken {
    pub name: String,
    pub upstream: bool,
    pub downstream: bool,
}

impl SelectToken {
    /// Parse a single select token.
    pub fn parse(raw: &str) -> Result<Self> {
        let s = raw.trim();
        if s.is_empty() {
            bail!("E_RBT_SELECT_EMPTY: empty --select token");
        }
        let upstream = s.starts_with('+');
        let body = if upstream { &s[1..] } else { s };
        let downstream = body.ends_with('+');
        let name = if downstream {
            body[..body.len().saturating_sub(1)].trim()
        } else {
            body.trim()
        };
        if name.is_empty() || name.contains('+') {
            bail!(
                "E_RBT_SELECT_INVALID: invalid --select token '{}'; expected name, +name, name+, or +name+",
                raw
            );
        }
        // Reject path-like or whitespace names early
        if name.contains('/') || name.contains('\\') || name.contains(' ') {
            bail!(
                "E_RBT_SELECT_INVALID: model name '{}' must not contain path separators or spaces",
                name
            );
        }
        Ok(Self {
            name: name.to_string(),
            upstream,
            downstream,
        })
    }
}

/// Parse a full `--select` string into tokens.
pub fn parse_select_spec(spec: &str) -> Result<Vec<SelectToken>> {
    let mut out = Vec::new();
    for part in spec.split([',', ' ']) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        out.push(SelectToken::parse(part)?);
    }
    if out.is_empty() {
        bail!("E_RBT_SELECT_EMPTY: --select produced no models");
    }
    Ok(out)
}

/// Whether a model declares frontmatter tests / grain / unique_key worth running under `rbt test`.
pub fn model_has_test_contract(node: &ModelNode) -> bool {
    node.frontmatter
        .as_ref()
        .map(|fm| {
            fm.tests
                .as_ref()
                .map(|t| !t.is_empty())
                .unwrap_or(false)
                || fm
                    .unique_key
                    .as_ref()
                    .map(|u| !u.is_empty())
                    .unwrap_or(false)
                || fm.grain.as_ref().map(|g| !g.is_empty()).unwrap_or(false)
        })
        .unwrap_or(false)
}

impl ModelDag {
    /// Resolve which model names are in the selection.
    pub fn resolve_select(&self, select: Option<&str>, mode: SelectMode) -> Result<HashSet<String>> {
        let Some(spec) = select.map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(self.node_map.keys().cloned().collect());
        };

        let tokens = parse_select_spec(spec)?;
        let mut keep: HashSet<String> = HashSet::new();

        for token in tokens {
            if !self.node_map.contains_key(&token.name) {
                let available: Vec<_> = {
                    let mut v: Vec<_> = self.node_map.keys().cloned().collect();
                    v.sort();
                    v
                };
                bail!(
                    "E_RBT_MODEL_NOT_FOUND: model '{}' not in project (select={}). Available: {}",
                    token.name,
                    spec,
                    if available.is_empty() {
                        "(none)".to_string()
                    } else {
                        available.join(", ")
                    }
                );
            }
            let mut up = token.upstream;
            let down = token.downstream;
            // Execute mode always pulls ancestors so refs resolve at runtime.
            if mode == SelectMode::Execute {
                up = true;
            }
            keep.insert(token.name.clone());
            if up {
                self.collect_ancestors(&token.name, &mut keep);
            }
            if down {
                self.collect_descendants(&token.name, &mut keep);
            }
        }

        Ok(keep)
    }

    /// Return a new DAG containing only `keep` models (must include all model deps).
    pub fn subgraph(&self, keep: &HashSet<String>) -> Result<ModelDag> {
        if keep.is_empty() {
            bail!("E_RBT_SELECT_EMPTY: selection resolved to zero models");
        }

        let mut out = ModelDag::new();
        for node in self.topological_sequence()? {
            if !keep.contains(&node.name) {
                continue;
            }
            for dep in &node.dependencies {
                if let DependencyRef::Model(dep_name) = dep {
                    if !keep.contains(dep_name) {
                        bail!(
                            "E_RBT_SELECT_INCOMPLETE: model '{}' depends on '{}' which is not selected; \
                             use SelectMode::Execute or include +upstream",
                            node.name,
                            dep_name
                        );
                    }
                }
            }
            let name = node.name.clone();
            let idx = out.graph.add_node(node);
            out.node_map.insert(name, idx);
        }

        // Rebuild edges among kept nodes (topo order already validated).
        let mut edges = Vec::new();
        for &idx in out.node_map.values() {
            let node = &out.graph[idx];
            for dep in &node.dependencies {
                if let DependencyRef::Model(dep_name) = dep {
                    if let Some(&dep_idx) = out.node_map.get(dep_name) {
                        edges.push((dep_idx, idx));
                    }
                }
            }
        }
        for (from, to) in edges {
            out.graph.add_edge(from, to, ());
        }
        Ok(out)
    }

    /// Apply `--select` and return a filtered DAG.
    pub fn apply_select(&self, select: Option<&str>, mode: SelectMode) -> Result<ModelDag> {
        let keep = self.resolve_select(select, mode)?;
        self.subgraph(&keep)
    }

    /// Names of models that declare a test contract (for default `rbt test`).
    pub fn models_with_test_contract(&self) -> Result<Vec<String>> {
        Ok(self
            .topological_sequence()?
            .into_iter()
            .filter(model_has_test_contract)
            .map(|n| n.name)
            .collect())
    }

    fn collect_ancestors(&self, name: &str, keep: &mut HashSet<String>) {
        let Some(&idx) = self.node_map.get(name) else {
            return;
        };
        let mut stack: Vec<_> = self
            .graph
            .neighbors_directed(idx, Direction::Incoming)
            .collect();
        while let Some(n) = stack.pop() {
            let n_name = self.graph[n].name.clone();
            if keep.insert(n_name) {
                stack.extend(self.graph.neighbors_directed(n, Direction::Incoming));
            }
        }
    }

    fn collect_descendants(&self, name: &str, keep: &mut HashSet<String>) {
        let Some(&idx) = self.node_map.get(name) else {
            return;
        };
        let mut stack: Vec<_> = self
            .graph
            .neighbors_directed(idx, Direction::Outgoing)
            .collect();
        while let Some(n) = stack.pop() {
            let n_name = self.graph[n].name.clone();
            if keep.insert(n_name) {
                stack.extend(self.graph.neighbors_directed(n, Direction::Outgoing));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{Materialization, OutputFormat};

    fn sample_dag() -> ModelDag {
        let mut dag = ModelDag::new();
        dag.add_model_with_format(
            "stg_a",
            "SELECT 1 AS id",
            Materialization::Table,
            OutputFormat::Parquet,
            None,
            "",
        )
        .unwrap();
        dag.add_model_with_format(
            "tf_b",
            "SELECT * FROM {{ ref('stg_a') }}",
            Materialization::Table,
            OutputFormat::Parquet,
            None,
            "",
        )
        .unwrap();
        dag.add_model_with_format(
            "fact_c",
            "SELECT * FROM {{ ref('tf_b') }}",
            Materialization::Table,
            OutputFormat::Parquet,
            None,
            "",
        )
        .unwrap();
        // parallel branch
        dag.add_model_with_format(
            "stg_x",
            "SELECT 2 AS id",
            Materialization::Table,
            OutputFormat::Parquet,
            None,
            "",
        )
        .unwrap();
        dag.build_graph().unwrap();
        dag
    }

    #[test]
    fn select_none_is_all() {
        let dag = sample_dag();
        let keep = dag.resolve_select(None, SelectMode::Execute).unwrap();
        assert_eq!(keep.len(), 4);
    }

    #[test]
    fn select_execute_includes_ancestors() {
        let dag = sample_dag();
        let keep = dag
            .resolve_select(Some("fact_c"), SelectMode::Execute)
            .unwrap();
        assert!(keep.contains("stg_a"));
        assert!(keep.contains("tf_b"));
        assert!(keep.contains("fact_c"));
        assert!(!keep.contains("stg_x"));
    }

    #[test]
    fn select_exact_bare_name_is_only_self() {
        let dag = sample_dag();
        let keep = dag
            .resolve_select(Some("fact_c"), SelectMode::Exact)
            .unwrap();
        assert_eq!(keep, HashSet::from(["fact_c".to_string()]));
    }

    #[test]
    fn select_downstream_plus() {
        let dag = sample_dag();
        let keep = dag
            .resolve_select(Some("stg_a+"), SelectMode::Exact)
            .unwrap();
        assert!(keep.contains("stg_a"));
        assert!(keep.contains("tf_b"));
        assert!(keep.contains("fact_c"));
    }

    #[test]
    fn select_upstream_plus_exact() {
        let dag = sample_dag();
        let keep = dag
            .resolve_select(Some("+fact_c"), SelectMode::Exact)
            .unwrap();
        assert!(keep.contains("stg_a") && keep.contains("tf_b") && keep.contains("fact_c"));
    }

    #[test]
    fn select_both_plus() {
        let dag = sample_dag();
        let keep = dag
            .resolve_select(Some("+tf_b+"), SelectMode::Exact)
            .unwrap();
        assert!(keep.contains("stg_a"));
        assert!(keep.contains("tf_b"));
        assert!(keep.contains("fact_c"));
    }

    #[test]
    fn select_comma_and_space() {
        let dag = sample_dag();
        let keep = dag
            .resolve_select(Some("stg_a, stg_x"), SelectMode::Exact)
            .unwrap();
        assert!(keep.contains("stg_a") && keep.contains("stg_x"));
        assert!(!keep.contains("fact_c"));
    }

    #[test]
    fn select_missing_errors() {
        let dag = sample_dag();
        let err = dag
            .resolve_select(Some("nope"), SelectMode::Execute)
            .unwrap_err()
            .to_string();
        assert!(err.contains("E_RBT_MODEL_NOT_FOUND"));
        assert!(err.contains("Available:"));
    }

    #[test]
    fn select_invalid_token() {
        assert!(SelectToken::parse("+").is_err());
        assert!(SelectToken::parse("a+b").is_err());
        assert!(parse_select_spec("  ,  ").is_err());
    }

    #[test]
    fn subgraph_execute_runnable() {
        let dag = sample_dag();
        let sub = dag
            .apply_select(Some("fact_c"), SelectMode::Execute)
            .unwrap();
        assert_eq!(sub.node_map.len(), 3);
        let tiers = sub.execution_tiers().unwrap();
        assert_eq!(tiers[0][0].name, "stg_a");
    }

    #[test]
    fn subgraph_exact_without_deps_fails() {
        let dag = sample_dag();
        let err = dag
            .apply_select(Some("fact_c"), SelectMode::Exact)
            .unwrap_err()
            .to_string();
        assert!(err.contains("E_RBT_SELECT_INCOMPLETE") || err.contains("depends on"));
    }
}

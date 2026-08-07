//! Project-level **value contracts** (enum registry) for bronze → silver scaling.
//!
//! Declared in `rbt_project.yml` under `contracts.enums`, referenced from model
//! frontmatter `tests.accepted_values` as a contract name (string) instead of an
//! inline list. Powers `rbt validate --contract-diff`.

use crate::core::dag::ModelDag;
use crate::core::frontmatter::{
    AcceptedValuesEntry, BronzeDiagnostic, DiagnosticSeverity, SourceFormat, StagingFrontmatter,
};
use crate::core::project::RbtProjectConfig;
use crate::core::receipt::try_apply_scope_to_frontmatter;
use crate::core::run_scope::RunScope;
use crate::scan::{LakeScanner, ScanRequest};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

/// How contract-diff treats values present in bronze but missing from the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnNewPolicy {
    /// Emit `E_RBT_CONTRACT_NEW_VALUE` and fail validate when `--contract-diff`.
    #[default]
    Fail,
    /// Emit `W_RBT_CONTRACT_NEW_VALUE` only.
    Warn,
    /// Record in report but no diagnostic severity.
    Allow,
}

impl OnNewPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fail => "fail",
            Self::Warn => "warn",
            Self::Allow => "allow",
        }
    }
}

/// Optional bronze probe for contract-diff when models do not reference the enum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct EnumProbe {
    /// Staging model name (must have scan_path).
    pub model: String,
    /// Column to sample for distinct values (jsonl/json object keys).
    pub column: String,
}

/// One named enum in the project registry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct EnumContract {
    /// Closed set of allowed values (order preserved for docs).
    #[serde(default)]
    pub values: Vec<String>,
    /// Severity for bronze values not in `values` during contract-diff.
    #[serde(default)]
    pub on_new: OnNewPolicy,
    /// Optional human labels (e.g. dim display names).
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Optional explicit bronze probe for contract-diff.
    #[serde(default)]
    pub probe: Option<EnumProbe>,
}

impl EnumContract {
    pub fn value_set(&self) -> HashSet<&str> {
        self.values.iter().map(String::as_str).collect()
    }
}

/// `contracts:` block in `rbt_project.yml`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ContractsConfig {
    /// Named enums: key e.g. `works.source`, `works.topic_track`.
    #[serde(default)]
    pub enums: BTreeMap<String, EnumContract>,
}

impl ContractsConfig {
    pub fn is_empty(&self) -> bool {
        self.enums.is_empty()
    }

    pub fn get_enum(&self, name: &str) -> Option<&EnumContract> {
        let key = strip_contract_prefix(name);
        self.enums.get(key)
    }

    /// Resolve one accepted_values entry to a concrete list.
    pub fn resolve_entry(&self, entry: &AcceptedValuesEntry) -> Result<Vec<String>> {
        match entry {
            AcceptedValuesEntry::List(v) => Ok(v.clone()),
            AcceptedValuesEntry::Contract(name) => {
                let key = strip_contract_prefix(name);
                let ec = self.enums.get(key).with_context(|| {
                    format!(
                        "E_RBT_CONTRACT_UNKNOWN: accepted_values references contract '{name}' \
                         but contracts.enums has no key '{key}'. \
                         Define it under contracts.enums in rbt_project.yml."
                    )
                })?;
                if ec.values.is_empty() {
                    bail!(
                        "E_RBT_CONTRACT_EMPTY: contracts.enums.{key} has no values"
                    );
                }
                Ok(ec.values.clone())
            }
        }
    }

    /// Resolve full accepted_values map for assertions.
    pub fn resolve_accepted_values(
        &self,
        map: &HashMap<String, AcceptedValuesEntry>,
    ) -> Result<HashMap<String, Vec<String>>> {
        let mut out = HashMap::new();
        for (col, entry) in map {
            out.insert(col.clone(), self.resolve_entry(entry)?);
        }
        Ok(out)
    }
}

/// Strip optional `$contract:` / `contract:` prefix from a contract ref string.
pub fn strip_contract_prefix(name: &str) -> &str {
    let n = name.trim();
    if let Some(rest) = n.strip_prefix("$contract:") {
        return rest.trim();
    }
    if let Some(rest) = n.strip_prefix("contract:") {
        return rest.trim();
    }
    if let Some(rest) = n.strip_prefix('$') {
        // `$works.source`
        return rest.trim();
    }
    n
}

/// One column probe result for contract-diff.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContractDiffColumn {
    pub enum_name: String,
    pub model: String,
    pub column: String,
    pub on_new: String,
    pub registered: Vec<String>,
    pub observed: Vec<String>,
    /// In bronze, not in registry.
    pub new_in_bronze: Vec<String>,
    /// In registry, not seen in sampled bronze (informational).
    pub unused_in_bronze: Vec<String>,
    pub files_sampled: usize,
    pub rows_sampled: usize,
}

/// Full contract-diff report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ContractDiffReport {
    pub ok: bool,
    pub enums_checked: usize,
    pub columns: Vec<ContractDiffColumn>,
    pub diagnostics: Vec<String>,
    pub notes: Vec<String>,
}

impl ContractDiffReport {
    pub fn has_errors(&self) -> bool {
        !self.ok
            || self
                .diagnostics
                .iter()
                .any(|d| d.contains("E_RBT_CONTRACT_"))
    }
}

/// Run bronze vs registry contract-diff.
///
/// Samples jsonl/json bronze for columns tied to `contracts.enums` via:
/// 1. explicit `probe: { model, column }`, or
/// 2. staging models whose `accepted_values` reference the enum.
pub fn run_contract_diff(
    project_dir: &Path,
    config: &RbtProjectConfig,
    dag: &ModelDag,
    scope: &RunScope,
) -> Result<ContractDiffReport> {
    let mut report = ContractDiffReport {
        ok: true,
        notes: Vec::new(),
        ..Default::default()
    };

    if config.contracts.is_empty() {
        report.notes.push(
            "no contracts.enums in rbt_project.yml — contract-diff is a no-op".into(),
        );
        return Ok(report);
    }

    for (enum_name, ec) in &config.contracts.enums {
        report.enums_checked += 1;
        let probes = resolve_probes_for_enum(enum_name, ec, dag);
        if probes.is_empty() {
            report.notes.push(format!(
                "contracts.enums.{enum_name}: no probe and no model accepted_values reference — skipped"
            ));
            continue;
        }

        for (model_name, column) in probes {
            let Some(node) = dag.node_map.get(&model_name).and_then(|&idx| {
                dag.graph.node_weight(idx)
            }) else {
                report.diagnostics.push(format!(
                    "E_RBT_CONTRACT_PROBE: enum '{enum_name}' probes model '{model_name}' \
                     which is not in the DAG"
                ));
                report.ok = false;
                continue;
            };

            let Some(fm0) = node.frontmatter.as_ref() else {
                report.diagnostics.push(format!(
                    "E_RBT_CONTRACT_PROBE: model '{model_name}' has no frontmatter for probe"
                ));
                report.ok = false;
                continue;
            };

            if !fm0.has_scan_contract() {
                report.diagnostics.push(format!(
                    "E_RBT_CONTRACT_PROBE: model '{model_name}' has no scan_path for bronze probe"
                ));
                report.ok = false;
                continue;
            }

            let fm = match try_apply_scope_to_frontmatter(fm0, scope) {
                Ok(f) => f,
                Err(e) => {
                    report.diagnostics.push(format!(
                        "E_RBT_CONTRACT_PROBE: scope apply for '{model_name}': {e}"
                    ));
                    report.ok = false;
                    continue;
                }
            };
            let sample = match sample_bronze_column(
                project_dir,
                config,
                &fm,
                &column,
            ) {
                Ok(s) => s,
                Err(e) => {
                    // Optional empty artifacts: soft note
                    if fm.on_missing_policy() == crate::core::run_scope::OnMissing::Empty {
                        report.notes.push(format!(
                            "contracts.enums.{enum_name} probe {model_name}.{column}: {e} (on_missing empty)"
                        ));
                        continue;
                    }
                    report.diagnostics.push(format!(
                        "E_RBT_CONTRACT_PROBE: {model_name}.{column}: {e}"
                    ));
                    report.ok = false;
                    continue;
                }
            };

            let registered: BTreeSet<String> = ec.values.iter().cloned().collect();
            let observed: BTreeSet<String> = sample.values.iter().cloned().collect();
            let new_in_bronze: Vec<String> = observed
                .difference(&registered)
                .cloned()
                .collect();
            let unused: Vec<String> = registered
                .difference(&observed)
                .cloned()
                .collect();

            let col_report = ContractDiffColumn {
                enum_name: enum_name.clone(),
                model: model_name.clone(),
                column: column.clone(),
                on_new: ec.on_new.as_str().to_string(),
                registered: ec.values.clone(),
                observed: observed.into_iter().collect(),
                new_in_bronze: new_in_bronze.clone(),
                unused_in_bronze: unused,
                files_sampled: sample.files,
                rows_sampled: sample.rows,
            };

            if !new_in_bronze.is_empty() {
                match ec.on_new {
                    OnNewPolicy::Fail => {
                        report.ok = false;
                        report.diagnostics.push(format!(
                            "E_RBT_CONTRACT_NEW_VALUE: enum '{enum_name}' column '{column}' \
                             (model {model_name}): bronze has values not in registry: {:?}. \
                             Add them to contracts.enums.{enum_name}.values or set on_new: warn|allow.",
                            new_in_bronze
                        ));
                    }
                    OnNewPolicy::Warn => {
                        report.diagnostics.push(format!(
                            "W_RBT_CONTRACT_NEW_VALUE: enum '{enum_name}' column '{column}' \
                             (model {model_name}): bronze has unregistered values: {:?}",
                            new_in_bronze
                        ));
                    }
                    OnNewPolicy::Allow => {
                        report.notes.push(format!(
                            "contracts.enums.{enum_name}: allowed new bronze values {:?}",
                            new_in_bronze
                        ));
                    }
                }
            }

            report.columns.push(col_report);
        }
    }

    Ok(report)
}

fn resolve_probes_for_enum(
    enum_name: &str,
    ec: &EnumContract,
    dag: &ModelDag,
) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    if let Some(p) = &ec.probe {
        out.push((p.model.clone(), p.column.clone()));
    }
    // Models that reference this contract in accepted_values
    for idx in dag.graph.node_indices() {
        let node = &dag.graph[idx];
        let Some(fm) = node.frontmatter.as_ref() else {
            continue;
        };
        let Some(tests) = fm.tests.as_ref() else {
            continue;
        };
        let Some(map) = tests.accepted_values.as_ref() else {
            continue;
        };
        for (col, entry) in map {
            if let AcceptedValuesEntry::Contract(name) = entry {
                if strip_contract_prefix(name) == enum_name {
                    let pair = (node.name.clone(), col.clone());
                    if !out.contains(&pair) {
                        out.push(pair);
                    }
                }
            }
        }
    }
    out
}

struct ColumnSample {
    values: HashSet<String>,
    files: usize,
    rows: usize,
}

fn sample_bronze_column(
    project_dir: &Path,
    config: &RbtProjectConfig,
    fm: &StagingFrontmatter,
    column: &str,
) -> Result<ColumnSample> {
    let mut req = ScanRequest::from_frontmatter_with_config(
        project_dir,
        fm,
        config.roots.clone(),
        &config.scan,
    )?;
    // Contract-diff should not fail hard on empty optional paths
    req.allow_empty = true;

    let scanner = LakeScanner::from_request(&req);
    let (_root, files) = scanner.list_files(&req)?;
    if files.is_empty() {
        bail!(
            "no bronze files matched scan_path='{}' path_glob={:?}",
            req.scan_path,
            req.path_glob
        );
    }

    match req.format {
        SourceFormat::Jsonl | SourceFormat::Json => {}
        other => {
            bail!(
                "contract-diff column sample supports jsonl/json only (got {})",
                other.as_str()
            );
        }
    }

    let mut values = HashSet::new();
    let mut rows = 0usize;
    const MAX_FILES: usize = 64;
    const MAX_LINES_PER_FILE: usize = 50_000;

    for path in files.iter().take(MAX_FILES) {
        sample_json_file(path, req.format, column, &mut values, &mut rows, MAX_LINES_PER_FILE)
            .with_context(|| format!("sampling {}", path.display()))?;
    }

    Ok(ColumnSample {
        values,
        files: files.len().min(MAX_FILES),
        rows,
    })
}

fn sample_json_file(
    path: &Path,
    format: SourceFormat,
    column: &str,
    values: &mut HashSet<String>,
    rows: &mut usize,
    max_lines: usize,
) -> Result<()> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    match format {
        SourceFormat::Jsonl => {
            for (i, line) in reader.lines().enumerate() {
                if i >= max_lines {
                    break;
                }
                let line = line?;
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let v: serde_json::Value = serde_json::from_str(line)
                    .with_context(|| format!("jsonl parse line {}", i + 1))?;
                collect_field(&v, column, values, rows);
            }
        }
        SourceFormat::Json => {
            let v: serde_json::Value = serde_json::from_reader(reader)?;
            match v {
                serde_json::Value::Array(arr) => {
                    for item in arr.into_iter().take(max_lines) {
                        collect_field(&item, column, values, rows);
                    }
                }
                other => {
                    collect_field(&other, column, values, rows);
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn collect_field(
    v: &serde_json::Value,
    column: &str,
    values: &mut HashSet<String>,
    rows: &mut usize,
) {
    *rows += 1;
    if let Some(field) = v.get(column) {
        match field {
            serde_json::Value::Null => {}
            serde_json::Value::String(s) => {
                values.insert(s.clone());
            }
            serde_json::Value::Number(n) => {
                values.insert(n.to_string());
            }
            serde_json::Value::Bool(b) => {
                values.insert(b.to_string());
            }
            other => {
                // arrays/objects: stable JSON string for set membership
                values.insert(other.to_string());
            }
        }
    }
}

/// Convert contract-diff diagnostics into bronze diagnostics for unified validate output.
pub fn contract_diff_to_bronze_diagnostics(report: &ContractDiffReport) -> Vec<BronzeDiagnostic> {
    let mut out = Vec::new();
    for d in &report.diagnostics {
        let (severity, code) = if d.starts_with("E_RBT_") {
            (DiagnosticSeverity::Error, "E_RBT_CONTRACT")
        } else if d.starts_with("W_RBT_") {
            (DiagnosticSeverity::Warning, "W_RBT_CONTRACT")
        } else {
            (DiagnosticSeverity::Warning, "W_RBT_CONTRACT")
        };
        // Best-effort model extract: "(model stg_works)" or "model 'stg_works'"
        let model = extract_model_from_diag(d).unwrap_or_else(|| "_contract".into());
        out.push(BronzeDiagnostic {
            model,
            severity,
            code,
            message: d.clone(),
        });
    }
    out
}

fn extract_model_from_diag(d: &str) -> Option<String> {
    if let Some(i) = d.find("(model ") {
        let rest = &d[i + 7..];
        let end = rest.find(')')?;
        return Some(rest[..end].trim().to_string());
    }
    if let Some(i) = d.find("model '") {
        let rest = &d[i + 7..];
        let end = rest.find('\'')?;
        return Some(rest[..end].to_string());
    }
    None
}

/// Path helper for tests / callers that need fixture paths.
#[allow(dead_code)]
pub fn contracts_doc_path() -> PathBuf {
    PathBuf::from("docs/COMPLEX_BRONZE_AND_RUN_SCOPE.md")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::frontmatter::AcceptedValuesEntry;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn strip_prefixes() {
        assert_eq!(strip_contract_prefix("works.source"), "works.source");
        assert_eq!(strip_contract_prefix("$works.source"), "works.source");
        assert_eq!(
            strip_contract_prefix("$contract:works.source"),
            "works.source"
        );
    }

    #[test]
    fn resolve_contract_entry() {
        let mut enums = BTreeMap::new();
        enums.insert(
            "works.source".into(),
            EnumContract {
                values: vec!["a".into(), "b".into()],
                on_new: OnNewPolicy::Fail,
                labels: BTreeMap::new(),
                probe: None,
            },
        );
        let c = ContractsConfig { enums };
        let v = c
            .resolve_entry(&AcceptedValuesEntry::Contract("works.source".into()))
            .unwrap();
        assert_eq!(v, vec!["a", "b"]);
        assert!(c
            .resolve_entry(&AcceptedValuesEntry::Contract("missing".into()))
            .is_err());
    }

    #[test]
    fn sample_jsonl_column() -> Result<()> {
        let dir = tempdir()?;
        let f = dir.path().join("works.jsonl");
        let mut file = File::create(&f)?;
        writeln!(file, r#"{{"source":"pubmed","title":"t1"}}"#)?;
        writeln!(file, r#"{{"source":"openalex","title":"t2"}}"#)?;
        writeln!(file, r#"{{"source":"pubmed","title":"t3"}}"#)?;
        drop(file);

        let mut values = HashSet::new();
        let mut rows = 0;
        sample_json_file(
            &f,
            SourceFormat::Jsonl,
            "source",
            &mut values,
            &mut rows,
            100,
        )?;
        assert_eq!(rows, 3);
        assert!(values.contains("pubmed"));
        assert!(values.contains("openalex"));
        Ok(())
    }
}

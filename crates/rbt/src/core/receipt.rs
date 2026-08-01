//! Run receipts and bronze content fingerprints (P5b).
//!
//! A receipt is the **consumer authority** for “did this scope succeed?” — not “file exists”.
//! Fingerprints cover the filtered bronze file set + contract version so identical re-drives
//! can skip materialize when `skip_if_fingerprint_match` is set.

use crate::core::dag::ModelDag;
use crate::core::frontmatter::StagingFrontmatter;
use crate::core::project::RbtProjectConfig;
use crate::core::run_scope::{fnv1a64, RunScope};
use crate::scan::{LakeScanner, ScanRequest};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Machine-readable result of one `rbt run` (or library execute with receipts enabled).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunReceipt {
    pub schema_version: u32,
    pub run_id: String,
    pub project: String,
    pub package_version: String,
    /// Project or CLI contract version used for skip decisions.
    pub contract_version: String,
    pub scope_key: String,
    pub vars: BTreeMap<String, String>,
    pub status: RunStatus,
    pub skipped: bool,
    pub skip_reason: Option<String>,
    pub bronze_fingerprint: String,
    pub models_executed: usize,
    pub total_rows: usize,
    pub bronze_sources: usize,
    pub model_results: Vec<ModelRunResult>,
    pub started_unix_ms: u128,
    pub finished_unix_ms: u128,
    pub wall_ms: u128,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Ok,
    Skipped,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelRunResult {
    pub name: String,
    pub rows: usize,
    pub output_path: Option<String>,
}

impl RunReceipt {
    pub const SCHEMA_VERSION: u32 = 1;

    pub fn receipt_dir(project_dir: &Path) -> PathBuf {
        project_dir.join(".rbt").join("runs")
    }

    pub fn path_for(project_dir: &Path, run_id: &str) -> PathBuf {
        Self::receipt_dir(project_dir).join(format!("{run_id}.json"))
    }

    /// Latest successful receipt for this scope (for skip-if-match).
    pub fn latest_path_for_scope(project_dir: &Path, scope_key: &str) -> PathBuf {
        Self::receipt_dir(project_dir).join(format!("latest_{scope_key}.json"))
    }

    pub fn write(&self, project_dir: &Path) -> Result<PathBuf> {
        let dir = Self::receipt_dir(project_dir);
        fs::create_dir_all(&dir)?;
        let path = Self::path_for(project_dir, &self.run_id);
        let body = serde_json::to_string_pretty(self)
            .context("E_RBT_RECEIPT: serialize RunReceipt")?;
        fs::write(&path, body).with_context(|| {
            format!("E_RBT_RECEIPT: write {}", path.display())
        })?;
        if matches!(self.status, RunStatus::Ok | RunStatus::Skipped) {
            let latest = Self::latest_path_for_scope(project_dir, &self.scope_key);
            fs::write(&latest, serde_json::to_string_pretty(self)?)?;
        }
        Ok(path)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("E_RBT_RECEIPT: read {}", path.display()))?;
        serde_json::from_str(&raw)
            .with_context(|| format!("E_RBT_RECEIPT: parse {}", path.display()))
    }

    pub fn load_latest_for_scope(project_dir: &Path, scope_key: &str) -> Option<Self> {
        let p = Self::latest_path_for_scope(project_dir, scope_key);
        Self::load(&p).ok()
    }
}

/// Effective contract version: run scope override → project yml → `"0"`.
pub fn effective_contract_version(config: &RbtProjectConfig, scope: &RunScope) -> String {
    scope
        .contract_version
        .clone()
        .or_else(|| config.contract_version.clone())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "0".into())
}

/// Build a stable bronze fingerprint for the DAG under this scope.
///
/// Manifest lines: `model\\tschema.table\\trel_or_abs\\tsize\\tmtime_secs` sorted, plus
/// `contract_version=…`. Fingerprint is `fnv1a64:` + hex of FNV over the manifest.
pub fn bronze_fingerprint(
    dag: &ModelDag,
    project_dir: &Path,
    config: &RbtProjectConfig,
    scope: &RunScope,
) -> Result<String> {
    let contract = effective_contract_version(config, scope);
    let mut lines: Vec<String> = Vec::new();
    lines.push(format!("contract_version={contract}"));

    for idx in dag.graph.node_indices() {
        let node = &dag.graph[idx];
        let Some(fm) = node.frontmatter.as_ref() else {
            continue;
        };
        if !fm.has_scan_contract() {
            continue;
        }
        let fm_eff = apply_scope_to_frontmatter(fm, scope);
        let mut req = ScanRequest::from_frontmatter_with_config(
            project_dir,
            &fm_eff,
            config.roots.clone(),
            &config.scan,
        )?;
        // Soft list: missing/empty contributes a marker line, not an error.
        req.allow_empty = true;
        let scanner = LakeScanner::from_request(&req);
        match scanner.list_files(&req) {
            Ok((root, files)) => {
                if files.is_empty() {
                    lines.push(format!(
                        "{}\tEMPTY\t{}",
                        node.name,
                        root.display()
                    ));
                } else {
                    for f in files {
                        let meta = fs::metadata(&f).ok();
                        let size = meta.as_ref().map(|m| m.len()).unwrap_or(0);
                        let mtime = meta
                            .and_then(|m| m.modified().ok())
                            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        let rel = f
                            .strip_prefix(&root)
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|_| f.display().to_string());
                        lines.push(format!(
                            "{}\t{}\t{}\t{}",
                            node.name, rel, size, mtime
                        ));
                    }
                }
            }
            Err(_) => {
                lines.push(format!("{}\tMISSING\t{}", node.name, req.scan_path));
            }
        }
    }

    lines.sort();
    let manifest = lines.join("\n");
    let digest = fnv1a64(manifest.as_bytes());
    Ok(format!("fnv1a64:{digest:016x}"))
}

/// Clone frontmatter with run-scope templates and effective require_partitions applied.
pub fn apply_scope_to_frontmatter(fm: &StagingFrontmatter, scope: &RunScope) -> StagingFrontmatter {
    let mut out = fm.clone();
    if let Some(sp) = out.scan_path.as_ref() {
        out.scan_path = Some(scope.expand_template(sp));
    }
    if let Some(globs) = out.path_glob.as_ref() {
        out.path_glob = Some(
            globs
                .iter()
                .map(|g| scope.expand_template(g))
                .collect(),
        );
    }
    let partition_by = out.partition_by.clone().unwrap_or_default();
    let base = out.require_partitions.clone().unwrap_or_default();
    let eff = scope.effective_require_partitions(&partition_by, &base);
    if !eff.is_empty() {
        out.require_partitions = Some(eff);
    }
    out
}

pub fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::run_scope::RunScope;

    #[test]
    fn receipt_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let r = RunReceipt {
            schema_version: 1,
            run_id: "run_test".into(),
            project: "p".into(),
            package_version: "0.6.0".into(),
            contract_version: "1".into(),
            scope_key: "s0".into(),
            vars: BTreeMap::new(),
            status: RunStatus::Ok,
            skipped: false,
            skip_reason: None,
            bronze_fingerprint: "fnv1a64:00".into(),
            models_executed: 1,
            total_rows: 2,
            bronze_sources: 1,
            model_results: vec![],
            started_unix_ms: 1,
            finished_unix_ms: 2,
            wall_ms: 1,
            error: None,
        };
        let p = r.write(dir.path()).unwrap();
        let loaded = RunReceipt::load(&p).unwrap();
        assert_eq!(loaded.run_id, "run_test");
        assert!(RunReceipt::latest_path_for_scope(dir.path(), "s0").exists());
    }

    #[test]
    fn apply_scope_expands_scan_path() {
        let fm = StagingFrontmatter {
            scan_path: Some("lake/{domain}/raw".into()),
            partition_by: Some(vec!["report_date".into()]),
            require_partitions: Some({
                let mut m = std::collections::HashMap::new();
                m.insert("report_date".into(), "{report_date}".into());
                m
            }),
            ..Default::default()
        };
        let scope = RunScope::new()
            .with_var("domain", "acme")
            .with_var("report_date", "2026-07-29");
        let e = apply_scope_to_frontmatter(&fm, &scope);
        assert_eq!(e.scan_path.as_deref(), Some("lake/acme/raw"));
        assert_eq!(
            e.require_partitions
                .as_ref()
                .and_then(|m| m.get("report_date"))
                .map(String::as_str),
            Some("2026-07-29")
        );
    }
}

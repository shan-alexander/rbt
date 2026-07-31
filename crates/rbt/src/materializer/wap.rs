//! Honest Write-Audit-Publish for lake files (not Iceberg branch theater).
//!
//! Protocol:
//! 1. **Write** model output to a staging path under `.wap/{run_id}/`
//! 2. **Audit** via streaming assertions (caller); fail leaves production dest untouched
//! 3. **Publish** atomic rename stage → production dest
//! 4. Write a small audit log JSON next to stage (kept on failure for debug)
//!
//! This is real atomic publish + test gate. It does **not** claim Iceberg WAP branches.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::materializer::stream::atomic_publish;
use crate::testing::ValidationResult;

/// Status of a WAP run for one model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WapPhase {
    Staged,
    AuditedOk,
    AuditedFail,
    Published,
    RolledBack,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WapAuditLog {
    pub run_id: String,
    pub model: String,
    pub stage_path: String,
    pub final_path: String,
    pub phase: WapPhase,
    pub rows: usize,
    pub assertion_errors: Vec<String>,
    pub published_at_ms: Option<u64>,
}

/// One model’s WAP paths for a run.
#[derive(Debug, Clone)]
pub struct WapModelPaths {
    pub run_id: String,
    pub stage_path: PathBuf,
    pub final_path: PathBuf,
    pub audit_log_path: PathBuf,
}

impl WapModelPaths {
    pub fn for_model(
        project_dir: &Path,
        run_id: &str,
        model_name: &str,
        final_path: &Path,
    ) -> Self {
        let stage_root = project_dir.join(".wap").join(run_id);
        let ext = final_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("parquet");
        let stage_path = stage_root.join(format!("{model_name}.{ext}"));
        let audit_log_path = stage_root.join(format!("{model_name}.audit.json"));
        Self {
            run_id: run_id.to_string(),
            stage_path,
            final_path: final_path.to_path_buf(),
            audit_log_path,
        }
    }
}

pub fn new_wap_run_id() -> String {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("wap_{ms}")
}

/// Publish staged file to final path after successful audit.
pub fn wap_publish(
    paths: &WapModelPaths,
    model: &str,
    rows: usize,
    validation: &ValidationResult,
) -> Result<WapAuditLog> {
    if validation.failed_assertions > 0 {
        let log = WapAuditLog {
            run_id: paths.run_id.clone(),
            model: model.to_string(),
            stage_path: paths.stage_path.display().to_string(),
            final_path: paths.final_path.display().to_string(),
            phase: WapPhase::AuditedFail,
            rows,
            assertion_errors: validation.errors.clone(),
            published_at_ms: None,
        };
        write_audit_log(paths, &log)?;
        bail!(
            "E_RBT_WAP_AUDIT: model '{model}' failed {} assertion(s); production dest left unchanged: {}",
            validation.failed_assertions,
            validation.errors.join("; ")
        );
    }

    if !paths.stage_path.exists() {
        bail!(
            "E_RBT_WAP_PUBLISH: stage missing {}",
            paths.stage_path.display()
        );
    }
    atomic_publish(&paths.stage_path, &paths.final_path).with_context(|| {
        format!(
            "E_RBT_WAP_PUBLISH: rename {} → {}",
            paths.stage_path.display(),
            paths.final_path.display()
        )
    })?;

    let log = WapAuditLog {
        run_id: paths.run_id.clone(),
        model: model.to_string(),
        stage_path: paths.stage_path.display().to_string(),
        final_path: paths.final_path.display().to_string(),
        phase: WapPhase::Published,
        rows,
        assertion_errors: Vec::new(),
        published_at_ms: Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        ),
    };
    write_audit_log(paths, &log)?;
    tracing::info!(
        "WAP published model '{}' ({} rows) → {}",
        model,
        rows,
        paths.final_path.display()
    );
    Ok(log)
}

fn write_audit_log(paths: &WapModelPaths, log: &WapAuditLog) -> Result<()> {
    if let Some(parent) = paths.audit_log_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut f = File::create(&paths.audit_log_path).with_context(|| {
        format!(
            "E_RBT_WAP: write audit log {}",
            paths.audit_log_path.display()
        )
    })?;
    writeln!(f, "{}", serde_json::to_string_pretty(log)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::ValidationResult;

    #[test]
    fn wap_publish_atomic() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let final_p = temp.path().join("out.parquet");
        let paths = WapModelPaths::for_model(temp.path(), "wap_1", "m", &final_p);
        fs::create_dir_all(paths.stage_path.parent().unwrap())?;
        fs::write(&paths.stage_path, b"staged-data")?;
        fs::write(&final_p, b"old")?;
        let ok = ValidationResult {
            total_rows: 1,
            passed_assertions: 1,
            failed_assertions: 0,
            errors: Vec::new(),
        };
        wap_publish(&paths, "m", 1, &ok)?;
        assert_eq!(fs::read(&final_p)?, b"staged-data");
        assert!(!paths.stage_path.exists());
        assert!(paths.audit_log_path.exists());
        Ok(())
    }

    #[test]
    fn wap_fail_keeps_production() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let final_p = temp.path().join("out.parquet");
        fs::write(&final_p, b"production")?;
        let paths = WapModelPaths::for_model(temp.path(), "wap_2", "m", &final_p);
        fs::create_dir_all(paths.stage_path.parent().unwrap())?;
        fs::write(&paths.stage_path, b"bad")?;
        let bad = ValidationResult {
            total_rows: 1,
            passed_assertions: 0,
            failed_assertions: 1,
            errors: vec!["dup".into()],
        };
        let err = wap_publish(&paths, "m", 1, &bad).unwrap_err().to_string();
        assert!(err.contains("E_RBT_WAP_AUDIT"));
        assert_eq!(fs::read(&final_p)?, b"production");
        Ok(())
    }
}

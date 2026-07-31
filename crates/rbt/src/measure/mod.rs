//! Measure packs — thesis proof harness (honest numbers only).
//!
//! Scenarios produce a machine-readable [`MeasureReport`] (wall time, rows, optional RSS).
//! Public “beats Spark” claims require checked-in packs + reports; this module is the pack runner.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::core::project::RbtProjectConfig;
use crate::core::select::SelectMode;
use crate::engine::TransformationEngine;

/// Built-in scenario names.
pub const SCENARIO_SMOKE_PIPELINE: &str = "smoke_pipeline";
pub const SCENARIO_VALIDATE_DX: &str = "validate_dx";
pub const SCENARIO_INCREMENTAL_APPEND: &str = "incremental_append";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasureReport {
    pub scenario: String,
    pub project: String,
    pub package_version: String,
    pub wall_ms: u128,
    pub models_executed: usize,
    pub total_rows: usize,
    pub bronze_sources: usize,
    pub peak_rss_kb: Option<u64>,
    pub notes: Vec<String>,
    pub ok: bool,
    pub error: Option<String>,
}

/// Linux-only VmRSS from /proc/self/status (no new deps).
pub fn read_peak_rss_kb() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let s = std::fs::read_to_string("/proc/self/status").ok()?;
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
                return Some(kb);
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Run a named measure scenario against a project.
pub async fn run_measure_scenario(
    scenario: &str,
    project_dir: &Path,
    output_dir: &Path,
) -> Result<MeasureReport> {
    match scenario {
        SCENARIO_SMOKE_PIPELINE | "pipeline" | "smoke" => {
            measure_pipeline(project_dir, output_dir).await
        }
        SCENARIO_VALIDATE_DX | "validate" | "dx" => measure_validate_dx(project_dir).await,
        SCENARIO_INCREMENTAL_APPEND | "incremental" => {
            measure_incremental_stub(project_dir, output_dir).await
        }
        other => bail!(
            "E_RBT_MEASURE: unknown scenario '{other}'. \
             Built-ins: {SCENARIO_SMOKE_PIPELINE}, {SCENARIO_VALIDATE_DX}, {SCENARIO_INCREMENTAL_APPEND}"
        ),
    }
}

async fn measure_pipeline(project_dir: &Path, output_dir: &Path) -> Result<MeasureReport> {
    let config = RbtProjectConfig::load(project_dir)?;
    let dag = config.build_dag(project_dir, None)?;
    let engine = TransformationEngine::new();
    let rss0 = read_peak_rss_kb();
    let start = Instant::now();
    let summary = engine
        .execute_dag(&dag, project_dir, output_dir)
        .await
        .context("E_RBT_MEASURE: pipeline execute failed")?;
    let wall_ms = start.elapsed().as_millis();
    let rss1 = read_peak_rss_kb();
    Ok(MeasureReport {
        scenario: SCENARIO_SMOKE_PIPELINE.into(),
        project: config.name,
        package_version: crate::VERSION.into(),
        wall_ms,
        models_executed: summary.models_executed,
        total_rows: summary.total_rows_produced,
        bronze_sources: summary.bronze_sources_registered,
        peak_rss_kb: rss1.or(rss0),
        notes: vec![
            "Full DAG materialize (stream default)".into(),
            format!("select=all models={}", summary.models_executed),
        ],
        ok: true,
        error: None,
    })
}

async fn measure_validate_dx(project_dir: &Path) -> Result<MeasureReport> {
    let config = RbtProjectConfig::load(project_dir)?;
    let start = Instant::now();
    let dag = config.build_dag(project_dir, None)?;
    let _tiers = dag.execution_tiers()?;
    let report = dag.validate_bronze_sources_with_roots(
        project_dir,
        crate::core::frontmatter::BronzeCheckMode::Fail,
        &config.roots,
    )?;
    let wall_ms = start.elapsed().as_millis();
    let ok = !report.has_errors();
    Ok(MeasureReport {
        scenario: SCENARIO_VALIDATE_DX.into(),
        project: config.name,
        package_version: crate::VERSION.into(),
        wall_ms,
        models_executed: 0,
        total_rows: 0,
        bronze_sources: 0,
        peak_rss_kb: read_peak_rss_kb(),
        notes: vec![
            format!("models={}", dag.node_map.len()),
            format!("bronze_errors={}", report.error_count()),
            "DX metric: load DAG + bronze check latency".into(),
        ],
        ok,
        error: if ok {
            None
        } else {
            Some(format!("{} bronze errors", report.error_count()))
        },
    })
}

async fn measure_incremental_stub(project_dir: &Path, output_dir: &Path) -> Result<MeasureReport> {
    // Run full pipeline twice; second run exercises stream overwrite (true incremental
    // models use materialization: incremental_append — reported in notes).
    let config = RbtProjectConfig::load(project_dir)?;
    let dag = config.build_dag(project_dir, None)?;
    let engine = TransformationEngine::new();
    let start = Instant::now();
    let s1 = engine
        .execute_dag(&dag, project_dir, output_dir)
        .await
        .context("E_RBT_MEASURE: incremental first pass")?;
    let s2 = engine
        .execute_dag(&dag, project_dir, output_dir)
        .await
        .context("E_RBT_MEASURE: incremental second pass")?;
    let wall_ms = start.elapsed().as_millis();
    Ok(MeasureReport {
        scenario: SCENARIO_INCREMENTAL_APPEND.into(),
        project: config.name,
        package_version: crate::VERSION.into(),
        wall_ms,
        models_executed: s1.models_executed + s2.models_executed,
        total_rows: s1.total_rows_produced + s2.total_rows_produced,
        bronze_sources: s1.bronze_sources_registered,
        peak_rss_kb: read_peak_rss_kb(),
        notes: vec![
            "Two full DAG runs (baseline for overwrite cost)".into(),
            "Models with materialization: incremental_append write part files".into(),
            format!("pass1_rows={} pass2_rows={}", s1.total_rows_produced, s2.total_rows_produced),
        ],
        ok: true,
        error: None,
    })
}

/// Write report JSON next to project or to given path.
pub fn write_measure_report(report: &MeasureReport, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec_pretty(report)?;
    std::fs::write(path, body)
        .with_context(|| format!("E_RBT_MEASURE: write report {}", path.display()))?;
    Ok(())
}

/// Default report path under project.
pub fn default_report_path(project_dir: &Path, scenario: &str) -> PathBuf {
    project_dir
        .join(".rbt")
        .join("measure")
        .join(format!("{scenario}.json"))
}

/// List built-in scenarios for CLI help.
pub fn list_scenarios() -> &'static [&'static str] {
    &[
        SCENARIO_SMOKE_PIPELINE,
        SCENARIO_VALIDATE_DX,
        SCENARIO_INCREMENTAL_APPEND,
    ]
}

// silence unused import in some builds
#[allow(dead_code)]
fn _select_mode() -> SelectMode {
    SelectMode::Execute
}

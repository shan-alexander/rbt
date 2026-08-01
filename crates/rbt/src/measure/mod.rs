//! Measure packs — thesis proof harness (honest numbers only).
//!
//! Scenarios produce a machine-readable [`MeasureReport`] (wall time, rows, optional RSS).
//! Public “beats Spark” claims require checked-in packs + reports; this module is the pack runner.
//!
//! ## P5c scenarios
//!
//! * [`SCENARIO_STREAM_VS_COLLECT`] — same DAG under stream vs collect; compare wall + RSS
//! * [`SCENARIO_WHALE_SYNTHETIC`] — synthetic multi-file bronze (row count via env) + stream materialize
//! * [`SCENARIO_COMPLEX_BRONZE`] — multi-artifact outer-join example with run scope

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::core::frontmatter::BronzeCheckMode;
use crate::core::project::{MaterializeConfig, MaterializeMode, RbtProjectConfig};
use crate::core::run_scope::RunScope;
use crate::engine::TransformationEngine;

/// Built-in scenario names.
pub const SCENARIO_SMOKE_PIPELINE: &str = "smoke_pipeline";
pub const SCENARIO_VALIDATE_DX: &str = "validate_dx";
pub const SCENARIO_INCREMENTAL_APPEND: &str = "incremental_append";
pub const SCENARIO_STREAM_VS_COLLECT: &str = "stream_vs_collect";
pub const SCENARIO_WHALE_SYNTHETIC: &str = "whale_synthetic";
pub const SCENARIO_COMPLEX_BRONZE: &str = "complex_bronze";

/// Default synthetic row count for whale scenario (override with `RBT_MEASURE_ROWS`).
pub const DEFAULT_WHALE_ROWS: usize = 100_000;
/// Default number of bronze part files for whale scenario (`RBT_MEASURE_PARTS`).
pub const DEFAULT_WHALE_PARTS: usize = 20;

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
    /// Present for stream-vs-collect comparisons (P5c).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode_compare: Option<ModeCompare>,
    /// Synthetic generator settings when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synthetic_rows: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synthetic_parts: Option<usize>,
}

/// Side-by-side stream vs collect timings / RSS (Linux VmRSS when available).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModeCompare {
    pub stream_wall_ms: u128,
    pub collect_wall_ms: u128,
    pub stream_rss_kb: Option<u64>,
    pub collect_rss_kb: Option<u64>,
    pub rows: usize,
    pub models: usize,
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

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
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
        SCENARIO_STREAM_VS_COLLECT | "stream_collect" | "stream-vs-collect" => {
            measure_stream_vs_collect(project_dir, output_dir).await
        }
        SCENARIO_WHALE_SYNTHETIC | "whale" | "synthetic" => {
            measure_whale_synthetic(output_dir).await
        }
        SCENARIO_COMPLEX_BRONZE | "complex" | "multi_artifact" => {
            measure_complex_bronze(project_dir, output_dir).await
        }
        other => bail!(
            "E_RBT_MEASURE: unknown scenario '{other}'. Built-ins: {}",
            list_scenarios().join(", ")
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
        mode_compare: None,
        synthetic_rows: None,
        synthetic_parts: None,
    })
}

async fn measure_validate_dx(project_dir: &Path) -> Result<MeasureReport> {
    let config = RbtProjectConfig::load(project_dir)?;
    let start = Instant::now();
    let dag = config.build_dag(project_dir, None)?;
    let _tiers = dag.execution_tiers()?;
    let report = dag.validate_bronze_sources_with_roots(
        project_dir,
        BronzeCheckMode::Fail,
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
        mode_compare: None,
        synthetic_rows: None,
        synthetic_parts: None,
    })
}

async fn measure_incremental_stub(project_dir: &Path, output_dir: &Path) -> Result<MeasureReport> {
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
            format!(
                "pass1_rows={} pass2_rows={}",
                s1.total_rows_produced, s2.total_rows_produced
            ),
        ],
        ok: true,
        error: None,
        mode_compare: None,
        synthetic_rows: None,
        synthetic_parts: None,
    })
}

/// Run the project DAG twice: stream (default) then collect; report both.
async fn measure_stream_vs_collect(
    project_dir: &Path,
    output_dir: &Path,
) -> Result<MeasureReport> {
    let config = RbtProjectConfig::load(project_dir)?;
    let dag = config.build_dag(project_dir, None)?;

    let stream_out = output_dir.join("measure_stream");
    let collect_out = output_dir.join("measure_collect");
    let _ = fs::remove_dir_all(&stream_out);
    let _ = fs::remove_dir_all(&collect_out);

    let mut stream_cfg = config.clone();
    stream_cfg.materialize.mode = MaterializeMode::Stream;
    // Fresh engine per mode so DF caches do not muddy RSS.
    let engine_s = TransformationEngine::new();
    let rss_before_s = read_peak_rss_kb();
    let t0 = Instant::now();
    let sum_s = engine_s
        .execute_dag_with_config(&dag, project_dir, &stream_out, &stream_cfg)
        .await
        .context("E_RBT_MEASURE: stream pass failed")?;
    let stream_wall_ms = t0.elapsed().as_millis();
    let stream_rss = read_peak_rss_kb().or(rss_before_s);

    let mut collect_cfg = config.clone();
    collect_cfg.materialize.mode = MaterializeMode::Collect;
    let engine_c = TransformationEngine::new();
    let rss_before_c = read_peak_rss_kb();
    let t1 = Instant::now();
    let sum_c = engine_c
        .execute_dag_with_config(&dag, project_dir, &collect_out, &collect_cfg)
        .await
        .context("E_RBT_MEASURE: collect pass failed")?;
    let collect_wall_ms = t1.elapsed().as_millis();
    let collect_rss = read_peak_rss_kb().or(rss_before_c);

    let rows = sum_s.total_rows_produced.max(sum_c.total_rows_produced);
    let compare = ModeCompare {
        stream_wall_ms,
        collect_wall_ms,
        stream_rss_kb: stream_rss,
        collect_rss_kb: collect_rss,
        rows,
        models: sum_s.models_executed,
    };

    let mut notes = vec![
        "Same DAG: materialize.mode=stream then collect (fresh SessionContext each)".into(),
        format!(
            "stream_wall_ms={} collect_wall_ms={} ratio_collect_over_stream={:.3}",
            stream_wall_ms,
            collect_wall_ms,
            if stream_wall_ms == 0 {
                0.0
            } else {
                collect_wall_ms as f64 / stream_wall_ms as f64
            }
        ),
    ];
    if let (Some(sr), Some(cr)) = (stream_rss, collect_rss) {
        notes.push(format!(
            "stream_rss_kb={sr} collect_rss_kb={cr} delta_kb={}",
            cr as i64 - sr as i64
        ));
        notes.push(
            "RSS is process VmRSS after each pass (not allocator peak); treat as directional".into(),
        );
    } else {
        notes.push("RSS unavailable on this platform (Linux VmRSS only)".into());
    }

    Ok(MeasureReport {
        scenario: SCENARIO_STREAM_VS_COLLECT.into(),
        project: config.name,
        package_version: crate::VERSION.into(),
        wall_ms: stream_wall_ms + collect_wall_ms,
        models_executed: sum_s.models_executed + sum_c.models_executed,
        total_rows: sum_s.total_rows_produced + sum_c.total_rows_produced,
        bronze_sources: sum_s.bronze_sources_registered,
        peak_rss_kb: collect_rss.or(stream_rss),
        notes,
        ok: sum_s.total_rows_produced == sum_c.total_rows_produced,
        error: if sum_s.total_rows_produced != sum_c.total_rows_produced {
            Some(format!(
                "row mismatch stream={} collect={}",
                sum_s.total_rows_produced, sum_c.total_rows_produced
            ))
        } else {
            None
        },
        mode_compare: Some(compare),
        synthetic_rows: None,
        synthetic_parts: None,
    })
}

/// Synthetic multi-part bronze + single staging→silver model at configurable scale.
async fn measure_whale_synthetic(output_dir: &Path) -> Result<MeasureReport> {
    let rows = env_usize("RBT_MEASURE_ROWS", DEFAULT_WHALE_ROWS);
    let parts = env_usize("RBT_MEASURE_PARTS", DEFAULT_WHALE_PARTS).max(1);
    let work = output_dir.join("whale_synth_project");
    let _ = fs::remove_dir_all(&work);
    write_whale_project(&work, rows, parts)?;

    let config = RbtProjectConfig::load(&work)?;
    let dag = config.build_dag(&work, None)?;
    let lake_out = work.join("lake").join("silver");

    // Stream pass (product default)
    let mut stream_cfg = config.clone();
    stream_cfg.materialize = MaterializeConfig {
        mode: MaterializeMode::Stream,
        ..config.materialize.clone()
    };
    let engine_s = TransformationEngine::new();
    let rss0 = read_peak_rss_kb();
    let t0 = Instant::now();
    let sum_s = engine_s
        .execute_dag_with_config(&dag, &work, &lake_out.join("stream_run"), &stream_cfg)
        .await
        .context("E_RBT_MEASURE: whale stream pass")?;
    let stream_wall_ms = t0.elapsed().as_millis();
    let stream_rss = read_peak_rss_kb().or(rss0);

    // Collect pass (memory pressure comparison)
    let mut collect_cfg = config.clone();
    collect_cfg.materialize = MaterializeConfig {
        mode: MaterializeMode::Collect,
        ..config.materialize.clone()
    };
    let engine_c = TransformationEngine::new();
    let rss1 = read_peak_rss_kb();
    let t1 = Instant::now();
    let sum_c = engine_c
        .execute_dag_with_config(&dag, &work, &lake_out.join("collect_run"), &collect_cfg)
        .await
        .context("E_RBT_MEASURE: whale collect pass")?;
    let collect_wall_ms = t1.elapsed().as_millis();
    let collect_rss = read_peak_rss_kb().or(rss1);

    let compare = ModeCompare {
        stream_wall_ms,
        collect_wall_ms,
        stream_rss_kb: stream_rss,
        collect_rss_kb: collect_rss,
        rows: sum_s.total_rows_produced,
        models: sum_s.models_executed,
    };

    let ok = sum_s.total_rows_produced == rows && sum_c.total_rows_produced == rows;
    let mut notes = vec![
        format!("Synthetic JSONL bronze: {rows} rows across {parts} part files"),
        "Env: RBT_MEASURE_ROWS, RBT_MEASURE_PARTS".into(),
        format!(
            "stream_wall_ms={stream_wall_ms} collect_wall_ms={collect_wall_ms} stream_rss_kb={stream_rss:?} collect_rss_kb={collect_rss:?}"
        ),
        "Whale-ish default is 100k rows / 20 parts — raise RBT_MEASURE_ROWS for larger packs".into(),
    ];
    if !ok {
        notes.push(format!(
            "expected_rows={rows} stream_rows={} collect_rows={}",
            sum_s.total_rows_produced, sum_c.total_rows_produced
        ));
    }

    Ok(MeasureReport {
        scenario: SCENARIO_WHALE_SYNTHETIC.into(),
        project: config.name,
        package_version: crate::VERSION.into(),
        wall_ms: stream_wall_ms + collect_wall_ms,
        models_executed: sum_s.models_executed + sum_c.models_executed,
        total_rows: sum_s.total_rows_produced + sum_c.total_rows_produced,
        bronze_sources: sum_s.bronze_sources_registered,
        peak_rss_kb: collect_rss.or(stream_rss),
        notes,
        ok,
        error: if ok {
            None
        } else {
            Some("row count did not match synthetic generator".into())
        },
        mode_compare: Some(compare),
        synthetic_rows: Some(rows),
        synthetic_parts: Some(parts),
    })
}

fn write_whale_project(root: &Path, total_rows: usize, parts: usize) -> Result<()> {
    let bronze = root.join("lake/bronze/events");
    fs::create_dir_all(&bronze)?;
    fs::create_dir_all(root.join("models/staging"))?;
    fs::create_dir_all(root.join("models/transforms"))?;
    fs::create_dir_all(root.join("models/marts"))?;

    let per = total_rows.div_ceil(parts);
    let mut written = 0usize;
    for p in 0..parts {
        if written >= total_rows {
            break;
        }
        let n = (total_rows - written).min(per);
        let path = bronze.join(format!("part-{p:04}.jsonl"));
        let mut f = fs::File::create(&path)?;
        for i in 0..n {
            let id = written + i;
            writeln!(
                f,
                "{{\"id\":{id},\"domain\":\"d{}\",\"amount\":{}}}",
                id % 97,
                (id % 1000) as f64 * 0.01
            )?;
        }
        written += n;
    }

    fs::write(
        root.join("rbt_project.yml"),
        r#"name: whale_synthetic
version: "0.1.0"
contract_version: "measure-whale-v1"
models_dir: models
target_path: lake/gold
roots:
  lake: lake
layers:
  staging:
    path: models/staging
    target_path: lake/silver
    default_format: parquet
  transforms:
    path: models/transforms
    target_path: lake/silver
    default_format: parquet
  marts:
    path: models/marts
    target_path: lake/gold
    default_format: parquet
"#,
    )?;

    // Single model keeps total_rows == synthetic row count (clean measure identity).
    fs::write(
        root.join("models/staging/stg_events.sql"),
        r#"---
description: Full-refresh silver mirror of multi-part bronze events (whale measure).
source_format: jsonl
scan_path: $lake/bronze/events
path_glob: "*.jsonl"
stage_mode: full_refresh
columns:
  id: { dtype: int64 }
  domain: { dtype: utf8 }
  amount: { dtype: float64 }
tests:
  not_null: [id]
---
SELECT id, domain, amount FROM {{ source('bronze', 'events') }}
"#,
    )?;

    Ok(())
}

/// Multi-artifact complex bronze example (when project_dir points at it).
async fn measure_complex_bronze(project_dir: &Path, output_dir: &Path) -> Result<MeasureReport> {
    // Prefer explicit path; if caller pointed at workspace root, use example.
    let project = if project_dir.join("models/staging/stg_plan.sql").exists() {
        project_dir.to_path_buf()
    } else {
        let candidate = project_dir.join("examples/complex_bronze_landing");
        if candidate.join("rbt_project.yml").exists() {
            candidate
        } else {
            // Try CWD-relative from crate workspace
            let alt = PathBuf::from("examples/complex_bronze_landing");
            if alt.join("rbt_project.yml").exists() {
                alt
            } else {
                bail!(
                    "E_RBT_MEASURE: complex_bronze needs examples/complex_bronze_landing \
                     (got project_dir={})",
                    project_dir.display()
                );
            }
        }
    };

    let config = RbtProjectConfig::load(&project)?;
    let dag = config.build_dag(&project, None)?;
    let mut scope = RunScope::new()
        .with_var("domain", "acme.com")
        .with_var("report_date", "2026-07-29")
        .with_var("run_id", "r1");
    scope.write_receipt = true;
    scope.skip_if_fingerprint_match = false;

    let engine = TransformationEngine::new();
    let rss0 = read_peak_rss_kb();
    let start = Instant::now();
    let summary = engine
        .execute_dag_with_scope(&dag, &project, output_dir, &config, &scope)
        .await
        .context("E_RBT_MEASURE: complex_bronze execute failed")?;
    let wall_ms = start.elapsed().as_millis();

    // Expect outer join to produce 3 unit rows on the sample lake.
    let ok = summary.total_rows_produced >= 3;
    Ok(MeasureReport {
        scenario: SCENARIO_COMPLEX_BRONZE.into(),
        project: config.name,
        package_version: crate::VERSION.into(),
        wall_ms,
        models_executed: summary.models_executed,
        total_rows: summary.total_rows_produced,
        bronze_sources: summary.bronze_sources_registered,
        peak_rss_kb: read_peak_rss_kb().or(rss0),
        notes: vec![
            "Multi-artifact bronze + on_missing empty + outer unit status".into(),
            format!("fingerprint={:?}", summary.bronze_fingerprint),
            format!("receipt={:?}", summary.receipt_path),
            "Run vars: domain=acme.com report_date=2026-07-29 run_id=r1".into(),
        ],
        ok,
        error: if ok {
            None
        } else {
            Some("expected at least 3 rows from sample outer join".into())
        },
        mode_compare: None,
        synthetic_rows: None,
        synthetic_parts: None,
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
pub fn list_scenarios() -> Vec<&'static str> {
    vec![
        SCENARIO_SMOKE_PIPELINE,
        SCENARIO_VALIDATE_DX,
        SCENARIO_INCREMENTAL_APPEND,
        SCENARIO_STREAM_VS_COLLECT,
        SCENARIO_WHALE_SYNTHETIC,
        SCENARIO_COMPLEX_BRONZE,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn whale_synthetic_small_ok() {
        std::env::set_var("RBT_MEASURE_ROWS", "500");
        std::env::set_var("RBT_MEASURE_PARTS", "4");
        let dir = tempfile::tempdir().unwrap();
        let report = measure_whale_synthetic(dir.path()).await.unwrap();
        assert!(report.ok, "{:?}", report.error);
        assert_eq!(report.synthetic_rows, Some(500));
        assert!(report.mode_compare.is_some());
        assert_eq!(report.mode_compare.as_ref().unwrap().rows, 500);
        std::env::remove_var("RBT_MEASURE_ROWS");
        std::env::remove_var("RBT_MEASURE_PARTS");
    }

    #[tokio::test]
    async fn stream_vs_collect_on_smoke() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/smoke_fixture");
        if !root.exists() {
            return;
        }
        let out = tempfile::tempdir().unwrap();
        let report = measure_stream_vs_collect(&root, out.path()).await.unwrap();
        assert!(report.ok, "{:?}", report.error);
        assert!(report.mode_compare.is_some());
    }
}

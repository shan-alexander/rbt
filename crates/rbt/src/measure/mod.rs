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
/// Synthetic N-key Type-1 upsert (RBT-A7); env `RBT_MEASURE_UPSERT_KEYS`.
pub const SCENARIO_ENTITY_REGISTRY_UPSERT: &str = "entity_registry_upsert";
/// RBT-C Phase 0 baseline: independent tier models run **serially** (concurrent path not yet).
pub const SCENARIO_CONCURRENT_TIER_VS_SERIAL: &str = "concurrent_tier_vs_serial";
/// RBT-C: multi-value IN-filter vs partition fan-out (Phase 0/1).
/// Set `RBT_MEASURE_FANOUT=1` to enable partition strategy fan-out path.
pub const SCENARIO_MULTI_VALUE_IN_VS_FANOUT: &str = "multi_value_in_vs_fanout";

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
        SCENARIO_ENTITY_REGISTRY_UPSERT | "keyed_upsert" | "upsert" => {
            measure_entity_registry_upsert(output_dir).await
        }
        SCENARIO_CONCURRENT_TIER_VS_SERIAL | "concurrent_tier" | "tier_serial" => {
            measure_concurrent_tier_vs_serial(output_dir).await
        }
        SCENARIO_MULTI_VALUE_IN_VS_FANOUT | "multi_value_in" | "mv_in_vs_fanout" => {
            measure_multi_value_in_vs_fanout(output_dir).await
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

    // Prefer lake/lz/LATEST_RUN.json from fetch_bronze.py; fallback vars for empty CI.
    let mut domain = "ai-semicon-agritech".to_string();
    let mut report_date = "2026-08-01".to_string();
    let mut run_id = "run20260802T023408Z".to_string();
    let pointer = project.join("lake/lz/LATEST_RUN.json");
    if pointer.is_file() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&std::fs::read_to_string(&pointer)?)
        {
            if let Some(s) = v.get("domain").and_then(|x| x.as_str()) {
                domain = s.to_string();
            }
            if let Some(s) = v.get("report_date").and_then(|x| x.as_str()) {
                report_date = s.to_string();
            }
            if let Some(s) = v.get("run_id").and_then(|x| x.as_str()) {
                run_id = s.to_string();
            }
        }
    }

    let mut scope = RunScope::new()
        .with_var("domain", domain.clone())
        .with_var("report_date", report_date.clone())
        .with_var("run_id", run_id.clone());
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

    // Research mini-lake: expect multi-model star with works + dims + fact.
    let ok = summary.total_rows_produced >= 10 && summary.models_executed >= 5;
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
            "Research papers mini-lake: PubMed/Crossref/EuropePMC/arXiv → silver stg → gold tf/marts".into(),
            format!("fingerprint={:?}", summary.bronze_fingerprint),
            format!("receipt={:?}", summary.receipt_path),
            format!("Run vars: domain={domain} report_date={report_date} run_id={run_id}"),
        ],
        ok,
        error: if ok {
            None
        } else {
            Some("expected multi-model research lake materialize (works+dims+fact)".into())
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
        SCENARIO_ENTITY_REGISTRY_UPSERT,
        SCENARIO_CONCURRENT_TIER_VS_SERIAL,
        SCENARIO_MULTI_VALUE_IN_VS_FANOUT,
    ]
}

/// Phase 0 baseline for RBT-C L1: two independent staging models in one tier (serial exec).
///
/// Notes record that concurrent tier execution is **not** implemented yet; this is the
/// serial wall_ms / RSS baseline for future `max_in_flight_models > 1` comparison.
async fn measure_concurrent_tier_vs_serial(output_dir: &Path) -> Result<MeasureReport> {
    let n = env_usize("RBT_MEASURE_TIER_ROWS", 20_000);
    let root = output_dir.join("concurrent_tier_vs_serial_measure");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("lake/bronze"))?;
    fs::create_dir_all(root.join("models/staging"))?;
    fs::create_dir_all(root.join("lake/silver"))?;

    // Two independent bronze files → two independent stg models (same tier).
    for (name, prefix) in [("events_a", "a"), ("events_b", "b")] {
        let path = root.join(format!("lake/bronze/{name}.jsonl"));
        let mut f = fs::File::create(&path)?;
        for i in 0..n {
            writeln!(f, r#"{{"id":{i},"k":"{prefix}","v":{}}}"#, i % 7)?;
        }
        fs::write(
            root.join(format!("models/staging/stg_{name}.sql")),
            format!(
                r#"---
description: Independent staging model for tier concurrency baseline
source_format: jsonl
scan_path: lake/bronze/{name}.jsonl
materialization: table
---
SELECT * FROM {{{{ source('bronze', '{name}') }}}}
"#
            ),
        )?;
    }

    fs::write(
        root.join("rbt_project.yml"),
        r#"
name: concurrent_tier_vs_serial_measure
version: "1"
models_dir: models
target_path: lake/silver
layers:
  staging:
    path: models/staging
    target_path: lake/silver
    default_format: parquet
materialize:
  mode: stream
  ref_strategy: parquet
"#,
    )?;

    let config = RbtProjectConfig::load(&root)?;
    let dag = config.build_dag(&root, None)?;
    let tiers = dag.execution_tiers()?;
    let tier0_len = tiers.first().map(|t| t.len()).unwrap_or(0);
    let engine = TransformationEngine::new();
    let rss0 = read_peak_rss_kb();
    let start = Instant::now();
    let summary = engine
        .execute_dag(&dag, &root, root.join("lake/silver"))
        .await
        .context("E_RBT_MEASURE: concurrent_tier_vs_serial execute failed")?;
    let wall_ms = start.elapsed().as_millis();
    let rss1 = read_peak_rss_kb();

    Ok(MeasureReport {
        scenario: SCENARIO_CONCURRENT_TIER_VS_SERIAL.into(),
        project: config.name,
        package_version: crate::VERSION.into(),
        wall_ms,
        models_executed: summary.models_executed,
        total_rows: summary.total_rows_produced,
        bronze_sources: summary.bronze_sources_registered,
        peak_rss_kb: rss1.or(rss0),
        notes: vec![
            format!(
                "SERIAL baseline only: tier0 has {tier0_len} independent model(s); engine still \
                 executes serially (RBT-C Phase 0). Concurrent path lands in Phase 1 (L1)."
            ),
            format!("rows_per_model≈{n} (env RBT_MEASURE_TIER_ROWS)"),
            "Compare later: same pack with execution.concurrency.max_workers>1".into(),
        ],
        ok: summary.models_executed >= 2 && tier0_len >= 2,
        error: if summary.models_executed >= 2 && tier0_len >= 2 {
            None
        } else {
            Some(format!(
                "expected ≥2 models in tier0 (got models={}, tier0={tier0_len})",
                summary.models_executed
            ))
        },
        mode_compare: None,
        synthetic_rows: Some(n.saturating_mul(2)),
        synthetic_parts: None,
    })
}

/// Phase 0 baseline for RBT-C L2: multi-value partition scope as one IN-filter plan.
///
/// Fan-out into per-value WorkUnits is **not** implemented yet; this measures the current
/// serial multi-value IN path for later comparison.
async fn measure_multi_value_in_vs_fanout(output_dir: &Path) -> Result<MeasureReport> {
    let entities = env_usize("RBT_MEASURE_MV_ENTITIES", 8);
    let rows_per = env_usize("RBT_MEASURE_MV_ROWS", 2_000);
    let root = output_dir.join("multi_value_in_vs_fanout_measure");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("lake/bronze"))?;
    fs::create_dir_all(root.join("models/staging"))?;
    fs::create_dir_all(root.join("lake/silver"))?;

    // Hive-ish entity=X dirs under bronze.
    for e in 0..entities {
        let ent = format!("e{e}");
        let dir = root.join(format!("lake/bronze/entity={ent}"));
        fs::create_dir_all(&dir)?;
        let path = dir.join("data.jsonl");
        let mut f = fs::File::create(&path)?;
        for i in 0..rows_per {
            writeln!(
                f,
                r#"{{"entity":"{ent}","id":{i},"v":{}}}"#,
                i % 5
            )?;
        }
    }

    let fanout = std::env::var("RBT_MEASURE_FANOUT")
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let mat = if fanout {
        "scoped_replace"
    } else {
        "table"
    };

    fs::write(
        root.join("models/staging/stg_events.sql"),
        format!(
            r#"---
description: Multi-value IN vs fan-out measure
source_format: jsonl
scan_path: lake/bronze
partition_by: [entity]
part_key: [entity]
materialization: {mat}
---
SELECT * FROM {{{{ source('bronze', 'events') }}}}
"#
        ),
    )?;

    let exec_yml = if fanout {
        r#"
execution:
  concurrency:
    enabled: true
    max_workers: 4
    strategy: partition
    multi_value_fanout_threshold: 2
"#
    } else {
        ""
    };

    fs::write(
        root.join("rbt_project.yml"),
        format!(
            r#"
name: multi_value_in_vs_fanout_measure
version: "1"
models_dir: models
target_path: lake/silver
layers:
  staging:
    path: models/staging
    target_path: lake/silver
    default_format: parquet
materialize:
  mode: stream
  ref_strategy: parquet
{exec_yml}
"#
        ),
    )?;

    let mut entity_list = Vec::new();
    for e in 0..entities {
        entity_list.push(format!("e{e}"));
    }

    let config = RbtProjectConfig::load(&root)?;
    let dag = config.build_dag(&root, None)?;
    let scope = RunScope::new().with_var_multi("entity", entity_list)?;
    let engine = TransformationEngine::new();
    let rss0 = read_peak_rss_kb();
    let start = Instant::now();
    let summary = engine
        .execute_dag_with_scope(&dag, &root, root.join("lake/silver"), &config, &scope)
        .await
        .context("E_RBT_MEASURE: multi_value_in_vs_fanout execute failed")?;
    let wall_ms = start.elapsed().as_millis();
    let rss1 = read_peak_rss_kb();
    let expected_rows = entities.saturating_mul(rows_per);

    Ok(MeasureReport {
        scenario: SCENARIO_MULTI_VALUE_IN_VS_FANOUT.into(),
        project: config.name,
        package_version: crate::VERSION.into(),
        wall_ms,
        models_executed: summary.models_executed,
        total_rows: summary.total_rows_produced,
        bronze_sources: summary.bronze_sources_registered,
        peak_rss_kb: rss1.or(rss0),
        notes: vec![
            if fanout {
                format!(
                    "FANOUT path: multi-value entity ({entities}) → WorkUnits + scoped_replace parts \
                     (RBT_MEASURE_FANOUT=1, strategy=partition, workers=4)"
                )
            } else {
                format!(
                    "IN-filter baseline: multi-value entity scope ({entities} values) is one \
                     filtered plan + one table write. Set RBT_MEASURE_FANOUT=1 for partition fan-out."
                )
            },
            format!("rows_per_entity={rows_per} (RBT_MEASURE_MV_ROWS); entities via RBT_MEASURE_MV_ENTITIES"),
            format!("models_executed={} (fan-out counts units)", summary.models_executed),
        ],
        ok: summary.total_rows_produced >= expected_rows.saturating_mul(9) / 10,
        error: if summary.total_rows_produced >= expected_rows.saturating_mul(9) / 10 {
            None
        } else {
            Some(format!(
                "expected ~{expected_rows} rows, got {}",
                summary.total_rows_produced
            ))
        },
        mode_compare: None,
        synthetic_rows: Some(expected_rows),
        synthetic_parts: Some(entities),
    })
}

/// Synthetic entity registry: first pass inserts N keys; second pass touch-only.
async fn measure_entity_registry_upsert(output_dir: &Path) -> Result<MeasureReport> {
    use crate::engine::TransformationEngine;
    use std::io::Write;

    let n = env_usize("RBT_MEASURE_UPSERT_KEYS", 5_000);
    let root = output_dir.join("entity_registry_upsert_measure");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("lake/bronze"))?;
    fs::create_dir_all(root.join("models/marts"))?;
    fs::create_dir_all(root.join("lake/gold"))?;

    let bronze_path = root.join("lake/bronze/sightings.jsonl");
    {
        let mut f = fs::File::create(&bronze_path)?;
        for i in 0..n {
            writeln!(
                f,
                r#"{{"entity_id":"e{i}","status":"ok","tier":"A","seen_at":"d1"}}"#
            )?;
        }
    }

    fs::write(
        root.join("rbt_project.yml"),
        r#"
name: entity_registry_upsert_measure
version: "1"
contract_version: "a7-measure"
models_dir: models
target_path: lake/gold
roots:
  lake: lake
layers:
  marts:
    path: models/marts
    target_path: lake/gold
    default_format: parquet
"#,
    )?;

    fs::write(
        root.join("models/marts/dim_entity.sql"),
        r#"---
materialization: keyed_upsert
unique_key: [entity_id]
touch_columns: [last_seen_at]
compare_columns: [status, tier]
source_format: jsonl
scan_path: $lake/bronze/sightings.jsonl
source_name: bronze
source_table: sightings
---
SELECT entity_id, status, tier, seen_at AS last_seen_at
FROM {{ source('bronze', 'sightings') }}
"#,
    )?;

    let config = RbtProjectConfig::load(&root)?;
    let dag = config.build_dag(&root, None)?;
    let engine = TransformationEngine::new();
    let out = root.join("out");
    let mut scope = RunScope::new();
    scope.write_receipt = true;

    let rss0 = read_peak_rss_kb();
    let start = Instant::now();
    let s1 = engine
        .execute_dag_with_scope(&dag, &root, &out, &config, &scope)
        .await
        .context("E_RBT_MEASURE: upsert first pass (insert)")?;

    {
        let mut f = fs::File::create(&bronze_path)?;
        for i in 0..n {
            writeln!(
                f,
                r#"{{"entity_id":"e{i}","status":"ok","tier":"A","seen_at":"d2"}}"#
            )?;
        }
    }
    let s2 = engine
        .execute_dag_with_scope(&dag, &root, &out, &config, &scope)
        .await
        .context("E_RBT_MEASURE: upsert second pass (touch)")?;
    let wall_ms = start.elapsed().as_millis();
    let rss1 = read_peak_rss_kb();

    let m1 = s1.model_results.iter().find(|m| m.name == "dim_entity");
    let m2 = s2.model_results.iter().find(|m| m.name == "dim_entity");

    let mut notes = vec![
        format!("synthetic keys N={n}"),
        "pass1: insert all; pass2: touch-only (same attrs)".into(),
        format!(
            "pass1 rows_inserted={:?} total={}",
            m1.and_then(|m| m.rows_inserted),
            s1.total_rows_produced
        ),
        format!(
            "pass2 rows_touched={:?} rows_updated={:?} total={}",
            m2.and_then(|m| m.rows_touched),
            m2.and_then(|m| m.rows_updated),
            s2.total_rows_produced
        ),
    ];

    let ok = m1.and_then(|m| m.rows_inserted).unwrap_or(0) == n
        && m2.and_then(|m| m.rows_touched).unwrap_or(0) == n
        && m2.and_then(|m| m.rows_updated).unwrap_or(1) == 0;

    if !ok {
        notes.push("expected pass1 all inserts, pass2 all touches".into());
    }

    Ok(MeasureReport {
        scenario: SCENARIO_ENTITY_REGISTRY_UPSERT.into(),
        project: config.name,
        package_version: crate::VERSION.into(),
        wall_ms,
        models_executed: s1.models_executed + s2.models_executed,
        total_rows: s2.total_rows_produced,
        bronze_sources: s1.bronze_sources_registered,
        peak_rss_kb: rss1.or(rss0),
        notes,
        ok,
        error: if ok {
            None
        } else {
            Some("upsert measure counters mismatch".into())
        },
        mode_compare: None,
        synthetic_rows: Some(n),
        synthetic_parts: None,
    })
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

    #[tokio::test]
    async fn entity_registry_upsert_small_ok() {
        std::env::set_var("RBT_MEASURE_UPSERT_KEYS", "50");
        let dir = tempfile::tempdir().unwrap();
        let report = measure_entity_registry_upsert(dir.path()).await.unwrap();
        assert!(report.ok, "{:?} notes={:?}", report.error, report.notes);
        assert_eq!(report.synthetic_rows, Some(50));
        std::env::remove_var("RBT_MEASURE_UPSERT_KEYS");
    }
}

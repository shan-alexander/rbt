//! Fair full-DAG wall-clock comparison (stg → tf → obt).
//!
//! | Run | Bronze | Register | TF | Lake out |
//! |-----|--------|----------|-----|----------|
//! | **Arrow+spill** | IPC hive | **force re-spill** | serial mega | `lake/compare_arrow_output/` |
//! | **Parquet** | parquet hive | DF listing (no spill) | serial mega | `lake/compare_parquet_output/` |
//! | **Parquet+parallel** | parquet hive | DF listing | RBT-C L2 WorkUnits | `lake/compare_parquet_parallel_output/` |
//!
//! Same multi-value symbol set (all `symbol=*/timeframe=1m`) so row scope matches.

use crate::models::{
    ObtStocks1m, StgOhlcv1m, StgOhlcvBronzeparquet1m, TfIndicators1m, OBT_STOCKS_1M_NAME,
    STG_OHLCV_1M_NAME, TF_INDICATORS_NAME,
};
use anyhow::{bail, Context, Result};
use rbt::{
    DagBuilder, Materialization, ModelLayer, ModelSpec, RbtEngineBuilder, RbtProjectConfig,
    RunScope, RustModel,
};
use std::path::Path;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct CompareRow {
    pub label: &'static str,
    pub wall: Duration,
    pub rows: usize,
    pub models: usize,
    pub stg_rows_hint: Option<usize>,
    pub notes: String,
}

/// Run the three comparable full DAGs; ensure parquet bronze exists; print + write FINDINGS.
pub async fn run_landing_compare(project: &Path, jobs: usize) -> Result<Vec<CompareRow>> {
    ensure_parquet_bronze(project, jobs)?;

    let syms = discover_1m_symbols(project)?;
    let n = syms.len();
    println!(
        "========== LANDING COMPARE (full DAG stg→tf→obt) =========="
    );
    println!("symbols={n} (all symbol=*/timeframe=1m with arrow); jobs={jobs} for parallel path");
    println!();

    let mut rows = Vec::new();

    // --- 1) Arrow IPC + forced spill, serial mega ---
    {
        let out = "lake/compare_arrow_output";
        wipe(project, out)?;
        // Force re-decode: remove spill cache so first register always spills.
        let _ = std::fs::remove_dir_all(project.join(".rbt/bronze_spill"));
        println!("--- Arrow IPC + force spill + serial mega ---");
        let r = run_full_dag(
            project,
            out,
            BronzeKind::ArrowIpc,
            /*partition_tf*/ false,
            jobs,
            &syms,
            /*force_bronze_register*/ true,
        )
        .await?;
        println!(
            "[{}] wall_secs={:.3} total_rows={} models={}",
            r.label,
            r.wall.as_secs_f64(),
            r.rows,
            r.models
        );
        rows.push(r);
    }

    // --- 2) Parquet bronze + serial mega ---
    {
        let out = "lake/compare_parquet_output";
        wipe(project, out)?;
        println!("--- Parquet bronze (listing) + serial mega ---");
        let r = run_full_dag(
            project,
            out,
            BronzeKind::ParquetHive,
            false,
            jobs,
            &syms,
            false,
        )
        .await?;
        println!(
            "[{}] wall_secs={:.3} total_rows={} models={}",
            r.label,
            r.wall.as_secs_f64(),
            r.rows,
            r.models
        );
        rows.push(r);
    }

    // --- 3) Parquet bronze + RBT-C parallel ---
    {
        let out = "lake/compare_parquet_parallel_output";
        wipe(project, out)?;
        println!("--- Parquet bronze (listing) + RBT-C partition jobs={jobs} ---");
        let r = run_full_dag(
            project,
            out,
            BronzeKind::ParquetHive,
            true,
            jobs,
            &syms,
            false,
        )
        .await?;
        println!(
            "[{}] wall_secs={:.3} total_rows={} models={}",
            r.label,
            r.wall.as_secs_f64(),
            r.rows,
            r.models
        );
        rows.push(r);
    }

    print_table(&rows);
    write_findings(project, &rows, n, jobs)?;
    Ok(rows)
}

#[derive(Clone, Copy)]
enum BronzeKind {
    ArrowIpc,
    ParquetHive,
}

async fn run_full_dag(
    project: &Path,
    out_root: &str,
    bronze: BronzeKind,
    partition_tf: bool,
    jobs: usize,
    syms: &[String],
    force_bronze_register: bool,
) -> Result<CompareRow> {
    let mut config = RbtProjectConfig::load(project).unwrap_or_default();
    config.scan = config.scan.with_env_overrides();
    if force_bronze_register {
        config.scan.reuse_register = false;
    }

    if partition_tf {
        config.execution.concurrency.enabled = true;
        config.execution.concurrency.strategy = rbt::ExecutionStrategy::Partition;
        config.execution.concurrency.multi_value_fanout_threshold = 2;
        config.execution.concurrency.dirty_part_skip = false;
        config.execution.concurrency.max_workers = jobs.max(1);
        if jobs > 1 {
            config.execution.concurrency.apply_jobs(jobs);
        }
    } else {
        config.execution.concurrency.enabled = false;
        config.execution.concurrency.strategy = rbt::ExecutionStrategy::Serial;
        config.execution.concurrency.max_workers = 1;
    }

    let silver_stg = project
        .join(out_root)
        .join("silver/stage/stg_ohlcv_1m.parquet")
        .to_string_lossy()
        .into_owned();
    let silver_tf = project
        .join(out_root)
        .join("silver/tf/tf_indicators_1m.parquet")
        .to_string_lossy()
        .into_owned();
    let gold_obt = project
        .join(out_root)
        .join("gold/obt_stocks_1m.parquet")
        .to_string_lossy()
        .into_owned();

    let stg_fm = match bronze {
        BronzeKind::ArrowIpc => {
            crate::models::silver::staging::stg_ohlcv_1m::scan_frontmatter()
        }
        BronzeKind::ParquetHive => {
            crate::models::silver::staging::stg_ohlcv_bronzeparquet_1m::scan_frontmatter()
        }
    };
    let sources = match bronze {
        BronzeKind::ArrowIpc => ("bronze", "ohlcv_1m"),
        BronzeKind::ParquetHive => ("bronze", "ohlcv_parquet_1m"),
    };

    let stg_spec = ModelSpec::rust(STG_OHLCV_1M_NAME)
        .sources([sources])
        .layer(ModelLayer::Staging)
        .materialization(Materialization::Table)
        .output_path(&silver_stg)
        .frontmatter(stg_fm);

    let mut tf_spec = ModelSpec::rust(TF_INDICATORS_NAME)
        .refs([STG_OHLCV_1M_NAME])
        .layer(ModelLayer::Transform)
        .output_path(&silver_tf)
        .description("finance-solution SMA/EMA/RSI");
    if partition_tf {
        tf_spec = tf_spec
            .materialization(Materialization::ScopedReplace)
            .frontmatter(
                crate::models::silver::transforms::tf_indicators_1m::partition_frontmatter(),
            );
    } else {
        tf_spec = tf_spec.materialization(Materialization::Table);
    }

    let obt_spec = ModelSpec::rust(OBT_STOCKS_1M_NAME)
        .refs([TF_INDICATORS_NAME])
        .layer(ModelLayer::Mart)
        .materialization(Materialization::Table)
        .output_path(&gold_obt);

    let dag = DagBuilder::new()
        .model(stg_spec)
        .model(tf_spec)
        .model(obt_spec)
        .build()
        .context("compare DAG")?;

    let mut scope = RunScope::new().with_var_multi("symbol", syms.to_vec())?;
    scope.write_receipt = true;
    scope.skip_if_fingerprint_match = false;

    let stg_model: std::sync::Arc<dyn RustModel> = match bronze {
        BronzeKind::ArrowIpc => std::sync::Arc::new(StgOhlcv1m),
        BronzeKind::ParquetHive => std::sync::Arc::new(StgOhlcvBronzeparquet1m),
    };

    let engine = RbtEngineBuilder::new()
        .with_rust_model_arc(stg_model)
        .with_rust_model(TfIndicators1m)
        .with_rust_model(ObtStocks1m)
        .build()
        .await?;

    let t0 = Instant::now();
    let summary = engine
        .execute_dag_with_scope(&dag, project, &project.join("lake"), &config, &scope)
        .await
        .with_context(|| format!("execute compare DAG out={out_root}"))?;
    let wall = t0.elapsed();

    let (label, notes) = match (bronze, partition_tf) {
        (BronzeKind::ArrowIpc, _) => (
            "Arrow+spill",
            "IPC hive; force-bronze-register; serial mega tf".to_string(),
        ),
        (BronzeKind::ParquetHive, false) => (
            "Parquet",
            "parquet hive listing; no spill; serial mega tf".to_string(),
        ),
        (BronzeKind::ParquetHive, true) => (
            "Parquet+par",
            format!("parquet hive listing; RBT-C partition jobs={jobs}"),
        ),
    };

    Ok(CompareRow {
        label,
        wall,
        rows: summary.total_rows_produced,
        models: summary.models_executed,
        stg_rows_hint: None,
        notes,
    })
}

fn ensure_parquet_bronze(project: &Path, jobs: usize) -> Result<()> {
    let marker = project.join("lake/bronze/lz_stock_bars_parquet");
    if marker.is_dir()
        && std::fs::read_dir(&marker)
            .map(|rd| rd.count() > 0)
            .unwrap_or(false)
    {
        println!("[compare] parquet bronze present: {}", marker.display());
        return Ok(());
    }
    println!("[compare] landing parquet bronze from Arrow (one-time)…");
    crate::spill_exp::land_parquet_bronze_from_arrow(project, jobs)
}

fn discover_1m_symbols(project: &Path) -> Result<Vec<String>> {
    let root = project.join("lake/bronze/lz_stock_bars");
    if !root.is_dir() {
        bail!("missing {}", root.display());
    }
    let mut syms = Vec::new();
    for e in std::fs::read_dir(&root)? {
        let e = e?;
        let name = e.file_name();
        let name = name.to_string_lossy();
        let Some(sym) = name.strip_prefix("symbol=") else {
            continue;
        };
        let tf = e.path().join("timeframe=1m");
        if tf.is_dir()
            && std::fs::read_dir(&tf)?
                .filter_map(|x| x.ok())
                .any(|f| f.path().extension().and_then(|x| x.to_str()) == Some("arrow"))
        {
            syms.push(sym.to_string());
        }
    }
    syms.sort();
    if syms.is_empty() {
        bail!("no 1m symbols under {}", root.display());
    }
    Ok(syms)
}

fn wipe(project: &Path, out_root: &str) -> Result<()> {
    let p = project.join(out_root);
    if p.exists() {
        std::fs::remove_dir_all(&p)?;
    }
    std::fs::create_dir_all(&p)?;
    Ok(())
}

fn print_table(rows: &[CompareRow]) {
    println!();
    println!("========== FULL DAG WALL CLOCK (stg→tf→obt) ==========");
    println!(
        "{:<14} {:>10} {:>12} {:>8}  {}",
        "run", "wall_secs", "total_rows", "models", "notes"
    );
    println!("{}", "-".repeat(88));
    let base = rows.first().map(|r| r.wall.as_secs_f64()).unwrap_or(1.0);
    for r in rows {
        let speedup = base / r.wall.as_secs_f64().max(1e-9);
        println!(
            "{:<14} {:>10.3} {:>12} {:>8}  {}  (vs Arrow {speedup:.2}x)",
            r.label,
            r.wall.as_secs_f64(),
            r.rows,
            r.models,
            r.notes
        );
    }
    println!("======================================================");
}

fn write_findings(project: &Path, rows: &[CompareRow], n_sym: usize, jobs: usize) -> Result<()> {
    let path = project.join("FINDINGS.md");
    let mut md = String::new();
    md.push_str("# full_e2e landing comparison — findings\n\n");
    md.push_str("Generated by `full-e2e-rbt-example compare` (stg → tf → obt, equal symbol scope).\n\n");
    md.push_str(&format!(
        "- **Symbols:** {n_sym} (`symbol=*/timeframe=1m` with Arrow files)\n"
    ));
    md.push_str(&format!("- **Parallel jobs (Parquet+par):** {jobs}\n"));
    md.push_str("- **Host:** release profile recommended\n\n");
    md.push_str("## Wall clocks\n\n");
    md.push_str("| Run | wall_secs | total_rows | models | Notes |\n");
    md.push_str("|-----|----------:|-----------:|-------:|-------|\n");
    let base = rows.first().map(|r| r.wall.as_secs_f64()).unwrap_or(1.0);
    for r in rows {
        let sp = base / r.wall.as_secs_f64().max(1e-9);
        md.push_str(&format!(
            "| **{}** | {:.3} | {} | {} | {} ({sp:.2}× vs Arrow) |\n",
            r.label,
            r.wall.as_secs_f64(),
            r.rows,
            r.models,
            r.notes
        ));
    }
    md.push_str("\n## What each run does\n\n");
    md.push_str("| Run | Bronze | Bronze register | Transform | Output root |\n");
    md.push_str("|-----|--------|-----------------|-----------|-------------|\n");
    md.push_str("| Arrow+spill | `lz_stock_bars` IPC hive | **force re-spill** | serial mega TA | `lake/compare_arrow_output/` |\n");
    md.push_str("| Parquet | `lz_stock_bars_parquet` | DF **listing** (no spill) | serial mega TA | `lake/compare_parquet_output/` |\n");
    md.push_str("| Parquet+par | same Parquet hive | DF listing | **RBT-C** partition WorkUnits | `lake/compare_parquet_parallel_output/` |\n");
    md.push_str("\n## Interpretation\n\n");
    md.push_str("- **Arrow+spill** pays IPC decode + spill encode every forced run.\n");
    md.push_str("- **Parquet** avoids spill; registration is listing — usually the largest win on bronze→stg.\n");
    md.push_str("- **Parquet+par** adds L2 concurrency on indicators; wins when TA/I-O per symbol is heavy enough to amortize WorkUnit overhead (on this demo, often close to serial Parquet).\n");
    md.push_str("- `total_rows` is the engine sum across models (stg + tf [+ units] + obt), not unique bar count.\n");
    md.push_str("\n## Reproduce\n\n```bash\n");
    md.push_str("cargo run -p full-e2e-rbt-example --release -- \\\n");
    md.push_str("  -p examples/full_e2e_rbt_example compare -j 8\n");
    md.push_str("```\n");
    std::fs::write(&path, md).with_context(|| format!("write {}", path.display()))?;
    println!("[compare] wrote {}", path.display());
    Ok(())
}

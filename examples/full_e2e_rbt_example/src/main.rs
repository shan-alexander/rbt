//! # full-e2e-rbt-example — Design A / B / C / D
//!
//! | Approach | Models | Lake outputs | Parallelism |
//! |----------|--------|--------------|-------------|
//! | **A** | SQL files | `sql_models_output/` | serial |
//! | **B** | one Rust `tf_indicators_1m` | `rust_models_output/` | serial **mega** table |
//! | **C** | same as B | `parallel_models_output/` | RBT-C **L2 WorkUnits** (scoped_replace parts) |
//! | **D** | **one Rust model per symbol** + UNION OBT | `design_d_models_output/` | RBT-C **L1 model_tier** |
//!
//! ## Why B ≈ C (or C slower) on this demo
//!
//! - Shared cost dominates: bronze spill + staging (~same for B and C).
//! - Design B mega path: one `collect` of silver → one TA pass over all symbols in-process.
//! - Design C: 82 WorkUnits × (open private session + filter stg + TA + part write +
//!   manifest lock). Parallelism helps **CPU-heavy** units; here TA is light and
//!   **I/O + session overhead** eats the gain.
//!
//! `diag` prints **segment** wall times (stg / tf / obt) so you can see where time goes.
//! Design D tests “DAG-level fan-out” (many named `tf_*` nodes) vs C’s engine WorkUnits.

mod compare_bench;
mod models;
mod spill_exp;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use models::silver::staging::stg_lake_reader::StgLakeReader;
use models::{
    obt_union_sql, ObtStocks1m, StgOhlcv1m, TfIndicators1m, TfSymbolIndicators,
    OBT_STOCKS_1M_NAME, STG_OHLCV_1M_NAME, TF_INDICATORS_NAME,
};
use rbt::{
    DagBuilder, Materialization, ModelDag, ModelKind, ModelLayer, ModelSpec, ParallelContract,
    RbtEngineBuilder, RbtProjectConfig, RunScope, RustModel, RustModelContext, RustModelOutput,
    SelectMode, TransformationEngine,
};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const SQL_PROJECT_YML: &str = "rbt_project.yml";
const RUST_PROJECT_YML: &str = "rbt_project_rust.yml";
const PARALLEL_PROJECT_YML: &str = "rbt_project_parallel.yml";

/// Shared result line for wall-clock tables.
#[derive(Debug, Clone)]
struct RunReport {
    design: &'static str,
    wall: Duration,
    total_rows: usize,
    models_executed: usize,
    symbols: Option<usize>,
    jobs: usize,
    notes: String,
}

#[derive(Parser, Debug)]
#[command(
    name = "full-e2e-rbt-example",
    about = "Design A (SQL) / B (Rust serial) / C (RBT-C parallel) medallion demo"
)]
struct Cli {
    /// Example project root (contains rbt_project*.yml + src/models/ + lake/)
    #[arg(short = 'p', long, default_value = ".")]
    project_dir: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Design A — pure SQL models → lake/sql_models_output/ (full universe, serial)
    Sql {
        #[arg(long, default_value = "stg_ohlcv_1m,tf_indicators_1m,obt_stocks_1m")]
        select: String,
        #[arg(long, default_value_t = true)]
        force: bool,
    },
    /// Design B — pure Rust serial mega → lake/rust_models_output/ (full universe)
    DesignB {
        /// Comma-separated symbols. Empty = full universe (unfiltered bronze).
        #[arg(long, default_value = "")]
        symbols: String,
        #[arg(long, default_value_t = true)]
        force: bool,
    },
    /// Design C — RBT-C partition workers → lake/parallel_models_output/
    ///
    /// Full universe of hive symbols as multi-value WorkUnits; concurrent with `--jobs`.
    DesignC {
        /// Worker count (L2 concurrent partition units). Default: available parallelism.
        #[arg(short = 'j', long, default_value_t = 0)]
        jobs: usize,
        #[arg(long, default_value_t = true)]
        force: bool,
    },
    /// Alias for design-c (historical)
    Parallel {
        #[arg(short = 'j', long, default_value_t = 0)]
        jobs: usize,
        #[arg(long, default_value_t = true)]
        force: bool,
    },
    /// Design A fast smoke: JSONL lander only
    Jsonl {
        #[arg(long, default_value_t = true)]
        force: bool,
    },
    /// Force-run Design A + B + C; print wall-clock comparison table
    Bench {
        /// Jobs for Design C only (0 = available parallelism)
        #[arg(short = 'j', long, default_value_t = 0)]
        jobs: usize,
    },
    /// Force-run A then B only (legacy)
    Both {
        #[arg(short = 'j', long, default_value_t = 0)]
        jobs: usize,
    },
    /// Design D — per-symbol `tf_indicators_1m_<SYM>` models + UNION ALL obt (L1 concurrent)
    DesignD {
        #[arg(short = 'j', long, default_value_t = 0)]
        jobs: usize,
        #[arg(long, default_value_t = true)]
        force: bool,
        /// Cap symbols for a smaller experiment (0 = all hive 1m symbols)
        #[arg(long, default_value_t = 0)]
        max_symbols: usize,
    },
    /// Segment wall clocks: bronze+stg / tf / obt for B, C, D (+ education)
    Diag {
        #[arg(short = 'j', long, default_value_t = 0)]
        jobs: usize,
        /// Cap symbols for B/C/D fair set (0 = all)
        #[arg(long, default_value_t = 0)]
        max_symbols: usize,
    },
    /// Experiment: mega spill vs partitioned-by-symbol spill (wall clock only)
    SpillBench {
        #[arg(short = 'j', long, default_value_t = 0)]
        jobs: usize,
    },
    /// Convert Arrow hive bronze → recommended Parquet hive (no-spill listing path)
    LandParquetBronze {
        #[arg(short = 'j', long, default_value_t = 0)]
        jobs: usize,
    },
    /// Fair full-DAG compare: Arrow+force-spill vs Parquet vs Parquet+RBT-C parallel
    Compare {
        #[arg(short = 'j', long, default_value_t = 0)]
        jobs: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    let project = cli.project_dir.canonicalize().unwrap_or(cli.project_dir);
    match cli.command {
        Commands::Sql { select, force } => {
            let _ = run_design_a_sql(&project, &select, force).await?;
        }
        Commands::DesignB { symbols, force } => {
            let _ = run_design_b(&project, &symbols, force).await?;
        }
        Commands::DesignC { jobs, force } | Commands::Parallel { jobs, force } => {
            let jobs = resolve_jobs(jobs);
            let _ = run_design_c(&project, jobs, force).await?;
        }
        Commands::Jsonl { force } => {
            let _ = run_design_a_sql(&project, "stg_ohlcv_jsonl_1m", force).await?;
        }
        Commands::Bench { jobs } | Commands::Both { jobs } => {
            let jobs = resolve_jobs(jobs);
            run_bench(&project, jobs).await?;
        }
        Commands::DesignD {
            jobs,
            force,
            max_symbols,
        } => {
            let jobs = resolve_jobs(jobs);
            let _ = run_design_d(&project, jobs, force, max_symbols).await?;
        }
        Commands::Diag { jobs, max_symbols } => {
            let jobs = resolve_jobs(jobs);
            run_diag(&project, jobs, max_symbols).await?;
        }
        Commands::SpillBench { jobs } => {
            let jobs = resolve_jobs(jobs);
            spill_exp::run_spill_experiment(&project, jobs)?;
        }
        Commands::LandParquetBronze { jobs } => {
            let jobs = resolve_jobs(jobs);
            spill_exp::land_parquet_bronze_from_arrow(&project, jobs)?;
        }
        Commands::Compare { jobs } => {
            let jobs = resolve_jobs(jobs);
            compare_bench::run_landing_compare(&project, jobs).await?;
        }
    }
    Ok(())
}

fn resolve_jobs(jobs: usize) -> usize {
    if jobs > 0 {
        return jobs;
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(2)
}

fn load_project_yml(project: &Path, yml_name: &str) -> Result<RbtProjectConfig> {
    let path = project.join(yml_name);
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("read project config {}", path.display()))?;
    let config: RbtProjectConfig = serde_yaml::from_str(&content)
        .with_context(|| format!("parse project config {}", path.display()))?;
    Ok(config)
}

fn parse_symbols(symbols: &str) -> Vec<String> {
    symbols
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// All tickers under bronze hive that have a `timeframe=1m` partition with arrow files.
fn discover_1m_symbols(project: &Path) -> Result<Vec<String>> {
    let root = project.join("lake/bronze/lz_stock_bars");
    if !root.is_dir() {
        bail!("bronze hive missing: {}", root.display());
    }
    let mut syms = Vec::new();
    for entry in std::fs::read_dir(&root).with_context(|| format!("read {}", root.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(sym) = name.strip_prefix("symbol=") else {
            continue;
        };
        let tf_dir = entry.path().join("timeframe=1m");
        if !tf_dir.is_dir() {
            continue;
        }
        let has_arrow = std::fs::read_dir(&tf_dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok()).any(|f| {
                    f.path()
                        .extension()
                        .and_then(|e| e.to_str())
                        .map(|e| e.eq_ignore_ascii_case("arrow"))
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);
        if has_arrow {
            syms.push(sym.to_string());
        }
    }
    syms.sort();
    if syms.is_empty() {
        bail!(
            "no symbol=*/timeframe=1m arrow partitions under {}",
            root.display()
        );
    }
    Ok(syms)
}

fn wipe_dir(path: &Path) -> Result<()> {
    if path.exists() {
        std::fs::remove_dir_all(path).with_context(|| format!("wipe {}", path.display()))?;
    }
    std::fs::create_dir_all(path).with_context(|| format!("mkdir {}", path.display()))?;
    Ok(())
}

fn print_report(r: &RunReport) {
    println!(
        "[full-e2e] {} WALL_CLOCK_SECS={:.3} TOTAL_ROWS={} models={} jobs={}{}",
        r.design,
        r.wall.as_secs_f64(),
        r.total_rows,
        r.models_executed,
        r.jobs,
        r.symbols
            .map(|s| format!(" symbols={s}"))
            .unwrap_or_default()
    );
    if !r.notes.is_empty() {
        println!("[full-e2e] {} note: {}", r.design, r.notes);
    }
}

fn print_comparison(reports: &[RunReport]) {
    println!();
    println!("========== WALL CLOCK COMPARISON ==========");
    println!(
        "{:<10} {:>12} {:>14} {:>8} {:>8}  {}",
        "Design", "wall_secs", "total_rows", "models", "jobs", "notes"
    );
    println!("{}", "-".repeat(78));
    for r in reports {
        println!(
            "{:<10} {:>12.3} {:>14} {:>8} {:>8}  {}",
            r.design,
            r.wall.as_secs_f64(),
            r.total_rows,
            r.models_executed,
            r.jobs,
            r.notes
        );
    }
    if let (Some(b), Some(c)) = (
        reports.iter().find(|r| r.design == "DesignB"),
        reports.iter().find(|r| r.design == "DesignC"),
    ) {
        if b.wall.as_secs_f64() > 0.0 {
            let speedup = b.wall.as_secs_f64() / c.wall.as_secs_f64();
            println!();
            println!(
                "Design C vs Design B (serial mega → RBT-C partition workers): \
                 speedup={speedup:.2}x  (B={:.1}s / C={:.1}s, jobs={})",
                b.wall.as_secs_f64(),
                c.wall.as_secs_f64(),
                c.jobs
            );
        }
    }
    println!("===========================================");
}

async fn run_bench(project: &Path, jobs_c: usize) -> Result<()> {
    let mut reports = Vec::new();
    // Same multi-value symbol set for B and C so wall clocks compare equal work.
    let syms = discover_1m_symbols(project)?;
    let sym_csv = syms.join(",");
    println!(
        "[full-e2e] bench universe: {} symbols (all symbol=*/timeframe=1m hive dirs)",
        syms.len()
    );

    println!("========== FORCE RUN: Design A (SQL) full universe ==========");
    reports.push(
        run_design_a_sql(project, "stg_ohlcv_1m,tf_indicators_1m,obt_stocks_1m", true).await?,
    );

    println!(
        "========== FORCE RUN: Design B (Rust serial mega) same {} symbols ==========",
        syms.len()
    );
    // Multi-value scope + serial table mega — same filtered grain as Design C, no WorkUnits.
    reports.push(run_design_b(project, &sym_csv, true).await?);

    println!(
        "========== FORCE RUN: Design C (RBT-C parallel) same {} symbols jobs={jobs_c} ==========",
        syms.len()
    );
    reports.push(run_design_c(project, jobs_c, true).await?);

    print_comparison(&reports);
    Ok(())
}

/// Design A: file-project SQL → `lake/sql_models_output/` (full universe).
async fn run_design_a_sql(project: &Path, select: &str, force: bool) -> Result<RunReport> {
    let t0 = Instant::now();
    println!("[full-e2e] Design A (SQL) select={select} force={force}");
    println!("[full-e2e] project={}", project.display());
    println!("[full-e2e] config={SQL_PROJECT_YML}");
    println!("[full-e2e] outputs → lake/sql_models_output/");
    println!("[full-e2e] scope: FULL UNIVERSE (no symbol filter)");

    if force {
        wipe_dir(&project.join("lake/sql_models_output"))?;
        println!("[full-e2e] wiped lake/sql_models_output/");
    }

    let mut config = load_project_yml(project, SQL_PROJECT_YML)?;
    config.execution.concurrency.enabled = false;

    let full = config.build_dag(project, None)?;
    let dag = full
        .apply_select(Some(select), SelectMode::Execute)
        .context("select")?;

    let mut scope = RunScope::new();
    scope.write_receipt = true;
    scope.skip_if_fingerprint_match = false;

    let engine = TransformationEngine::new();
    let out = project.join("lake");
    let summary = engine
        .execute_dag_with_scope(&dag, project, &out, &config, &scope)
        .await?;
    let wall = t0.elapsed();
    let report = RunReport {
        design: "DesignA",
        wall,
        total_rows: summary.total_rows_produced,
        models_executed: summary.models_executed,
        symbols: None,
        jobs: 1,
        notes: "serial SQL; OBT alias may not double-count rows".into(),
    };
    println!(
        "[full-e2e] Design A DONE wall_secs={:.3} models={} rows={} skipped={}",
        wall.as_secs_f64(),
        summary.models_executed,
        summary.total_rows_produced,
        summary.skipped
    );
    print_report(&report);
    Ok(report)
}

/// Design B: pure Rust serial mega table → `lake/rust_models_output/`.
///
/// * `symbols` empty → unfiltered full bronze scan (all timeframe=1m paths).
/// * `symbols` set (bench) → multi-value IN filter, still **serial mega** (no WorkUnits),
///   so wall clock is comparable to Design C on the same symbol set.
async fn run_design_b(project: &Path, symbols: &str, force: bool) -> Result<RunReport> {
    let t0 = Instant::now();
    let syms = parse_symbols(symbols);
    let full_universe = syms.is_empty();
    if full_universe {
        let discovered = discover_1m_symbols(project)?;
        println!(
            "[full-e2e] Design B FULL UNIVERSE (no multi-value filter); \
             hive has {} symbol=*/timeframe=1m dirs",
            discovered.len()
        );
    } else {
        println!(
            "[full-e2e] Design B serial mega with multi-value scope: {} symbols",
            syms.len()
        );
    }

    println!(
        "[full-e2e] Design B (Rust serial mega) full_universe={full_universe} force={force}"
    );
    println!("[full-e2e] project={}", project.display());
    println!("[full-e2e] config={RUST_PROJECT_YML}");
    println!("[full-e2e] outputs → lake/rust_models_output/");

    if force {
        wipe_dir(&project.join("lake/rust_models_output"))?;
        println!("[full-e2e] wiped lake/rust_models_output/");
    }

    let mut config = load_project_yml(project, RUST_PROJECT_YML)?;
    // Serial mega path — no WorkUnit fan-out (even when multi-value is set).
    config.execution.concurrency.enabled = false;
    config.execution.concurrency.strategy = rbt::ExecutionStrategy::Serial;
    config.execution.concurrency.max_workers = 1;

    let out_root = "lake/rust_models_output";
    let (dag, paths) = build_rust_dag(project, out_root, /*partition*/ false)?;

    let mut scope = RunScope::new();
    if !full_universe {
        scope = scope.with_var_multi("symbol", syms.clone())?;
    }
    scope.write_receipt = true;
    scope.skip_if_fingerprint_match = false;

    let engine = RbtEngineBuilder::new()
        .with_rust_model(StgOhlcv1m)
        .with_rust_model(TfIndicators1m)
        .with_rust_model(ObtStocks1m)
        .build()
        .await
        .context("build engine Design B")?;

    let summary = engine
        .execute_dag_with_scope(&dag, project, &project.join("lake"), &config, &scope)
        .await
        .context("execute Design B")?;

    let wall = t0.elapsed();
    let report = RunReport {
        design: "DesignB",
        wall,
        total_rows: summary.total_rows_produced,
        models_executed: summary.models_executed,
        symbols: if full_universe {
            None
        } else {
            Some(syms.len())
        },
        jobs: 1,
        notes: if full_universe {
            "serial mega table; unfiltered 1m bronze".into()
        } else {
            format!(
                "serial mega table; multi-value {} symbols (same set as Design C)",
                syms.len()
            )
        },
    };
    println!(
        "[full-e2e] Design B DONE wall_secs={:.3} models={} rows={} skipped={}",
        wall.as_secs_f64(),
        summary.models_executed,
        summary.total_rows_produced,
        summary.skipped
    );
    print_report(&report);
    println!(
        "[full-e2e] outputs:\n  {}\n  {}\n  {}",
        paths.0, paths.1, paths.2
    );
    Ok(report)
}

/// Design C: RBT-C parallel partition WorkUnits → `lake/parallel_models_output/`.
///
/// Full refresh on the **same grain** as Design B by multi-value-scoping every
/// `symbol=*/timeframe=1m` ticker (complete hive symbol set). Staging still
/// dedupes to unique `(symbol, timestamp_ns)`. Indicators fan out one unit per
/// symbol; `jobs` concurrent workers (private sessions).
async fn run_design_c(project: &Path, jobs: usize, force: bool) -> Result<RunReport> {
    let t0 = Instant::now();
    let syms = discover_1m_symbols(project)?;
    let n_sym = syms.len();

    println!(
        "[full-e2e] Design C (RBT-C parallel) symbols={n_sym} jobs={jobs} force={force}"
    );
    println!("[full-e2e] project={}", project.display());
    println!("[full-e2e] config={PARALLEL_PROJECT_YML}");
    println!("[full-e2e] outputs → lake/parallel_models_output/");
    println!(
        "[full-e2e] plan: multi-value symbol fan-out → {} partition WorkUnits for tf_indicators_1m",
        n_sym
    );

    if force {
        wipe_dir(&project.join("lake/parallel_models_output"))?;
        println!("[full-e2e] wiped lake/parallel_models_output/");
    }

    let mut config = load_project_yml(project, PARALLEL_PROJECT_YML)?;
    config.execution.concurrency.enabled = true;
    config.execution.concurrency.strategy = rbt::ExecutionStrategy::Partition;
    config.execution.concurrency.multi_value_fanout_threshold = 2;
    // Force every part dirty on full refresh (no dirty-part skip).
    config.execution.concurrency.dirty_part_skip = false;
    config.execution.concurrency.large_parts_first = true;
    config.execution.concurrency.max_workers = jobs.max(1);
    if jobs > 1 {
        config.execution.concurrency.apply_jobs(jobs);
    } else {
        // jobs==1: still fan-out units (layout honesty) but serial L2.
        config.execution.concurrency.enabled = true;
        config.execution.concurrency.max_workers = 1;
    }

    let out_root = "lake/parallel_models_output";
    let (dag, paths) = build_rust_dag(project, out_root, /*partition*/ true)?;

    assert!(matches!(
        TfIndicators1m.parallel_contract(),
        ParallelContract::PartitionLocal { .. }
    ));

    let mut scope = RunScope::new().with_var_multi("symbol", syms.clone())?;
    scope.write_receipt = true;
    scope.skip_if_fingerprint_match = false;

    let engine = RbtEngineBuilder::new()
        .with_rust_model(StgOhlcv1m)
        .with_rust_model(TfIndicators1m)
        .with_rust_model(ObtStocks1m)
        .build()
        .await
        .context("build engine Design C")?;

    let summary = engine
        .execute_dag_with_scope(&dag, project, &project.join("lake"), &config, &scope)
        .await
        .context("execute Design C")?;

    let wall = t0.elapsed();
    let report = RunReport {
        design: "DesignC",
        wall,
        total_rows: summary.total_rows_produced,
        models_executed: summary.models_executed,
        symbols: Some(n_sym),
        jobs,
        notes: format!(
            "RBT-C partition strategy; {n_sym} symbol WorkUnits; concurrent workers={jobs}"
        ),
    };
    println!(
        "[full-e2e] Design C DONE wall_secs={:.3} models={} rows={} skipped={} symbols={n_sym} jobs={jobs}",
        wall.as_secs_f64(),
        summary.models_executed,
        summary.total_rows_produced,
        summary.skipped,
    );
    print_report(&report);
    println!(
        "[full-e2e] outputs:\n  {}\n  {}\n  {}",
        paths.0, paths.1, paths.2
    );
    Ok(report)
}

/// Shared pure-Rust medallion DAG (stg → tf → obt) with optional partition tf.
fn build_rust_dag(
    project: &Path,
    out_root: &str,
    partition: bool,
) -> Result<(ModelDag, (String, String, String))> {
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

    let stg_spec = ModelSpec::rust(STG_OHLCV_1M_NAME)
        .sources([("bronze", "ohlcv_1m")])
        .layer(ModelLayer::Staging)
        .materialization(Materialization::Table)
        .output_path(&silver_stg)
        .frontmatter(models::silver::staging::stg_ohlcv_1m::scan_frontmatter())
        .description("pure Rust staging: 1m OHLCV grain-unique");

    let mut tf_spec = ModelSpec::rust(TF_INDICATORS_NAME)
        .refs([STG_OHLCV_1M_NAME])
        .layer(ModelLayer::Transform)
        .output_path(&silver_tf)
        .description("finance-solution SMA/EMA/RSI");

    if partition {
        tf_spec = tf_spec
            .materialization(Materialization::ScopedReplace)
            .frontmatter(models::silver::transforms::tf_indicators_1m::partition_frontmatter());
    } else {
        tf_spec = tf_spec.materialization(Materialization::Table);
    }

    let obt_spec = ModelSpec::rust(OBT_STOCKS_1M_NAME)
        .refs([TF_INDICATORS_NAME])
        .layer(ModelLayer::Mart)
        .materialization(Materialization::Table)
        .output_path(&gold_obt)
        .description("gold OBT identity of indicators");

    let dag = DagBuilder::new()
        .model(stg_spec)
        .model(tf_spec)
        .model(obt_spec)
        .build()
        .context("build pure-Rust DAG")?;

    assert_eq!(dag.graph[dag.node_map[STG_OHLCV_1M_NAME]].kind, ModelKind::Rust);
    assert_eq!(dag.graph[dag.node_map[TF_INDICATORS_NAME]].kind, ModelKind::Rust);
    assert_eq!(dag.graph[dag.node_map[OBT_STOCKS_1M_NAME]].kind, ModelKind::Rust);

    Ok((dag, (silver_stg, silver_tf, gold_obt)))
}

// =============================================================================
// Design D — per-symbol tf_* models + UNION OBT (L1 concurrent)
// =============================================================================

fn limit_symbols(mut syms: Vec<String>, max: usize) -> Vec<String> {
    if max > 0 && max < syms.len() {
        syms.truncate(max);
    }
    syms
}

/// Design D: stg → N× `tf_indicators_1m_<SYM>` → obt UNION ALL.
async fn run_design_d(
    project: &Path,
    jobs: usize,
    force: bool,
    max_symbols: usize,
) -> Result<RunReport> {
    let t0 = Instant::now();
    let syms = limit_symbols(discover_1m_symbols(project)?, max_symbols);
    let n = syms.len();
    println!(
        "[full-e2e] Design D (per-symbol tf models + UNION obt) symbols={n} jobs={jobs} force={force}"
    );
    println!("[full-e2e] outputs → lake/design_d_models_output/");

    if force {
        wipe_dir(&project.join("lake/design_d_models_output"))?;
    }

    let mut config = load_project_yml(project, RUST_PROJECT_YML)?;
    // L1: concurrent independent models in a tier (the N per-symbol tf_*).
    config.execution.concurrency.enabled = true;
    config.execution.concurrency.strategy = rbt::ExecutionStrategy::ModelTier;
    config.execution.concurrency.max_workers = jobs.max(1);
    if jobs > 1 {
        config.execution.concurrency.apply_jobs(jobs);
        config.execution.concurrency.strategy = rbt::ExecutionStrategy::ModelTier;
    }

    let out_root = "lake/design_d_models_output";
    let silver_stg = project
        .join(out_root)
        .join("silver/stage/stg_ohlcv_1m.parquet")
        .to_string_lossy()
        .into_owned();
    let gold_obt = project
        .join(out_root)
        .join("gold/obt_stocks_1m.parquet")
        .to_string_lossy()
        .into_owned();

    let stg_spec = ModelSpec::rust(STG_OHLCV_1M_NAME)
        .sources([("bronze", "ohlcv_1m")])
        .layer(ModelLayer::Staging)
        .materialization(Materialization::Table)
        .output_path(&silver_stg)
        .frontmatter(models::silver::staging::stg_ohlcv_1m::scan_frontmatter())
        .description("Design D staging (shared)");

    let mut builder = DagBuilder::new().model(stg_spec);
    let mut tf_names = Vec::with_capacity(n);
    let mut per_sym_models = Vec::with_capacity(n);

    for sym in &syms {
        let m = TfSymbolIndicators::new(sym.clone());
        let name = m.model_name.clone();
        let tf_path = project
            .join(out_root)
            .join(format!("silver/tf/{name}.parquet"))
            .to_string_lossy()
            .into_owned();
        builder = builder.model(
            ModelSpec::rust(&name)
                .refs([STG_OHLCV_1M_NAME])
                .layer(ModelLayer::Transform)
                .materialization(Materialization::Table)
                .output_path(&tf_path)
                .description(format!("per-symbol indicators for {sym}")),
        );
        tf_names.push(name);
        per_sym_models.push(m);
    }

    let obt_sql = obt_union_sql(&tf_names);
    builder = builder.model(
        ModelSpec::sql(OBT_STOCKS_1M_NAME, obt_sql)
            .layer(ModelLayer::Mart)
            .materialization(Materialization::Table)
            .output_path(&gold_obt)
            .description("UNION ALL of per-symbol tf_*"),
    );

    let dag = builder.build().context("build Design D DAG")?;

    // Multi-value not required: each tf filters its own symbol in SQL.
    let mut scope = RunScope::new();
    scope.write_receipt = true;
    scope.skip_if_fingerprint_match = false;

    let mut eng = RbtEngineBuilder::new().with_rust_model(StgOhlcv1m);
    for m in per_sym_models {
        eng = eng.with_rust_model(m);
    }
    let engine = eng.build().await.context("build Design D engine")?;

    let summary = engine
        .execute_dag_with_scope(&dag, project, &project.join("lake"), &config, &scope)
        .await
        .context("execute Design D")?;

    let wall = t0.elapsed();
    let report = RunReport {
        design: "DesignD",
        wall,
        total_rows: summary.total_rows_produced,
        models_executed: summary.models_executed,
        symbols: Some(n),
        jobs,
        notes: format!(
            "per-symbol tf_* ({n} models) + UNION obt; L1 model_tier workers={jobs}"
        ),
    };
    println!(
        "[full-e2e] Design D DONE wall_secs={:.3} models={} rows={} symbols={n} jobs={jobs}",
        wall.as_secs_f64(),
        summary.models_executed,
        summary.total_rows_produced,
    );
    print_report(&report);
    Ok(report)
}

// =============================================================================
// Segment diagnosis — where wall clock actually goes
// =============================================================================

#[derive(Debug, Clone, Default)]
struct SegmentTimes {
    label: &'static str,
    bronze_stg_s: f64,
    tf_s: f64,
    obt_s: f64,
    total_s: f64,
    jobs: usize,
    symbols: usize,
    notes: String,
}

async fn run_diag(project: &Path, jobs: usize, max_symbols: usize) -> Result<()> {
    println!("========== EDUCATION: Design B vs C vs D ==========");
    println!(
        r#"
Design B — ONE Rust model `tf_indicators_1m`, materialization=table (mega):
  • Engine runs stg once, then ONE execute() that loads all silver rows and
    runs finance-solution TA grouped by symbol in a single process.
  • No WorkUnits, no per-part manifest merge, one Parquet write.

Design C — SAME model name, materialization=scoped_replace + multi-value symbols:
  • Planner fans multi-value symbol → N WorkUnits (one per ticker).
  • With jobs>1, L2 runs units concurrently (private SessionContext each).
  • Each unit: filter stg → TA for one symbol → write part-*.parquet → merge
    manifest under a lock.
  • End state: N part files (not one mega table). OBT still streams the whole.

Design D — N *named* models `tf_indicators_1m_<SYM>` as separate DAG nodes:
  • Topo tier after stg has width N; L1 model_tier can run them concurrently.
  • Each model is a full table write; OBT is SQL UNION ALL of all refs.
  • Tests whether “DAG-level fan-out” beats engine WorkUnits (C).

Why C often fails to beat B on *this* lake:
  1. Bronze spill + staging is ~half+ of wall clock and is serial in all designs.
  2. TA (SMA/EMA/RSI) is cheap vs Parquet I/O; mega B already saturates disks.
  3. C pays fixed overhead per unit (session, plan, part write, lock) × 82.
  4. Debug builds exaggerate overhead; always compare --release.
"#
    );

    let syms = limit_symbols(discover_1m_symbols(project)?, max_symbols);
    let n = syms.len();
    let sym_csv = syms.join(",");
    println!("[diag] fair symbol set: {n} tickers; jobs={jobs}");
    println!();

    // --- Design B segments ---
    wipe_dir(&project.join("lake/rust_models_output"))?;
    let b_stg = timed_rust_phase(
        project,
        "lake/rust_models_output",
        &sym_csv,
        Phase::StgOnly,
        false,
        1,
    )
    .await?;
    let b_tf = timed_rust_phase(
        project,
        "lake/rust_models_output",
        &sym_csv,
        Phase::TfOnlyReuseStg,
        false,
        1,
    )
    .await?;
    let b_obt = timed_rust_phase(
        project,
        "lake/rust_models_output",
        &sym_csv,
        Phase::ObtOnlyReuseTf,
        false,
        1,
    )
    .await?;
    let b_total = b_stg + b_tf + b_obt;
    let seg_b = SegmentTimes {
        label: "DesignB",
        bronze_stg_s: b_stg.as_secs_f64(),
        tf_s: b_tf.as_secs_f64(),
        obt_s: b_obt.as_secs_f64(),
        total_s: b_total.as_secs_f64(),
        jobs: 1,
        symbols: n,
        notes: "mega table TA".into(),
    };

    // --- Design C segments ---
    wipe_dir(&project.join("lake/parallel_models_output"))?;
    let c_stg = timed_rust_phase(
        project,
        "lake/parallel_models_output",
        &sym_csv,
        Phase::StgOnly,
        true,
        jobs,
    )
    .await?;
    let c_tf = timed_rust_phase(
        project,
        "lake/parallel_models_output",
        &sym_csv,
        Phase::TfOnlyReuseStg,
        true,
        jobs,
    )
    .await?;
    let c_obt = timed_rust_phase(
        project,
        "lake/parallel_models_output",
        &sym_csv,
        Phase::ObtOnlyReuseTf,
        true,
        jobs,
    )
    .await?;
    let c_total = c_stg + c_tf + c_obt;
    let seg_c = SegmentTimes {
        label: "DesignC",
        bronze_stg_s: c_stg.as_secs_f64(),
        tf_s: c_tf.as_secs_f64(),
        obt_s: c_obt.as_secs_f64(),
        total_s: c_total.as_secs_f64(),
        jobs,
        symbols: n,
        notes: "L2 WorkUnits + parts".into(),
    };

    // --- Design D full (segmented similarly) ---
    wipe_dir(&project.join("lake/design_d_models_output"))?;
    let d_stg = timed_design_d_phase(project, &syms, jobs, Phase::StgOnly).await?;
    let d_tf = timed_design_d_phase(project, &syms, jobs, Phase::TfOnlyReuseStg).await?;
    let d_obt = timed_design_d_phase(project, &syms, jobs, Phase::ObtOnlyReuseTf).await?;
    let d_total = d_stg + d_tf + d_obt;
    let seg_d = SegmentTimes {
        label: "DesignD",
        bronze_stg_s: d_stg.as_secs_f64(),
        tf_s: d_tf.as_secs_f64(),
        obt_s: d_obt.as_secs_f64(),
        total_s: d_total.as_secs_f64(),
        jobs,
        symbols: n,
        notes: "N named tf_* + UNION".into(),
    };

    println!();
    println!("========== SEGMENT WALL CLOCKS (seconds) ==========");
    println!(
        "{:<10} {:>10} {:>10} {:>10} {:>10} {:>6}  {}",
        "Design", "bronze+stg", "tf", "obt", "sum", "jobs", "notes"
    );
    println!("{}", "-".repeat(78));
    for s in [&seg_b, &seg_c, &seg_d] {
        println!(
            "{:<10} {:>10.3} {:>10.3} {:>10.3} {:>10.3} {:>6}  {}",
            s.label, s.bronze_stg_s, s.tf_s, s.obt_s, s.total_s, s.jobs, s.notes
        );
    }
    println!();
    if seg_b.tf_s > 0.0 {
        println!(
            "tf speedup C/B = {:.2}x   (C_tf={:.3}s / B_tf={:.3}s)",
            seg_b.tf_s / seg_c.tf_s.max(1e-9),
            seg_c.tf_s,
            seg_b.tf_s
        );
        println!(
            "tf speedup D/B = {:.2}x   (D_tf={:.3}s / B_tf={:.3}s)",
            seg_b.tf_s / seg_d.tf_s.max(1e-9),
            seg_d.tf_s,
            seg_b.tf_s
        );
    }
    println!(
        "total speedup C/B = {:.2}x   total speedup D/B = {:.2}x",
        seg_b.total_s / seg_c.total_s.max(1e-9),
        seg_b.total_s / seg_d.total_s.max(1e-9)
    );
    println!("===================================================");
    Ok(())
}

#[derive(Clone, Copy)]
enum Phase {
    StgOnly,
    TfOnlyReuseStg,
    ObtOnlyReuseTf,
}

/// Timed B/C phase. For Tf/Obt, reuses lake files via StgLakeReader (no bronze).
async fn timed_rust_phase(
    project: &Path,
    out_root: &str,
    symbols_csv: &str,
    phase: Phase,
    partition: bool,
    jobs: usize,
) -> Result<Duration> {
    let syms = parse_symbols(symbols_csv);
    let silver_stg = project
        .join(out_root)
        .join("silver/stage/stg_ohlcv_1m.parquet");
    let silver_tf = project
        .join(out_root)
        .join("silver/tf/tf_indicators_1m.parquet");
    let gold_obt = project
        .join(out_root)
        .join("gold/obt_stocks_1m.parquet");

    let mut config = if partition {
        load_project_yml(project, PARALLEL_PROJECT_YML)?
    } else {
        load_project_yml(project, RUST_PROJECT_YML)?
    };

    if partition {
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

    let t0 = Instant::now();
    match phase {
        Phase::StgOnly => {
            let stg_path = silver_stg.to_string_lossy().into_owned();
            let dag = DagBuilder::new()
                .model(
                    ModelSpec::rust(STG_OHLCV_1M_NAME)
                        .sources([("bronze", "ohlcv_1m")])
                        .layer(ModelLayer::Staging)
                        .materialization(Materialization::Table)
                        .output_path(&stg_path)
                        .frontmatter(models::silver::staging::stg_ohlcv_1m::scan_frontmatter()),
                )
                .build()?;
            let mut scope = RunScope::new().with_var_multi("symbol", syms)?;
            scope.skip_if_fingerprint_match = false;
            scope.write_receipt = false;
            let engine = RbtEngineBuilder::new()
                .with_rust_model(StgOhlcv1m)
                .build()
                .await?;
            engine
                .execute_dag_with_scope(&dag, project, &project.join("lake"), &config, &scope)
                .await?;
        }
        Phase::TfOnlyReuseStg => {
            let stg_path = silver_stg.to_string_lossy().into_owned();
            let tf_path = silver_tf.to_string_lossy().into_owned();
            let mut tf_spec = ModelSpec::rust(TF_INDICATORS_NAME)
                .refs([STG_OHLCV_1M_NAME])
                .layer(ModelLayer::Transform)
                .output_path(&tf_path);
            if partition {
                tf_spec = tf_spec
                    .materialization(Materialization::ScopedReplace)
                    .frontmatter(
                        models::silver::transforms::tf_indicators_1m::partition_frontmatter(),
                    );
            } else {
                tf_spec = tf_spec.materialization(Materialization::Table);
            }
            let dag = DagBuilder::new()
                .model(
                    ModelSpec::rust(STG_OHLCV_1M_NAME)
                        .layer(ModelLayer::Staging)
                        .materialization(Materialization::Table)
                        .output_path(&stg_path)
                        .description("reuse lake stg (no bronze)"),
                )
                .model(tf_spec)
                .build()?;
            let mut scope = RunScope::new().with_var_multi("symbol", syms)?;
            scope.skip_if_fingerprint_match = false;
            scope.write_receipt = false;
            let engine = RbtEngineBuilder::new()
                .with_rust_model(StgLakeReader {
                    parquet_path: silver_stg.clone(),
                })
                .with_rust_model(TfIndicators1m)
                .build()
                .await?;
            engine
                .execute_dag_with_scope(&dag, project, &project.join("lake"), &config, &scope)
                .await?;
        }
        Phase::ObtOnlyReuseTf => {
            // Stream indicators from lake: either single parquet or parts dir via ref.
            // We re-run tf as StgLakeReader-style... simpler: Obt reads tf via SQL on path.
            // Build: fake tf as lake reader + obt identity.
            let tf_path = if partition {
                project
                    .join(out_root)
                    .join("silver/tf/tf_indicators_1m.parts")
            } else {
                silver_tf.clone()
            };
            let gold = gold_obt.to_string_lossy().into_owned();
            // Use Obt that SQLs from registered name; register tf via a tiny passthrough.
            // Simplest: ModelSpec sql obt that scans is hard. Use ObtStocks1m + register
            // tf_indicators_1m by running a no-op? 
            // Re-use Tf as lake file for identity obt: register parquet/parts as table.
            let dag = DagBuilder::new()
                .model(
                    ModelSpec::rust(TF_INDICATORS_NAME)
                        .layer(ModelLayer::Transform)
                        .materialization(Materialization::Table)
                        .output_path(silver_tf.to_string_lossy().as_ref())
                        .description("identity placeholder for obt ref"),
                )
                .model(
                    ModelSpec::rust(OBT_STOCKS_1M_NAME)
                        .refs([TF_INDICATORS_NAME])
                        .layer(ModelLayer::Mart)
                        .materialization(Materialization::Table)
                        .output_path(&gold),
                )
                .build()?;
            let mut scope = RunScope::new();
            scope.skip_if_fingerprint_match = false;
            scope.write_receipt = false;
            // TfIndicators passthrough from lake: use StgLakeReader pattern for indicators schema
            // Register existing tf file under model name via a thin wrapper.
            let engine = RbtEngineBuilder::new()
                .with_rust_model(TfLakeReader {
                    parquet_or_parts: tf_path,
                })
                .with_rust_model(ObtStocks1m)
                .build()
                .await?;
            // Disable partition for obt phase (no multi fan-out needed).
            config.execution.concurrency.enabled = false;
            config.execution.concurrency.strategy = rbt::ExecutionStrategy::Serial;
            engine
                .execute_dag_with_scope(&dag, project, &project.join("lake"), &config, &scope)
                .await?;
        }
    }
    Ok(t0.elapsed())
}

/// Re-read indicators lake output (file or parts dir) as `tf_indicators_1m`.
struct TfLakeReader {
    parquet_or_parts: PathBuf,
}

#[async_trait::async_trait]
impl RustModel for TfLakeReader {
    fn name(&self) -> &str {
        TF_INDICATORS_NAME
    }
    fn output_schema(&self) -> rbt::arrow::datatypes::SchemaRef {
        models::silver::transforms::ta_kernels::indicators_schema()
    }
    async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput> {
        let p = &self.parquet_or_parts;
        if !p.exists() {
            bail!("E_DEMO: TfLakeReader missing {}", p.display());
        }
        let path = p.to_str().context("path utf8")?;
        let tmp = "__rbt_tf_lake_src";
        if ctx.session.table_exist(tmp).unwrap_or(false) {
            let _ = ctx.session.deregister_table(tmp);
        }
        ctx.session
            .register_parquet(tmp, path, rbt::datafusion::prelude::ParquetReadOptions::default())
            .await
            .context("register tf lake")?;
        let df = ctx
            .session
            .sql(&format!(r#"SELECT * FROM "{tmp}""#))
            .await?;
        Ok(RustModelOutput::Stream(df.execute_stream().await?))
    }
}

async fn timed_design_d_phase(
    project: &Path,
    syms: &[String],
    jobs: usize,
    phase: Phase,
) -> Result<Duration> {
    let out_root = "lake/design_d_models_output";
    let silver_stg = project
        .join(out_root)
        .join("silver/stage/stg_ohlcv_1m.parquet");
    let gold_obt = project
        .join(out_root)
        .join("gold/obt_stocks_1m.parquet")
        .to_string_lossy()
        .into_owned();

    let mut config = load_project_yml(project, RUST_PROJECT_YML)?;
    config.execution.concurrency.enabled = true;
    config.execution.concurrency.strategy = rbt::ExecutionStrategy::ModelTier;
    config.execution.concurrency.max_workers = jobs.max(1);
    if jobs > 1 {
        config.execution.concurrency.apply_jobs(jobs);
        config.execution.concurrency.strategy = rbt::ExecutionStrategy::ModelTier;
    }

    let t0 = Instant::now();
    match phase {
        Phase::StgOnly => {
            let stg_path = silver_stg.to_string_lossy().into_owned();
            let dag = DagBuilder::new()
                .model(
                    ModelSpec::rust(STG_OHLCV_1M_NAME)
                        .sources([("bronze", "ohlcv_1m")])
                        .layer(ModelLayer::Staging)
                        .materialization(Materialization::Table)
                        .output_path(&stg_path)
                        .frontmatter(models::silver::staging::stg_ohlcv_1m::scan_frontmatter()),
                )
                .build()?;
            // No multi-value: full symbol set written (filter optional).
            let mut scope = RunScope::new().with_var_multi("symbol", syms.to_vec())?;
            scope.skip_if_fingerprint_match = false;
            scope.write_receipt = false;
            let engine = RbtEngineBuilder::new()
                .with_rust_model(StgOhlcv1m)
                .build()
                .await?;
            engine
                .execute_dag_with_scope(&dag, project, &project.join("lake"), &config, &scope)
                .await?;
        }
        Phase::TfOnlyReuseStg => {
            let stg_path = silver_stg.to_string_lossy().into_owned();
            let mut builder = DagBuilder::new().model(
                ModelSpec::rust(STG_OHLCV_1M_NAME)
                    .layer(ModelLayer::Staging)
                    .materialization(Materialization::Table)
                    .output_path(&stg_path),
            );
            let mut models_v = Vec::new();
            for sym in syms {
                let m = TfSymbolIndicators::new(sym.clone());
                let name = m.model_name.clone();
                let tf_path = project
                    .join(out_root)
                    .join(format!("silver/tf/{name}.parquet"))
                    .to_string_lossy()
                    .into_owned();
                builder = builder.model(
                    ModelSpec::rust(&name)
                        .refs([STG_OHLCV_1M_NAME])
                        .layer(ModelLayer::Transform)
                        .materialization(Materialization::Table)
                        .output_path(&tf_path),
                );
                models_v.push(m);
            }
            let dag = builder.build()?;
            let mut scope = RunScope::new();
            scope.skip_if_fingerprint_match = false;
            scope.write_receipt = false;
            let mut eng = RbtEngineBuilder::new().with_rust_model(StgLakeReader {
                parquet_path: silver_stg.clone(),
            });
            for m in models_v {
                eng = eng.with_rust_model(m);
            }
            let engine = eng.build().await?;
            engine
                .execute_dag_with_scope(&dag, project, &project.join("lake"), &config, &scope)
                .await?;
        }
        Phase::ObtOnlyReuseTf => {
            // Single model: UNION ALL over already-written per-symbol parquet files
            // (no re-materialize of each tf_*).
            let mut paths = Vec::new();
            for sym in syms {
                let name = models::tf_symbol_model_name(sym);
                paths.push(
                    project
                        .join(out_root)
                        .join(format!("silver/tf/{name}.parquet")),
                );
            }
            let dag = DagBuilder::new()
                .model(
                    ModelSpec::rust(OBT_STOCKS_1M_NAME)
                        .layer(ModelLayer::Mart)
                        .materialization(Materialization::Table)
                        .output_path(&gold_obt)
                        .description("UNION lake per-symbol tf parquets"),
                )
                .build()?;
            let mut scope = RunScope::new();
            scope.skip_if_fingerprint_match = false;
            scope.write_receipt = false;
            config.execution.concurrency.enabled = false;
            let engine = RbtEngineBuilder::new()
                .with_rust_model(ObtUnionPaths { paths })
                .build()
                .await?;
            engine
                .execute_dag_with_scope(&dag, project, &project.join("lake"), &config, &scope)
                .await?;
        }
    }
    Ok(t0.elapsed())
}

/// Design D obt-only timer: stream UNION ALL of existing per-symbol parquet files.
struct ObtUnionPaths {
    paths: Vec<PathBuf>,
}

#[async_trait::async_trait]
impl RustModel for ObtUnionPaths {
    fn name(&self) -> &str {
        OBT_STOCKS_1M_NAME
    }
    fn output_schema(&self) -> rbt::arrow::datatypes::SchemaRef {
        models::silver::transforms::ta_kernels::indicators_schema()
    }
    async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput> {
        let mut parts = Vec::new();
        for (i, p) in self.paths.iter().enumerate() {
            if !p.is_file() {
                bail!("E_DEMO: missing per-symbol tf {}", p.display());
            }
            let tmp = format!("__rbt_d_obt_{i}");
            let path = p.to_str().context("utf8 path")?;
            if ctx.session.table_exist(&tmp).unwrap_or(false) {
                let _ = ctx.session.deregister_table(&tmp);
            }
            ctx.session
                .register_parquet(
                    &tmp,
                    path,
                    rbt::datafusion::prelude::ParquetReadOptions::default(),
                )
                .await
                .with_context(|| format!("register {}", p.display()))?;
            parts.push(format!(r#"SELECT * FROM "{tmp}""#));
        }
        if parts.is_empty() {
            bail!("E_DEMO: ObtUnionPaths empty");
        }
        let sql = parts.join(" UNION ALL ");
        let df = ctx.session.sql(&sql).await.context("union all per-symbol")?;
        Ok(RustModelOutput::Stream(df.execute_stream().await?))
    }
}

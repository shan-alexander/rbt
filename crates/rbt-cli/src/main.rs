use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use rbt_core::dag::OutputFormat;
use rbt_core::{model_has_test_contract, BronzeCheckMode, RbtProjectConfig, SelectMode};
use rbt_engine::TransformationEngine;

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "rbt",
    version,
    about = "Rust lake build tool: medallion SQL DAGs on Parquet/Iceberg-style tables"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
enum CliFormat {
    /// Single Parquet file per model (default full-rewrite path)
    Parquet,
    Jsonl,
    Csv,
    /// Filesystem Iceberg-style table directory (data/ + metadata/)
    Iceberg,
    /// Dual-write: flat .parquet + sibling .iceberg/ table dir
    ParquetAndIceberg,
    /// Currently materializes Parquet (clone semantics reserved)
    ZeroCopyClone,
}

impl From<CliFormat> for OutputFormat {
    fn from(fmt: CliFormat) -> Self {
        match fmt {
            CliFormat::Parquet => OutputFormat::Parquet,
            CliFormat::Jsonl => OutputFormat::Jsonl,
            CliFormat::Csv => OutputFormat::Csv,
            CliFormat::Iceberg => OutputFormat::Iceberg,
            CliFormat::ParquetAndIceberg => OutputFormat::ParquetAndIceberg,
            CliFormat::ZeroCopyClone => OutputFormat::ZeroCopyClone,
        }
    }
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
enum CliBronzeCheck {
    Off,
    Warn,
    Fail,
}

impl From<CliBronzeCheck> for BronzeCheckMode {
    fn from(v: CliBronzeCheck) -> Self {
        match v {
            CliBronzeCheck::Off => BronzeCheckMode::Off,
            CliBronzeCheck::Warn => BronzeCheckMode::Warn,
            CliBronzeCheck::Fail => BronzeCheckMode::Fail,
        }
    }
}

impl FromStr for CliBronzeCheck {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "warn" | "warning" => Ok(Self::Warn),
            "fail" | "error" => Ok(Self::Fail),
            other => Err(format!(
                "invalid bronze-check '{}'; expected off|warn|fail",
                other
            )),
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Compile SQL models, build DAG, validate bronze frontmatter scan paths
    Compile {
        #[arg(short, long, default_value = ".")]
        project_dir: PathBuf,
        /// Model selector: name, +name, name+, comma-separated (dbt-like)
        #[arg(short = 's', long)]
        select: Option<String>,
        /// How to treat missing/invalid bronze scan_path: off | warn | fail
        #[arg(long, value_enum, default_value_t = CliBronzeCheck::Warn)]
        bronze_check: CliBronzeCheck,
    },
    /// Execute transformation pipeline DAG with frontmatter-driven bronze registration
    Run {
        #[arg(short, long, default_value = ".")]
        project_dir: PathBuf,
        /// Model selector: name, +name, name+, comma-separated. Ancestors always included.
        #[arg(short = 's', long)]
        select: Option<String>,
        #[arg(short, long, default_value = "./target/output")]
        output_dir: PathBuf,
        /// Output format override for all models in this run
        #[arg(short, long, value_enum, default_value_t = CliFormat::Parquet)]
        format: CliFormat,
        /// Pre-flight bronze path check before execution (default: fail)
        #[arg(long, value_enum, default_value_t = CliBronzeCheck::Fail)]
        bronze_check: CliBronzeCheck,
    },
    /// Run frontmatter tests for selected models (executes subgraph then asserts)
    Test {
        #[arg(short, long, default_value = ".")]
        project_dir: PathBuf,
        /// Model selector (default: all models that declare tests or grain/unique_key)
        #[arg(short = 's', long)]
        select: Option<String>,
        /// Optional single model (alias for --select)
        #[arg(short, long)]
        model: Option<String>,
        #[arg(short, long, default_value = "./target/output")]
        output_dir: PathBuf,
        #[arg(short, long, value_enum, default_value_t = CliFormat::Parquet)]
        format: CliFormat,
        #[arg(long, value_enum, default_value_t = CliBronzeCheck::Fail)]
        bronze_check: CliBronzeCheck,
    },
    /// Run micro-benchmarks measuring transformation engine throughput (rows/sec)
    Bench {
        #[arg(short, long, default_value_t = 1000000)]
        num_rows: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Compile {
            project_dir,
            select,
            bronze_check,
        } => {
            println!(
                "[rbt] Compiling project {:?} (select={:?}, bronze_check={})...",
                project_dir,
                select,
                BronzeCheckMode::from(bronze_check.clone())
            );
            let config = RbtProjectConfig::load(&project_dir)?;
            let full = config.build_dag(&project_dir, None)?;
            let dag = full.apply_select(select.as_deref(), SelectMode::Exact)?;
            let tiers = dag.execution_tiers()?;
            println!(
                "[rbt] DAG built with {} model(s) in {} tier(s):",
                dag.node_map.len(),
                tiers.len()
            );
            for (i, tier) in tiers.iter().enumerate() {
                let names: Vec<&str> = tier.iter().map(|m| m.name.as_str()).collect();
                println!("  Tier {}: {:?}", i, names);
            }

            // Bronze validation on full project (or selected subgraph for paths)
            let report = dag.validate_bronze_sources(&project_dir, bronze_check.into())?;
            for d in &report.diagnostics {
                eprintln!("{d}");
            }
            if report.has_errors() {
                bail!(
                    "[rbt] compile failed: {} bronze error(s), {} warning(s)",
                    report.error_count(),
                    report.warning_count()
                );
            }
            if report.warning_count() > 0 {
                println!(
                    "[rbt] compile succeeded with {} bronze warning(s)",
                    report.warning_count()
                );
            } else {
                println!("[rbt] compile succeeded (bronze sources ok)");
            }
        }
        Commands::Run {
            project_dir,
            select,
            output_dir,
            format,
            bronze_check,
        } => {
            println!(
                "[rbt] Executing pipeline from {:?} (select: {:?}, format: {:?}, output: {:?}, bronze_check={:?})...",
                project_dir, select, format, output_dir, bronze_check
            );
            let start = Instant::now();

            let config = RbtProjectConfig::load(&project_dir)?;
            let full = config.build_dag(&project_dir, Some(format.into()))?;
            let dag = full
                .apply_select(select.as_deref(), SelectMode::Execute)
                .context("invalid --select")?;

            let report = dag.validate_bronze_sources(&project_dir, bronze_check.into())?;
            for d in &report.diagnostics {
                eprintln!("{d}");
            }
            if report.has_errors() {
                bail!(
                    "[rbt] run aborted: {} bronze error(s) — fix scan_path/frontmatter or use --bronze-check=warn",
                    report.error_count()
                );
            }

            println!(
                "[rbt] Running {} model(s): {:?}",
                dag.node_map.len(),
                dag.topological_sequence()?
                    .iter()
                    .map(|m| m.name.as_str())
                    .collect::<Vec<_>>()
            );

            let engine = TransformationEngine::new();
            let summary = engine
                .execute_dag(&dag, &project_dir, &output_dir)
                .await?;
            let duration = start.elapsed();

            println!(
                "[rbt] Completed {} models ({} rows, {} bronze sources) in {:.2?}",
                summary.models_executed,
                summary.total_rows_produced,
                summary.bronze_sources_registered,
                duration
            );
        }
        Commands::Test {
            project_dir,
            select,
            model,
            output_dir,
            format,
            bronze_check,
        } => {
            let select = match (select, model) {
                (Some(s), Some(m)) => Some(format!("{},{}", s, m)),
                (Some(s), None) => Some(s),
                (None, Some(m)) => Some(m),
                (None, None) => None,
            };

            println!(
                "[rbt] Testing project {:?} (select={:?})...",
                project_dir, select
            );

            let config = RbtProjectConfig::load(&project_dir)?;
            let full = config.build_dag(&project_dir, Some(format.into()))?;

            // Default: models that declare tests or grain/unique_key
            let select_spec = if let Some(s) = select {
                s
            } else {
                let names = full.models_with_test_contract()?;
                if names.is_empty() {
                    println!("[rbt] No models with frontmatter tests/grain/unique_key found.");
                    return Ok(());
                }
                names.join(",")
            };

            let dag = full
                .apply_select(Some(&select_spec), SelectMode::Execute)
                .context("invalid --select / --model")?;

            let report = dag.validate_bronze_sources(&project_dir, bronze_check.into())?;
            for d in &report.diagnostics {
                eprintln!("{d}");
            }
            if report.has_errors() {
                bail!(
                    "[rbt] test aborted: {} bronze error(s)",
                    report.error_count()
                );
            }

            println!(
                "[rbt] Executing {} model(s) then validating frontmatter tests...",
                dag.node_map.len()
            );

            let engine = TransformationEngine::new();
            // execute_dag already runs frontmatter tests and fails hard on error
            let summary = engine
                .execute_dag(&dag, &project_dir, &output_dir)
                .await?;

            // Summarize which models had tests (already enforced during materialize)
            let mut tested = 0usize;
            let mut without = 0usize;
            for node in dag.topological_sequence()? {
                if model_has_test_contract(&node) {
                    tested += 1;
                    println!(
                        "  PASS  {} (frontmatter tests executed during materialize)",
                        node.name
                    );
                } else {
                    without += 1;
                    println!("  SKIP  {} (no tests/grain/unique_key)", node.name);
                }
            }

            println!(
                "[rbt] Tests finished: {} models with tests, {} skipped, {} rows produced across run",
                tested, without, summary.total_rows_produced
            );
        }
        Commands::Bench { num_rows } => {
            println!(
                "[rbt BENCHMARK] Generating & transforming {} rows in-memory...",
                num_rows
            );
            let start = Instant::now();

            let engine = TransformationEngine::new();
            let query = format!(
                "SELECT id, id * 2 AS val, CONCAT('user_', CAST(id AS VARCHAR)) AS name FROM (SELECT \"range()\".value AS id FROM range(0, {}))",
                num_rows
            );

            let df = engine.ctx.sql(&query).await?;
            let batches = df.collect().await?;
            let duration = start.elapsed();

            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let rows_per_sec = (total_rows as f64) / duration.as_secs_f64();

            println!(
                "[rbt BENCHMARK RESULT] Processed {} rows in {:.2?} ({:.0} rows/sec)",
                total_rows, duration, rows_per_sec
            );
        }
    }

    Ok(())
}

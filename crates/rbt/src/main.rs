use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use rbt::{
    consolidate_parts_to_parquet, model_has_test_contract, parts_dir_for_parquet, run_contract_diff,
    BronzeCheckMode, MaterializeWriteOptions, OutputFormat, RbtProjectConfig, RunScope, SelectMode,
    TransformationEngine,
};

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
use tracing_subscriber::EnvFilter;

/// Shared run-scope flags for execute paths (P5a/P5b).
#[derive(Debug, Clone, clap::Args)]
struct RunScopeArgs {
    /// Run variable / partition bind (`key=value`). Repeatable.
    ///
    /// Repeated keys with different values become a multi-value set (RBT-A1):
    /// `--var entity=a.com --var entity=b.com` → partition filter `entity IN (...)`.
    /// JSON array form: `--var entity:=["a.com","b.com"]`.
    /// Merges into `require_partitions` / `require_partitions_in` for keys in
    /// model `partition_by`. Expands `{key}` / `${key}` in paths (**scalar only**).
    #[arg(long = "var", value_name = "KEY=VALUE")]
    vars: Vec<String>,
    /// Load multi-value var from a file (`key=path`, one value per line, `#` comments).
    #[arg(long = "var-file", value_name = "KEY=PATH")]
    var_files: Vec<String>,
    /// Contract version for fingerprints (overrides `contract_version` in yml).
    #[arg(long)]
    contract_version: Option<String>,
    /// Explicit run id for receipts (default: generated).
    #[arg(long)]
    run_id: Option<String>,
    /// Skip materialize when bronze fingerprint matches last successful receipt for this scope.
    #[arg(long, default_value_t = false)]
    skip_if_match: bool,
    /// Write `.rbt/runs/{run_id}.json` receipt (default: true for run/test).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    write_receipt: bool,
    /// Emit run receipt JSON to stdout after success.
    #[arg(long, default_value_t = false)]
    receipt_json: bool,
    /// Override bronze fingerprint mode for this run: `path_stat` | `content_hash` (RBT-A4).
    /// Also: env `RBT_FINGERPRINT_MODE`. Project default in `fingerprint.mode` yml.
    #[arg(long, value_name = "MODE")]
    fingerprint_mode: Option<String>,
}

impl RunScopeArgs {
    fn to_scope(&self) -> Result<RunScope> {
        let mut scope = RunScope::new();
        scope.write_receipt = self.write_receipt;
        scope.skip_if_fingerprint_match = self.skip_if_match;
        scope.contract_version = self.contract_version.clone();
        scope.run_id = self.run_id.clone();
        scope.extend_from_env();
        scope.extend_from_kv_pairs(self.vars.iter().map(String::as_str))?;
        scope.extend_from_var_files(self.var_files.iter().map(String::as_str))?;
        Ok(scope)
    }

    /// Apply CLI fingerprint mode onto a loaded project config (after `RbtProjectConfig::load`).
    fn apply_fingerprint_override(
        &self,
        config: &mut rbt::RbtProjectConfig,
    ) -> Result<()> {
        if let Some(ref m) = self.fingerprint_mode {
            config.fingerprint.mode = rbt::FingerprintMode::parse(m)?;
        }
        Ok(())
    }
}

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
        /// Emit machine-readable run summary JSON to stdout (A3.5). Suppresses most human banners.
        /// Includes models[] (phase/tags/elapsed_ms). Full on-disk receipt still written when enabled.
        #[arg(long, default_value_t = false)]
        json: bool,
        #[command(flatten)]
        scope: RunScopeArgs,
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
        #[command(flatten)]
        scope: RunScopeArgs,
    },
    /// Static validation: load project, build DAG, bronze paths, layer rules (no execute)
    Validate {
        #[arg(short, long, default_value = ".")]
        project_dir: PathBuf,
        #[arg(short = 's', long)]
        select: Option<String>,
        #[arg(long, value_enum, default_value_t = CliBronzeCheck::Fail)]
        bronze_check: CliBronzeCheck,
        /// Sample bronze for `contracts.enums` and report values missing from the registry
        #[arg(long, default_value_t = false)]
        contract_diff: bool,
        /// Emit JSON report to stdout (machine-readable)
        #[arg(long, default_value_t = false)]
        json: bool,
        /// Optional run vars for contract-diff partition filters (`key=value`)
        #[arg(long = "var", value_name = "KEY=VALUE")]
        vars: Vec<String>,
        /// Multi-value var from file for contract-diff (`key=path`)
        #[arg(long = "var-file", value_name = "KEY=PATH")]
        var_files: Vec<String>,
    },
    /// Explain a model: compiled SQL, deps, layer, bronze contract, output path
    Explain {
        #[arg(short, long, default_value = ".")]
        project_dir: PathBuf,
        /// Model name (required)
        #[arg(short = 's', long)]
        select: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Preview model rows: materialize ancestors, run target SQL with LIMIT (no target write)
    Preview {
        #[arg(short, long, default_value = ".")]
        project_dir: PathBuf,
        /// Single model name
        #[arg(short = 's', long)]
        select: String,
        #[arg(short, long, default_value = "./target/output")]
        output_dir: PathBuf,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(short, long, value_enum, default_value_t = CliFormat::Parquet)]
        format: CliFormat,
        #[arg(long, value_enum, default_value_t = CliBronzeCheck::Fail)]
        bronze_check: CliBronzeCheck,
    },
    /// Thesis measure packs (wall time, rows, optional RSS) — honest experiment harness
    Measure {
        #[arg(short, long, default_value = ".")]
        project_dir: PathBuf,
        /// Scenario: smoke_pipeline | validate_dx | incremental_append |
        /// stream_vs_collect | whale_synthetic | complex_bronze
        #[arg(long, default_value = "smoke_pipeline")]
        scenario: String,
        #[arg(short, long, default_value = "./target/output")]
        output_dir: PathBuf,
        /// Write JSON report (default: {project}/.rbt/measure/{scenario}.json)
        #[arg(long)]
        report: Option<PathBuf>,
        /// Print JSON report to stdout
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Run micro-benchmarks measuring transformation engine throughput (rows/sec)
    Bench {
        #[arg(short, long, default_value_t = 1000000)]
        num_rows: usize,
    },
    /// Rebuild a single monolith parquet from a model's `.parts/` directory (RBT-A5 ops)
    Consolidate {
        #[arg(short, long, default_value = ".")]
        project_dir: PathBuf,
        /// Single model name whose parts should be consolidated
        #[arg(short = 's', long)]
        select: String,
        /// Fallback output dir when the model has no resolved `output_path`
        #[arg(short, long, default_value = "./target/output")]
        output_dir: PathBuf,
        /// Emit JSON result to stdout
        #[arg(long, default_value_t = false)]
        json: bool,
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
            let report = dag.validate_bronze_sources_with_roots(
                &project_dir,
                bronze_check.into(),
                &config.roots,
            )?;
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
            json,
            scope,
        } => {
            let run_scope = scope.to_scope()?;
            let quiet = json; // machine summary owns stdout
            if !quiet {
                println!(
                    "[rbt] Executing pipeline from {:?} (select: {:?}, format: {:?}, output: {:?}, bronze_check={:?}, vars={:?})...",
                    project_dir, select, format, output_dir, bronze_check, run_scope.vars
                );
            }
            let start = Instant::now();

            let mut config = RbtProjectConfig::load(&project_dir)?;
            scope.apply_fingerprint_override(&mut config)?;
            let full = config.build_dag(&project_dir, Some(format.into()))?;
            let dag = full
                .apply_select(select.as_deref(), SelectMode::Execute)
                .context("invalid --select")?;

            let report = dag.validate_bronze_sources_with_roots(
                &project_dir,
                bronze_check.into(),
                &config.roots,
            )?;
            for d in &report.diagnostics {
                eprintln!("{d}");
            }
            if report.has_errors() {
                bail!(
                    "[rbt] run aborted: {} bronze error(s) — fix scan_path/frontmatter or use --bronze-check=warn",
                    report.error_count()
                );
            }

            if !quiet {
                println!(
                    "[rbt] Running {} model(s): {:?}",
                    dag.node_map.len(),
                    dag.topological_sequence()?
                        .iter()
                        .map(|m| m.name.as_str())
                        .collect::<Vec<_>>()
                );
            }

            let engine = TransformationEngine::new();
            let summary = engine
                .execute_dag_with_scope(&dag, &project_dir, &output_dir, &config, &run_scope)
                .await?;
            let duration = start.elapsed();
            let wall_ms = duration.as_millis();

            if json {
                // A3.5: compact run summary for hosts (serde_json — not jshift; jshift is bronze extract).
                let body = serde_json::json!({
                    "ok": !summary.skipped || summary.skip_reason.is_some(),
                    "skipped": summary.skipped,
                    "skip_reason": summary.skip_reason,
                    "run_id": summary.run_id,
                    "project": config.name,
                    "package_version": rbt::VERSION,
                    "models_executed": summary.models_executed,
                    "total_rows": summary.total_rows_produced,
                    "bronze_sources": summary.bronze_sources_registered,
                    "bronze_fingerprint": summary.bronze_fingerprint,
                    "receipt_path": summary.receipt_path.as_ref().map(|p| p.display().to_string()),
                    "wall_ms": wall_ms,
                    "models": summary.model_results,
                });
                println!("{}", serde_json::to_string_pretty(&body)?);
            } else {
                if summary.skipped {
                    println!(
                        "[rbt] SKIPPED materialize (fingerprint match) in {:.2?} — {}",
                        duration,
                        summary.skip_reason.as_deref().unwrap_or("identical bronze")
                    );
                } else {
                    println!(
                        "[rbt] Completed {} models ({} rows, {} bronze sources) in {:.2?}",
                        summary.models_executed,
                        summary.total_rows_produced,
                        summary.bronze_sources_registered,
                        duration
                    );
                }
                if let Some(ref fp) = summary.bronze_fingerprint {
                    println!("[rbt] bronze_fingerprint={fp}");
                }
                if let Some(ref p) = summary.receipt_path {
                    println!("[rbt] receipt={}", p.display());
                }
            }
            // Full on-disk receipt dump (may coexist with --json for debugging)
            if scope.receipt_json {
                if let Some(ref p) = summary.receipt_path {
                    if json {
                        // avoid mixing two JSON docs on stdout; put full receipt path only in summary
                        eprintln!("[rbt] full receipt at {}", p.display());
                    } else {
                        print!("{}", std::fs::read_to_string(p)?);
                    }
                }
            }
        }
        Commands::Test {
            project_dir,
            select,
            model,
            output_dir,
            format,
            bronze_check,
            scope,
        } => {
            let select = match (select, model) {
                (Some(s), Some(m)) => Some(format!("{},{}", s, m)),
                (Some(s), None) => Some(s),
                (None, Some(m)) => Some(m),
                (None, None) => None,
            };
            let run_scope = scope.to_scope()?;
            let mut config = RbtProjectConfig::load(&project_dir)?;
            scope.apply_fingerprint_override(&mut config)?;

            println!(
                "[rbt] Testing project {:?} (select={:?})...",
                project_dir, select
            );

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

            let report = dag.validate_bronze_sources_with_roots(
                &project_dir,
                bronze_check.into(),
                &config.roots,
            )?;
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
            let summary = engine
                .execute_dag_with_scope(&dag, &project_dir, &output_dir, &config, &run_scope)
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
        Commands::Validate {
            project_dir,
            select,
            bronze_check,
            contract_diff,
            json,
            vars,
            var_files,
        } => {
            let config = RbtProjectConfig::load(&project_dir)?;
            let full = config.build_dag(&project_dir, None)?;
            let dag = full.apply_select(select.as_deref(), SelectMode::Exact)?;
            let tiers = dag.execution_tiers()?;
            let report = dag.validate_bronze_sources_with_roots(
                &project_dir,
                bronze_check.into(),
                &config.roots,
            )?;

            let mut scope = RunScope::new();
            scope.extend_from_kv_pairs(vars.iter().map(String::as_str))?;
            scope.extend_from_var_files(var_files.iter().map(String::as_str))?;

            let mut contract_diff_report = None;
            if contract_diff {
                contract_diff_report =
                    Some(run_contract_diff(&project_dir, &config, &full, &scope)?);
            }

            let mut issues: Vec<String> = Vec::new();
            for d in &report.diagnostics {
                issues.push(d.to_string());
            }
            // Layer / ref hygiene: every ref must resolve in full project
            for node in full.topological_sequence()? {
                for dep in &node.dependencies {
                    if let rbt::DependencyRef::Model(name) = dep {
                        if !full.node_map.contains_key(name) {
                            issues.push(format!(
                                "E_RBT_VALIDATE_REF: model '{}' refs unknown model '{}'",
                                node.name, name
                            ));
                        }
                    }
                }
            }
            if let Some(cd) = &contract_diff_report {
                for d in &cd.diagnostics {
                    issues.push(d.clone());
                }
                for n in &cd.notes {
                    issues.push(format!("NOTE: {n}"));
                }
            }

            let ok = !report.has_errors()
                && !issues.iter().any(|i| i.contains("E_RBT_VALIDATE_REF"))
                && contract_diff_report
                    .as_ref()
                    .map(|c| c.ok && !c.has_errors())
                    .unwrap_or(true);

            if json {
                let body = serde_json::json!({
                    "ok": ok,
                    "project": config.name,
                    "models": dag.node_map.len(),
                    "tiers": tiers.len(),
                    "bronze_errors": report.error_count(),
                    "bronze_warnings": report.warning_count(),
                    "issues": issues,
                    "modeling_warnings": report.diagnostics.iter()
                        .filter(|d| d.code.starts_with("W_RBT_"))
                        .map(|d| d.to_string())
                        .collect::<Vec<_>>(),
                    "tier_plan": tiers.iter().map(|t| {
                        t.iter().map(|m| m.name.clone()).collect::<Vec<_>>()
                    }).collect::<Vec<_>>(),
                    "contract_diff": contract_diff_report,
                });
                println!("{}", serde_json::to_string_pretty(&body)?);
            } else {
                println!(
                    "[rbt] validate project {:?} ({} model(s), {} tier(s))",
                    project_dir,
                    dag.node_map.len(),
                    tiers.len()
                );
                for (i, tier) in tiers.iter().enumerate() {
                    let names: Vec<&str> = tier.iter().map(|m| m.name.as_str()).collect();
                    println!("  Tier {i}: {names:?}");
                }
                for d in &report.diagnostics {
                    eprintln!("{d}");
                }
                for i in &issues {
                    if i.starts_with("E_RBT_VALIDATE")
                        || i.starts_with("E_RBT_CONTRACT")
                        || i.starts_with("W_RBT_CONTRACT")
                        || i.starts_with("NOTE:")
                    {
                        eprintln!("{i}");
                    }
                }
                if let Some(cd) = &contract_diff_report {
                    println!(
                        "[rbt] contract-diff: {} enum(s), {} column probe(s)",
                        cd.enums_checked,
                        cd.columns.len()
                    );
                    for col in &cd.columns {
                        println!(
                            "  {} {}.{}: observed={} new_in_bronze={} files={} rows={}",
                            col.enum_name,
                            col.model,
                            col.column,
                            col.observed.len(),
                            col.new_in_bronze.len(),
                            col.files_sampled,
                            col.rows_sampled
                        );
                        if !col.new_in_bronze.is_empty() {
                            println!("    new: {:?}", col.new_in_bronze);
                        }
                    }
                }
                if ok {
                    println!(
                        "[rbt] validate OK (bronze errors={}, warnings={})",
                        report.error_count(),
                        report.warning_count()
                    );
                } else {
                    bail!(
                        "[rbt] validate FAILED (bronze errors={}, issues={})",
                        report.error_count(),
                        issues.len()
                    );
                }
            }
            if !ok && json {
                std::process::exit(1);
            }
        }
        Commands::Explain {
            project_dir,
            select,
            json,
        } => {
            let config = RbtProjectConfig::load(&project_dir)?;
            let full = config.build_dag(&project_dir, None)?;
            let name = select.trim();
            let node = full
                .topological_sequence()?
                .into_iter()
                .find(|m| m.name == name)
                .ok_or_else(|| anyhow::anyhow!("E_RBT_EXPLAIN: unknown model '{name}'"))?;

            let deps: Vec<String> = node
                .dependencies
                .iter()
                .map(|d| match d {
                    rbt::DependencyRef::Model(n) => format!("ref:{n}"),
                    rbt::DependencyRef::Source {
                        source_name,
                        table_name,
                    } => format!("source:{source_name}.{table_name}"),
                })
                .collect();
            let fm = node.frontmatter.as_ref();
            if json {
                let body = serde_json::json!({
                    "name": node.name,
                    "layer": format!("{:?}", node.layer),
                    "materialization": format!("{:?}", node.materialization),
                    "output_format": format!("{:?}", node.output_format),
                    "output_path": node.output_path,
                    "dependencies": deps,
                    "description": node.description,
                    "compiled_sql": node.compiled_sql,
                    "bronze": fm.map(|f| serde_json::json!({
                        "scan_path": f.scan_path,
                        "source_format": f.source_format.as_ref().map(|s| s.as_str()),
                        "path_glob": f.path_glob,
                        "partition_by": f.partition_by,
                        "require_partitions": f.require_partitions,
                    })),
                });
                println!("{}", serde_json::to_string_pretty(&body)?);
            } else {
                println!("[rbt] explain model '{}'", node.name);
                println!("  layer:           {:?}", node.layer);
                println!("  materialization: {:?}", node.materialization);
                println!("  output_format:   {:?}", node.output_format);
                if let Some(p) = &node.output_path {
                    println!("  output_path:     {p}");
                }
                println!("  dependencies:    {deps:?}");
                if let Some(f) = fm {
                    if let Some(sp) = &f.scan_path {
                        println!("  bronze.scan_path: {sp}");
                    }
                    if let Some(sf) = &f.source_format {
                        println!("  bronze.format:    {}", sf.as_str());
                    }
                    if let Some(g) = &f.path_glob {
                        println!("  bronze.path_glob: {g:?}");
                    }
                }
                println!("--- compiled SQL ---");
                println!("{}", node.compiled_sql);
            }
        }
        Commands::Preview {
            project_dir,
            select,
            output_dir,
            limit,
            format,
            bronze_check,
        } => {
            let config = RbtProjectConfig::load(&project_dir)?;
            let full = config.build_dag(&project_dir, Some(format.into()))?;
            let report = full.validate_bronze_sources_with_roots(
                &project_dir,
                bronze_check.into(),
                &config.roots,
            )?;
            for d in &report.diagnostics {
                eprintln!("{d}");
            }
            if report.has_errors() {
                bail!(
                    "[rbt] preview aborted: {} bronze error(s)",
                    report.error_count()
                );
            }
            println!(
                "[rbt] preview model '{}' (limit={limit}, ancestors materialize if needed)...",
                select
            );
            let engine = TransformationEngine::new();
            let result = engine
                .preview_model(&full, &project_dir, &output_dir, select.trim(), limit)
                .await?;
            println!(
                "[rbt] preview '{}': {} row(s) (limit {}), {} ancestor model(s) executed",
                result.model, result.rows, result.limit, result.ancestors_executed
            );
            // Print a simple table: schema + first values
            if let Some(batch) = result.batches.first() {
                let schema = batch.schema();
                let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
                println!("columns: {cols:?}");
                let n = batch.num_rows().min(limit);
                for row in 0..n {
                    let mut cells = Vec::new();
                    for c in 0..batch.num_columns() {
                        let arr = batch.column(c);
                        cells.push(arrow_cell_display(arr, row));
                    }
                    println!("  row[{row}]: {}", cells.join(" | "));
                }
                if result.batches.len() > 1 {
                    println!(
                        "  … {} additional batch(es) not printed",
                        result.batches.len() - 1
                    );
                }
            } else {
                println!("  (no rows)");
            }
        }
        Commands::Measure {
            project_dir,
            scenario,
            output_dir,
            report,
            json,
        } => {
            println!(
                "[rbt] measure scenario='{scenario}' project={project_dir:?}"
            );
            let report_data =
                rbt::run_measure_scenario(&scenario, &project_dir, &output_dir).await?;
            let out = report.unwrap_or_else(|| {
                rbt::default_report_path(&project_dir, &scenario)
            });
            rbt::write_measure_report(&report_data, &out)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report_data)?);
            } else {
                println!(
                    "[rbt] measure OK={} wall_ms={} models={} rows={} bronze={} rss_kb={:?}",
                    report_data.ok,
                    report_data.wall_ms,
                    report_data.models_executed,
                    report_data.total_rows,
                    report_data.bronze_sources,
                    report_data.peak_rss_kb
                );
                if let Some(ref mc) = report_data.mode_compare {
                    println!(
                        "[rbt] mode_compare stream_ms={} collect_ms={} stream_rss={:?} collect_rss={:?} rows={}",
                        mc.stream_wall_ms,
                        mc.collect_wall_ms,
                        mc.stream_rss_kb,
                        mc.collect_rss_kb,
                        mc.rows
                    );
                }
                println!("[rbt] report written → {}", out.display());
                for n in &report_data.notes {
                    println!("  note: {n}");
                }
            }
            if !report_data.ok {
                bail!(
                    "[rbt] measure scenario failed: {}",
                    report_data.error.unwrap_or_default()
                );
            }
        }
        Commands::Consolidate {
            project_dir,
            select,
            output_dir,
            json,
        } => {
            let config = RbtProjectConfig::load(&project_dir)?;
            let full = config.build_dag(&project_dir, None)?;
            let name = select.trim();
            let node = full
                .topological_sequence()?
                .into_iter()
                .find(|m| m.name == name)
                .ok_or_else(|| {
                    anyhow::anyhow!("E_RBT_CONSOLIDATE: unknown model '{name}'")
                })?;
            let dest_path = node
                .output_path
                .as_ref()
                .map(PathBuf::from)
                .unwrap_or_else(|| output_dir.join(format!("{}.parquet", node.name)));
            let parts = parts_dir_for_parquet(&dest_path);
            if !parts.is_dir() {
                bail!(
                    "E_RBT_CONSOLIDATE: model '{name}' has no parts directory at {} \
                     (run incremental_append / scoped_replace / consolidate:never first)",
                    parts.display()
                );
            }
            let write_opts = MaterializeWriteOptions::from_config(&config.materialize, true);
            let stats = consolidate_parts_to_parquet(&dest_path, &write_opts)
                .await
                .with_context(|| {
                    format!("E_RBT_CONSOLIDATE: consolidate model '{name}' failed")
                })?;
            if json {
                let body = serde_json::json!({
                    "ok": true,
                    "model": name,
                    "parts_dir": parts.display().to_string(),
                    "monolith": dest_path.display().to_string(),
                    "rows": stats.rows,
                    "bytes_written": stats.bytes_written,
                });
                println!("{}", serde_json::to_string_pretty(&body)?);
            } else {
                println!(
                    "[rbt] consolidate model='{name}' rows={} → {}",
                    stats.rows,
                    dest_path.display()
                );
                println!(
                    "  parts remain authoritative at {}; monolith is a convenience rebuild",
                    parts.display()
                );
            }
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

fn arrow_cell_display(array: &arrow::array::ArrayRef, row: usize) -> String {
    use arrow::array::Array;
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    if array.is_null(row) {
        return "NULL".into();
    }
    let opts = FormatOptions::default().with_display_error(true);
    if let Ok(fmt) = ArrayFormatter::try_new(array.as_ref(), &opts) {
        return fmt.value(row).to_string();
    }
    "?".to_string()
}

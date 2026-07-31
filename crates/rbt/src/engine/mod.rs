//! `rbt::engine`: Apache DataFusion query engine integration, bronze registration, and DAG execution.

pub mod bronze;

use crate::core::dag::{ModelDag, ModelNode, OutputFormat};
use crate::core::project::{
    MaterializeConfig, MaterializeMode, RbtProjectConfig, RefBackend,
};
use crate::materializer::{
    load_parquet_batches, materialize_stream, sibling_iceberg_dir, MaterializeWriteOptions,
    MultiFormatWriter, StreamWriteStats,
};
use crate::testing::{assertions_from_model_tests, Assertion, RecordBatchValidator};
use anyhow::{bail, Context, Result};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{CsvReadOptions, JsonReadOptions, ParquetReadOptions};
use iceberg::Catalog;
use iceberg_datafusion::IcebergCatalogProvider;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub use bronze::{
    register_bronze_for_model, register_bronze_sources_for_dag, BronzeRegistrationMode,
    BronzeSourceMeta, BronzeTableProvider,
};

/// Execution metric summary for a executed model DAG.
#[derive(Debug, Clone)]
pub struct DagExecutionSummary {
    pub models_executed: usize,
    pub total_rows_produced: usize,
    pub bronze_sources_registered: usize,
}

/// Result of `preview` — limited rows from one model without materializing it.
#[derive(Debug, Clone)]
pub struct PreviewResult {
    pub model: String,
    pub compiled_sql: String,
    pub limit: usize,
    pub rows: usize,
    pub batches: Vec<arrow::record_batch::RecordBatch>,
    pub ancestors_executed: usize,
}

/// Fluent Builder for configuring and launching `TransformationEngine` instances.
#[derive(Default)]
pub struct RbtEngineBuilder {
    catalogs: Vec<(String, Arc<dyn Catalog>)>,
}

impl RbtEngineBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_catalog(mut self, name: impl Into<String>, catalog: Arc<dyn Catalog>) -> Self {
        self.catalogs.push((name.into(), catalog));
        self
    }

    pub async fn build(self) -> Result<TransformationEngine> {
        let engine = TransformationEngine::new();
        for (name, cat) in self.catalogs {
            engine.register_iceberg_catalog(&name, cat).await?;
        }
        Ok(engine)
    }
}

pub struct TransformationEngine {
    pub ctx: SessionContext,
    /// Cached project config keyed by canonical project_dir (roots, materialize, scan limits).
    ///
    /// Avoids re-reading `rbt_project.yml` once per bronze model on large DAGs.
    project_cache: Mutex<Option<(PathBuf, Arc<RbtProjectConfig>)>>,
}

impl Default for TransformationEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TransformationEngine {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
            project_cache: Mutex::new(None),
        }
    }

    /// Load (or reuse cached) project config for `project_dir`.
    pub fn load_project_config(&self, project_dir: &Path) -> Result<Arc<RbtProjectConfig>> {
        let key = project_dir
            .canonicalize()
            .unwrap_or_else(|_| project_dir.to_path_buf());
        let mut guard = self
            .project_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("E_RBT_ENGINE: project config cache lock poisoned"))?;
        if let Some((ref cached_dir, ref cfg)) = *guard {
            if *cached_dir == key {
                return Ok(Arc::clone(cfg));
            }
        }
        let cfg = Arc::new(RbtProjectConfig::load(project_dir).with_context(|| {
            format!(
                "E_RBT_PROJECT_LOAD: failed loading rbt_project.yml under {}",
                project_dir.display()
            )
        })?);
        *guard = Some((key, Arc::clone(&cfg)));
        Ok(cfg)
    }

    /// Clear cached project config (tests / multi-project hosts).
    pub fn clear_project_cache(&self) {
        if let Ok(mut guard) = self.project_cache.lock() {
            *guard = None;
        }
    }

    /// Registers an Apache Iceberg catalog directly into the DataFusion query context.
    pub async fn register_iceberg_catalog(
        &self,
        catalog_name: &str,
        catalog: Arc<dyn Catalog>,
    ) -> Result<()> {
        tracing::info!(
            "Registering Iceberg catalog '{}' into DataFusion SessionContext",
            catalog_name
        );
        let provider = IcebergCatalogProvider::try_new(catalog).await?;
        self.ctx.register_catalog(catalog_name, Arc::new(provider));
        Ok(())
    }

    /// Executes a SQL transform query against registered tables.
    pub async fn execute_sql(&self, sql: &str) -> Result<SendableRecordBatchStream> {
        tracing::info!(
            "Executing SQL transform via Apache DataFusion engine: {}",
            sql
        );
        let df = self.ctx.sql(sql).await?;
        let stream = df.execute_stream().await?;
        Ok(stream)
    }

    /// Executes a full pipeline DAG tier by tier.
    ///
    /// Loads `materialize:` policy from `rbt_project.yml` when present (defaults to
    /// lake-as-truth Parquet re-read for `ref()`).
    ///
    /// Before any model SQL runs, bronze sources declared in staging frontmatter are
    /// registered via [`register_bronze_sources_for_dag`].
    pub async fn execute_dag(
        &self,
        dag: &ModelDag,
        project_dir: impl AsRef<Path>,
        output_dir: impl AsRef<Path>,
    ) -> Result<DagExecutionSummary> {
        let project_dir = project_dir.as_ref();
        let config = self.load_project_config(project_dir)?;
        self.execute_dag_with_config(dag, project_dir, output_dir, &config)
            .await
    }

    /// Like [`execute_dag`] but with an explicit [`MaterializeConfig`] (tests / library).
    pub async fn execute_dag_with_materialize(
        &self,
        dag: &ModelDag,
        project_dir: impl AsRef<Path>,
        output_dir: impl AsRef<Path>,
        materialize: &MaterializeConfig,
    ) -> Result<DagExecutionSummary> {
        let project_dir = project_dir.as_ref();
        let mut config = (*self.load_project_config(project_dir)?).clone();
        config.materialize = materialize.clone();
        self.execute_dag_with_config(dag, project_dir, output_dir, &config)
            .await
    }

    /// Preview a single model: materialize ancestors, then run target SQL with `LIMIT`.
    ///
    /// Does **not** write the target model to the lake. Bronze + ancestor `ref()` tables
    /// are registered as for a normal run. `limit` is clamped to `1..=10_000`.
    pub async fn preview_model(
        &self,
        full_dag: &ModelDag,
        project_dir: impl AsRef<Path>,
        output_dir: impl AsRef<Path>,
        model_name: &str,
        limit: usize,
    ) -> Result<PreviewResult> {
        let project_dir = project_dir.as_ref();
        let config = self.load_project_config(project_dir)?;
        let limit = limit.clamp(1, 10_000);

        let sub = full_dag
            .apply_select(Some(model_name), crate::core::SelectMode::Execute)
            .with_context(|| {
                format!(
                    "E_RBT_PREVIEW: cannot select model '{model_name}' (check name / --select)"
                )
            })?;
        let seq = sub.topological_sequence()?;
        let target = seq
            .iter()
            .find(|m| m.name == model_name)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("E_RBT_PREVIEW: model '{model_name}' not found in project DAG")
            })?;
        self.preview_model_inner(full_dag, project_dir, output_dir, &target, limit, &config)
            .await
    }

    async fn preview_model_inner(
        &self,
        full_dag: &ModelDag,
        project_dir: &Path,
        output_dir: impl AsRef<Path>,
        target: &ModelNode,
        limit: usize,
        config: &RbtProjectConfig,
    ) -> Result<PreviewResult> {
        let output_dir = output_dir.as_ref();
        let sub = full_dag
            .apply_select(Some(&target.name), crate::core::SelectMode::Execute)?;
        let seq = sub.topological_sequence()?;
        let ancestor_names: Vec<String> = seq
            .iter()
            .map(|m| m.name.clone())
            .filter(|n| n != &target.name)
            .collect();

        let mut ancestors_executed = 0usize;
        if !ancestor_names.is_empty() {
            let anc_select = ancestor_names.join(",");
            let anc_dag = full_dag
                .apply_select(Some(&anc_select), crate::core::SelectMode::Execute)
                .context("E_RBT_PREVIEW: ancestor select failed")?;
            let summary = self
                .execute_dag_with_config(&anc_dag, project_dir, output_dir, config)
                .await
                .context("E_RBT_PREVIEW: ancestor materialize failed")?;
            ancestors_executed = summary.models_executed;
        } else {
            // Still need bronze for staging-only preview
            let mut registered = HashSet::new();
            register_bronze_sources_for_dag(
                &self.ctx,
                &sub,
                project_dir,
                &mut registered,
                config,
            )
            .await
            .context("E_RBT_PREVIEW: bronze registration failed")?;
        }

        // Ensure target bronze contract is registered (staging models).
        let mut registered = HashSet::new();
        register_bronze_for_model(&self.ctx, target, project_dir, &mut registered, config)
            .await?;

        let preview_sql = format!(
            "SELECT * FROM (\n{}\n) AS _rbt_preview LIMIT {}",
            target.compiled_sql.trim().trim_end_matches(';'),
            limit
        );
        let df = self.ctx.sql(&preview_sql).await.with_context(|| {
            format!(
                "E_RBT_PREVIEW: SQL failed for model '{}': {preview_sql}",
                target.name
            )
        })?;
        let batches = df.collect().await.with_context(|| {
            format!("E_RBT_PREVIEW: collect failed for model '{}'", target.name)
        })?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        Ok(PreviewResult {
            model: target.name.clone(),
            compiled_sql: target.compiled_sql.clone(),
            limit,
            rows,
            batches,
            ancestors_executed,
        })
    }

    /// Full DAG execution with a pre-loaded project config (roots, scan limits, materialize).
    pub async fn execute_dag_with_config(
        &self,
        dag: &ModelDag,
        project_dir: impl AsRef<Path>,
        output_dir: impl AsRef<Path>,
        config: &RbtProjectConfig,
    ) -> Result<DagExecutionSummary> {
        let project_dir = project_dir.as_ref();
        let output_base = output_dir.as_ref();
        let materialize = &config.materialize;
        tokio::fs::create_dir_all(output_base).await?;

        let mut registered = HashSet::new();
        let bronze_sources_registered =
            register_bronze_sources_for_dag(&self.ctx, dag, project_dir, &mut registered, config)
                .await
                .context("frontmatter-driven bronze registration failed")?;

        let tiers = dag.execution_tiers()?;
        let mut models_executed = 0;
        let mut total_rows_produced = 0;

        for (tier_idx, tier) in tiers.iter().enumerate() {
            tracing::info!(
                "Executing DAG Tier {} with {} parallel models",
                tier_idx,
                tier.len()
            );

            for model in tier {
                tracing::info!("Executing model '{}'...", model.name);

                // Late-bind: if this model carries frontmatter not registered yet
                register_bronze_for_model(&self.ctx, model, project_dir, &mut registered, config)
                    .await?;

                let dest_path = model
                    .output_path
                    .as_ref()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| match model.output_format {
                        OutputFormat::Iceberg => output_base.join(&model.name),
                        OutputFormat::Jsonl => output_base.join(format!("{}.jsonl", model.name)),
                        OutputFormat::Csv => output_base.join(format!("{}.csv", model.name)),
                        _ => output_base.join(format!("{}.parquet", model.name)),
                    });

                if let Some(parent) = dest_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                let (assertions, fail_on_error) = model_assertions(model);
                let write_opts =
                    MaterializeWriteOptions::from_config(materialize, fail_on_error);
                let mode = materialize.effective_mode();

                let row_count = match mode {
                    MaterializeMode::Stream => {
                        let stats = execute_model_stream(
                            &self.ctx,
                            model,
                            &dest_path,
                            &write_opts,
                            &assertions,
                            fail_on_error,
                        )
                        .await?;
                        stats.rows
                    }
                    MaterializeMode::Collect => {
                        execute_model_collect(
                            &self.ctx,
                            model,
                            &dest_path,
                            &write_opts,
                            &assertions,
                            fail_on_error,
                        )
                        .await?
                    }
                };

                // Expose model for downstream {{ ref() }} per project materialize policy.
                if row_count > 0
                    || matches!(
                        model.output_format,
                        OutputFormat::Parquet
                            | OutputFormat::Iceberg
                            | OutputFormat::ParquetAndIceberg
                            | OutputFormat::ZeroCopyClone
                    )
                {
                    // Empty parquet still may exist after stream (schema-only); skip ref if no file.
                    let backend = materialize.choose_ref_backend(row_count);
                    if row_count > 0 {
                        register_model_for_ref(
                            &self.ctx,
                            &model.name,
                            &model.output_format,
                            &dest_path,
                            backend,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "E_RBT_REF_REGISTER: model '{}' (backend={:?}, rows={}, mode={:?})",
                                model.name, backend, row_count, mode
                            )
                        })?;
                        tracing::debug!(
                            model = %model.name,
                            rows = row_count,
                            ?backend,
                            ?mode,
                            strategy = ?materialize.ref_strategy,
                            "registered model for ref()"
                        );
                    }
                }

                models_executed += 1;
                total_rows_produced += row_count;
            }
        }

        Ok(DagExecutionSummary {
            models_executed,
            total_rows_produced,
            bronze_sources_registered,
        })
    }
}

/// Build frontmatter assertion list + fail-on-error policy for a model.
fn model_assertions(model: &ModelNode) -> (Vec<Assertion>, bool) {
    let mut fail_on_error = true;
    let assertions = if let Some(fm) = model.frontmatter.as_ref() {
        if let Some(tests) = fm.tests.as_ref() {
            fail_on_error = tests.should_fail_on_error();
            if tests.is_empty() {
                Vec::new()
            } else {
                let unique = tests
                    .unique
                    .clone()
                    .or_else(|| fm.unique_key.clone())
                    .or_else(|| fm.grain.clone());
                assertions_from_model_tests(
                    tests.not_null.as_deref(),
                    unique.as_deref(),
                    tests.accepted_values.as_ref(),
                )
            }
        } else if let Some(uk) = fm
            .unique_key
            .as_ref()
            .or(fm.grain.as_ref())
            .filter(|v| !v.is_empty())
        {
            fail_on_error = true;
            assertions_from_model_tests(None, Some(uk.as_slice()), None)
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    (assertions, fail_on_error)
}

async fn execute_model_stream(
    ctx: &SessionContext,
    model: &ModelNode,
    dest_path: &Path,
    write_opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
    fail_on_error: bool,
) -> Result<StreamWriteStats> {
    let df = ctx.sql(&model.compiled_sql).await.with_context(|| {
        format!(
            "E_RBT_SQL: execution failed for model '{}' (compiled: {})",
            model.name, model.compiled_sql
        )
    })?;
    let stream = df.execute_stream().await.with_context(|| {
        format!(
            "E_RBT_SQL: execute_stream failed for model '{}'",
            model.name
        )
    })?;

    let stats = materialize_stream(
        stream,
        &model.output_format,
        dest_path,
        write_opts,
        assertions,
    )
    .await
    .with_context(|| {
        format!(
            "E_RBT_MATERIALIZE: stream write failed for model '{}' → {}",
            model.name,
            dest_path.display()
        )
    })?;

    if stats.validation.failed_assertions > 0 {
        let msg = format!(
            "model '{}' failed {} test(s): {}",
            model.name,
            stats.validation.failed_assertions,
            stats.validation.errors.join("; ")
        );
        if fail_on_error {
            bail!("{msg}");
        }
        tracing::warn!("{msg}");
    } else if !assertions.is_empty() {
        tracing::info!(
            "model '{}': {} assertion(s) passed ({} rows, {} batches, stream)",
            model.name,
            stats.validation.passed_assertions,
            stats.rows,
            stats.batches
        );
    } else {
        tracing::debug!(
            model = %model.name,
            rows = stats.rows,
            batches = stats.batches,
            bytes = stats.bytes_written,
            "stream materialize complete"
        );
    }
    Ok(stats)
}

async fn execute_model_collect(
    ctx: &SessionContext,
    model: &ModelNode,
    dest_path: &Path,
    write_opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
    fail_on_error: bool,
) -> Result<usize> {
    let df = ctx.sql(&model.compiled_sql).await.with_context(|| {
        format!(
            "E_RBT_SQL: execution failed for model '{}' (compiled: {})",
            model.name, model.compiled_sql
        )
    })?;
    let batches = df.collect().await.with_context(|| {
        format!(
            "E_RBT_SQL: collect failed for model '{}'",
            model.name
        )
    })?;
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

    MultiFormatWriter::write_batches(&batches, &model.output_format, dest_path)?;
    // Prefer atomic parquet path for primary formats already handled inside MultiFormatWriter.

    if !assertions.is_empty() {
        let result = RecordBatchValidator::validate_batches(&batches, assertions);
        if result.failed_assertions > 0 {
            let msg = format!(
                "model '{}' failed {} test(s): {}",
                model.name,
                result.failed_assertions,
                result.errors.join("; ")
            );
            if fail_on_error {
                bail!("{msg}");
            }
            tracing::warn!("{msg}");
        } else {
            tracing::info!(
                "model '{}': {} assertion(s) passed ({} rows, collect)",
                model.name,
                result.passed_assertions,
                result.total_rows
            );
        }
    }

    let _ = write_opts; // reserved for collect-path parquet props if we unify further
    Ok(row_count)
}

/// Path used to re-read a model from the lake after materialize.
fn lake_read_path(format: &OutputFormat, dest_path: &Path) -> PathBuf {
    match format {
        OutputFormat::Iceberg => dest_path.join("data/part-00000.parquet"),
        OutputFormat::ParquetAndIceberg => {
            // Flat parquet is the primary dual-write artifact for ref().
            if dest_path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                dest_path.to_path_buf()
            } else {
                dest_path.with_extension("parquet")
            }
        }
        _ => dest_path.to_path_buf(),
    }
}

/// Register a completed model so later SQL `ref('name')` resolves.
///
/// Never requires an in-memory `Vec<RecordBatch>` for the default lake-file backend.
/// MemTable backend re-reads the written lake path (only used for small tables).
async fn register_model_for_ref(
    ctx: &SessionContext,
    name: &str,
    format: &OutputFormat,
    dest_path: &Path,
    backend: RefBackend,
) -> Result<()> {
    let _ = ctx.deregister_table(name);

    match backend {
        RefBackend::MemTable => {
            let batches = match format {
                OutputFormat::Parquet
                | OutputFormat::ZeroCopyClone
                | OutputFormat::Iceberg
                | OutputFormat::ParquetAndIceberg => {
                    let path = resolve_lake_read_path(format, dest_path)?;
                    load_parquet_batches(&path).with_context(|| {
                        format!(
                            "E_RBT_REF_MEMTABLE: load {} for ref('{name}')",
                            path.display()
                        )
                    })?
                }
                OutputFormat::Jsonl | OutputFormat::Csv => {
                    // Register lake file then collect into MemTable (small tables only).
                    Box::pin(async {
                        // temporarily use lake registration path then table scan
                        register_model_for_ref(ctx, name, format, dest_path, RefBackend::LakeFile)
                            .await?;
                        let df = ctx.table(name).await.map_err(|e| {
                            anyhow::anyhow!("E_RBT_REF_MEMTABLE: table '{name}': {e}")
                        })?;
                        df.collect().await.map_err(|e| {
                            anyhow::anyhow!("E_RBT_REF_MEMTABLE: collect '{name}': {e}")
                        })
                    })
                    .await?
                }
            };
            if batches.is_empty() {
                bail!("E_RBT_REF_MEMTABLE: no batches for ref('{name}')");
            }
            let _ = ctx.deregister_table(name);
            let schema = batches[0].schema();
            let mem_table = MemTable::try_new(schema, vec![batches])
                .map_err(|e| anyhow::anyhow!("MemTable::try_new: {e}"))?;
            ctx.register_table(name, Arc::new(mem_table))
                .map_err(|e| anyhow::anyhow!("register_table MemTable: {e}"))?;
        }
        RefBackend::LakeFile => match format {
            OutputFormat::Parquet
            | OutputFormat::ZeroCopyClone
            | OutputFormat::Iceberg
            | OutputFormat::ParquetAndIceberg => {
                let path = resolve_lake_read_path(format, dest_path)?;
                ctx.register_parquet(
                    name,
                    path.to_str().unwrap_or_default(),
                    ParquetReadOptions::default(),
                )
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "E_RBT_REF_REGISTER: register_parquet {} for '{name}': {e}",
                        path.display()
                    )
                })?;
            }
            OutputFormat::Jsonl => {
                let p = dest_path.to_str().unwrap_or_default();
                let opts = JsonReadOptions::default()
                    .file_extension(".jsonl")
                    .newline_delimited(true);
                if let Err(e) = ctx.register_json(name, p, opts).await {
                    tracing::debug!("jsonl register failed ({e}); retry default");
                    ctx.register_json(name, p, JsonReadOptions::default())
                        .await
                        .map_err(|e| anyhow::anyhow!("E_RBT_REF_REGISTER: register_json: {e}"))?;
                }
            }
            OutputFormat::Csv => {
                ctx.register_csv(
                    name,
                    dest_path.to_str().unwrap_or_default(),
                    CsvReadOptions::default(),
                )
                .await
                .map_err(|e| anyhow::anyhow!("E_RBT_REF_REGISTER: register_csv: {e}"))?;
            }
        },
    }
    Ok(())
}

fn resolve_lake_read_path(format: &OutputFormat, dest_path: &Path) -> Result<PathBuf> {
    let mut path = lake_read_path(format, dest_path);
    if !path.exists() && matches!(format, OutputFormat::ParquetAndIceberg) {
        let alt = sibling_iceberg_dir(dest_path).join("data/part-00000.parquet");
        if alt.exists() {
            path = alt;
        }
    }
    if !path.exists() {
        bail!(
            "E_RBT_REF_MISSING: lake file missing for ref(): expected {}",
            path.display()
        );
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::dag::{Materialization, ModelDag, OutputFormat};

    #[tokio::test]
    async fn test_engine_initialization() -> Result<()> {
        let engine = TransformationEngine::new();
        let df = engine.ctx.sql("SELECT 1 AS col").await?;
        let batches = df.collect().await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_dag_execution_multi_format() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let engine = TransformationEngine::new();

        let mut dag = ModelDag::new();
        dag.add_model_with_format(
            "users",
            "SELECT 1 AS id, 'Alice' AS name",
            Materialization::Table,
            OutputFormat::Jsonl,
            None,
            "",
        )?;
        dag.add_model_with_format(
            "active_users",
            "SELECT * FROM {{ ref('users') }} WHERE id = 1",
            Materialization::Table,
            OutputFormat::Parquet,
            None,
            "",
        )?;
        dag.build_graph()?;

        let summary = engine
            .execute_dag(&dag, temp_dir.path(), temp_dir.path())
            .await?;
        assert_eq!(summary.models_executed, 2);
        assert_eq!(summary.total_rows_produced, 2);
        assert!(temp_dir.path().join("users.jsonl").exists());
        assert!(temp_dir.path().join("active_users.parquet").exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_frontmatter_bronze_end_to_end() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let bronze_dir = temp.path().join("lake/bronze");
        std::fs::create_dir_all(&bronze_dir)?;
        std::fs::write(
            bronze_dir.join("raw_stock_trades.jsonl"),
            r#"{"ticker":"NVDA","timestamp":"2026-07-24T09:30:01Z","price":125.5,"volume":100}
{"ticker":"AAPL","timestamp":"2026-07-24T09:30:05Z","price":190.0,"volume":50}
"#,
        )?;

        let sql = r#"---
source_format: jsonl
scan_path: "lake/bronze/raw_stock_trades.jsonl"
---
SELECT ticker, price, volume FROM {{ source('bronze', 'raw_stock_trades') }}
"#;

        let mut dag = ModelDag::new();
        dag.add_model_with_format(
            "stg_stock_trades",
            sql,
            Materialization::Table,
            OutputFormat::Parquet,
            Some(
                temp.path()
                    .join("lake/silver/stg_stock_trades.parquet")
                    .to_string_lossy()
                    .into(),
            ),
            "",
        )?;
        dag.build_graph()?;

        let engine = TransformationEngine::new();
        let summary = engine
            .execute_dag(&dag, temp.path(), temp.path().join("out"))
            .await?;
        assert_eq!(summary.bronze_sources_registered, 1);
        assert_eq!(summary.models_executed, 1);
        assert_eq!(summary.total_rows_produced, 2);
        assert!(temp
            .path()
            .join("lake/silver/stg_stock_trades.parquet")
            .exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_ref_via_parquet_reread_default() -> Result<()> {
        use crate::core::project::{MaterializeConfig, RefStrategy};

        let temp = tempfile::tempdir()?;
        let mut dag = ModelDag::new();
        dag.add_model_with_format(
            "stg_a",
            "SELECT 1 AS id, 10 AS v UNION ALL SELECT 2, 20",
            Materialization::Table,
            OutputFormat::Parquet,
            Some(temp.path().join("stg_a.parquet").to_string_lossy().into()),
            "",
        )?;
        dag.add_model_with_format(
            "tf_b",
            "SELECT id, v * 2 AS v2 FROM {{ ref('stg_a') }}",
            Materialization::Table,
            OutputFormat::Parquet,
            Some(temp.path().join("tf_b.parquet").to_string_lossy().into()),
            "",
        )?;
        dag.build_graph()?;

        let mat = MaterializeConfig {
            ref_strategy: RefStrategy::Parquet,
            memtable_max_rows: 50_000,
            ..Default::default()
        };
        let engine = TransformationEngine::new();
        let summary = engine
            .execute_dag_with_materialize(&dag, temp.path(), temp.path(), &mat)
            .await?;
        assert_eq!(summary.models_executed, 2);
        assert_eq!(summary.total_rows_produced, 4);
        assert!(temp.path().join("tf_b.parquet").exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_ref_via_memtable_when_configured() -> Result<()> {
        use crate::core::project::{MaterializeConfig, RefStrategy};

        let temp = tempfile::tempdir()?;
        let mut dag = ModelDag::new();
        dag.add_model_with_format(
            "stg_a",
            "SELECT 1 AS id UNION ALL SELECT 2",
            Materialization::Table,
            OutputFormat::Parquet,
            Some(temp.path().join("stg_a.parquet").to_string_lossy().into()),
            "",
        )?;
        dag.add_model_with_format(
            "tf_b",
            "SELECT count(*) AS c FROM {{ ref('stg_a') }}",
            Materialization::Table,
            OutputFormat::Parquet,
            Some(temp.path().join("tf_b.parquet").to_string_lossy().into()),
            "",
        )?;
        dag.build_graph()?;

        let mat = MaterializeConfig {
            ref_strategy: RefStrategy::Memtable,
            memtable_max_rows: 50_000,
            ..Default::default()
        };
        let engine = TransformationEngine::new();
        let summary = engine
            .execute_dag_with_materialize(&dag, temp.path(), temp.path(), &mat)
            .await?;
        assert_eq!(summary.models_executed, 2);
        assert!(temp.path().join("tf_b.parquet").exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_memtable_falls_back_to_lake_above_cutoff() -> Result<()> {
        use crate::core::project::{MaterializeConfig, RefStrategy};

        // Cutoff 1 → 2-row model must use lake re-read.
        let temp = tempfile::tempdir()?;
        let mut dag = ModelDag::new();
        dag.add_model_with_format(
            "stg_a",
            "SELECT 1 AS id UNION ALL SELECT 2",
            Materialization::Table,
            OutputFormat::Parquet,
            Some(temp.path().join("stg_a.parquet").to_string_lossy().into()),
            "",
        )?;
        dag.add_model_with_format(
            "tf_b",
            "SELECT * FROM {{ ref('stg_a') }}",
            Materialization::Table,
            OutputFormat::Parquet,
            Some(temp.path().join("tf_b.parquet").to_string_lossy().into()),
            "",
        )?;
        dag.build_graph()?;

        let mat = MaterializeConfig {
            ref_strategy: RefStrategy::Memtable,
            memtable_max_rows: 1,
            ..Default::default()
        };
        let engine = TransformationEngine::new();
        let summary = engine
            .execute_dag_with_materialize(&dag, temp.path(), temp.path(), &mat)
            .await?;
        assert_eq!(summary.models_executed, 2);
        assert_eq!(summary.total_rows_produced, 4);
        Ok(())
    }
}

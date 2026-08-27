//! `rbt::engine`: Apache DataFusion query engine integration, bronze registration, and DAG execution.

pub mod bronze;
pub mod rust_model;
pub mod sk_registry;
pub mod stages;
pub mod surrogate_key;
pub mod udf;

use crate::core::dag::{Materialization, ModelDag, ModelKind, ModelNode, OutputFormat};
use crate::engine::rust_model::{
    batches_to_stream, build_partition_input, empty_batch_for_schema, is_partition_not_implemented,
    validate_batches_schema, PartitionKey, RustModel, RustModelContext, RustModelOutput,
    RustModelRegistry,
};
use crate::core::project::{
    MaterializeConfig, MaterializeMode, RbtProjectConfig, RefBackend,
};
use crate::core::receipt::{
    effective_contract_version, now_unix_ms, ModelRunResult, RunReceipt, RunStatus,
};
use crate::core::run_scope::RunScope;
use crate::core::work_unit::{
    enrich_plan_from_manifests, plan_execution, scope_for_unit, ExecutionPlan, WorkUnit,
};
use crate::core::project::ExecutionStrategy;
use crate::materializer::{
    alias_ref_path, consolidate_parts_to_parquet, incremental_ref_path, load_parquet_batches,
    materialize_alias, materialize_incremental_append_stream, materialize_keyed_upsert,
    materialize_scoped_replace_stream, materialize_scoped_replace_stream_with,
    materialize_table_parts_only_stream, part_is_clean,
    resolve_alias_upstream, resolve_part_keys, resolve_parts_layout, scope_part_id,
    table_layout_root, upstream_lake_path, uses_parts_directory, materialize_stream, new_wap_run_id,
    sibling_iceberg_dir, stamp_batch, wap_publish, write_empty_parquet, write_parquet_batches_atomic,
    LineageStamp, MaterializeWriteOptions, MultiFormatWriter, ScopedPartPublish, StreamWriteStats,
    WapModelPaths,
};
use crate::core::receipt::bronze_fingerprint;
use datafusion::physical_plan::SendableRecordBatchStream;
use crate::engine::udf::register_builtin_udfs;
use crate::testing::{assertions_from_model_tests, Assertion, RecordBatchValidator};
use anyhow::{bail, Context, Result};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{CsvReadOptions, JsonReadOptions, ParquetReadOptions};
#[cfg(feature = "iceberg")]
use iceberg::Catalog;
#[cfg(feature = "iceberg")]
use iceberg_datafusion::IcebergCatalogProvider;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub use bronze::{
    register_bronze_for_model, register_bronze_for_model_scoped, register_bronze_sources_for_dag,
    register_bronze_sources_for_dag_scoped, BronzeRegistrationMode, BronzeSourceMeta,
    BronzeTableProvider,
};

/// Execution metric summary for a executed model DAG.
#[derive(Debug, Clone)]
pub struct DagExecutionSummary {
    pub models_executed: usize,
    pub total_rows_produced: usize,
    pub bronze_sources_registered: usize,
    /// True when fingerprint skip short-circuited materialize (P5b).
    pub skipped: bool,
    pub skip_reason: Option<String>,
    pub bronze_fingerprint: Option<String>,
    pub run_id: Option<String>,
    pub receipt_path: Option<PathBuf>,
    pub model_results: Vec<ModelRunResult>,
}

impl Default for DagExecutionSummary {
    fn default() -> Self {
        Self {
            models_executed: 0,
            total_rows_produced: 0,
            bronze_sources_registered: 0,
            skipped: false,
            skip_reason: None,
            bronze_fingerprint: None,
            run_id: None,
            receipt_path: None,
            model_results: Vec::new(),
        }
    }
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
///
/// # Patterns
///
/// Creational **Builder**: optional Iceberg catalogs (feature `iceberg`) and UDF
/// registration hooks ([`RbtEngineBuilder::with_udfs`], [`RbtEngineBuilder::with_udf_pack`]).
/// Host packs run **after** built-in `rbt_*` UDFs (RBT-L1.5 / ADR-008).
///
/// ```rust,no_run
/// use rbt::RbtEngineBuilder;
/// # async fn demo() -> anyhow::Result<()> {
/// let engine = RbtEngineBuilder::new()
///     .with_udfs(|ctx| {
///         let _ = ctx; // register host ScalarUDFs
///         Ok(())
///     })
///     .build()
///     .await?;
/// let _ = engine;
/// # Ok(())
/// # }
/// ```
pub struct RbtEngineBuilder {
    #[cfg(feature = "iceberg")]
    catalogs: Vec<(String, Arc<dyn Catalog>)>,
    /// Host UDF registration (Design A / L1.5). Runs after builtins on build.
    udf_hooks: Vec<Box<dyn FnOnce(&SessionContext) -> Result<()> + Send>>,
    /// Design B Rust models registered at build.
    rust_models: Vec<Arc<dyn RustModel>>,
}

impl Default for RbtEngineBuilder {
    fn default() -> Self {
        Self {
            #[cfg(feature = "iceberg")]
            catalogs: Vec::new(),
            udf_hooks: Vec::new(),
            rust_models: Vec::new(),
        }
    }
}

impl RbtEngineBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an Iceberg catalog into the session (requires feature `iceberg`).
    #[cfg(feature = "iceberg")]
    pub fn with_catalog(mut self, name: impl Into<String>, catalog: Arc<dyn Catalog>) -> Self {
        self.catalogs.push((name.into(), catalog));
        self
    }

    /// Register host UDFs after built-ins (RBT-L1.5). Prefer one hook that registers a pack.
    ///
    /// Enables Strategy / plugin-style extension without subclassing the engine.
    /// Multiple calls run in registration order after builtins.
    pub fn with_udfs<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&SessionContext) -> Result<()> + Send + 'static,
    {
        self.udf_hooks.push(Box::new(f));
        self
    }

    /// Register a [`crate::engine::udf::UdfPack`] after built-ins (owned, `'static`).
    pub fn with_udf_pack<P>(self, pack: P) -> Self
    where
        P: crate::engine::udf::UdfPack + 'static,
    {
        self.with_udfs(move |ctx| pack.register(ctx))
    }

    /// Register a Design B [`RustModel`] (host whole-node transform).
    pub fn with_rust_model<M>(mut self, model: M) -> Self
    where
        M: RustModel + 'static,
    {
        self.rust_models.push(Arc::new(model));
        self
    }

    /// Register an already-`Arc`'d Design B model.
    pub fn with_rust_model_arc(mut self, model: Arc<dyn RustModel>) -> Self {
        self.rust_models.push(model);
        self
    }

    pub async fn build(self) -> Result<TransformationEngine> {
        let engine = TransformationEngine::new();
        for hook in self.udf_hooks {
            hook(&engine.ctx)?;
        }
        for m in self.rust_models {
            engine.register_rust_model(m)?;
        }
        #[cfg(feature = "iceberg")]
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
    /// Design B host Rust models (name → impl).
    rust_models: Mutex<RustModelRegistry>,
}

impl Default for TransformationEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TransformationEngine {
    pub fn new() -> Self {
        let ctx = SessionContext::new();
        if let Err(e) = register_builtin_udfs(&ctx) {
            tracing::warn!("E_RBT_UDF: failed to register builtins: {e}");
        }
        Self {
            ctx,
            project_cache: Mutex::new(None),
            rust_models: Mutex::new(RustModelRegistry::new()),
        }
    }

    /// Register a Design B [`RustModel`] on the live engine.
    pub fn register_rust_model(&self, model: Arc<dyn RustModel>) -> Result<()> {
        self.rust_models
            .lock()
            .map_err(|_| anyhow::anyhow!("E_RBT_ENGINE: rust model registry lock poisoned"))?
            .register(model)
    }

    /// Drop all Design B models (tests / reconfiguration).
    /// Snapshot registered Design B models for cloning onto private L1/L2 worker engines.
    pub fn snapshot_rust_models(&self) -> Vec<Arc<dyn RustModel>> {
        self.rust_models
            .lock()
            .map(|g| g.snapshot())
            .unwrap_or_default()
    }

    pub fn clear_rust_models(&self) {
        if let Ok(mut g) = self.rust_models.lock() {
            g.clear();
        }
    }

    fn lookup_rust_model(&self, name: &str) -> Result<Arc<dyn RustModel>> {
        self.rust_models
            .lock()
            .map_err(|_| anyhow::anyhow!("E_RBT_ENGINE: rust model registry lock poisoned"))?
            .get(name)
    }

    /// Design B: execute host Rust model → batches/stream → materialize.
    ///
    /// Supports table, keyed_upsert, scoped_replace, incremental_append, and table+parts
    /// (consolidate:never). `dest_path` is the published lake path; `write_path` may be a WAP stage.
    ///
    /// # Phase 2 partition path
    ///
    /// When `partition_bindings` is non-empty (WorkUnit slice), the engine:
    /// 1. Builds [`crate::PartitionInput`] for upstream `ref()`s filtered to this partition
    /// 2. Calls [`RustModel::execute_partition`] if the host overrode the default
    /// 3. Falls back to [`RustModel::execute`] with the already-narrowed `scope` if not implemented
    async fn materialize_rust_model(
        &self,
        model: &ModelNode,
        project_dir: &Path,
        scope: &RunScope,
        dest_path: &Path,
        write_path: &Path,
        write_opts: &MaterializeWriteOptions,
        assertions: &[crate::testing::Assertion],
        fail_on_error: bool,
        run_id: &str,
        contract: &str,
        fp: &str,
        table_parts_only: bool,
        materialize: &MaterializeConfig,
        dag: &ModelDag,
        partition_bindings: &BTreeMap<String, String>,
    ) -> Result<(
        usize,
        Option<StreamWriteStats>,
        Option<crate::materializer::UpsertStats>,
    )> {
        let rust = self.lookup_rust_model(&model.name)?;
        let declared = rust.output_schema();
        let ctx = RustModelContext {
            session: &self.ctx,
            project_dir,
            scope,
            model_name: &model.name,
            run_id,
            contract_version: contract,
            bronze_fingerprint: Some(fp),
        };

        // Phase 2: prefer execute_partition for partition WorkUnit slices.
        let out = if !partition_bindings.is_empty() {
            let part = PartitionKey::from_map(partition_bindings);
            let host_contract = rust.parallel_contract();
            let input = build_partition_input(&self.ctx, dag, model, &part)
                .await
                .with_context(|| {
                    format!(
                        "E_RBT_RUST_PARTITION: build input for '{}' part={part}",
                        model.name
                    )
                })?;
            tracing::info!(
                model = %model.name,
                part = %part,
                contract = host_contract.as_str(),
                upstream = ?input.upstream_models,
                paths = input.paths.len(),
                has_stream = input.stream.is_some(),
                "Design B partition unit (Phase 2)"
            );
            match rust.execute_partition(&ctx, &part, input).await {
                Ok(o) => o,
                Err(e) if is_partition_not_implemented(&e) => {
                    tracing::debug!(
                        model = %model.name,
                        "execute_partition not implemented; falling back to execute() with unit scope"
                    );
                    rust.execute(&ctx).await.with_context(|| {
                        format!("E_RBT_RUST_MODEL: execute '{}' (partition fallback)", model.name)
                    })?
                }
                Err(e) => {
                    return Err(e).context(format!(
                        "E_RBT_RUST_PARTITION: execute_partition failed for '{}' part={part}",
                        model.name
                    ));
                }
            }
        } else {
            rust.execute(&ctx)
                .await
                .with_context(|| format!("E_RBT_RUST_MODEL: execute '{}'", model.name))?
        };

        // Normalize to either collected batches (upsert / small table) or a stream (parts/table stream).
        enum Out {
            Batches(Vec<arrow::record_batch::RecordBatch>),
            Stream(SendableRecordBatchStream),
        }
        let prepared = match out {
            RustModelOutput::Batches(mut batches) => {
                validate_batches_schema(&batches, declared.as_ref()).with_context(|| {
                    format!("E_RBT_RUST_SCHEMA: model '{}'", model.name)
                })?;
                if batches.is_empty() {
                    batches.push(empty_batch_for_schema(declared.clone())?);
                }
                if let Some(ref lin) = write_opts.lineage {
                    batches = batches
                        .iter()
                        .map(|b| stamp_batch(b, lin))
                        .collect::<Result<Vec<_>>>()?;
                }
                if let Some(ref d) = write_opts.declared_schema {
                    batches = batches
                        .iter()
                        .map(|b| crate::core::schema_emit::ensure_declared_columns(b, d.as_ref()))
                        .collect::<Result<Vec<_>>>()?;
                }
                if !assertions.is_empty() {
                    let validation = crate::testing::RecordBatchValidator::validate_batches(
                        &batches,
                        assertions,
                    );
                    if validation.failed_assertions > 0 && fail_on_error {
                        bail!(
                            "E_RBT_TEST: model '{}': {} assertion(s) failed (Design B): {}",
                            model.name,
                            validation.failed_assertions,
                            validation.errors.join("; ")
                        );
                    }
                }
                Out::Batches(batches)
            }
            RustModelOutput::Stream(stream) => Out::Stream(stream),
        };

        let parquet_fmt = matches!(
            model.output_format,
            OutputFormat::Parquet | OutputFormat::ZeroCopyClone
        );
        if !parquet_fmt {
            bail!(
                "E_RBT_RUST_MAT: Rust model '{}' requires parquet output (got {:?})",
                model.name,
                model.output_format
            );
        }

        match &model.materialization {
            Materialization::Table if table_parts_only => {
                let stream = match prepared {
                    Out::Batches(b) => {
                        let schema = b
                            .first()
                            .map(|x| x.schema())
                            .unwrap_or_else(|| declared.clone());
                        batches_to_stream(schema, b)
                    }
                    Out::Stream(s) => s,
                };
                let stats = materialize_table_parts_only_stream(
                    stream,
                    dest_path,
                    write_opts,
                    assertions,
                )
                .await
                .with_context(|| {
                    format!(
                        "E_RBT_CONSOLIDATE: Rust model '{}' table parts-only failed",
                        model.name
                    )
                })?;
                log_assertion_result(model, &stats, fail_on_error)?;
                maybe_consolidate_after_parts(materialize, dest_path, write_opts, &model.name)
                    .await?;
                Ok((stats.rows, Some(stats), None))
            }
            Materialization::Table => {
                match prepared {
                    Out::Batches(mut batches) => {
                        if let Some(ref sk) = write_opts.surrogate_key {
                            for b in &mut batches {
                                *b = crate::engine::surrogate_key::apply_surrogate_key(b, sk)
                                    .with_context(|| {
                                        format!(
                                            "E_RBT_SK: Rust model '{}' SK stamp failed",
                                            model.name
                                        )
                                    })?;
                            }
                        }
                        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                        if rows == 0 {
                            write_empty_parquet(declared, write_path, write_opts).with_context(
                                || {
                                    format!(
                                        "E_RBT_RUST_MAT: empty parquet for Rust model '{}'",
                                        model.name
                                    )
                                },
                            )?;
                        } else {
                            write_parquet_batches_atomic(&batches, write_path, write_opts)
                                .with_context(|| {
                                    format!(
                                        "E_RBT_RUST_MAT: parquet write for Rust model '{}'",
                                        model.name
                                    )
                                })?;
                        }
                        tracing::info!(
                            model = %model.name,
                            rows,
                            "Design B Rust model materialize (table parquet)"
                        );
                        Ok((
                            rows,
                            Some(StreamWriteStats {
                                rows,
                                batches: batches.len(),
                                path: write_path.to_path_buf(),
                                bytes_written: std::fs::metadata(write_path)
                                    .map(|m| m.len())
                                    .unwrap_or(0),
                                validation: crate::testing::ValidationResult {
                                    total_rows: rows,
                                    passed_assertions: assertions.len(),
                                    failed_assertions: 0,
                                    errors: Vec::new(),
                                },
                            }),
                            None,
                        ))
                    }
                    Out::Stream(stream) => {
                        let stats = materialize_stream(
                            stream,
                            &model.output_format,
                            write_path,
                            write_opts,
                            assertions,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "E_RBT_RUST_MAT: stream table write for Rust model '{}'",
                                model.name
                            )
                        })?;
                        log_assertion_result(model, &stats, fail_on_error)?;
                        Ok((stats.rows, Some(stats), None))
                    }
                }
            }
            Materialization::KeyedUpsert => {
                let batches = match prepared {
                    Out::Batches(b) => b,
                    Out::Stream(mut s) => {
                        // Memory-bound collect for upsert v1.
                        let mut all = Vec::new();
                        use futures::StreamExt;
                        while let Some(item) = s.next().await {
                            all.push(item.map_err(|e| {
                                anyhow::anyhow!(
                                    "E_RBT_RUST_MAT: stream collect for upsert '{}': {e}",
                                    model.name
                                )
                            })?);
                        }
                        if all.is_empty() {
                            all.push(empty_batch_for_schema(declared.clone())?);
                        }
                        validate_batches_schema(&all, declared.as_ref())?;
                        all
                    }
                };
                let fm = model.frontmatter.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "E_RBT_UPSERT_KEY: Rust model '{}': keyed_upsert requires frontmatter \
                         with unique_key",
                        model.name
                    )
                })?;
                let upsert_cfg = fm.keyed_upsert_config().with_context(|| {
                    format!(
                        "E_RBT_UPSERT_KEY: Rust model '{}' keyed_upsert config invalid",
                        model.name
                    )
                })?;
                let (stats, upsert_stats) = materialize_keyed_upsert(
                    write_path,
                    &batches,
                    &upsert_cfg,
                    write_opts,
                    assertions,
                )
                .with_context(|| {
                    format!(
                        "E_RBT_UPSERT_SCHEMA: Rust model '{}' keyed_upsert failed",
                        model.name
                    )
                })?;
                Ok((stats.rows, Some(stats), Some(upsert_stats)))
            }
            Materialization::ScopedReplace => {
                let stream = match prepared {
                    Out::Batches(b) => {
                        let schema = b
                            .first()
                            .map(|x| x.schema())
                            .unwrap_or_else(|| declared.clone());
                        batches_to_stream(schema, b)
                    }
                    Out::Stream(s) => s,
                };
                let part_keys = resolve_part_keys(
                    model
                        .frontmatter
                        .as_ref()
                        .and_then(|f| f.part_key.as_deref()),
                    model
                        .frontmatter
                        .as_ref()
                        .and_then(|f| f.partition_by.as_deref()),
                    scope,
                );
                let sid = scope_part_id(&model.name, contract, &part_keys, scope).with_context(
                    || {
                        format!(
                            "E_RBT_PART_KEY: Rust model '{}' cannot build scope_id",
                            model.name
                        )
                    },
                )?;
                let stats = materialize_scoped_replace_stream(
                    stream,
                    dest_path,
                    write_opts,
                    assertions,
                    &sid,
                )
                .await
                .with_context(|| {
                    format!(
                        "E_RBT_INCREMENTAL: Rust model '{}' scoped_replace failed (scope_id={sid})",
                        model.name
                    )
                })?;
                log_assertion_result(model, &stats, fail_on_error)?;
                maybe_consolidate_after_parts(materialize, dest_path, write_opts, &model.name)
                    .await?;
                Ok((stats.rows, Some(stats), None))
            }
            Materialization::IncrementalAppend => {
                let stream = match prepared {
                    Out::Batches(b) => {
                        let schema = b
                            .first()
                            .map(|x| x.schema())
                            .unwrap_or_else(|| declared.clone());
                        batches_to_stream(schema, b)
                    }
                    Out::Stream(s) => s,
                };
                let stats = materialize_incremental_append_stream(
                    stream,
                    dest_path,
                    write_opts,
                    assertions,
                )
                .await
                .with_context(|| {
                    format!(
                        "E_RBT_INCREMENTAL: Rust model '{}' append failed",
                        model.name
                    )
                })?;
                log_assertion_result(model, &stats, fail_on_error)?;
                maybe_consolidate_after_parts(materialize, dest_path, write_opts, &model.name)
                    .await?;
                Ok((stats.rows, Some(stats), None))
            }
            mat => {
                bail!(
                    "E_RBT_RUST_MAT: Rust model '{}' does not support materialization={} \
                     (supported: table, keyed_upsert, scoped_replace, incremental_append)",
                    model.name,
                    mat.as_str()
                );
            }
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
    ///
    /// Requires cargo feature `iceberg`.
    #[cfg(feature = "iceberg")]
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

    /// Register host UDFs on the live engine (after construction).
    pub fn register_udfs<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&SessionContext) -> Result<()>,
    {
        f(&self.ctx)
    }

    /// Register a [`crate::engine::udf::UdfPack`] on the live engine.
    pub fn register_udf_pack(&self, pack: &dyn crate::engine::udf::UdfPack) -> Result<()> {
        crate::engine::udf::register_udf_pack(&self.ctx, pack)
    }

    /// Stage 2 — register bronze sources for the DAG under `scope` (no materialize).
    ///
    /// Hosts that already called this can re-run [`Self::stage_execute_tiers`] without
    /// re-fingerprinting (Stage 1). Safe to call more than once (tables deregister+replace).
    pub async fn stage_register_bronze(
        &self,
        dag: &ModelDag,
        project_dir: impl AsRef<Path>,
        config: &RbtProjectConfig,
        scope: &RunScope,
    ) -> Result<usize> {
        let project_dir = project_dir.as_ref();
        let mut registered = HashSet::new();
        register_bronze_sources_for_dag_scoped(
            &self.ctx,
            dag,
            project_dir,
            &mut registered,
            config,
            Some(scope),
        )
        .await
        .context("frontmatter-driven bronze registration failed")
    }

    /// Stage 3 — execute topo tiers (materialize), optionally filtered to model names.
    ///
    /// Does **not** run fingerprint skip or write receipts. Compose with
    /// [`stages::stage_plan_skip`] and [`stages::stage_write_receipt`].
    ///
    /// ```rust,no_run
    /// # async fn demo(engine: &rbt::TransformationEngine, dag: &rbt::ModelDag,
    /// #   cfg: &rbt::RbtProjectConfig, scope: &rbt::RunScope) -> anyhow::Result<()> {
    /// use rbt::engine::stages::ExecuteTiersOptions;
    /// // Skip Stage 1; re-register bronze; force one model (+ ancestors by default):
    /// engine.stage_register_bronze(dag, ".", cfg, scope).await?;
    /// let _ = engine
    ///     .stage_execute_tiers(dag, ".", "./out", cfg, scope, ExecuteTiersOptions::only(["dim_x"]))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stage_execute_tiers(
        &self,
        dag: &ModelDag,
        project_dir: impl AsRef<Path>,
        output_dir: impl AsRef<Path>,
        config: &RbtProjectConfig,
        scope: &RunScope,
        opts: stages::ExecuteTiersOptions,
    ) -> Result<stages::StageExecuteResult> {
        let project_dir = project_dir.as_ref();
        // Optional model filter → runnable subgraph (ancestors included by default).
        let filtered: Option<ModelDag> = match &opts.only_models {
            None => None,
            Some(only) => {
                let keep =
                    stages::expand_model_selection(dag, only, opts.include_ancestors)?;
                Some(dag.subgraph(&keep).with_context(|| {
                    "E_RBT_SELECT: stage_execute_tiers model filter produced incomplete subgraph \
                     (include ancestors or materialize deps first)"
                })?)
            }
        };
        let work_dag = filtered.as_ref().unwrap_or(dag);

        // Never short-circuit on fingerprint for host stage re-entry; still compute
        // fingerprint for lineage unless the host provided one via opts.
        let mut run_scope = scope.clone();
        run_scope.skip_if_fingerprint_match = false;
        let host_wants_receipt = run_scope.write_receipt;
        run_scope.write_receipt = false;

        let summary = self
            .execute_dag_with_scope(work_dag, project_dir, output_dir, config, &run_scope)
            .await?;

        if host_wants_receipt {
            let finished = crate::core::receipt::now_unix_ms();
            let started = finished.saturating_sub(summary.model_results.iter().filter_map(|m| m.elapsed_ms).sum::<u64>() as u128);
            let _ = stages::stage_write_receipt(stages::ReceiptWriteArgs {
                project_dir,
                config,
                scope: &run_scope,
                run_id: summary.run_id.clone().unwrap_or_else(|| run_scope.resolve_run_id()),
                scope_key: run_scope.scope_key(),
                contract_version: effective_contract_version(config, &run_scope),
                bronze_fingerprint: opts
                    .bronze_fingerprint
                    .clone()
                    .or(summary.bronze_fingerprint.clone())
                    .unwrap_or_default(),
                models_executed: summary.models_executed,
                total_rows: summary.total_rows_produced,
                bronze_sources: summary.bronze_sources_registered,
                model_results: summary.model_results.clone(),
                started_unix_ms: started,
                finished_unix_ms: finished,
                skipped: false,
                skip_reason: None,
                error: None,
            })?;
        }

        Ok(stages::StageExecuteResult {
            models_executed: summary.models_executed,
            total_rows_produced: summary.total_rows_produced,
            model_results: summary.model_results,
            bronze_fingerprint: opts
                .bronze_fingerprint
                .or(summary.bronze_fingerprint),
        })
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
        let sql_schema: arrow::datatypes::SchemaRef = {
            let df_schema = df.schema();
            std::sync::Arc::new(df_schema.as_arrow().clone())
        };
        let mut batches = df.collect().await.with_context(|| {
            format!("E_RBT_PREVIEW: collect failed for model '{}'", target.name)
        })?;
        // RBT-A6: preview also exposes declared columns when SQL is zero-row / missing cols.
        let declared = target
            .frontmatter
            .as_ref()
            .and_then(|fm| crate::core::schema_emit::try_declared_schema(fm).ok().flatten());
        batches = crate::core::schema_emit::align_batches_to_declared(
            &batches,
            sql_schema.as_ref(),
            declared.as_deref(),
        )?;
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
        self.execute_dag_with_scope(dag, project_dir, output_dir, config, &RunScope::default())
            .await
    }

    /// DAG execution with run scope (vars, partition binds, fingerprint skip, receipts).
    pub async fn execute_dag_with_scope(
        &self,
        dag: &ModelDag,
        project_dir: impl AsRef<Path>,
        output_dir: impl AsRef<Path>,
        config: &RbtProjectConfig,
        scope: &RunScope,
    ) -> Result<DagExecutionSummary> {
        let project_dir = project_dir.as_ref();
        let output_base = output_dir.as_ref();
        let materialize = &config.materialize;
        tokio::fs::create_dir_all(output_base).await?;

        let started = now_unix_ms();
        let run_id = scope.resolve_run_id();
        let scope_key = scope.scope_key();
        let contract = effective_contract_version(config, scope);

        // Stage 1 — PlanSkip (named stage; same SoT as ops::plan_skip / CLI --skip-if-match)
        let skip_plan = stages::stage_plan_skip(dag, project_dir, config, scope)
            .context("E_RBT_FINGERPRINT: bronze fingerprint / plan_skip failed")?;
        let fp = skip_plan.current_fingerprint.clone();

        if scope.skip_if_fingerprint_match && skip_plan.should_skip {
            let finished = now_unix_ms();
            let mut summary = DagExecutionSummary {
                models_executed: 0,
                total_rows_produced: 0,
                bronze_sources_registered: 0,
                skipped: true,
                skip_reason: Some(skip_plan.reason.clone()),
                bronze_fingerprint: Some(fp.clone()),
                run_id: Some(run_id.clone()),
                receipt_path: None,
                model_results: Vec::new(),
            };
            if scope.write_receipt {
                let receipt = RunReceipt {
                    schema_version: RunReceipt::SCHEMA_VERSION,
                    run_id: run_id.clone(),
                    project: config.name.clone(),
                    package_version: crate::VERSION.into(),
                    contract_version: contract.clone(),
                    scope_key: scope_key.clone(),
                    vars: scope.vars.clone(),
                    status: RunStatus::Skipped,
                    skipped: true,
                    skip_reason: summary.skip_reason.clone(),
                    bronze_fingerprint: fp.clone(),
                    models_executed: 0,
                    total_rows: 0,
                    bronze_sources: 0,
                    model_results: Vec::new(),
                    started_unix_ms: started,
                    finished_unix_ms: finished,
                    wall_ms: finished.saturating_sub(started),
                    error: None,
                };
                summary.receipt_path = Some(receipt.write(project_dir)?);
            }
            tracing::info!(
                "E_RBT_SKIP: identical bronze fingerprint for scope {scope_key}; skipping materialize"
            );
            return Ok(summary);
        }

        // Stage 2 — RegisterBronze
        let mut registered = HashSet::new();
        let bronze_sources_registered = register_bronze_sources_for_dag_scoped(
            &self.ctx,
            dag,
            project_dir,
            &mut registered,
            config,
            Some(scope),
        )
        .await
        .context("frontmatter-driven bronze registration failed")?;

        let tiers = dag.execution_tiers()?;
        let mut exec_plan = plan_execution(dag, scope, &config.execution.concurrency)
            .context("E_RBT_WORK_UNIT: plan_execution failed")?;
        if config.execution.concurrency.large_parts_first {
            enrich_plan_from_manifests(&mut exec_plan, dag);
        }
        for n in &exec_plan.notes {
            tracing::info!(target: "rbt::plan", "{n}");
        }
        tracing::info!(
            strategy = ?exec_plan.strategy,
            max_workers = exec_plan.max_workers,
            units = exec_plan.units.len(),
            partition_slices = exec_plan.partition_slice_count(),
            "RBT-C execution plan"
        );

        let mut models_executed = 0;
        let mut total_rows_produced = 0;
        let mut model_results: Vec<ModelRunResult> = Vec::new();
        let wap_run_id = if materialize.wap {
            Some(new_wap_run_id())
        } else {
            None
        };
        // Resolve WAP stage root once per run (default: {project_dir}/.wap).
        let wap_root: Option<PathBuf> = if materialize.wap {
            let root = match materialize.wap_root.as_deref() {
                Some(raw) if !raw.trim().is_empty() => {
                    crate::core::paths::resolve_project_path(project_dir, raw, &config.roots)
                        .with_context(|| {
                            format!(
                                "E_RBT_WAP: failed resolving materialize.wap_root '{raw}' \
                                 (project_dir={})",
                                project_dir.display()
                            )
                        })?
                }
                _ => project_dir.join(".wap"),
            };
            Some(root)
        } else {
            None
        };

        // Spill root for concurrent workers (C1.11).
        let spill_root = project_dir
            .join(".rbt")
            .join("spill")
            .join(&run_id);
        if exec_plan.concurrency_enabled && exec_plan.max_workers > 1 {
            let _ = std::fs::create_dir_all(&spill_root);
        }

        for (tier_idx, tier) in tiers.iter().enumerate() {
            let l1_concurrent = exec_plan.concurrency_enabled
                && exec_plan.max_workers > 1
                && matches!(
                    exec_plan.strategy,
                    ExecutionStrategy::ModelTier | ExecutionStrategy::Auto
                )
                && tier.len() > 1
                && exec_plan
                    .units
                    .iter()
                    .filter(|u| tier.iter().any(|m| m.name == u.model))
                    .all(|u| !u.is_partition_slice);

            if l1_concurrent {
                tracing::info!(
                    "Executing DAG Tier {} with {} independent model(s) (concurrent, max_workers={})",
                    tier_idx,
                    tier.len(),
                    exec_plan.max_workers
                );
                let (rows, results) = self
                    .execute_tier_models_concurrent(
                        dag,
                        tier,
                        project_dir,
                        output_base,
                        config,
                        scope,
                        &run_id,
                        &contract,
                        &fp,
                        &exec_plan,
                        &spill_root,
                    )
                    .await?;
                models_executed += results.len();
                total_rows_produced += rows;
                model_results.extend(results);
                // Register each model for ref on the coordinator session.
                for model in tier {
                    if let Some(ref op) = model.output_path {
                        let dest = PathBuf::from(op);
                        let ref_path = if uses_parts_directory(&model.materialization) {
                            incremental_ref_path(&dest)
                        } else {
                            dest
                        };
                        if ref_path.exists() {
                            let _ = register_model_for_ref(
                                &self.ctx,
                                &model.name,
                                &model.output_format,
                                &ref_path,
                                materialize.choose_ref_backend(1),
                            )
                            .await;
                        }
                    }
                }
                continue;
            }

            tracing::info!(
                "Executing DAG Tier {} with {} independent model(s) ({})",
                tier_idx,
                tier.len(),
                if exec_plan.concurrency_enabled && exec_plan.max_workers > 1 {
                    "serial models; partition units may concurrent"
                } else {
                    "serial exec"
                }
            );

            for model in tier {
                let model_units: Vec<WorkUnit> = exec_plan
                    .units
                    .iter()
                    .filter(|u| u.model == model.name)
                    .cloned()
                    .collect();
                let model_units = if model_units.is_empty() {
                    vec![WorkUnit {
                        id: model.name.clone(),
                        model: model.name.clone(),
                        partition_bindings: Default::default(),
                        deps: Vec::new(),
                        estimated_bytes: None,
                        estimated_rows: None,
                        parallel_contract: crate::core::work_unit::ParallelContract::Unknown,
                        is_partition_slice: false,
                        skip_if_clean: false,
                    }]
                } else {
                    model_units
                };

                let l2_concurrent = exec_plan.concurrency_enabled
                    && exec_plan.max_workers > 1
                    && matches!(
                        exec_plan.strategy,
                        ExecutionStrategy::Partition | ExecutionStrategy::Auto
                    )
                    && model_units.len() > 1
                    && model_units.iter().all(|u| u.is_partition_slice);

                if l2_concurrent {
                    tracing::info!(
                        model = %model.name,
                        units = model_units.len(),
                        max_workers = exec_plan.max_workers,
                        "L2 concurrent partition units"
                    );
                    let (rows, mrr) = self
                        .execute_partition_units_concurrent(
                            dag,
                            model,
                            &model_units,
                            project_dir,
                            output_base,
                            config,
                            scope,
                            &run_id,
                            &contract,
                            &fp,
                            &exec_plan,
                            &spill_root,
                        )
                        .await?;
                    models_executed += 1;
                    total_rows_produced += rows;
                    // Register parts for ref
                    if let Some(ref op) = model.output_path {
                        let dest = PathBuf::from(op);
                        let ref_path = incremental_ref_path(&dest);
                        if ref_path.is_dir() {
                            let _ = register_model_for_ref(
                                &self.ctx,
                                &model.name,
                                &model.output_format,
                                &ref_path,
                                materialize.choose_ref_backend(rows.max(1)),
                            )
                            .await;
                        }
                    }
                    model_results.push(mrr);
                    continue;
                }

                for unit in &model_units {
                let effective_scope = scope_for_unit(scope, unit);
                let scope = &effective_scope; // shadow: rest of body uses unit scope
                tracing::info!(
                    "Executing model '{}'{}...",
                    model.name,
                    if unit.is_partition_slice {
                        format!(" unit={}", unit.id)
                    } else {
                        String::new()
                    }
                );
                let model_started = now_unix_ms();

                // Dirty-part skip (C1.7 / Phase 3 hive-aware)
                if unit.is_partition_slice && unit.skip_if_clean {
                    if let Some(ref op) = model.output_path {
                        let dest_path = PathBuf::from(op);
                        let part_keys = resolve_part_keys(
                            model.frontmatter.as_ref().and_then(|f| f.part_key.as_deref()),
                            model
                                .frontmatter
                                .as_ref()
                                .and_then(|f| f.partition_by.as_deref()),
                            scope,
                        );
                        if let Ok(sid) =
                            scope_part_id(&model.name, &contract, &part_keys, scope)
                        {
                            let layout = resolve_parts_layout(
                                model
                                    .frontmatter
                                    .as_ref()
                                    .and_then(|f| f.parts_layout.as_deref()),
                                materialize.default_parts_layout.as_deref(),
                            );
                            let layout_root = table_layout_root(&dest_path, layout);
                            let part_rel = crate::materializer::scoped_part_rel_path(
                                layout,
                                &sid,
                                &unit.partition_bindings,
                            );
                            let unit_fp = bronze_fingerprint(dag, project_dir, config, scope)
                                .unwrap_or_else(|_| fp.clone());
                            if part_is_clean(&layout_root, &part_rel, &unit_fp) {
                                tracing::info!(
                                    model = %model.name,
                                    part = %part_rel,
                                    "dirty-part skip: source_fp match"
                                );
                                models_executed += 1;
                                let elapsed_ms =
                                    now_unix_ms().saturating_sub(model_started) as u64;
                                model_results.push(
                                    ModelRunResult::success(
                                        format!("{}@{}", model.name, unit.id),
                                        0,
                                        Some(dest_path.display().to_string()),
                                        None,
                                        vec!["dirty_part_skip".into()],
                                        Some(elapsed_ms),
                                    )
                                    .with_kind(model.kind.as_str())
                                    .with_materialization(model.materialization.as_str()),
                                );
                                continue;
                            }
                        }
                    }
                }

                // Late-bind: if this model carries frontmatter not registered yet
                register_bronze_for_model_scoped(
                    &self.ctx,
                    model,
                    project_dir,
                    &mut registered,
                    config,
                    Some(scope),
                )
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

                let (assertions, fail_on_error) = model_assertions(model, config)?;
                let mut write_opts =
                    MaterializeWriteOptions::from_config(materialize, fail_on_error);
                if model
                    .frontmatter
                    .as_ref()
                    .map(|f| f.wants_lineage_stamp())
                    .unwrap_or(false)
                {
                    write_opts = write_opts.with_lineage(LineageStamp {
                        run_id: run_id.clone(),
                        contract_version: contract.clone(),
                        model: model.name.clone(),
                        bronze_fingerprint: Some(fp.clone()),
                    });
                }
                // RBT-A6: merge declared columns into published schema (zero-row + missing cols)
                if let Some(fm) = model.frontmatter.as_ref() {
                    if let Some(schema) = crate::core::schema_emit::try_declared_schema(fm)
                        .with_context(|| {
                            format!(
                                "E_RBT_SCHEMA_EMIT: model '{}' declared columns invalid",
                                model.name
                            )
                        })?
                    {
                        write_opts = write_opts.with_declared_schema(schema);
                    }
                    // ADR-009 / RBT-A16: optional SK stamp from grain (hash or MIISK registry)
                    if let Some(sk) = crate::engine::surrogate_key::SurrogateKeyConfig::from_frontmatter(fm)
                        .with_context(|| {
                            format!("E_RBT_SK: model '{}' surrogate_key config", model.name)
                        })?
                    {
                        let sk = sk.with_registry_for_model(project_dir, &model.name);
                        write_opts = write_opts.with_surrogate_key(sk);
                    }
                }
                let mode = materialize.effective_mode();

                // WAP: write to stage path first; publish only after audit.
                // Parts strategies (incl. table + consolidate:never) skip WAP staging.
                let table_parts_only = matches!(model.materialization, Materialization::Table)
                    && !materialize.consolidate.table_writes_monolith();
                // Alias is zero-copy at dest; never stage via WAP (no rewrite).
                let is_alias = model.materialization.is_alias();
                let (write_path, wap_paths) = if let (Some(ref run_id), Some(ref wap_root)) =
                    (wap_run_id.as_ref(), wap_root.as_ref())
                {
                    if matches!(
                        model.output_format,
                        OutputFormat::Parquet | OutputFormat::ZeroCopyClone
                    ) && !uses_parts_directory(&model.materialization)
                        && !table_parts_only
                        && !is_alias
                    {
                        let paths = WapModelPaths::for_model_with_root(
                            wap_root,
                            run_id,
                            &model.name,
                            &dest_path,
                        );
                        if let Some(p) = paths.stage_path.parent() {
                            std::fs::create_dir_all(p)?;
                        }
                        (paths.stage_path.clone(), Some(paths))
                    } else {
                        (dest_path.clone(), None)
                    }
                } else {
                    (dest_path.clone(), None)
                };

                let mut upsert_for_model: Option<crate::materializer::UpsertStats> = None;
                // Track alias parts for ref() registration.
                let mut alias_parts = false;
                let mut alias_register_path: Option<PathBuf> = None;

                // Design B: host Rust model (skip SQL path).
                let (row_count, write_stats) = if model.kind == ModelKind::Rust {
                    if is_alias {
                        bail!(
                            "E_RBT_ALIAS: Rust model '{}' cannot use materialization=alias \
                             (alias is SQL/file identity only; implement a no-op host model or use SQL)",
                            model.name
                        );
                    }
                    let (rows, stats, upsert) = self
                        .materialize_rust_model(
                            model,
                            project_dir,
                            scope,
                            &dest_path,
                            &write_path,
                            &write_opts,
                            &assertions,
                            fail_on_error,
                            &run_id,
                            &contract,
                            &fp,
                            table_parts_only,
                            materialize,
                            dag,
                            &unit.partition_bindings,
                        )
                        .await?;
                    upsert_for_model = upsert;
                    (rows, stats)
                } else {
                let (row_count, write_stats) = match (
                    &model.materialization,
                    &model.output_format,
                    mode,
                ) {
                    // RBT-C Phase 0: identity / zero-copy (no SQL execute, no re-encode).
                    (Materialization::ZeroCopyClone, _, _) => {
                        if write_opts.lineage.is_some()
                            || model
                                .frontmatter
                                .as_ref()
                                .map(|f| f.wants_lineage_stamp())
                                .unwrap_or(false)
                        {
                            bail!(
                                "E_RBT_ALIAS: model '{}': lineage_stamp is incompatible with \
                                 materialization alias (would require rewrite). Remove lineage_stamp \
                                 or use materialization: table",
                                model.name
                            );
                        }
                        if !assertions.is_empty() {
                            bail!(
                                "E_RBT_ALIAS: model '{}': tests/assertions require a rewrite path; \
                                 use materialization: table (or drop tests on pure identity marts)",
                                model.name
                            );
                        }
                        let upstream_name = resolve_alias_upstream(model)?;
                        let upstream_node = dag
                            .node_map
                            .get(&upstream_name)
                            .map(|&idx| &dag.graph[idx])
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "E_RBT_ALIAS: model '{}': alias_of/upstream '{}' is not in the DAG",
                                    model.name,
                                    upstream_name
                                )
                            })?;
                        let upstream_dest = upstream_node
                            .output_path
                            .as_ref()
                            .map(PathBuf::from)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "E_RBT_ALIAS: model '{}': upstream '{}' has no output_path",
                                    model.name,
                                    upstream_name
                                )
                            })?;
                        let source = upstream_lake_path(&upstream_dest);
                        alias_parts = source.is_dir();
                        let stats = materialize_alias(&dest_path, &source, &upstream_name)
                            .with_context(|| {
                                format!(
                                    "E_RBT_ALIAS: model '{}' alias of '{}' failed",
                                    model.name, upstream_name
                                )
                            })?;
                        alias_register_path = Some(alias_ref_path(
                            &dest_path,
                            &stats.path,
                            alias_parts,
                        ));
                        (stats.rows, Some(stats))
                    }
                    (
                        Materialization::IncrementalAppend,
                        OutputFormat::Parquet | OutputFormat::ZeroCopyClone,
                        MaterializeMode::Stream,
                    ) => {
                        let df = sql_for_model(&self.ctx, model).await?;
                        let stream = df.execute_stream().await?;
                        let stats = materialize_incremental_append_stream(
                            stream,
                            &dest_path,
                            &write_opts,
                            &assertions,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "E_RBT_INCREMENTAL: model '{}' append failed",
                                model.name
                            )
                        })?;
                        log_assertion_result(model, &stats, fail_on_error)?;
                        maybe_consolidate_after_parts(
                            materialize,
                            &dest_path,
                            &write_opts,
                            &model.name,
                        )
                        .await?;
                        (stats.rows, Some(stats))
                    }
                    (
                        Materialization::ScopedReplace,
                        OutputFormat::Parquet | OutputFormat::ZeroCopyClone,
                        MaterializeMode::Stream,
                    ) => {
                        let contract = effective_contract_version(config, scope);
                        let part_keys = resolve_part_keys(
                            model
                                .frontmatter
                                .as_ref()
                                .and_then(|f| f.part_key.as_deref()),
                            model
                                .frontmatter
                                .as_ref()
                                .and_then(|f| f.partition_by.as_deref()),
                            scope,
                        );
                        let sid = scope_part_id(
                            &model.name,
                            &contract,
                            &part_keys,
                            scope,
                        )
                        .with_context(|| {
                            format!(
                                "E_RBT_PART_KEY: model '{}' cannot build scope_id",
                                model.name
                            )
                        })?;
                        let unit_source_fp =
                            bronze_fingerprint(dag, project_dir, config, scope).ok();
                        let mut keys = BTreeMap::new();
                        for k in &part_keys {
                            if let Some(sv) = scope.vars.get(k) {
                                if let Some(v) = sv.as_single() {
                                    keys.insert(k.clone(), v.to_string());
                                } else {
                                    keys.insert(k.clone(), sv.canonical());
                                }
                            }
                        }
                        let df = sql_for_model(&self.ctx, model).await?;
                        let stream = df.execute_stream().await?;
                        let fm = model.frontmatter.as_ref();
                        let layout = resolve_parts_layout(
                            fm.and_then(|f| f.parts_layout.as_deref()),
                            materialize.default_parts_layout.as_deref(),
                        );
                        let stats = materialize_scoped_replace_stream_with(
                            stream,
                            &dest_path,
                            &write_opts,
                            &assertions,
                            &sid,
                            &ScopedPartPublish {
                                keys,
                                source_fp: unit_source_fp,
                                layout,
                                model: Some(model.name.clone()),
                                grain: fm.and_then(|f| f.grain.clone()),
                                partition_by: fm.and_then(|f| f.partition_by.clone()),
                                part_key: fm.and_then(|f| f.part_key.clone()),
                                sort_within_part: fm.and_then(|f| f.sort_within_part.clone()),
                                parallel_safe: fm.and_then(|f| f.parallel_safe),
                                collect_stats: true,
                            },
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "E_RBT_INCREMENTAL: model '{}' scoped_replace failed (scope_id={sid})",
                                model.name
                            )
                        })?;
                        log_assertion_result(model, &stats, fail_on_error)?;
                        maybe_consolidate_after_parts(
                            materialize,
                            &dest_path,
                            &write_opts,
                            &model.name,
                        )
                        .await?;
                        (stats.rows, Some(stats))
                    }
                    (
                        Materialization::Table,
                        OutputFormat::Parquet | OutputFormat::ZeroCopyClone,
                        MaterializeMode::Stream,
                    ) if !materialize.consolidate.table_writes_monolith() => {
                        // A5: consolidate: never → parts only for table
                        let df = sql_for_model(&self.ctx, model).await?;
                        let stream = df.execute_stream().await?;
                        let stats = materialize_table_parts_only_stream(
                            stream,
                            &dest_path,
                            &write_opts,
                            &assertions,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "E_RBT_CONSOLIDATE: model '{}' table parts-only failed",
                                model.name
                            )
                        })?;
                        log_assertion_result(model, &stats, fail_on_error)?;
                        (stats.rows, Some(stats))
                    }
                    (
                        Materialization::KeyedUpsert,
                        OutputFormat::Parquet | OutputFormat::ZeroCopyClone,
                        _,
                    ) => {
                        // RBT-A7: collect SQL (v1 memory bound), upsert vs existing parquet, rewrite.
                        let fm = model.frontmatter.as_ref().ok_or_else(|| {
                            anyhow::anyhow!(
                                "E_RBT_UPSERT_KEY: model '{}': keyed_upsert requires frontmatter \
                                 with unique_key",
                                model.name
                            )
                        })?;
                        let upsert_cfg = fm.keyed_upsert_config().with_context(|| {
                            format!(
                                "E_RBT_UPSERT_KEY: model '{}' keyed_upsert config invalid",
                                model.name
                            )
                        })?;
                        let df = sql_for_model(&self.ctx, model).await?;
                        let mut batches = df.collect().await.with_context(|| {
                            format!("E_RBT_SQL: collect for keyed_upsert model '{}'", model.name)
                        })?;
                        if let Some(ref lin) = write_opts.lineage {
                            batches = batches
                                .iter()
                                .map(|b| stamp_batch(b, lin))
                                .collect::<Result<Vec<_>>>()?;
                        }
                        if let Some(ref d) = write_opts.declared_schema {
                            batches = batches
                                .iter()
                                .map(|b| {
                                    crate::core::schema_emit::ensure_declared_columns(b, d.as_ref())
                                })
                                .collect::<Result<Vec<_>>>()?;
                        }
                        let (stats, upsert_stats) = materialize_keyed_upsert(
                            &dest_path,
                            &batches,
                            &upsert_cfg,
                            &write_opts,
                            &assertions,
                        )
                        .with_context(|| {
                            format!(
                                "E_RBT_UPSERT_SCHEMA: model '{}' keyed_upsert failed",
                                model.name
                            )
                        })?;
                        log_assertion_result(model, &stats, fail_on_error)?;
                        tracing::info!(
                            model = %model.name,
                            inserted = upsert_stats.rows_inserted,
                            updated = upsert_stats.rows_updated,
                            touched = upsert_stats.rows_touched,
                            kept = upsert_stats.rows_kept,
                            total = upsert_stats.total_rows,
                            "keyed_upsert complete"
                        );
                        upsert_for_model = Some(upsert_stats);
                        (stats.rows, Some(stats))
                    }
                    (
                        Materialization::IncrementalMerge,
                        _,
                        _,
                    ) => {
                        bail!(
                            "E_RBT_INCREMENTAL: model '{}': incremental_merge is not implemented yet \
                             (use incremental_append or scoped_replace for part files)",
                            model.name
                        );
                    }
                    (
                        Materialization::KeyedUpsert,
                        fmt,
                        _,
                    ) => {
                        bail!(
                            "E_RBT_UPSERT_SCHEMA: model '{}': keyed_upsert requires parquet \
                             (got {:?})",
                            model.name,
                            fmt
                        );
                    }
                    (
                        Materialization::ScopedReplace | Materialization::IncrementalAppend,
                        _,
                        MaterializeMode::Collect,
                    ) => {
                        bail!(
                            "E_RBT_INCREMENTAL: model '{}': parts strategies (scoped_replace / \
                             incremental_append) require stream materialize mode \
                             (set materialize.mode: stream or omit for default)",
                            model.name
                        );
                    }
                    (
                        Materialization::Table,
                        OutputFormat::Parquet | OutputFormat::ZeroCopyClone,
                        MaterializeMode::Collect,
                    ) if !materialize.consolidate.table_writes_monolith() => {
                        bail!(
                            "E_RBT_CONSOLIDATE: model '{}': consolidate: never with table \
                             materialization requires stream mode \
                             (set materialize.mode: stream or omit for default)",
                            model.name
                        );
                    }
                    (_, _, MaterializeMode::Stream) => {
                        let stats = execute_model_stream(
                            &self.ctx,
                            model,
                            &write_path,
                            &write_opts,
                            &assertions,
                            fail_on_error,
                        )
                        .await?;
                        (stats.rows, Some(stats))
                    }
                    (_, _, MaterializeMode::Collect) => {
                        let rows = execute_model_collect(
                            &self.ctx,
                            model,
                            &write_path,
                            &write_opts,
                            &assertions,
                            fail_on_error,
                        )
                        .await?;
                        (rows, None)
                    }
                };
                (row_count, write_stats)
                };

                // WAP publish after successful write+audit (stream assertions already applied).
                if let Some(ref paths) = wap_paths {
                    let validation = write_stats
                        .as_ref()
                        .map(|s| s.validation.clone())
                        .unwrap_or_else(|| crate::testing::ValidationResult {
                            total_rows: row_count,
                            passed_assertions: 0,
                            failed_assertions: 0,
                            errors: Vec::new(),
                        });
                    wap_publish(paths, &model.name, row_count, &validation)?;
                }

                // Expose model for downstream {{ ref() }} per project materialize policy.
                // Parts strategies re-register even when this scope wrote 0 rows so peer
                // parts remain visible to later models in the same run.
                let parts_strategy = (uses_parts_directory(&model.materialization)
                    || alias_parts
                    || (matches!(model.materialization, Materialization::Table)
                        && !materialize.consolidate.table_writes_monolith()))
                    && matches!(
                        model.output_format,
                        OutputFormat::Parquet | OutputFormat::ZeroCopyClone
                    );
                if row_count > 0
                    || parts_strategy
                    || is_alias
                    || matches!(
                        model.output_format,
                        OutputFormat::Parquet
                            | OutputFormat::Iceberg
                            | OutputFormat::ParquetAndIceberg
                            | OutputFormat::ZeroCopyClone
                    )
                {
                    let backend = materialize.choose_ref_backend(row_count);
                    if row_count > 0 || parts_strategy || is_alias {
                        let ref_path = if let Some(p) = alias_register_path.clone() {
                            p
                        } else if parts_strategy {
                            incremental_ref_path(&dest_path)
                        } else {
                            dest_path.clone()
                        };
                        // Skip register if parts dir missing (never ran successfully)
                        let should_register = is_alias
                            || !parts_strategy
                            || ref_path.is_dir()
                            || row_count > 0;
                        if should_register {
                            register_model_for_ref(
                                &self.ctx,
                                &model.name,
                                &model.output_format,
                                &ref_path,
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
                                mat = ?model.materialization,
                                "registered model for ref()"
                            );
                        }
                    }
                }

                // Relationship tests after model is registered for ref() (parent dims must exist).
                if let Some(fm) = model.frontmatter.as_ref() {
                    if let Some(tests) = fm.tests.as_ref() {
                        if let Some(rels) = tests.relationships.as_ref() {
                            if !rels.is_empty() {
                                check_relationships(
                                    &self.ctx,
                                    &model.name,
                                    rels,
                                    tests.should_fail_on_error(),
                                )
                                .await?;
                            }
                        }
                    }
                }

                models_executed += 1;
                total_rows_produced += row_count;
                let model_finished = now_unix_ms();
                let elapsed_ms = model_finished.saturating_sub(model_started) as u64;
                let (phase, tags) = model
                    .frontmatter
                    .as_ref()
                    .map(|f| {
                        (
                            f.phase.clone(),
                            f.tags.clone().unwrap_or_default(),
                        )
                    })
                    .unwrap_or((None, Vec::new()));
                let result_name = if unit.is_partition_slice {
                    format!("{}@{}", model.name, unit.id)
                } else {
                    model.name.clone()
                };
                let mut mrr = ModelRunResult::success(
                    result_name,
                    row_count,
                    Some(dest_path.display().to_string()),
                    phase,
                    tags,
                    Some(elapsed_ms),
                )
                .with_kind(model.kind.as_str())
                .with_materialization(model.materialization.as_str());
                if let Some(u) = upsert_for_model {
                    mrr = mrr.with_upsert_stats(
                        u.rows_inserted,
                        u.rows_updated,
                        u.rows_touched,
                        u.rows_kept,
                    );
                }
                model_results.push(mrr);
                } // end for unit in model_units
            } // end for model in tier
        }

        let finished = now_unix_ms();
        let mut summary = DagExecutionSummary {
            models_executed,
            total_rows_produced,
            bronze_sources_registered,
            skipped: false,
            skip_reason: None,
            bronze_fingerprint: Some(fp.clone()),
            run_id: Some(run_id.clone()),
            receipt_path: None,
            model_results: model_results.clone(),
        };

        if scope.write_receipt {
            let receipt = RunReceipt {
                schema_version: RunReceipt::SCHEMA_VERSION,
                run_id: run_id.clone(),
                project: config.name.clone(),
                package_version: crate::VERSION.into(),
                contract_version: contract,
                scope_key,
                vars: scope.vars.clone(),
                status: RunStatus::Ok,
                skipped: false,
                skip_reason: None,
                bronze_fingerprint: fp,
                models_executed,
                total_rows: total_rows_produced,
                bronze_sources: bronze_sources_registered,
                model_results,
                started_unix_ms: started,
                finished_unix_ms: finished,
                wall_ms: finished.saturating_sub(started),
                error: None,
            };
            summary.receipt_path = Some(receipt.write(project_dir)?);
        }

        Ok(summary)
    }
}

/// After parts strategies, optionally rebuild monolith when `consolidate: always`.
async fn maybe_consolidate_after_parts(
    materialize: &MaterializeConfig,
    dest_path: &Path,
    write_opts: &crate::materializer::MaterializeWriteOptions,
    model_name: &str,
) -> Result<()> {
    if !materialize.consolidate.parts_also_write_monolith() {
        return Ok(());
    }
    let stats = consolidate_parts_to_parquet(dest_path, write_opts)
        .await
        .with_context(|| {
            format!(
                "E_RBT_CONSOLIDATE: model '{model_name}' always-consolidate failed"
            )
        })?;
    tracing::info!(
        model = %model_name,
        rows = stats.rows,
        path = %dest_path.display(),
        "consolidated parts → monolith parquet (consolidate: always)"
    );
    Ok(())
}

/// Build frontmatter assertion list + fail-on-error policy for a model.
///
/// Resolves `accepted_values` contract references via `config.contracts.enums`.
fn model_assertions(
    model: &ModelNode,
    config: &RbtProjectConfig,
) -> Result<(Vec<Assertion>, bool)> {
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
                let resolved = if let Some(map) = tests.accepted_values.as_ref() {
                    Some(
                        config
                            .contracts
                            .resolve_accepted_values(map)
                            .with_context(|| {
                                format!(
                                    "E_RBT_CONTRACT: resolve accepted_values for model '{}'",
                                    model.name
                                )
                            })?,
                    )
                } else {
                    None
                };
                assertions_from_model_tests(
                    tests.not_null.as_deref(),
                    unique.as_deref(),
                    resolved.as_ref(),
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
    Ok((assertions, fail_on_error))
}

fn log_assertion_result(
    model: &ModelNode,
    stats: &StreamWriteStats,
    fail_on_error: bool,
) -> Result<()> {
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
    } else if stats.validation.passed_assertions > 0 {
        tracing::info!(
            "model '{}': {} assertion(s) passed ({} rows)",
            model.name,
            stats.validation.passed_assertions,
            stats.rows
        );
    }
    Ok(())
}

async fn execute_model_stream(
    ctx: &SessionContext,
    model: &ModelNode,
    dest_path: &Path,
    write_opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
    fail_on_error: bool,
) -> Result<StreamWriteStats> {
    let df = sql_for_model(ctx, model).await?;
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
    use crate::core::schema_emit::align_batches_to_declared;
    use crate::materializer::write_empty_parquet;
    use datafusion::common::DFSchema;

    let df = sql_for_model(ctx, model).await?;
    // DFSchema → Arrow Schema for declared merge on zero-row collect.
    let sql_schema: arrow::datatypes::SchemaRef = {
        let df_schema: &DFSchema = df.schema();
        std::sync::Arc::new(df_schema.as_arrow().clone())
    };
    let mut batches = df.collect().await.with_context(|| {
        format!(
            "E_RBT_SQL: collect failed for model '{}'",
            model.name
        )
    })?;
    batches = align_batches_to_declared(
        &batches,
        sql_schema.as_ref(),
        write_opts.declared_schema.as_deref(),
    )?;
    if let Some(ref lin) = write_opts.lineage {
        batches = batches
            .iter()
            .map(|b| stamp_batch(b, lin))
            .collect::<Result<Vec<_>>>()?;
    }
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Zero-row: still publish schema-stable parquet when we have a batch/schema (A6).
    if row_count == 0
        && matches!(
            model.output_format,
            OutputFormat::Parquet | OutputFormat::ZeroCopyClone
        )
    {
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or(sql_schema);
        write_empty_parquet(schema, dest_path, write_opts).with_context(|| {
            format!(
                "E_RBT_SCHEMA_EMIT: zero-row write for model '{}' → {}",
                model.name,
                dest_path.display()
            )
        })?;
    } else {
        MultiFormatWriter::write_batches(&batches, &model.output_format, dest_path)?;
    }

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

    Ok(row_count)
}

/// FK-ish relationship checks: orphan child keys vs parent model table.
async fn check_relationships(
    ctx: &SessionContext,
    child_model: &str,
    rels: &[crate::core::frontmatter::RelationshipTest],
    fail_on_error: bool,
) -> Result<()> {
    for rel in rels {
        let parent_col = rel.parent_column();
        // Escape double-quotes in identifiers lightly (model names are [a-z0-9_]).
        let sql = format!(
            r#"SELECT COUNT(*) AS orphan_cnt FROM "{child}" c
WHERE c."{child_col}" IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM "{parent}" p WHERE p."{parent_col}" = c."{child_col}"
  )"#,
            child = child_model.replace('"', ""),
            child_col = rel.column.replace('"', ""),
            parent = rel.to_model.replace('"', ""),
            parent_col = parent_col.replace('"', ""),
        );
        let df = ctx.sql(&sql).await.with_context(|| {
            format!(
                "E_RBT_RELATIONSHIP: SQL failed for {}.{} → {}.{}: {sql}",
                child_model, rel.column, rel.to_model, parent_col
            )
        })?;
        let batches = df.collect().await?;
        use arrow::array::Array;
        let orphans = batches
            .first()
            .map(|b| {
                let col = b.column(0);
                if col.len() == 0 || col.is_null(0) {
                    return 0i64;
                }
                if let Some(a) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                    return a.value(0);
                }
                if let Some(a) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
                    return a.value(0) as i64;
                }
                if let Some(a) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    return i64::from(a.value(0));
                }
                0
            })
            .unwrap_or(0);
        if orphans > 0 {
            let msg = format!(
                "E_RBT_RELATIONSHIP: model '{child_model}' column '{}' has {orphans} value(s) \
                 not found in {}.{}",
                rel.column, rel.to_model, parent_col
            );
            if fail_on_error {
                bail!("{msg}");
            }
            tracing::warn!("{msg}");
        } else {
            tracing::info!(
                "model '{}': relationship {} → {}.{} ok",
                child_model,
                rel.column,
                rel.to_model,
                parent_col
            );
        }
    }
    Ok(())
}

/// Path used to re-read a model from the lake after materialize.
fn lake_read_path(format: &OutputFormat, dest_path: &Path) -> PathBuf {
    match format {
        OutputFormat::Iceberg => {
            // Catalog SoR: prefer `.rbt_iceberg_data` hint, then any parquet under table root.
            let hint = dest_path.join(".rbt_iceberg_data");
            if let Ok(p) = std::fs::read_to_string(&hint) {
                let p = PathBuf::from(p.trim());
                if p.exists() {
                    return p;
                }
            }
            let preferred = dest_path.join("data/part-00000.parquet");
            if preferred.exists() {
                preferred
            } else if let Some(p) = find_first_parquet_under(dest_path) {
                p
            } else {
                preferred
            }
        }
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

fn find_first_parquet_under(dir: &Path) -> Option<PathBuf> {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let entries = std::fs::read_dir(&d).ok()?;
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|x| x.to_str()) == Some("parquet") {
                return Some(p);
            }
        }
    }
    None
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
                    let path = resolve_lake_read_path_for(format, dest_path, Some(name), None)?;
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
                let path = resolve_lake_read_path_for(format, dest_path, Some(name), None)?;
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

fn resolve_lake_read_path_for(
    format: &OutputFormat,
    dest_path: &Path,
    upstream_model: Option<&str>,
    current_model: Option<&str>,
) -> Result<PathBuf> {
    let mut path = lake_read_path(format, dest_path);
    if !path.exists() && matches!(format, OutputFormat::ParquetAndIceberg) {
        let alt = sibling_iceberg_dir(dest_path).join("data/part-00000.parquet");
        if alt.exists() {
            path = alt;
        }
    }
    if !path.exists() {
        let name = upstream_model.unwrap_or_else(|| {
            dest_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
        });
        let layer_hint = if name.starts_with("stg_") {
            Some("staging")
        } else if name.starts_with("tf_") || name.starts_with("int_") {
            Some("transforms")
        } else if name.starts_with("dim_") || name.starts_with("fact_") || name.starts_with("obt_") {
            Some("marts")
        } else {
            None
        };
        return Err(crate::core::diagnostics::ref_missing_report(
            name,
            &path,
            current_model,
            layer_hint,
        )
        .into_error());
    }
    Ok(path)
}

/// Best-effort list of bare table names registered on the session (for diagnostics).
fn list_registered_table_names(ctx: &SessionContext) -> Vec<String> {
    let mut names = Vec::new();
    // catalog.schema.table walk — DataFusion SessionState
    let state = ctx.state();
    let catalog_list = state.catalog_list();
    for cat_name in catalog_list.catalog_names() {
        let Some(cat) = catalog_list.catalog(&cat_name) else {
            continue;
        };
        for schema_name in cat.schema_names() {
            let Some(schema) = cat.schema(&schema_name) else {
                continue;
            };
            for t in schema.table_names() {
                names.push(t);
            }
        }
    }
    names.sort();
    names.dedup();
    names
}

/// Rewrap SQL failures: DF "table not found" → structured `E_RBT_SQL_TABLE`.
fn enrich_sql_error(
    ctx: &SessionContext,
    model: &ModelNode,
    err: anyhow::Error,
) -> anyhow::Error {
    let full = format!("{err:#}");
    if crate::core::diagnostics::is_table_not_found_error(&full) {
        let missing = crate::core::diagnostics::extract_missing_table_hint(&full);
        let registered = list_registered_table_names(ctx);
        let snippet: String = model
            .compiled_sql
            .chars()
            .take(240)
            .collect();
        return crate::core::diagnostics::sql_table_report(
            &model.name,
            &full,
            missing.as_deref(),
            &registered,
            Some(&snippet),
        )
        .into_error();
    }
    err
}

/// Run SQL for a model with enriched table-not-found diagnostics.
async fn sql_for_model(
    ctx: &SessionContext,
    model: &ModelNode,
) -> Result<datafusion::dataframe::DataFrame> {
    ctx.sql(&model.compiled_sql)
        .await
        .map_err(|e| {
            enrich_sql_error(
                ctx,
                model,
                anyhow::anyhow!(e).context(format!("E_RBT_SQL: model '{}'", model.name)),
            )
        })
}

impl TransformationEngine {
    /// L1: concurrent independent models in a tier (private sessions).
    async fn execute_tier_models_concurrent(
        &self,
        dag: &ModelDag,
        tier: &[ModelNode],
        project_dir: &Path,
        output_base: &Path,
        config: &RbtProjectConfig,
        scope: &RunScope,
        _run_id: &str,
        _contract: &str,
        _fp: &str,
        exec_plan: &ExecutionPlan,
        spill_root: &Path,
    ) -> Result<(usize, Vec<ModelRunResult>)> {
        use futures::stream::{self, StreamExt};
        use crate::core::parser::DependencyRef;

        let max_w = exec_plan.max_workers.max(1);
        let lake_index = dag_model_output_index(dag);
        // Private workers need host Design B models too (registry is per-engine).
        let rust_models = self.snapshot_rust_models();
        let results = stream::iter(tier.iter().cloned().enumerate())
            .map(|(worker_i, model)| {
                let project_dir = project_dir.to_path_buf();
                let output_base = output_base.to_path_buf();
                let config = config.clone();
                let scope = scope.clone();
                let spill = spill_root.join(format!("worker_{worker_i}"));
                let lake_index = lake_index.clone();
                let rust_models = rust_models.clone();
                let model_deps: Vec<String> = model
                    .dependencies
                    .iter()
                    .filter_map(|d| match d {
                        DependencyRef::Model(n) => Some(n.clone()),
                        _ => None,
                    })
                    .collect();
                let mini = clone_single_model_dag_bronze_only(&model);
                async move {
                    let _ = std::fs::create_dir_all(&spill);
                    let engine = TransformationEngine::new();
                    for m in &rust_models {
                        engine
                            .register_rust_model(Arc::clone(m))
                            .context("E_RBT_CONCURRENT: clone RustModel onto L1 worker")?;
                    }
                    let mut unit_scope = scope;
                    unit_scope.write_receipt = false;
                    unit_scope.skip_if_fingerprint_match = false;
                    let mut cfg = config;
                    cfg.execution.concurrency.enabled = false;
                    cfg.execution.concurrency.max_workers = 1;
                    cfg.execution.concurrency.strategy = ExecutionStrategy::Serial;

                    for dep_name in &model_deps {
                        if let Some((path, fmt, mat)) = lake_index.get(dep_name) {
                            let ref_path = if uses_parts_directory(mat) {
                                incremental_ref_path(path)
                            } else {
                                path.clone()
                            };
                            let _ = register_model_for_ref(
                                &engine.ctx,
                                dep_name,
                                fmt,
                                &ref_path,
                                RefBackend::LakeFile,
                            )
                            .await;
                        }
                    }

                    let summary = engine
                        .execute_dag_with_scope(
                            &mini,
                            &project_dir,
                            &output_base,
                            &cfg,
                            &unit_scope,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "E_RBT_CONCURRENT: L1 worker failed for model '{}'",
                                model.name
                            )
                        })?;
                    let rows = summary.total_rows_produced;
                    let mrr = summary
                        .model_results
                        .into_iter()
                        .next()
                        .unwrap_or_else(|| {
                            ModelRunResult::success(
                                model.name.clone(),
                                rows,
                                model.output_path.clone(),
                                None,
                                vec!["l1_concurrent".into()],
                                None,
                            )
                        });
                    let _ = spill;
                    Ok::<_, anyhow::Error>((rows, mrr))
                }
            })
            .buffer_unordered(max_w)
            .collect::<Vec<_>>()
            .await;

        let mut total = 0usize;
        let mut out = Vec::new();
        for r in results {
            let (rows, mrr) = r?;
            total += rows;
            out.push(mrr);
        }
        Ok((total, out))
    }

    /// L2: concurrent partition WorkUnits for one scoped_replace model (private sessions).
    ///
    /// Each worker **must** call [`Self::materialize_rust_model`] (or the SQL unit path)
    /// with the unit's `partition_bindings` — re-planning a mini-DAG as serial mega would
    /// drop bindings and re-process the full upstream table per unit.
    async fn execute_partition_units_concurrent(
        &self,
        dag: &ModelDag,
        model: &ModelNode,
        units: &[WorkUnit],
        project_dir: &Path,
        output_base: &Path,
        config: &RbtProjectConfig,
        scope: &RunScope,
        run_id: &str,
        contract: &str,
        fp: &str,
        exec_plan: &ExecutionPlan,
        spill_root: &Path,
    ) -> Result<(usize, ModelRunResult)> {
        use crate::core::parser::DependencyRef;
        use futures::stream::{self, StreamExt};

        let max_w = exec_plan.max_workers.max(1);
        let started = now_unix_ms();
        let rust_models = self.snapshot_rust_models();
        let dag_full_paths = dag_model_output_index(dag);

        let results = stream::iter(units.iter().cloned().enumerate())
            .map(|(worker_i, unit)| {
                let project_dir = project_dir.to_path_buf();
                let output_base = output_base.to_path_buf();
                let config = config.clone();
                let scope = scope.clone();
                let model = model.clone();
                let spill = spill_root.join(format!("worker_{worker_i}"));
                let dag_full_paths = dag_full_paths.clone();
                let rust_models = rust_models.clone();
                let run_id = run_id.to_string();
                let contract = contract.to_string();
                let fp = fp.to_string();
                async move {
                    let _ = std::fs::create_dir_all(&spill);
                    let engine = TransformationEngine::new();
                    for m in &rust_models {
                        engine
                            .register_rust_model(Arc::clone(m))
                            .context("E_RBT_CONCURRENT: clone RustModel onto L2 worker")?;
                    }

                    // Register upstream model deps from lake (stg already written by tier 0).
                    for dep in &model.dependencies {
                        if let DependencyRef::Model(dep_name) = dep {
                            if let Some((path, fmt, mat)) = dag_full_paths.get(dep_name) {
                                let ref_path = if uses_parts_directory(mat) {
                                    incremental_ref_path(path)
                                } else {
                                    path.clone()
                                };
                                register_model_for_ref(
                                    &engine.ctx,
                                    dep_name,
                                    fmt,
                                    &ref_path,
                                    RefBackend::LakeFile,
                                )
                                .await
                                .with_context(|| {
                                    format!(
                                        "E_RBT_CONCURRENT: L2 register ref '{dep_name}' for unit {}",
                                        unit.id
                                    )
                                })?;
                            }
                        }
                    }

                    // Mini DAG for partition-input lookup (same model node).
                    let mini = clone_single_model_dag_bronze_only(&model);
                    let unit_scope = scope_for_unit(&scope, &unit);
                    let dest_path = model
                        .output_path
                        .as_ref()
                        .map(PathBuf::from)
                        .unwrap_or_else(|| {
                            output_base.join(format!("{}.parquet", model.name))
                        });
                    if let Some(parent) = dest_path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }

                    let (assertions, fail_on_error) = model_assertions(&model, &config)?;
                    let materialize = &config.materialize;
                    let mut write_opts =
                        MaterializeWriteOptions::from_config(materialize, fail_on_error);
                    if model
                        .frontmatter
                        .as_ref()
                        .map(|f| f.wants_lineage_stamp())
                        .unwrap_or(false)
                    {
                        write_opts = write_opts.with_lineage(LineageStamp {
                            run_id: run_id.clone(),
                            contract_version: contract.clone(),
                            model: model.name.clone(),
                            bronze_fingerprint: Some(fp.clone()),
                        });
                    }
                    if let Some(fm) = model.frontmatter.as_ref() {
                        if let Some(sk) =
                            crate::engine::surrogate_key::SurrogateKeyConfig::from_frontmatter(fm)?
                        {
                            write_opts = write_opts
                                .with_surrogate_key(sk.with_registry_for_model(&project_dir, &model.name));
                        }
                    }

                    let rows = if model.kind == ModelKind::Rust {
                        let (rows, _stats, _upsert) = engine
                            .materialize_rust_model(
                                &model,
                                &project_dir,
                                &unit_scope,
                                &dest_path,
                                &dest_path,
                                &write_opts,
                                &assertions,
                                fail_on_error,
                                &run_id,
                                &contract,
                                &fp,
                                false,
                                materialize,
                                &mini,
                                &unit.partition_bindings,
                            )
                            .await
                            .with_context(|| {
                                format!(
                                    "E_RBT_CONCURRENT: L2 unit '{}' materialize_rust failed",
                                    unit.id
                                )
                            })?;
                        rows
                    } else {
                        // SQL partition units: serial mini-DAG with unit scope (IN filter).
                        let mut cfg = config.clone();
                        cfg.execution.concurrency.enabled = false;
                        cfg.execution.concurrency.max_workers = 1;
                        cfg.execution.concurrency.strategy = ExecutionStrategy::Serial;
                        let mut us = unit_scope;
                        us.write_receipt = false;
                        us.skip_if_fingerprint_match = false;
                        let summary = engine
                            .execute_dag_with_scope(
                                &mini,
                                &project_dir,
                                &output_base,
                                &cfg,
                                &us,
                            )
                            .await
                            .with_context(|| {
                                format!(
                                    "E_RBT_CONCURRENT: L2 unit '{}' SQL path failed",
                                    unit.id
                                )
                            })?;
                        summary.total_rows_produced
                    };
                    let _ = spill;
                    Ok::<_, anyhow::Error>(rows)
                }
            })
            .buffer_unordered(max_w)
            .collect::<Vec<_>>()
            .await;

        let mut total = 0usize;
        for r in results {
            total += r?;
        }
        let elapsed = now_unix_ms().saturating_sub(started) as u64;
        let mrr = ModelRunResult::success(
            model.name.clone(),
            total,
            model.output_path.clone(),
            None,
            vec![
                "l2_concurrent".into(),
                format!("units={}", units.len()),
            ],
            Some(elapsed),
        )
        .with_kind(model.kind.as_str())
        .with_materialization(model.materialization.as_str());
        Ok((total, mrr))
    }
}

/// Single-node DAG; model refs stripped so build_graph succeeds (register lake deps separately).
fn clone_single_model_dag_bronze_only(model: &ModelNode) -> ModelDag {
    use crate::core::parser::DependencyRef;
    let mut d = ModelDag::new();
    let mut m = model.clone();
    m.dependencies
        .retain(|dep| matches!(dep, DependencyRef::Source { .. }));
    let idx = d.graph.add_node(m.clone());
    d.node_map.insert(m.name.clone(), idx);
    let _ = d.build_graph();
    d
}

fn dag_model_output_index(
    dag: &ModelDag,
) -> BTreeMap<String, (PathBuf, OutputFormat, Materialization)> {
    let mut m = BTreeMap::new();
    for (name, &idx) in &dag.node_map {
        let node = &dag.graph[idx];
        if let Some(ref op) = node.output_path {
            m.insert(
                name.clone(),
                (
                    PathBuf::from(op),
                    node.output_format.clone(),
                    node.materialization.clone(),
                ),
            );
        }
    }
    m
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

    #[tokio::test]
    async fn test_zero_row_emits_declared_schema() -> Result<()> {
        use crate::core::frontmatter::{ColumnMeta, StagingFrontmatter};
        use crate::core::project::MaterializeConfig;
        use arrow::datatypes::DataType;
        use std::collections::BTreeMap;

        let temp = tempfile::tempdir()?;
        let dest = temp.path().join("stg_empty.parquet");
        let mut dag = ModelDag::new();
        // Zero-row SQL — only exposes `id` from SELECT; declared adds name + entity
        let idx = dag.add_model_with_format(
            "stg_empty",
            "SELECT CAST(1 AS BIGINT) AS id WHERE false",
            Materialization::Table,
            OutputFormat::Parquet,
            Some(dest.to_string_lossy().into()),
            "",
        )?;
        let mut cols = BTreeMap::new();
        cols.insert(
            "id".into(),
            ColumnMeta {
                dtype: Some("int64".into()),
                ..Default::default()
            },
        );
        cols.insert(
            "name".into(),
            ColumnMeta {
                dtype: Some("utf8".into()),
                ..Default::default()
            },
        );
        cols.insert(
            "docs_only".into(),
            ColumnMeta {
                description: Some("no dtype — soft skip".into()),
                ..Default::default()
            },
        );
        dag.graph[idx].frontmatter = Some(StagingFrontmatter {
            columns: Some(cols),
            partition_by: Some(vec!["entity".into()]),
            ..Default::default()
        });
        dag.build_graph()?;

        let engine = TransformationEngine::new();
        let summary = engine
            .execute_dag_with_materialize(
                &dag,
                temp.path(),
                temp.path(),
                &MaterializeConfig::default(),
            )
            .await?;
        assert_eq!(summary.models_executed, 1);
        assert_eq!(summary.total_rows_produced, 0);
        assert!(dest.exists());

        let file = std::fs::File::open(&dest)?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema();
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("name").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("entity").unwrap().data_type(),
            &DataType::Utf8
        );
        // docs_only without dtype must not appear
        assert!(schema.index_of("docs_only").is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_table_consolidate_never_parts_only_and_ref() -> Result<()> {
        use crate::core::project::{ConsolidatePolicy, MaterializeConfig};
        use crate::materializer::parts_dir_for_parquet;

        let temp = tempfile::tempdir()?;
        let stg = temp.path().join("stg_a.parquet");
        let tf = temp.path().join("tf_b.parquet");
        let mut dag = ModelDag::new();
        dag.add_model_with_format(
            "stg_a",
            "SELECT 1 AS id UNION ALL SELECT 2",
            Materialization::Table,
            OutputFormat::Parquet,
            Some(stg.to_string_lossy().into()),
            "",
        )?;
        dag.add_model_with_format(
            "tf_b",
            "SELECT count(*) AS c FROM {{ ref('stg_a') }}",
            Materialization::Table,
            OutputFormat::Parquet,
            Some(tf.to_string_lossy().into()),
            "",
        )?;
        dag.build_graph()?;

        let mat = MaterializeConfig {
            consolidate: ConsolidatePolicy::Never,
            ..Default::default()
        };
        let engine = TransformationEngine::new();
        let summary = engine
            .execute_dag_with_materialize(&dag, temp.path(), temp.path(), &mat)
            .await?;
        assert_eq!(summary.models_executed, 2);
        assert_eq!(summary.total_rows_produced, 3); // 2 upstream + 1 count row
        assert!(!stg.exists(), "never must not write monolith for table");
        let parts = parts_dir_for_parquet(&stg);
        assert!(parts.join("part-full.parquet").exists());
        // Project-level never applies to all table models; ref() still sees parts
        assert!(!tf.exists());
        assert!(parts_dir_for_parquet(&tf).join("part-full.parquet").exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_incremental_auto_no_monolith_always_writes_monolith() -> Result<()> {
        use crate::core::project::{ConsolidatePolicy, MaterializeConfig};
        use crate::materializer::parts_dir_for_parquet;

        let temp = tempfile::tempdir()?;
        let dest = temp.path().join("stg_inc.parquet");
        let mut dag = ModelDag::new();
        dag.add_model_with_format(
            "stg_inc",
            "SELECT 1 AS id UNION ALL SELECT 2",
            Materialization::IncrementalAppend,
            OutputFormat::Parquet,
            Some(dest.to_string_lossy().into()),
            "",
        )?;
        dag.build_graph()?;

        // auto: parts only, no monolith
        let mat_auto = MaterializeConfig {
            consolidate: ConsolidatePolicy::Auto,
            ..Default::default()
        };
        let engine = TransformationEngine::new();
        engine
            .execute_dag_with_materialize(&dag, temp.path(), temp.path(), &mat_auto)
            .await?;
        assert!(!dest.exists());
        assert!(parts_dir_for_parquet(&dest).join("part-0000000000001.parquet").exists()
            || parts_dir_for_parquet(&dest)
                .read_dir()?
                .any(|e| e
                    .ok()
                    .map(|e| e.path().extension().and_then(|x| x.to_str()) == Some("parquet"))
                    .unwrap_or(false)));

        // always: parts + monolith rebuild
        let mat_always = MaterializeConfig {
            consolidate: ConsolidatePolicy::Always,
            ..Default::default()
        };
        // clear and re-run with always
        let _ = std::fs::remove_dir_all(parts_dir_for_parquet(&dest));
        let _ = std::fs::remove_file(&dest);
        engine
            .execute_dag_with_materialize(&dag, temp.path(), temp.path(), &mat_always)
            .await?;
        assert!(dest.exists(), "always must rebuild monolith from parts");
        assert!(parts_dir_for_parquet(&dest).is_dir());
        Ok(())
    }

    /// Design B: SQL stg → Rust tf → SQL mart via ref().
    #[tokio::test]
    async fn design_b_sql_rust_sql_ref_chain() -> Result<()> {
        use crate::core::dag::{Materialization, ModelKind, ModelLayer};
        use crate::engine::rust_model::{RustModel, RustModelContext, RustModelOutput};
        use crate::{DagBuilder, ModelSpec, RbtEngineBuilder, RbtProjectConfig, RunScope};
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        struct DoubleIds;
        #[async_trait::async_trait]
        impl RustModel for DoubleIds {
            fn name(&self) -> &str {
                "tf_double"
            }
            fn output_schema(&self) -> arrow::datatypes::SchemaRef {
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
            }
            async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput> {
                let df = ctx.session.sql(r#"SELECT id FROM "stg_seed""#).await?;
                let batches = df.collect().await?;
                let mut ids = Vec::new();
                for b in batches {
                    let col = b
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| anyhow::anyhow!("expected Int64 id"))?;
                    for i in 0..col.len() {
                        ids.push(col.value(i) * 2);
                    }
                }
                let batch = RecordBatch::try_new(
                    self.output_schema(),
                    vec![Arc::new(Int64Array::from(ids))],
                )?;
                Ok(RustModelOutput::Batches(vec![batch]))
            }
        }

        let temp = tempfile::tempdir()?;
        let stg = temp.path().join("stg_seed.parquet");
        let tf = temp.path().join("tf_double.parquet");
        let mart = temp.path().join("obt_out.parquet");

        // L1.10: empty catalog_prefix default → bare table names after materialize.
        let dag = DagBuilder::new()
            .model(
                ModelSpec::sql("stg_seed", "SELECT 1 AS id UNION ALL SELECT 2 AS id")
                    .layer(ModelLayer::Staging)
                    .materialization(Materialization::Table)
                    .output_path(stg.to_string_lossy()),
            )
            .model(
                ModelSpec::rust("tf_double")
                    .refs(["stg_seed"])
                    .layer(ModelLayer::Transform)
                    .materialization(Materialization::Table)
                    .output_path(tf.to_string_lossy()),
            )
            .model(
                ModelSpec::sql(
                    "obt_out",
                    r#"SELECT id FROM {{ ref('tf_double') }} WHERE id > 0"#,
                )
                .layer(ModelLayer::Mart)
                .materialization(Materialization::Table)
                .output_path(mart.to_string_lossy()),
            )
            .build()?;
        assert_eq!(dag.graph[dag.node_map["tf_double"]].kind, ModelKind::Rust);

        let engine = RbtEngineBuilder::new()
            .with_rust_model(DoubleIds)
            .build()
            .await?;
        let cfg = RbtProjectConfig::default();
        let scope = RunScope::default();
        let summary = engine
            .execute_dag_with_scope(&dag, temp.path(), temp.path(), &cfg, &scope)
            .await?;
        assert_eq!(summary.models_executed, 3);
        assert!(tf.exists());
        assert!(mart.exists());
        let out = load_parquet_batches(&mart)?;
        let n: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 2);
        let col = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut vals: Vec<i64> = (0..col.len()).map(|i| col.value(i)).collect();
        vals.sort();
        assert_eq!(vals, vec![2, 4]);
        // B4: receipt kind on model results
        let rust_mrr = summary
            .model_results
            .iter()
            .find(|m| m.name == "tf_double")
            .expect("tf_double result");
        assert_eq!(rust_mrr.kind.as_deref(), Some("rust"));
        assert_eq!(rust_mrr.materialization.as_deref(), Some("table"));
        let sql_mrr = summary
            .model_results
            .iter()
            .find(|m| m.name == "stg_seed")
            .unwrap();
        assert_eq!(sql_mrr.kind.as_deref(), Some("sql"));
        Ok(())
    }

    /// B3 + B5: Rust scoped_replace via Stream output.
    #[tokio::test]
    async fn design_b_scoped_replace_stream() -> Result<()> {
        use crate::core::dag::{Materialization, ModelLayer};
        use crate::core::frontmatter::StagingFrontmatter;
        use crate::engine::rust_model::{
            batches_to_stream, RustModel, RustModelContext, RustModelOutput,
        };
        use crate::{DagBuilder, ModelSpec, RbtEngineBuilder, RbtProjectConfig, RunScope};
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        struct ScopeEmit;
        #[async_trait::async_trait]
        impl RustModel for ScopeEmit {
            fn name(&self) -> &str {
                "stg_parts"
            }
            fn output_schema(&self) -> arrow::datatypes::SchemaRef {
                Arc::new(Schema::new(vec![
                    Field::new("entity", DataType::Utf8, false),
                    Field::new("n", DataType::Int64, false),
                ]))
            }
            async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput> {
                let entity = ctx
                    .scope
                    .vars
                    .get("entity")
                    .map(|v| v.as_single().unwrap_or("?").to_string())
                    .unwrap_or_else(|| "?".into());
                let schema = self.output_schema();
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from(vec![entity.as_str()])),
                        Arc::new(Int64Array::from(vec![1i64])),
                    ],
                )?;
                // B5: stream path
                Ok(RustModelOutput::Stream(batches_to_stream(
                    schema,
                    vec![batch],
                )))
            }
        }

        let temp = tempfile::tempdir()?;
        let dest = temp.path().join("stg_parts.parquet");
        let mut fm = StagingFrontmatter::default();
        fm.partition_by = Some(vec!["entity".into()]);
        fm.part_key = Some(vec!["entity".into()]);

        let dag = DagBuilder::new()
            .model(
                ModelSpec::rust("stg_parts")
                    .layer(ModelLayer::Staging)
                    .materialization(Materialization::ScopedReplace)
                    .frontmatter(fm)
                    .output_path(dest.to_string_lossy()),
            )
            .build()?;

        let engine = RbtEngineBuilder::new()
            .with_rust_model(ScopeEmit)
            .build()
            .await?;
        let cfg = RbtProjectConfig::default();
        let scope = RunScope::new().with_var("entity", "a.com");
        let summary = engine
            .execute_dag_with_scope(&dag, temp.path(), temp.path(), &cfg, &scope)
            .await?;
        assert_eq!(summary.models_executed, 1);
        assert!(crate::materializer::parts_dir_for_parquet(&dest).is_dir());
        let mrr = &summary.model_results[0];
        assert_eq!(mrr.kind.as_deref(), Some("rust"));
        assert_eq!(mrr.materialization.as_deref(), Some("scoped_replace"));
        Ok(())
    }

    /// RBT-C Phase 2: Design B execute_partition receives partition-local input.
    #[tokio::test]
    async fn design_b_execute_partition_is_called() -> Result<()> {
        use crate::core::dag::{Materialization, ModelKind, ModelLayer};
        use crate::core::frontmatter::StagingFrontmatter;
        use crate::core::project::{ConcurrencyConfig, ExecutionStrategy};
        use crate::core::run_scope::RunScope;
        use crate::core::work_unit::ParallelContract;
        use crate::engine::rust_model::{
            PartitionInput, PartitionKey, RustModel, RustModelContext, RustModelOutput,
        };
        use crate::{DagBuilder, ModelSpec, RbtEngineBuilder, RbtProjectConfig};
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        static PARTITION_CALLS: AtomicUsize = AtomicUsize::new(0);

        struct PerEntity;
        #[async_trait::async_trait]
        impl RustModel for PerEntity {
            fn name(&self) -> &str {
                "tf_per_entity"
            }
            fn output_schema(&self) -> arrow::datatypes::SchemaRef {
                Arc::new(Schema::new(vec![
                    Field::new("entity", DataType::Utf8, false),
                    Field::new("n", DataType::Int64, false),
                ]))
            }
            fn parallel_contract(&self) -> ParallelContract {
                ParallelContract::PartitionLocal {
                    keys: vec!["entity".into()],
                }
            }
            async fn execute(&self, _ctx: &RustModelContext<'_>) -> Result<RustModelOutput> {
                // Mega fallback should not be used when execute_partition works.
                bail!("execute should not be called for partition slices in this test")
            }
            async fn execute_partition(
                &self,
                _ctx: &RustModelContext<'_>,
                part: &PartitionKey,
                _input: PartitionInput,
            ) -> Result<RustModelOutput> {
                PARTITION_CALLS.fetch_add(1, Ordering::SeqCst);
                let ent = part.get("entity").unwrap_or("?").to_string();
                let schema = self.output_schema();
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from(vec![ent.as_str()])),
                        Arc::new(Int64Array::from(vec![1i64])),
                    ],
                )?;
                Ok(RustModelOutput::Batches(vec![batch]))
            }
        }

        let temp = tempfile::tempdir()?;
        let dest = temp.path().join("tf_per_entity.parquet");
        let mut fm = StagingFrontmatter::default();
        fm.partition_by = Some(vec!["entity".into()]);
        fm.part_key = Some(vec!["entity".into()]);
        fm.materialization = Some("scoped_replace".into());

        let dag = DagBuilder::new()
            .model(
                ModelSpec::rust("tf_per_entity")
                    .layer(ModelLayer::Transform)
                    .materialization(Materialization::ScopedReplace)
                    .frontmatter(fm)
                    .output_path(dest.to_string_lossy()),
            )
            .build()?;
        assert_eq!(dag.graph[dag.node_map["tf_per_entity"]].kind, ModelKind::Rust);

        let engine = RbtEngineBuilder::new()
            .with_rust_model(PerEntity)
            .build()
            .await?;
        let mut cfg = RbtProjectConfig::default();
        cfg.execution.concurrency = ConcurrencyConfig {
            enabled: true,
            max_workers: 1,
            strategy: ExecutionStrategy::Partition,
            multi_value_fanout_threshold: 2,
            dirty_part_skip: false,
            partition_keys: None,
            large_parts_first: true,
            max_inflight_bytes: None,
        };
        let scope = RunScope::new().with_var_multi("entity", ["a.com", "b.com", "c.com"])?;
        PARTITION_CALLS.store(0, Ordering::SeqCst);
        let summary = engine
            .execute_dag_with_scope(&dag, temp.path(), temp.path(), &cfg, &scope)
            .await?;
        assert!(
            PARTITION_CALLS.load(Ordering::SeqCst) >= 3,
            "execute_partition should run per entity"
        );
        assert!(summary.models_executed >= 3);
        assert!(crate::materializer::parts_dir_for_parquet(&dest).is_dir());
        Ok(())
    }

    /// RBT-C Phase 1: multi-value fan-out → N scoped_replace parts (serial units).
    #[tokio::test]
    async fn partition_fanout_scoped_replace_serial() -> Result<()> {
        use crate::core::frontmatter::StagingFrontmatter;
        use crate::core::project::{ConcurrencyConfig, ExecutionStrategy};
        use crate::core::run_scope::RunScope;
        use std::io::Write;

        let temp = tempfile::tempdir()?;
        let bronze = temp.path().join("bronze");
        for ent in ["a", "b", "c", "d"] {
            let dir = bronze.join(format!("entity={ent}"));
            std::fs::create_dir_all(&dir)?;
            let mut f = std::fs::File::create(dir.join("data.jsonl"))?;
            writeln!(f, r#"{{"entity":"{ent}","n":1}}"#)?;
        }
        let dest = temp.path().join("stg_e.parquet");
        let mut fm = StagingFrontmatter::default();
        fm.source_format = Some(crate::core::frontmatter::SourceFormat::Jsonl);
        fm.scan_path = Some(bronze.to_string_lossy().into());
        fm.partition_by = Some(vec!["entity".into()]);
        fm.part_key = Some(vec!["entity".into()]);
        fm.materialization = Some("scoped_replace".into());

        let mut dag = ModelDag::new();
        let sql = r#"SELECT * FROM {{ source('bronze', 'events') }}"#;
        // inject frontmatter via raw
        let raw = format!(
            "---\nsource_format: jsonl\nscan_path: {}\npartition_by: [entity]\npart_key: [entity]\nmaterialization: scoped_replace\n---\n{sql}",
            bronze.display()
        );
        dag.add_model_with_format(
            "stg_e",
            &raw,
            Materialization::ScopedReplace,
            OutputFormat::Parquet,
            Some(dest.to_string_lossy().into()),
            "",
        )?;
        dag.build_graph()?;

        let mut cfg = RbtProjectConfig::default();
        cfg.execution.concurrency = ConcurrencyConfig {
            enabled: true,
            max_workers: 1,
            strategy: ExecutionStrategy::Partition,
            multi_value_fanout_threshold: 4,
            dirty_part_skip: true,
            partition_keys: None,
            large_parts_first: true,
            max_inflight_bytes: None,
        };
        let scope = RunScope::new().with_var_multi("entity", ["a", "b", "c", "d"])?;
        let engine = TransformationEngine::new();
        let summary = engine
            .execute_dag_with_scope(&dag, temp.path(), temp.path(), &cfg, &scope)
            .await?;
        assert!(summary.models_executed >= 4, "expected 4 partition units");
        let parts = crate::materializer::parts_dir_for_parquet(&dest);
        assert!(parts.is_dir());
        let files: Vec<_> = std::fs::read_dir(&parts)?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("part-") && n.ends_with(".parquet"))
            .collect();
        assert_eq!(files.len(), 4, "expected 4 part files, got {files:?}");
        let man = crate::materializer::load_manifest(&parts)?;
        assert_eq!(man.parts.len(), 4);
        assert!(man.schema_version >= 1);
        let _ = fm;
        Ok(())
    }

    /// RBT-C Phase 0: materialization alias zero-copy; downstream ref() reads same bytes.
    #[tokio::test]
    async fn alias_zero_copy_identity_mart() -> Result<()> {
        use crate::materializer::read_alias_sidecar;

        let temp = tempfile::tempdir()?;
        let up = temp.path().join("tf_up.parquet");
        let mart = temp.path().join("obt_alias.parquet");

        let mut dag = ModelDag::new();
        dag.add_model_with_format(
            "tf_up",
            "SELECT 1 AS id, 'x' AS k",
            Materialization::Table,
            OutputFormat::Parquet,
            Some(up.to_string_lossy().into()),
            "",
        )?;
        dag.add_model_with_format(
            "obt_alias",
            r#"---
materialization: alias
alias_of: tf_up
---
SELECT * FROM {{ ref('tf_up') }}
"#,
            Materialization::Table, // overridden by frontmatter
            OutputFormat::Parquet,
            Some(mart.to_string_lossy().into()),
            "",
        )?;
        dag.build_graph()?;

        let engine = TransformationEngine::new();
        let summary = engine
            .execute_dag(&dag, temp.path(), temp.path())
            .await?;
        assert_eq!(summary.models_executed, 2);
        assert!(up.is_file());
        // Alias published: hardlink/symlink or pointer sidecar
        let side = read_alias_sidecar(&mart);
        assert!(
            side.is_some(),
            "expected _rbt_alias sidecar for obt_alias"
        );
        assert!(
            mart.exists() || side.as_ref().map(|s| Path::new(&s.source_path).exists()).unwrap_or(false),
            "alias dest or source must exist"
        );
        // Same content when link succeeded
        if mart.is_file() {
            let a = std::fs::read(&up)?;
            let b = std::fs::read(&mart)?;
            assert_eq!(a, b, "hardlink/symlink should share file bytes");
            // Prefer hardlink: same inode when possible
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let ia = std::fs::metadata(&up)?.ino();
                let ib = std::fs::metadata(&mart)?.ino();
                // hardlink OR identical content via symlink target
                assert!(
                    ia == ib || mart.symlink_metadata()?.file_type().is_symlink(),
                    "expected hardlink (same inode) or symlink"
                );
            }
        }
        Ok(())
    }
}

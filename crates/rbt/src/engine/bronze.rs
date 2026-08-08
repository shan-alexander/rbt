//! Frontmatter-driven bronze source registration.
//!
//! ## Architecture
//!
//! * **Path A (DataFusion listing / external tables)** — Parquet, CSV, JSON/JSONL (no
//!   jshift projection), Arrow IPC file: register via DataFusion native readers, then
//!   wrap the resulting provider in [`BronzeTableProvider`]. Listing predicate
//!   pushdown is available on this path.
//! * **Path B (scan → MemTable)** — used when rbt must apply its own filters or
//!   inject path-derived columns. **Any non-empty `path_glob` forces Path B** (as do
//!   `partition_by` / `require_partitions` / `inject_source_path` / `force_scan` and
//!   formats that require scan: log, txt, toml, Arrow IPC stream, protobuf).
//!   DataFusion directory listing pushdown is **disabled** for that source by design.
//!
//! [`BronzeTableProvider`] is intentionally thin: it delegates scan/schema to the
//! inner provider and carries bronze metadata for lineage / debugging.

use crate::core::dag::{ModelDag, ModelNode};
use crate::core::frontmatter::{SourceFormat, StagingFrontmatter};
use crate::core::run_scope::{OnMissing, RunScope};
use crate::scan::{LakeScanner, ScanRequest};
use anyhow::{bail, Context, Result};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::ArrowReadOptions;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{CsvReadOptions, JsonReadOptions, ParquetReadOptions};
use std::any::Any;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Metadata retained on the bronze provider for debugging and future lineage.
#[derive(Debug, Clone)]
pub struct BronzeSourceMeta {
    pub model_name: String,
    pub source_schema: String,
    pub source_table: String,
    pub format: SourceFormat,
    pub scan_path: PathBuf,
    pub registration_mode: BronzeRegistrationMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BronzeRegistrationMode {
    /// Inner provider is a DataFusion listing / external table.
    DataFusionListing,
    /// Inner provider is a MemTable filled by `rbt::scan` (small / non-spill formats).
    ScanMemTable,
    /// Arrow IPC (etc.) spilled file-by-file to Parquet then listed — bounded peak RAM.
    ScanSpillParquet,
}

/// Thin `TableProvider` wrapper around a DataFusion listing table or MemTable.
#[derive(Debug)]
pub struct BronzeTableProvider {
    pub meta: BronzeSourceMeta,
    inner: Arc<dyn TableProvider>,
}

impl BronzeTableProvider {
    pub fn wrap(inner: Arc<dyn TableProvider>, meta: BronzeSourceMeta) -> Self {
        Self { meta, inner }
    }

    pub fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

#[async_trait]
impl TableProvider for BronzeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
}

/// Registers all bronze sources declared by model frontmatter into `ctx`.
///
/// Idempotent per `(schema, table)` within a single run (tracked by `registered`).
/// Uses `config.roots` and `config.scan` (no re-read of yml per model).
/// Optional [`RunScope`] expands `{var}` templates and partition binds (P5a).
pub async fn register_bronze_sources_for_dag(
    ctx: &SessionContext,
    dag: &ModelDag,
    project_dir: &Path,
    registered: &mut HashSet<(String, String)>,
    config: &crate::core::project::RbtProjectConfig,
) -> Result<usize> {
    register_bronze_sources_for_dag_scoped(ctx, dag, project_dir, registered, config, None).await
}

/// Like [`register_bronze_sources_for_dag`] with an explicit run scope.
pub async fn register_bronze_sources_for_dag_scoped(
    ctx: &SessionContext,
    dag: &ModelDag,
    project_dir: &Path,
    registered: &mut HashSet<(String, String)>,
    config: &crate::core::project::RbtProjectConfig,
    scope: Option<&RunScope>,
) -> Result<usize> {
    let mut count = 0;
    for idx in dag.graph.node_indices() {
        let node = &dag.graph[idx];
        if let Some(n) =
            register_bronze_for_model_scoped(ctx, node, project_dir, registered, config, scope)
                .await?
        {
            count += n;
        }
    }
    Ok(count)
}

/// Register bronze for a single model if it has a scan contract.
pub async fn register_bronze_for_model(
    ctx: &SessionContext,
    node: &ModelNode,
    project_dir: &Path,
    registered: &mut HashSet<(String, String)>,
    config: &crate::core::project::RbtProjectConfig,
) -> Result<Option<usize>> {
    register_bronze_for_model_scoped(ctx, node, project_dir, registered, config, None).await
}

/// Register bronze with optional run scope (partition binds + var templates + on_missing).
pub async fn register_bronze_for_model_scoped(
    ctx: &SessionContext,
    node: &ModelNode,
    project_dir: &Path,
    registered: &mut HashSet<(String, String)>,
    config: &crate::core::project::RbtProjectConfig,
    scope: Option<&RunScope>,
) -> Result<Option<usize>> {
    let Some(fm_raw) = node.frontmatter.as_ref() else {
        return Ok(None);
    };
    if !fm_raw.has_scan_contract() {
        return Ok(None);
    }

    let default_scope = RunScope::default();
    let scope = scope.unwrap_or(&default_scope);
    let fm_owned = crate::core::receipt::try_apply_scope_to_frontmatter(fm_raw, scope)
        .with_context(|| {
            format!(
                "E_RBT_VAR: apply run scope to bronze model '{}'",
                node.name
            )
        })?;
    let fm = &fm_owned;

    let (schema_name, table_name) = ModelDag::bronze_source_ident(node).with_context(|| {
        format!(
            "model '{}': frontmatter has scan_path but no source identity \
             (add source() in SQL or source_name/source_table in frontmatter)",
            node.name
        )
    })?;

    let key = (schema_name.clone(), table_name.clone());
    if registered.contains(&key) {
        tracing::debug!(
            "Bronze source {}.{} already registered; skipping model '{}'",
            schema_name,
            table_name,
            node.name
        );
        return Ok(None);
    }

    ensure_schema(ctx, &schema_name).await?;

    let format = fm
        .resolve_format()
        .with_context(|| format!("model '{}': cannot resolve source_format", node.name))?;

    let raw_scan = fm.scan_path.as_deref().unwrap();
    let resolved = crate::core::paths::resolve_project_path(project_dir, raw_scan, &config.roots)
        .with_context(|| {
        format!(
            "E_RBT_BRONZE_PATH: model '{}': cannot resolve scan_path '{}'. \
                     Check absolute paths and `roots:` templates in rbt_project.yml.",
            node.name, raw_scan
        )
    })?;
    let on_missing = fm.on_missing_policy();
    if !resolved.exists() && !crate::core::frontmatter::is_remote_uri(raw_scan) {
        if on_missing == OnMissing::Empty {
            let provider = empty_memtable(fm, scope).with_context(|| {
                format!(
                    "model '{}': on_missing empty frame failed (scan_path missing)",
                    node.name
                )
            })?;
            return finish_register(
                ctx,
                registered,
                key,
                schema_name,
                table_name,
                node,
                format,
                resolved,
                provider,
                BronzeRegistrationMode::ScanMemTable,
            )
            .await;
        }
        bail!(
            "E_RBT_BRONZE_SCAN_PATH_NOT_FOUND: model '{}': bronze scan_path does not exist: {} \
             (resolved {}). Hint: verify the lake path and `$root` expansion, \
             or set on_missing: empty for optional artifact families.",
            node.name,
            raw_scan,
            resolved.display()
        );
    }

    let path_str = resolved.to_string_lossy().to_string();

    // P6: multi-part parquet directories (manifest-aware).
    let parts_mode = fm.wants_parts_source()
        || crate::scan::parts::is_parts_directory(&resolved)
        || matches!(format, SourceFormat::Parquet)
            && resolved.is_dir()
            && crate::scan::parts::manifest_path(&resolved).is_file();

    if parts_mode {
        let provider = register_parts_parquet(ctx, &resolved)
            .await
            .with_context(|| {
                format!(
                    "model '{}': parts parquet registration failed for {}",
                    node.name,
                    resolved.display()
                )
            })?;
        return finish_register(
            ctx,
            registered,
            key,
            schema_name,
            table_name,
            node,
            SourceFormat::Parquet,
            resolved,
            provider,
            BronzeRegistrationMode::DataFusionListing,
        )
        .await;
    }

    let use_scan = should_use_scan_path(fm, format);

    let (inner, mode) = if use_scan {
        if should_spill_to_parquet(format, config) {
            match scan_spill_to_listing(
                ctx,
                project_dir,
                fm,
                format,
                config,
                &schema_name,
                &table_name,
            )
            .await
            {
                Ok(provider) => (provider, BronzeRegistrationMode::ScanSpillParquet),
                Err(e) if on_missing == OnMissing::Empty && is_empty_or_missing_err(&e) => {
                    (
                        empty_memtable(fm, scope)?,
                        BronzeRegistrationMode::ScanMemTable,
                    )
                }
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!(
                            "model '{}': bronze spill→parquet failed (format={})",
                            node.name, format
                        )
                    })
                }
            }
        } else {
            match scan_to_memtable(project_dir, fm, format, config).await {
                Ok(provider) => (provider, BronzeRegistrationMode::ScanMemTable),
                Err(e) if on_missing == OnMissing::Empty && is_empty_or_missing_err(&e) => {
                    (
                        empty_memtable(fm, scope)?,
                        BronzeRegistrationMode::ScanMemTable,
                    )
                }
                Err(e) => {
                    return Err(e).with_context(|| format!("model '{}': bronze scan failed", node.name))
                }
            }
        }
    } else {
        let provider = listing_table_provider(ctx, &path_str, format)
            .await
            .with_context(|| {
                format!(
                    "model '{}': DataFusion listing registration failed for {}",
                    node.name, path_str
                )
            })?;
        (provider, BronzeRegistrationMode::DataFusionListing)
    };

    finish_register(
        ctx,
        registered,
        key,
        schema_name,
        table_name,
        node,
        format,
        resolved,
        inner,
        mode,
    )
    .await
}

async fn finish_register(
    ctx: &SessionContext,
    registered: &mut HashSet<(String, String)>,
    key: (String, String),
    schema_name: String,
    table_name: String,
    node: &ModelNode,
    format: SourceFormat,
    resolved: PathBuf,
    inner: Arc<dyn TableProvider>,
    mode: BronzeRegistrationMode,
) -> Result<Option<usize>> {

    let meta = BronzeSourceMeta {
        model_name: node.name.clone(),
        source_schema: schema_name.clone(),
        source_table: table_name.clone(),
        format,
        scan_path: resolved,
        registration_mode: mode,
    };

    let bronze = Arc::new(BronzeTableProvider::wrap(inner, meta));
    let table_ref = TableReference::partial(schema_name.clone(), table_name.clone());

    // Replace if present (re-runs / tests)
    let _ = ctx.deregister_table(table_ref.clone());
    ctx.register_table(table_ref, bronze)
        .map_err(|e| anyhow::anyhow!("register {}.{}: {}", schema_name, table_name, e))?;

    registered.insert(key);
    tracing::info!(
        "Registered bronze source {}.{} from model '{}' ({:?}, format={})",
        schema_name,
        table_name,
        node.name,
        mode,
        format
    );
    Ok(Some(1))
}

fn is_empty_or_missing_err(e: &anyhow::Error) -> bool {
    let s = format!("{e:#}");
    s.contains("E_RBT_BRONZE_SCAN_EMPTY")
        || s.contains("E_RBT_BRONZE_SCAN_PATH_NOT_FOUND")
        || s.contains("bronze scan produced zero batches")
}

/// Zero-row MemTable with declared schema (RBT-A6 `empty_batch_for_frontmatter`).
fn empty_memtable(fm: &StagingFrontmatter, scope: &RunScope) -> Result<Arc<dyn TableProvider>> {
    let batch = crate::core::schema_emit::empty_batch_for_frontmatter(fm)?;
    let schema = batch.schema();
    let _ = scope; // reserved: future constant partition columns on empty frames
    let mem = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| anyhow::anyhow!("MemTable empty: {e}"))?;
    Ok(Arc::new(mem))
}

fn should_use_scan_path(fm: &StagingFrontmatter, format: SourceFormat) -> bool {
    if fm.force_scan.unwrap_or(false) {
        return true;
    }
    // Hive partition injection / filters / source path / path_glob require the scan path
    // (DataFusion listing does not inject path-derived columns or apply rbt globs).
    if fm
        .partition_by
        .as_ref()
        .map(|p| !p.is_empty())
        .unwrap_or(false)
        || fm
            .require_partitions
            .as_ref()
            .map(|p| !p.is_empty())
            .unwrap_or(false)
        || fm
            .require_partitions_in
            .as_ref()
            .map(|p| !p.is_empty())
            .unwrap_or(false)
        || fm
            .path_glob
            .as_ref()
            .map(|p| !p.is_empty())
            .unwrap_or(false)
        || fm.inject_source_path.unwrap_or(false)
    {
        return true;
    }
    // jshift selective extract
    if matches!(format, SourceFormat::Jsonl | SourceFormat::Json)
        && fm.paths.as_ref().map(|p| !p.is_empty()).unwrap_or(false)
    {
        return true;
    }
    // Nested hive dirs, stream IPC, and opaque protobuf need the scan path.
    matches!(
        format,
        SourceFormat::Log
            | SourceFormat::Txt
            | SourceFormat::Toml
            | SourceFormat::ArrowIpc
            | SourceFormat::ArrowIpcStream
            | SourceFormat::Protobuf
    )
}

async fn ensure_schema(ctx: &SessionContext, schema_name: &str) -> Result<()> {
    // DataFusion accepts CREATE SCHEMA via SQL
    let sql = format!(
        "CREATE SCHEMA IF NOT EXISTS \"{}\"",
        schema_name.replace('"', "")
    );
    ctx.sql(&sql)
        .await
        .with_context(|| format!("CREATE SCHEMA {}", schema_name))?
        .collect()
        .await
        .with_context(|| format!("CREATE SCHEMA {} collect", schema_name))?;
    Ok(())
}

/// Register a multi-file parquet parts directory as one table provider.
async fn register_parts_parquet(
    ctx: &SessionContext,
    parts_dir: &Path,
) -> Result<Arc<dyn TableProvider>> {
    let files = crate::scan::parts::list_part_files(parts_dir)?;
    tracing::info!(
        "Parts source: {} file(s) under {} (manifest_rows={:?})",
        files.len(),
        parts_dir.display(),
        crate::scan::parts::manifest_total_rows(parts_dir)
    );
    // DataFusion lists directories recursively; registering the directory is enough when
    // only part-*.parquet files live there. Prefer directory registration for pushdown.
    let path_str = parts_dir.to_string_lossy().to_string();
    listing_table_provider(ctx, &path_str, SourceFormat::Parquet)
        .await
        .with_context(|| {
            format!(
                "E_RBT_PARTS: register_parquet on parts dir {} ({} files)",
                parts_dir.display(),
                files.len()
            )
        })
}

/// Path A: materialize a DF listing provider, then return it for wrapping.
async fn listing_table_provider(
    ctx: &SessionContext,
    path: &str,
    format: SourceFormat,
) -> Result<Arc<dyn TableProvider>> {
    // Register under a private temp name, extract provider, deregister.
    let tmp = format!(
        "__rbt_bronze_tmp_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );

    match format {
        SourceFormat::Parquet => {
            ctx.register_parquet(&tmp, path, ParquetReadOptions::default())
                .await?;
        }
        SourceFormat::Csv => {
            ctx.register_csv(&tmp, path, CsvReadOptions::default())
                .await?;
        }
        SourceFormat::Jsonl => {
            let opts = JsonReadOptions::default()
                .file_extension(".jsonl")
                .newline_delimited(true);
            // DF register_type_check requires path to end with extension; if directory, ok
            if let Err(e) = ctx.register_json(&tmp, path, opts).await {
                // Fallback: .json extension / generic
                tracing::debug!("jsonl register with .jsonl failed ({e}); retrying default");
                ctx.register_json(&tmp, path, JsonReadOptions::default())
                    .await?;
            }
        }
        SourceFormat::Json => {
            let opts = JsonReadOptions::default().newline_delimited(false);
            ctx.register_json(&tmp, path, opts).await?;
        }
        SourceFormat::ArrowIpc => {
            ctx.register_arrow(&tmp, path, ArrowReadOptions::default())
                .await?;
        }
        other => bail!("listing_table_provider does not support format {}", other),
    }

    let provider = ctx
        .table_provider(TableReference::bare(tmp.as_str()))
        .await
        .with_context(|| format!("lookup temp bronze table {}", tmp))?;
    let _ = ctx.deregister_table(TableReference::bare(tmp.as_str()))?;
    Ok(provider)
}

fn should_spill_to_parquet(
    format: SourceFormat,
    config: &crate::core::project::RbtProjectConfig,
) -> bool {
    config.scan.spill_arrow_ipc
        && matches!(
            format,
            SourceFormat::ArrowIpc | SourceFormat::ArrowIpcStream
        )
}

async fn scan_to_memtable(
    project_dir: &Path,
    fm: &StagingFrontmatter,
    format: SourceFormat,
    config: &crate::core::project::RbtProjectConfig,
) -> Result<Arc<dyn TableProvider>> {
    let mut req = ScanRequest::from_frontmatter_with_config(
        project_dir,
        fm,
        config.roots.clone(),
        &config.scan,
    )?;
    req.format = format;
    let scanner = LakeScanner::from_request(&req);
    let batches = scanner.scan(&req).await?;
    if batches.is_empty() {
        if req.allow_empty {
            let schema = fm.empty_frame_schema().unwrap_or_else(|_| {
                Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                    "_empty",
                    arrow::datatypes::DataType::Utf8,
                    true,
                )]))
            });
            let batch = RecordBatch::new_empty(schema.clone());
            let mem = MemTable::try_new(schema, vec![vec![batch]])
                .map_err(|e| anyhow::anyhow!("MemTable::try_new empty: {e}"))?;
            return Ok(Arc::new(mem));
        }
        bail!(
            "E_RBT_BRONZE_SCAN_EMPTY: bronze scan produced zero batches for {}",
            req.resolved_path()?.display()
        );
    }
    let schema = batches[0].schema();
    // MemTable expects Vec<Vec<RecordBatch>> partitions
    let mem = MemTable::try_new(schema, vec![batches])
        .map_err(|e| anyhow::anyhow!("MemTable::try_new: {}", e))?;
    Ok(Arc::new(mem))
}

/// Stream Arrow IPC (etc.) file-by-file into a project spill Parquet, then DF-list it.
async fn scan_spill_to_listing(
    ctx: &SessionContext,
    project_dir: &Path,
    fm: &StagingFrontmatter,
    format: SourceFormat,
    config: &crate::core::project::RbtProjectConfig,
    schema_name: &str,
    table_name: &str,
) -> Result<Arc<dyn TableProvider>> {
    let mut req = ScanRequest::from_frontmatter_with_config(
        project_dir,
        fm,
        config.roots.clone(),
        &config.scan,
    )?;
    req.format = format;
    let scanner = LakeScanner::from_request(&req);

    let spill_root = crate::core::paths::resolve_project_path(
        project_dir,
        &config.scan.spill_dir,
        &config.roots,
    )
    .with_context(|| {
        format!(
            "E_RBT_BRONZE_SPILL: resolve spill_dir '{}'",
            config.scan.spill_dir
        )
    })?;
    std::fs::create_dir_all(&spill_root).with_context(|| {
        format!(
            "E_RBT_BRONZE_SPILL: mkdir {}",
            spill_root.display()
        )
    })?;
    let safe = format!(
        "{}__{}.parquet",
        schema_name.replace('/', "_"),
        table_name.replace('/', "_")
    );
    let spill_path = spill_root.join(safe);

    let opts = crate::materializer::MaterializeWriteOptions::from_config(&config.materialize, true);
    let stats = scanner
        .scan_spill_to_parquet(&req, &spill_path, &opts)
        .with_context(|| {
            format!(
                "E_RBT_BRONZE_SPILL: spill to {}",
                spill_path.display()
            )
        })?;
    tracing::info!(
        "Bronze {}.{} spilled {} rows ({} batches) → {}",
        schema_name,
        table_name,
        stats.rows,
        stats.batches,
        spill_path.display()
    );

    listing_table_provider(
        ctx,
        spill_path.to_str().unwrap_or_default(),
        SourceFormat::Parquet,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::dag::{Materialization, ModelDag, OutputFormat};

    #[tokio::test]
    async fn register_arrow_ipc_spills_to_parquet() -> Result<()> {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::ipc::writer::FileWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let temp = tempfile::tempdir()?;
        let bronze = temp.path().join("lake/bronze/symbol=X/timeframe=1m");
        std::fs::create_dir_all(&bronze)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X", "X"])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )?;
        let f = std::fs::File::create(bronze.join("chunk.arrow"))?;
        let mut w = FileWriter::try_new(f, &schema)?;
        w.write(&batch)?;
        w.finish()?;

        let sql = r#"---
source_format: arrow_ipc
scan_path: "lake/bronze"
path_glob: "**/*.arrow"
partition_by: [symbol, timeframe]
require_partitions:
  timeframe: "1m"
inject_source_path: true
---
SELECT symbol, timeframe, v FROM {{ source('bronze', 'ohlcv') }}
"#;
        let mut dag = ModelDag::new();
        dag.add_model_with_format(
            "stg_ohlcv",
            sql,
            Materialization::Table,
            OutputFormat::Parquet,
            None,
            "",
        )?;
        dag.build_graph()?;

        let ctx = SessionContext::new();
        let mut registered = HashSet::new();
        let cfg = crate::core::project::RbtProjectConfig::default();
        assert!(cfg.scan.spill_arrow_ipc);
        let n = register_bronze_sources_for_dag(&ctx, &dag, temp.path(), &mut registered, &cfg)
            .await?;
        assert_eq!(n, 1);

        let spill = temp
            .path()
            .join(".rbt/bronze_spill/bronze__ohlcv.parquet");
        assert!(
            spill.exists(),
            "expected spill parquet at {}",
            spill.display()
        );

        let df = ctx
            .sql("SELECT COUNT(*) AS c FROM bronze.ohlcv")
            .await?;
        let batches = df.collect().await?;
        // 2 data rows
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, 2);
        Ok(())
    }

    #[tokio::test]
    async fn register_jsonl_from_frontmatter() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let bronze = temp.path().join("raw.jsonl");
        std::fs::write(
            &bronze,
            r#"{"ticker":"NVDA","price":1.5}
{"ticker":"AAPL","price":2.5}
"#,
        )?;

        let sql = format!(
            r#"---
source_format: jsonl
scan_path: "{}"
---
SELECT ticker, price FROM {{{{ source('bronze', 'raw_trades') }}}}
"#,
            bronze.file_name().unwrap().to_string_lossy()
        );

        let mut dag = ModelDag::new();
        dag.add_model_with_format(
            "stg_trades",
            &sql,
            Materialization::Table,
            OutputFormat::Parquet,
            None,
            "",
        )?;
        dag.build_graph()?;

        let engine_ctx = SessionContext::new();
        let mut registered = HashSet::new();
        let cfg = crate::core::project::RbtProjectConfig::default();
        let n =
            register_bronze_sources_for_dag(&engine_ctx, &dag, temp.path(), &mut registered, &cfg)
                .await?;
        assert_eq!(n, 1);

        let df = engine_ctx
            .sql("SELECT COUNT(*) AS c FROM bronze.raw_trades")
            .await?;
        let batches = df.collect().await?;
        assert_eq!(batches[0].num_rows(), 1);
        Ok(())
    }
}

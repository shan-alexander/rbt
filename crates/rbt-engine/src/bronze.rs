//! Frontmatter-driven bronze source registration.
//!
//! ## Architecture
//!
//! * **Path A (DataFusion listing / external tables)** — Parquet, CSV, JSON/JSONL (no
//!   jshift projection), Arrow IPC file: register via DataFusion native readers, then
//!   wrap the resulting provider in [`BronzeTableProvider`].
//! * **Path B (scan → MemTable)** — jshift-projected JSONL, Arrow IPC stream, `.log`,
//!   `.txt`, TOML, or `force_scan: true`: load via `rbt-scan` into a `MemTable`, then
//!   wrap in [`BronzeTableProvider`].
//!
//! [`BronzeTableProvider`] is intentionally thin: it delegates scan/schema to the
//! inner provider and carries bronze metadata for lineage / debugging.

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::common::TableReference;
use datafusion::execution::options::ArrowReadOptions;
use datafusion::prelude::{CsvReadOptions, JsonReadOptions, ParquetReadOptions};
use rbt_core::dag::{ModelDag, ModelNode};
use rbt_core::frontmatter::{resolve_scan_path, SourceFormat, StagingFrontmatter};
use rbt_scan::{LakeScanner, ScanRequest};
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
    /// Inner provider is a MemTable filled by `rbt-scan`.
    ScanMemTable,
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
pub async fn register_bronze_sources_for_dag(
    ctx: &SessionContext,
    dag: &ModelDag,
    project_dir: &Path,
    registered: &mut HashSet<(String, String)>,
) -> Result<usize> {
    let mut count = 0;
    for idx in dag.graph.node_indices() {
        let node = &dag.graph[idx];
        if let Some(n) = register_bronze_for_model(ctx, node, project_dir, registered).await? {
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
) -> Result<Option<usize>> {
    let Some(fm) = node.frontmatter.as_ref() else {
        return Ok(None);
    };
    if !fm.has_scan_contract() {
        return Ok(None);
    }

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

    let format = fm.resolve_format().with_context(|| {
        format!("model '{}': cannot resolve source_format", node.name)
    })?;

    let resolved = resolve_scan_path(project_dir, fm.scan_path.as_deref().unwrap());
    if !resolved.exists() && !rbt_core::frontmatter::is_remote_uri(fm.scan_path.as_deref().unwrap())
    {
        bail!(
            "model '{}': bronze scan_path does not exist: {} (resolved {})",
            node.name,
            fm.scan_path.as_deref().unwrap(),
            resolved.display()
        );
    }

    let path_str = resolved.to_string_lossy().to_string();
    let use_scan = should_use_scan_path(fm, format);

    let (inner, mode) = if use_scan {
        let provider = scan_to_memtable(project_dir, fm, format)
            .await
            .with_context(|| format!("model '{}': bronze scan failed", node.name))?;
        (provider, BronzeRegistrationMode::ScanMemTable)
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

fn should_use_scan_path(fm: &StagingFrontmatter, format: SourceFormat) -> bool {
    if fm.force_scan.unwrap_or(false) {
        return true;
    }
    // Hive partition injection / filters / source path require the scan path
    // (DataFusion listing does not inject path-derived columns).
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
    // Nested hive dirs + stream IPC are not reliably handled by DF listing alone.
    matches!(
        format,
        SourceFormat::Log
            | SourceFormat::Txt
            | SourceFormat::Toml
            | SourceFormat::ArrowIpc
            | SourceFormat::ArrowIpcStream
    )
}

async fn ensure_schema(ctx: &SessionContext, schema_name: &str) -> Result<()> {
    // DataFusion accepts CREATE SCHEMA via SQL
    let sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", schema_name.replace('"', ""));
    ctx.sql(&sql)
        .await
        .with_context(|| format!("CREATE SCHEMA {}", schema_name))?
        .collect()
        .await
        .with_context(|| format!("CREATE SCHEMA {} collect", schema_name))?;
    Ok(())
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

async fn scan_to_memtable(
    project_dir: &Path,
    fm: &StagingFrontmatter,
    format: SourceFormat,
) -> Result<Arc<dyn TableProvider>> {
    let mut req = ScanRequest::from_frontmatter(project_dir, fm)?;
    req.format = format;
    let scanner = LakeScanner::from_request(&req);
    let batches = scanner.scan(&req).await?;
    if batches.is_empty() {
        bail!(
            "bronze scan produced zero batches for {}",
            req.resolved_path().display()
        );
    }
    let schema = batches[0].schema();
    // MemTable expects Vec<Vec<RecordBatch>> partitions
    let mem = MemTable::try_new(schema, vec![batches])
        .map_err(|e| anyhow::anyhow!("MemTable::try_new: {}", e))?;
    Ok(Arc::new(mem))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbt_core::dag::{Materialization, ModelDag, OutputFormat};

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
        let n =
            register_bronze_sources_for_dag(&engine_ctx, &dag, temp.path(), &mut registered)
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

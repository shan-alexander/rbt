//! Official Apache Iceberg **catalog** materialize (P2 proof gate).
//!
//! Path: create namespace/table → write data files via `DataFileWriter` →
//! `Transaction::fast_append` → `commit(catalog)` → load/read.
//!
//! Uses in-process [`MemoryCatalog`] with a **filesystem warehouse** under the
//! table root (durable on disk). This is a real snapshot commit through the
//! official `iceberg` crate — **not** a hand-rolled JSON metadata layout.
//!
//! Honesty: multi-writer REST/Glue OCC is still a later catalog choice; the
//! proof gate is create → write → commit → read on a local warehouse.

use anyhow::{bail, Context, Result};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::DataFileFormat;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::materializer::stream::{MaterializeWriteOptions, StreamWriteStats};
use crate::testing::{Assertion, StreamingAssertionRunner, ValidationResult};

/// Result of a catalog-backed Iceberg materialize.
#[derive(Debug, Clone)]
pub struct IcebergCatalogWriteStats {
    pub rows: usize,
    pub batches: usize,
    pub table_ident: String,
    pub metadata_location: String,
    pub snapshot_id: Option<i64>,
    pub data_files: usize,
    pub warehouse: PathBuf,
}

/// Options for catalog Iceberg writes.
#[derive(Debug, Clone)]
pub struct IcebergCatalogOptions {
    /// Namespace (single level) for MemoryCatalog tables. Default: `rbt`.
    pub namespace: String,
    /// Optional warehouse override; default = `table_root` itself.
    pub warehouse: Option<PathBuf>,
}

impl Default for IcebergCatalogOptions {
    fn default() -> Self {
        Self {
            namespace: "rbt".into(),
            warehouse: None,
        }
    }
}

fn path_to_warehouse_uri(path: &Path) -> Result<String> {
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("E_RBT_ICEBERG_CATALOG: cwd")?
            .join(path)
    };
    std::fs::create_dir_all(&abs).with_context(|| {
        format!(
            "E_RBT_ICEBERG_CATALOG: mkdir warehouse {}",
            abs.display()
        )
    })?;
    // MemoryCatalog accepts plain paths or file:// URIs.
    Ok(abs
        .canonicalize()
        .unwrap_or(abs)
        .to_string_lossy()
        .into_owned())
}

async fn open_memory_catalog(warehouse: &Path) -> Result<impl Catalog> {
    use iceberg::io::LocalFsStorageFactory;
    let wh = path_to_warehouse_uri(warehouse)?;
    // Critical: default MemoryCatalog uses **in-memory** storage; force local FS so
    // data files + metadata land on the warehouse path (durable SoR, ref() can re-read).
    MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "memory",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), wh)]),
        )
        .await
        .map_err(|e| {
            anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: MemoryCatalog load warehouse failed: {e}")
        })
}

fn table_name_from_root(table_root: &Path) -> Result<String> {
    table_root
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "E_RBT_ICEBERG_CATALOG: cannot derive table name from {}",
                table_root.display()
            )
        })
}

/// Full-refresh write via official Iceberg catalog + snapshot commit.
///
/// Drops existing table identity if present, creates schema, writes data files,
/// commits a fast-append snapshot, returns stats.
pub async fn write_iceberg_catalog_batches(
    batches: &[RecordBatch],
    table_root: &Path,
    cat_opts: &IcebergCatalogOptions,
) -> Result<IcebergCatalogWriteStats> {
    if batches.is_empty() {
        bail!("E_RBT_ICEBERG_CATALOG: empty batches for {}", table_root.display());
    }
    let schema = batches[0].schema();
    for (i, b) in batches.iter().enumerate().skip(1) {
        if b.schema().as_ref() != schema.as_ref() {
            bail!(
                "E_RBT_ICEBERG_CATALOG: schema mismatch at batch {i} for {}",
                table_root.display()
            );
        }
    }
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    write_iceberg_catalog_inner(
        schema.as_ref(),
        batches,
        table_root,
        cat_opts,
        &MaterializeWriteOptions::default(),
        &[],
        row_count,
        batches.len(),
    )
    .await
}

/// Stream path: assertions + DataFileWriter + catalog commit (no full-result retention beyond writer).
pub async fn write_iceberg_catalog_stream(
    stream: &mut SendableRecordBatchStream,
    table_root: &Path,
    cat_opts: &IcebergCatalogOptions,
    write_opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
) -> Result<StreamWriteStats> {
    let arrow_schema = stream.schema();
    let mut runner = StreamingAssertionRunner::new(assertions, write_opts.fail_fast_assertions);
    let mut rows = 0usize;
    let mut batches_n = 0usize;

    // Iceberg DataFileWriter is async and needs the table first; we create table from
    // stream schema, then write as we pull (bounded: one batch at a time into writer).
    // For simplicity and schema stability we buffer nothing beyond the writer itself —
    // but table creation needs schema only. Open catalog + table first, then write.
    let warehouse = cat_opts
        .warehouse
        .clone()
        .unwrap_or_else(|| table_root.to_path_buf());
    let catalog = open_memory_catalog(&warehouse).await?;
    let table_name = table_name_from_root(table_root)?;
    let ns = NamespaceIdent::new(cat_opts.namespace.clone());
    let ident = TableIdent::new(ns.clone(), table_name.clone());

    // Ensure namespace
    if catalog.namespace_exists(&ns).await.unwrap_or(false) {
        // ok
    } else {
        catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .map_err(|e| {
                anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: create_namespace '{}': {e}", cat_opts.namespace)
            })?;
    }

    // Full refresh: drop existing table identity
    if catalog.table_exists(&ident).await.unwrap_or(false) {
        catalog.drop_table(&ident).await.map_err(|e| {
            anyhow::anyhow!(
                "E_RBT_ICEBERG_CATALOG: drop_table {} before refresh: {e}",
                ident
            )
        })?;
    }

    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(arrow_schema.as_ref())
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: arrow→iceberg schema: {e}"))?;
    let location = path_to_warehouse_uri(table_root)?;
    let creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(iceberg_schema)
        .location(location)
        .build();

    let table = catalog
        .create_table(&ns, creation)
        .await
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: create_table '{table_name}': {e}"))?;

    let location_generator = DefaultLocationGenerator::new(table.metadata())
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: location generator: {e}"))?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "rbt".to_string(),
        None,
        DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(write_opts.max_row_group_rows.max(1)))
            .build(),
        table.metadata().current_schema().clone(),
    );
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let mut data_writer = DataFileWriterBuilder::new(rolling)
        .build(None)
        .await
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: DataFileWriter build: {e}"))?;

    // Project batches to iceberg arrow schema (field ids) when needed.
    let target_arrow = schema_to_arrow_schema(table.metadata().current_schema().as_ref())
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: schema_to_arrow: {e}"))?;
    let target_schema = Arc::new(target_arrow);

    while let Some(item) = stream.next().await {
        let batch = item.map_err(|e| {
            anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: stream error: {e}")
        })?;
        if !runner.is_empty() {
            runner
                .observe_batch(&batch)
                .map_err(|e| anyhow::anyhow!("E_RBT_MATERIALIZE_ASSERT: {e}"))?;
        }
        let batch = align_batch_to_schema(batch, &target_schema)?;
        rows += batch.num_rows();
        batches_n += 1;
        data_writer
            .write(batch)
            .await
            .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: write batch: {e}"))?;
    }

    let validation = runner.finish();
    if validation.failed_assertions > 0 {
        bail!(
            "E_RBT_MATERIALIZE_ASSERT: {} assertion(s) failed: {}",
            validation.failed_assertions,
            validation.errors.join("; ")
        );
    }

    let data_files = data_writer
        .close()
        .await
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: close DataFileWriter: {e}"))?;
    let n_files = data_files.len();
    let first_data_path = data_files
        .first()
        .map(|d| PathBuf::from(strip_file_uri(d.file_path())))
        .unwrap_or_else(|| table_root.to_path_buf());

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(data_files)
        .apply(tx)
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: fast_append apply: {e}"))?;
    let table = tx
        .commit(&catalog)
        .await
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: commit snapshot: {e}"))?;

    let snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
    let meta_loc = table
        .metadata_location()
        .unwrap_or("unknown")
        .to_string();

    tracing::info!(
        "Iceberg catalog table committed: {} ({} rows, {} data files, snapshot={:?}, meta={}, data={})",
        ident,
        rows,
        n_files,
        snapshot_id,
        meta_loc,
        first_data_path.display()
    );

    // Hint file for ref() / operators: first data parquet path (catalog UUID names).
    if first_data_path.exists() {
        let _ = std::fs::write(
            table_root.join(".rbt_iceberg_data"),
            first_data_path.to_string_lossy().as_bytes(),
        );
    }

    Ok(StreamWriteStats {
        rows,
        batches: batches_n,
        path: if first_data_path.exists() {
            first_data_path
        } else {
            table_root.to_path_buf()
        },
        bytes_written: 0,
        validation: ValidationResult {
            total_rows: rows,
            passed_assertions: validation.passed_assertions,
            failed_assertions: 0,
            errors: Vec::new(),
        },
    })
}

fn strip_file_uri(path: &str) -> String {
    path.strip_prefix("file://")
        .unwrap_or(path)
        .to_string()
}

#[allow(clippy::too_many_arguments)]
async fn write_iceberg_catalog_inner(
    arrow_schema: &ArrowSchema,
    batches: &[RecordBatch],
    table_root: &Path,
    cat_opts: &IcebergCatalogOptions,
    write_opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
    row_count: usize,
    batches_n: usize,
) -> Result<IcebergCatalogWriteStats> {
    let mut runner = StreamingAssertionRunner::new(assertions, write_opts.fail_fast_assertions);
    for b in batches {
        if !runner.is_empty() {
            runner.observe_batch(b)?;
        }
    }
    let validation = runner.finish();
    if validation.failed_assertions > 0 {
        bail!(
            "E_RBT_MATERIALIZE_ASSERT: {} assertion(s) failed: {}",
            validation.failed_assertions,
            validation.errors.join("; ")
        );
    }

    let warehouse = cat_opts
        .warehouse
        .clone()
        .unwrap_or_else(|| table_root.to_path_buf());
    let catalog = open_memory_catalog(&warehouse).await?;
    let table_name = table_name_from_root(table_root)?;
    let ns = NamespaceIdent::new(cat_opts.namespace.clone());
    let ident = TableIdent::new(ns.clone(), table_name.clone());

    if !catalog.namespace_exists(&ns).await.unwrap_or(false) {
        catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "E_RBT_ICEBERG_CATALOG: create_namespace '{}': {e}",
                    cat_opts.namespace
                )
            })?;
    }
    if catalog.table_exists(&ident).await.unwrap_or(false) {
        catalog.drop_table(&ident).await.map_err(|e| {
            anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: drop_table {ident}: {e}")
        })?;
    }

    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(arrow_schema)
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: arrow→iceberg schema: {e}"))?;
    let location = path_to_warehouse_uri(table_root)?;
    let creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(iceberg_schema)
        .location(location)
        .build();
    let table = catalog
        .create_table(&ns, creation)
        .await
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: create_table: {e}"))?;

    let location_generator = DefaultLocationGenerator::new(table.metadata())
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: location generator: {e}"))?;
    let file_name_generator =
        DefaultFileNameGenerator::new("rbt".into(), None, DataFileFormat::Parquet);
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(write_opts.max_row_group_rows.max(1)))
            .build(),
        table.metadata().current_schema().clone(),
    );
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let mut data_writer = DataFileWriterBuilder::new(rolling)
        .build(None)
        .await
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: DataFileWriter: {e}"))?;

    let target_arrow = schema_to_arrow_schema(table.metadata().current_schema().as_ref())
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: schema_to_arrow: {e}"))?;
    let target_schema = Arc::new(target_arrow);

    for batch in batches {
        let batch = align_batch_to_schema(batch.clone(), &target_schema)?;
        data_writer
            .write(batch)
            .await
            .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: write: {e}"))?;
    }
    let data_files = data_writer
        .close()
        .await
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: close writer: {e}"))?;
    let n_files = data_files.len();

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(data_files)
        .apply(tx)
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: fast_append: {e}"))?;
    let table = tx
        .commit(&catalog)
        .await
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: commit: {e}"))?;

    let snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
    let meta_loc = table
        .metadata_location()
        .unwrap_or("unknown")
        .to_string();

    // Proof gate: load table back and confirm snapshot (same catalog process).
    let reloaded = catalog
        .load_table(&ident)
        .await
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: load_table after commit: {e}"))?;
    if reloaded.metadata().current_snapshot().is_none() {
        bail!("E_RBT_ICEBERG_CATALOG: no current snapshot after commit for {ident}");
    }

    // Scan via same catalog handle (MemoryCatalog is process-local).
    {
        use futures::TryStreamExt;
        let stream = reloaded
            .scan()
            .build()
            .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: post-commit scan: {e}"))?
            .to_arrow()
            .await
            .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: post-commit to_arrow: {e}"))?;
        let scanned: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: post-commit collect: {e}"))?;
        let scanned_rows: usize = scanned.iter().map(|b| b.num_rows()).sum();
        if scanned_rows != row_count {
            bail!(
                "E_RBT_ICEBERG_CATALOG: post-commit scan rows {scanned_rows} != written {row_count}"
            );
        }
    }

    tracing::info!(
        "Iceberg catalog table committed: {} ({} rows, {} files, snapshot={:?}, meta={})",
        ident,
        row_count,
        n_files,
        snapshot_id,
        meta_loc
    );

    Ok(IcebergCatalogWriteStats {
        rows: row_count,
        batches: batches_n,
        table_ident: ident.to_string(),
        metadata_location: meta_loc,
        snapshot_id,
        data_files: n_files,
        warehouse,
    })
}

/// Best-effort align batch columns to Iceberg arrow schema (names + types).
fn align_batch_to_schema(
    batch: RecordBatch,
    target: &Arc<ArrowSchema>,
) -> Result<RecordBatch> {
    if batch.schema().as_ref() == target.as_ref() {
        return Ok(batch);
    }
    // Rebuild by name order from target.
    let mut cols = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let idx = batch
            .schema()
            .index_of(field.name())
            .map_err(|_| {
                anyhow::anyhow!(
                    "E_RBT_ICEBERG_CATALOG: batch missing column '{}' required by Iceberg schema",
                    field.name()
                )
            })?;
        cols.push(batch.column(idx).clone());
    }
    RecordBatch::try_new(target.clone(), cols)
        .context("E_RBT_ICEBERG_CATALOG: rebuild RecordBatch for Iceberg field ids")
}

/// Verify proof gate from a **metadata file path** (MemoryCatalog is process-local;
/// reload via [`StaticTable`] + filesystem FileIO).
pub async fn verify_iceberg_catalog_table(
    metadata_location: &str,
    namespace: &str,
    table_name: &str,
    expect_min_rows: usize,
) -> Result<usize> {
    use futures::TryStreamExt;
    use iceberg::io::FileIO;
    use iceberg::table::StaticTable;

    let file_io = FileIO::new_with_fs();
    let ident = TableIdent::new(NamespaceIdent::new(namespace.to_string()), table_name.into());
    let table = StaticTable::from_metadata_file(metadata_location, ident, file_io)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "E_RBT_ICEBERG_CATALOG: StaticTable::from_metadata_file({metadata_location}): {e}"
            )
        })?;
    if table.metadata().current_snapshot().is_none() {
        bail!("E_RBT_ICEBERG_CATALOG: verify: no snapshot at {metadata_location}");
    }
    let stream = table
        .scan()
        .build()
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: scan build: {e}"))?
        .to_arrow()
        .await
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: to_arrow: {e}"))?;
    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("E_RBT_ICEBERG_CATALOG: collect scan: {e}"))?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if rows < expect_min_rows {
        bail!(
            "E_RBT_ICEBERG_CATALOG: verify row count {rows} < expected {expect_min_rows}"
        );
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn catalog_create_write_commit_read() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let table_root = temp.path().join("warehouse").join("dim_demo");
        std::fs::create_dir_all(&table_root)?;
        let batch = sample_batch();
        let opts = IcebergCatalogOptions {
            namespace: "rbt".into(),
            warehouse: Some(temp.path().join("warehouse")),
        };
        let stats = write_iceberg_catalog_batches(std::slice::from_ref(&batch), &table_root, &opts)
            .await?;
        assert_eq!(stats.rows, 3);
        assert!(stats.snapshot_id.is_some());
        assert!(stats.data_files >= 1);
        // Durable FS proof: metadata + data on disk, StaticTable scan matches.
        let meta = stats.metadata_location.strip_prefix("file://").unwrap_or(&stats.metadata_location);
        assert!(
            Path::new(meta).exists(),
            "metadata missing on disk: {meta}"
        );
        let rows = verify_iceberg_catalog_table(meta, "rbt", "dim_demo", 3).await?;
        assert_eq!(rows, 3);

        // Second full-refresh: drop + recreate + commit
        let batch2 = sample_batch();
        let stats2 =
            write_iceberg_catalog_batches(std::slice::from_ref(&batch2), &table_root, &opts)
                .await?;
        assert_eq!(stats2.rows, 3);
        assert!(stats2.snapshot_id.is_some());
        let meta2 = stats2
            .metadata_location
            .strip_prefix("file://")
            .unwrap_or(&stats2.metadata_location);
        assert!(Path::new(meta2).exists());
        Ok(())
    }
}

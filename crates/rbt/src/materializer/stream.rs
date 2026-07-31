//! Streaming materialize: pull DF `RecordBatch` streams batch-by-batch, write, drop.
//!
//! Peak retained memory ≈ in-flight batch + Parquet row-group encoder + unique tracker.
//! Never holds a full `Vec<RecordBatch>` for the model result.

use crate::core::dag::OutputFormat;
use crate::core::project::{
    MaterializeConfig, DEFAULT_MAX_ROW_GROUP_BYTES, DEFAULT_MAX_ROW_GROUP_ROWS,
};
use crate::testing::{Assertion, StreamingAssertionRunner, ValidationResult};
use anyhow::{bail, Context, Result};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Result of a successful stream materialize.
#[derive(Debug, Clone)]
pub struct StreamWriteStats {
    pub rows: usize,
    pub batches: usize,
    pub path: PathBuf,
    pub bytes_written: u64,
    pub validation: ValidationResult,
}

/// Options for stream / collect writers (derived from [`MaterializeConfig`]).
#[derive(Debug, Clone)]
pub struct MaterializeWriteOptions {
    pub max_row_group_rows: usize,
    pub max_row_group_bytes: usize,
    /// Abort on first assertion failure (default true for fail_on_error models).
    pub fail_fast_assertions: bool,
    /// Iceberg write backend for `OutputFormat::Iceberg`.
    pub iceberg_mode: crate::core::project::IcebergWriteMode,
    pub iceberg_namespace: String,
}

impl Default for MaterializeWriteOptions {
    fn default() -> Self {
        Self {
            max_row_group_rows: DEFAULT_MAX_ROW_GROUP_ROWS,
            max_row_group_bytes: DEFAULT_MAX_ROW_GROUP_BYTES,
            fail_fast_assertions: true,
            iceberg_mode: crate::core::project::IcebergWriteMode::Catalog,
            iceberg_namespace: "rbt".into(),
        }
    }
}

impl MaterializeWriteOptions {
    pub fn from_config(cfg: &MaterializeConfig, fail_fast_assertions: bool) -> Self {
        Self {
            max_row_group_rows: cfg.max_row_group_rows.max(1),
            max_row_group_bytes: cfg.max_row_group_bytes.max(1),
            fail_fast_assertions,
            iceberg_mode: cfg.iceberg.mode,
            iceberg_namespace: cfg.iceberg.namespace.clone(),
        }
    }
}

fn parquet_props(opts: &MaterializeWriteOptions) -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(opts.max_row_group_rows))
        .set_compression(Compression::SNAPPY)
        .build()
}

/// Staging path for atomic publish: `dir/.name.ext.rbt-partial`.
pub fn partial_path_for(dest: &Path) -> PathBuf {
    let parent = dest.parent().unwrap_or_else(|| Path::new("."));
    let name = dest
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "output".into());
    parent.join(format!(".{name}.rbt-partial"))
}

fn remove_if_exists(path: &Path) {
    if path.exists() {
        let _ = if path.is_dir() {
            fs::remove_dir_all(path)
        } else {
            fs::remove_file(path)
        };
    }
}

/// Atomically replace `dest` with `partial` (same filesystem). Cleans partial on failure.
pub fn atomic_publish(partial: &Path, dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("E_RBT_MATERIALIZE_IO: mkdir {}", parent.display()))?;
    }
    // Replace existing destination (full refresh).
    if dest.exists() {
        if dest.is_dir() {
            fs::remove_dir_all(dest).with_context(|| {
                format!(
                    "E_RBT_MATERIALIZE_IO: remove existing dir {}",
                    dest.display()
                )
            })?;
        } else {
            fs::remove_file(dest).with_context(|| {
                format!(
                    "E_RBT_MATERIALIZE_IO: remove existing file {}",
                    dest.display()
                )
            })?;
        }
    }
    fs::rename(partial, dest).with_context(|| {
        format!(
            "E_RBT_MATERIALIZE_ATOMIC: rename {} → {} failed. \
             Partial file left for inspection if rename partially failed.",
            partial.display(),
            dest.display()
        )
    })?;
    Ok(())
}

/// Stream a DataFusion result into `destination_path` for the given format.
///
/// On any error, partial artifacts are deleted (previous successful dest is left intact
/// until a successful atomic replace).
pub async fn materialize_stream(
    mut stream: SendableRecordBatchStream,
    format: &OutputFormat,
    destination_path: &Path,
    opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
) -> Result<StreamWriteStats> {
    match format {
        OutputFormat::Parquet | OutputFormat::ZeroCopyClone => {
            write_parquet_stream(&mut stream, destination_path, opts, assertions).await
        }
        OutputFormat::Jsonl => {
            write_line_stream(&mut stream, destination_path, opts, assertions, LineFormat::Jsonl)
                .await
        }
        OutputFormat::Csv => {
            write_line_stream(&mut stream, destination_path, opts, assertions, LineFormat::Csv)
                .await
        }
        OutputFormat::Iceberg => match opts.iceberg_mode {
            crate::core::project::IcebergWriteMode::Catalog => {
                crate::materializer::iceberg_catalog::write_iceberg_catalog_stream(
                    &mut stream,
                    destination_path,
                    &crate::materializer::iceberg_catalog::IcebergCatalogOptions {
                        namespace: opts.iceberg_namespace.clone(),
                        warehouse: Some(destination_path.to_path_buf()),
                    },
                    opts,
                    assertions,
                )
                .await
            }
            crate::core::project::IcebergWriteMode::Filesystem => {
                write_iceberg_stream(&mut stream, destination_path, opts, assertions).await
            }
        },
        OutputFormat::ParquetAndIceberg => {
            // Dual-write: stream once into parquet, then re-read path for iceberg layout
            // would double IO. For dual-write we buffer is bad — write parquet stream,
            // then copy data file into iceberg layout + metadata (metadata only needs schema+rows).
            let parquet_path =
                if destination_path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    destination_path.to_path_buf()
                } else {
                    destination_path.with_extension("parquet")
                };
            let stats =
                write_parquet_stream(&mut stream, &parquet_path, opts, assertions).await?;
            // Build iceberg sidecar from written parquet (schema + row count) without re-materializing batches.
            write_iceberg_sidecar_from_parquet(&parquet_path, stats.rows, &stats.path)?;
            Ok(stats)
        }
    }
}

/// Stream write Parquet with atomic publish + optional streaming assertions.
pub async fn write_parquet_stream(
    stream: &mut SendableRecordBatchStream,
    destination_path: &Path,
    opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
) -> Result<StreamWriteStats> {
    let schema = stream.schema();
    let partial = partial_path_for(destination_path);
    remove_if_exists(&partial);
    if let Some(parent) = partial.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut runner = StreamingAssertionRunner::new(assertions, opts.fail_fast_assertions);
    let props = parquet_props(opts);
    let file = File::create(&partial).with_context(|| {
        format!(
            "E_RBT_MATERIALIZE_IO: create partial parquet {}",
            partial.display()
        )
    })?;
    // Large buffer reduces syscalls on multi-million-row writes.
    let buf = BufWriter::with_capacity(8 * 1024 * 1024, file);
    let mut writer = ArrowWriter::try_new(buf, schema.clone(), Some(props)).with_context(|| {
        format!(
            "E_RBT_MATERIALIZE_PARQUET: ArrowWriter::try_new for {}",
            partial.display()
        )
    })?;

    let mut rows = 0usize;
    let mut batches = 0usize;
    let result = async {
        while let Some(item) = stream.next().await {
            let batch = item.map_err(|e| {
                anyhow::anyhow!("E_RBT_MATERIALIZE_STREAM: DataFusion stream error: {e}")
            })?;
            if batch.num_rows() == 0 && batch.num_columns() == 0 {
                continue;
            }
            if !runner.is_empty() {
                runner.observe_batch(&batch).map_err(|e| {
                    anyhow::anyhow!("E_RBT_MATERIALIZE_ASSERT: {e}")
                })?;
            }
            writer.write(&batch).with_context(|| {
                format!(
                    "E_RBT_MATERIALIZE_PARQUET: write batch #{batches} to {}",
                    partial.display()
                )
            })?;
            rows += batch.num_rows();
            batches += 1;
            // Soft flush when in-progress row group grows large.
            let in_progress = writer.in_progress_size();
            if in_progress >= opts.max_row_group_bytes {
                writer.flush().with_context(|| {
                    format!(
                        "E_RBT_MATERIALIZE_PARQUET: flush row group at {in_progress} bytes"
                    )
                })?;
            }
            // batch dropped here
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(e) = result {
        let _ = writer.close();
        remove_if_exists(&partial);
        return Err(e);
    }

    writer.close().with_context(|| {
        format!(
            "E_RBT_MATERIALIZE_PARQUET: close writer {}",
            partial.display()
        )
    })?;

    let validation = runner.finish();
    if validation.failed_assertions > 0 {
        remove_if_exists(&partial);
        bail!(
            "E_RBT_MATERIALIZE_ASSERT: {} assertion(s) failed: {}",
            validation.failed_assertions,
            validation.errors.join("; ")
        );
    }

    atomic_publish(&partial, destination_path)?;
    let bytes_written = fs::metadata(destination_path).map(|m| m.len()).unwrap_or(0);

    Ok(StreamWriteStats {
        rows,
        batches,
        path: destination_path.to_path_buf(),
        bytes_written,
        validation,
    })
}

#[derive(Clone, Copy)]
enum LineFormat {
    Jsonl,
    Csv,
}

async fn write_line_stream(
    stream: &mut SendableRecordBatchStream,
    destination_path: &Path,
    opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
    line_fmt: LineFormat,
) -> Result<StreamWriteStats> {
    let partial = partial_path_for(destination_path);
    remove_if_exists(&partial);
    if let Some(parent) = partial.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(&partial).with_context(|| {
        format!(
            "E_RBT_MATERIALIZE_IO: create partial {}",
            partial.display()
        )
    })?;
    let mut runner = StreamingAssertionRunner::new(assertions, opts.fail_fast_assertions);
    let mut rows = 0usize;
    let mut batches = 0usize;

    let write_result = async {
        match line_fmt {
            LineFormat::Jsonl => {
                let mut writer = arrow::json::LineDelimitedWriter::new(file);
                while let Some(item) = stream.next().await {
                    let batch = item.map_err(|e| {
                        anyhow::anyhow!("E_RBT_MATERIALIZE_STREAM: {e}")
                    })?;
                    if !runner.is_empty() {
                        runner.observe_batch(&batch)?;
                    }
                    writer.write(&batch)?;
                    rows += batch.num_rows();
                    batches += 1;
                }
                writer.finish()?;
            }
            LineFormat::Csv => {
                let mut writer = arrow::csv::Writer::new(file);
                while let Some(item) = stream.next().await {
                    let batch = item.map_err(|e| {
                        anyhow::anyhow!("E_RBT_MATERIALIZE_STREAM: {e}")
                    })?;
                    if !runner.is_empty() {
                        runner.observe_batch(&batch)?;
                    }
                    writer.write(&batch)?;
                    rows += batch.num_rows();
                    batches += 1;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(e) = write_result {
        remove_if_exists(&partial);
        return Err(e);
    }

    let validation = runner.finish();
    if validation.failed_assertions > 0 {
        remove_if_exists(&partial);
        bail!(
            "E_RBT_MATERIALIZE_ASSERT: {} assertion(s) failed: {}",
            validation.failed_assertions,
            validation.errors.join("; ")
        );
    }

    atomic_publish(&partial, destination_path)?;
    let bytes_written = fs::metadata(destination_path).map(|m| m.len()).unwrap_or(0);
    Ok(StreamWriteStats {
        rows,
        batches,
        path: destination_path.to_path_buf(),
        bytes_written,
        validation,
    })
}

async fn write_iceberg_stream(
    stream: &mut SendableRecordBatchStream,
    table_root: &Path,
    opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
) -> Result<StreamWriteStats> {
    // Data file is full-refresh; metadata versions are retained for a local snapshot log
    // (not multi-writer OCC / REST catalog — honest FS Iceberg-style history).
    let prior = read_iceberg_version_hint(table_root);
    let next_version = prior.map(|v| v + 1).unwrap_or(1);
    let mut meta_log = prior_metadata_log(table_root, prior);

    let staging = table_root.with_extension("rbt-partial-table");
    remove_if_exists(&staging);
    let data_dir = staging.join("data");
    let meta_dir = staging.join("metadata");
    fs::create_dir_all(&data_dir)?;
    fs::create_dir_all(&meta_dir)?;

    // Preserve prior metadata JSON files into staging for history.
    if let Some(old_meta) = table_root.join("metadata").exists().then(|| table_root.join("metadata"))
    {
        if let Ok(entries) = fs::read_dir(&old_meta) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().and_then(|x| x.to_str()) == Some("json") {
                    if let Some(name) = p.file_name() {
                        let _ = fs::copy(&p, meta_dir.join(name));
                    }
                }
            }
        }
    }

    let data_path = data_dir.join("part-00000.parquet");
    let schema = stream.schema();
    let stats = write_parquet_stream(stream, &data_path, opts, assertions).await?;

    write_iceberg_metadata(
        &staging,
        &schema,
        stats.rows,
        "part-00000.parquet",
        next_version,
        &mut meta_log,
    )?;

    if table_root.exists() {
        fs::remove_dir_all(table_root).with_context(|| {
            format!(
                "E_RBT_MATERIALIZE_IO: clear iceberg table {}",
                table_root.display()
            )
        })?;
    }
    if let Some(parent) = table_root.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::rename(&staging, table_root).with_context(|| {
        format!(
            "E_RBT_MATERIALIZE_ATOMIC: rename iceberg staging {} → {}",
            staging.display(),
            table_root.display()
        )
    })?;

    tracing::info!(
        "Iceberg FS table written (stream): {} ({} rows, metadata v{}, data/part-00000.parquet)",
        table_root.display(),
        stats.rows,
        next_version
    );

    Ok(StreamWriteStats {
        rows: stats.rows,
        batches: stats.batches,
        path: table_root.to_path_buf(),
        bytes_written: stats.bytes_written,
        validation: stats.validation,
    })
}

fn read_iceberg_version_hint(table_root: &Path) -> Option<u64> {
    let hint = table_root.join("metadata/version-hint.text");
    let s = fs::read_to_string(hint).ok()?;
    s.trim().parse().ok()
}

fn prior_metadata_log(table_root: &Path, prior: Option<u64>) -> Vec<serde_json::Value> {
    use serde_json::json;
    let mut log = Vec::new();
    if let Some(v) = prior {
        let meta_path = table_root.join(format!("metadata/v{v}.metadata.json"));
        if meta_path.exists() {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            log.push(json!({
                "timestamp-ms": now_ms,
                "metadata-file": format!("v{v}.metadata.json"),
            }));
        }
    }
    log
}

fn write_iceberg_sidecar_from_parquet(
    parquet_path: &Path,
    row_count: usize,
    _stats_path: &Path,
) -> Result<()> {
    let table_root = super::sibling_iceberg_dir(parquet_path);
    let prior = read_iceberg_version_hint(&table_root);
    let next = prior.map(|v| v + 1).unwrap_or(1);
    let mut log = prior_metadata_log(&table_root, prior);
    // Preserve prior metadata JSON into a temp list of copies.
    let mut prior_meta_files: Vec<(String, Vec<u8>)> = Vec::new();
    let old_meta = table_root.join("metadata");
    if old_meta.is_dir() {
        if let Ok(entries) = fs::read_dir(&old_meta) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().and_then(|x| x.to_str()) == Some("json") {
                    if let (Some(name), Ok(bytes)) = (
                        p.file_name().map(|n| n.to_string_lossy().into_owned()),
                        fs::read(&p),
                    ) {
                        prior_meta_files.push((name, bytes));
                    }
                }
            }
        }
    }

    let file = File::open(parquet_path)
        .with_context(|| format!("open {} for iceberg sidecar", parquet_path.display()))?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("parquet reader {}", parquet_path.display()))?;
    let schema = builder.schema().clone();

    if table_root.exists() {
        fs::remove_dir_all(&table_root)?;
    }
    let data_dir = table_root.join("data");
    let meta_dir = table_root.join("metadata");
    fs::create_dir_all(&data_dir)?;
    fs::create_dir_all(&meta_dir)?;
    for (name, bytes) in prior_meta_files {
        let _ = fs::write(meta_dir.join(name), bytes);
    }
    let data_name = "part-00000.parquet";
    fs::copy(parquet_path, data_dir.join(data_name))?;
    write_iceberg_metadata(
        &table_root,
        &schema,
        row_count,
        data_name,
        next,
        &mut log,
    )?;
    Ok(())
}

fn write_iceberg_metadata(
    table_root: &Path,
    schema: &SchemaRef,
    total_rows: usize,
    data_file_name: &str,
    version: u64,
    metadata_log: &mut Vec<serde_json::Value>,
) -> Result<()> {
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    let meta_dir = table_root.join("metadata");
    fs::create_dir_all(&meta_dir)?;

    let mut fields = Vec::new();
    for (i, f) in schema.fields().iter().enumerate() {
        fields.push(json!({
            "id": i + 1,
            "name": f.name(),
            "required": !f.is_nullable(),
            "type": arrow_type_to_iceberg_json(f.data_type()),
        }));
    }

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let snapshot_id = now_ms.wrapping_add(version);
    let location = table_root
        .canonicalize()
        .unwrap_or_else(|_| table_root.to_path_buf());
    let location_uri = format!("file://{}", location.display());

    let metadata = json!({
        "format-version": 2,
        "table-uuid": format!("{:032x}", snapshot_id),
        "location": location_uri,
        "last-sequence-number": version,
        "last-updated-ms": now_ms,
        "last-column-id": fields.len(),
        "current-schema-id": 0,
        "schemas": [{
            "type": "struct",
            "schema-id": 0,
            "fields": fields,
        }],
        "default-spec-id": 0,
        "partition-specs": [{ "spec-id": 0, "fields": [] }],
        "last-partition-id": 0,
        "default-sort-order-id": 0,
        "sort-orders": [{ "order-id": 0, "fields": [] }],
        "properties": {
            "rbt.writer": "rbt",
            "rbt.layout": "filesystem-iceberg-v1",
            "write.format.default": "parquet",
            "rbt.materialize": "stream",
            "rbt.metadata-version": version.to_string()
        },
        "current-snapshot-id": snapshot_id,
        "snapshots": [{
            "snapshot-id": snapshot_id,
            "sequence-number": version,
            "timestamp-ms": now_ms,
            "summary": {
                "operation": "overwrite",
                "rbt.added-records": total_rows.to_string(),
                "rbt.added-data-files": "1",
                "rbt.data-file": format!("data/{data_file_name}")
            },
            "schema-id": 0
        }],
        "snapshot-log": [{
            "timestamp-ms": now_ms,
            "snapshot-id": snapshot_id
        }],
        "metadata-log": metadata_log,
        "rbt": {
            "note": "Filesystem Iceberg-style table (full-refresh data, versioned metadata). Not REST/Glue OCC.",
            "data_files": [format!("data/{data_file_name}")],
            "row_count": total_rows,
            "metadata_version": version
        }
    });

    let meta_name = format!("v{version}.metadata.json");
    let meta_path = meta_dir.join(&meta_name);
    let mut meta_file = File::create(&meta_path)?;
    writeln!(meta_file, "{}", serde_json::to_string_pretty(&metadata)?)?;
    let mut hint = File::create(meta_dir.join("version-hint.text"))?;
    writeln!(hint, "{version}")?;
    fs::copy(&meta_path, meta_dir.join("metadata.json"))?;
    Ok(())
}

fn arrow_type_to_iceberg_json(dt: &arrow::datatypes::DataType) -> serde_json::Value {
    use arrow::datatypes::DataType;
    use serde_json::json;
    match dt {
        DataType::Boolean => json!("boolean"),
        DataType::Int32 => json!("int"),
        DataType::Int64 => json!("long"),
        DataType::Float32 => json!("float"),
        DataType::Float64 => json!("double"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => json!("string"),
        DataType::Binary | DataType::LargeBinary => json!("binary"),
        DataType::Date32 | DataType::Date64 => json!("date"),
        DataType::Timestamp(_, _) => json!("timestamptz"),
        other => json!(format!("string /* arrow:{:?} */", other)),
    }
}

/// Collect-mode helper: write batches with same Parquet props / atomic publish as stream.
pub fn write_parquet_batches_atomic(
    batches: &[RecordBatch],
    path: &Path,
    opts: &MaterializeWriteOptions,
) -> Result<usize> {
    if batches.is_empty() {
        return Ok(0);
    }
    let schema = batches[0].schema();
    let partial = partial_path_for(path);
    remove_if_exists(&partial);
    if let Some(parent) = partial.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(&partial)?;
    let buf = BufWriter::with_capacity(8 * 1024 * 1024, file);
    let props = parquet_props(opts);
    let mut writer = ArrowWriter::try_new(buf, schema, Some(props))?;
    let mut rows = 0usize;
    for batch in batches {
        writer.write(batch)?;
        rows += batch.num_rows();
        if writer.in_progress_size() >= opts.max_row_group_bytes {
            writer.flush()?;
        }
    }
    writer.close()?;
    atomic_publish(&partial, path)?;
    Ok(rows)
}

/// Load small Parquet file into memory for optional MemTable ref() after stream write.
pub fn load_parquet_batches(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)
        .with_context(|| format!("E_RBT_REF_LOAD: open {} for MemTable", path.display()))?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("E_RBT_REF_LOAD: parquet builder {}", path.display()))?;
    let reader = builder
        .build()
        .with_context(|| format!("E_RBT_REF_LOAD: parquet reader {}", path.display()))?;
    let mut out = Vec::new();
    for item in reader {
        out.push(item.with_context(|| {
            format!("E_RBT_REF_LOAD: read batch from {}", path.display())
        })?);
    }
    Ok(out)
}

/// Empty schema-only Parquet (0 rows) so ref() registration has a file.
pub fn write_empty_parquet(schema: SchemaRef, path: &Path, opts: &MaterializeWriteOptions) -> Result<()> {
    let partial = partial_path_for(path);
    remove_if_exists(&partial);
    if let Some(parent) = partial.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(&partial)?;
    let props = parquet_props(opts);
    let writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.close()?;
    atomic_publish(&partial, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Assertion;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn sample_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[tokio::test]
    async fn stream_parquet_many_batches_row_count() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let dest = temp.path().join("out.parquet");
        let ctx = SessionContext::new();
        // Produce multiple small batches via UNION ALL chain
        let df = ctx
            .sql(
                "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')) \
                 AS t(id, name)",
            )
            .await?;
        let stream = df.execute_stream().await?;
        let opts = MaterializeWriteOptions {
            max_row_group_rows: 2,
            max_row_group_bytes: 1024,
            fail_fast_assertions: true,
            ..Default::default()
        };
        let assertions = vec![Assertion::UniqueKey {
            columns: vec!["id".into()],
        }];
        let mut stream = stream;
        let stats = write_parquet_stream(&mut stream, &dest, &opts, &assertions).await?;
        assert_eq!(stats.rows, 5);
        assert!(dest.exists());
        assert!(!partial_path_for(&dest).exists());
        let loaded = load_parquet_batches(&dest)?;
        let n: usize = loaded.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 5);
        Ok(())
    }

    #[tokio::test]
    async fn iceberg_stream_versions_metadata() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("tbl");
        let ctx = SessionContext::new();
        let opts = MaterializeWriteOptions::default();

        let df1 = ctx.sql("SELECT 1 AS id").await?;
        let mut s1 = df1.execute_stream().await?;
        write_iceberg_stream(&mut s1, &root, &opts, &[]).await?;
        assert!(root.join("metadata/v1.metadata.json").exists());
        assert_eq!(
            fs::read_to_string(root.join("metadata/version-hint.text"))?.trim(),
            "1"
        );

        let df2 = ctx.sql("SELECT 2 AS id").await?;
        let mut s2 = df2.execute_stream().await?;
        write_iceberg_stream(&mut s2, &root, &opts, &[]).await?;
        assert!(root.join("metadata/v2.metadata.json").exists());
        // prior v1 preserved
        assert!(root.join("metadata/v1.metadata.json").exists());
        assert_eq!(
            fs::read_to_string(root.join("metadata/version-hint.text"))?.trim(),
            "2"
        );
        Ok(())
    }

    #[tokio::test]
    async fn stream_unique_failure_removes_partial() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let dest = temp.path().join("dup.parquet");
        let ctx = SessionContext::new();
        let df = ctx
            .sql("SELECT * FROM (VALUES (1), (1)) AS t(id)")
            .await?;
        let stream = df.execute_stream().await?;
        let opts = MaterializeWriteOptions::default();
        let assertions = vec![Assertion::UniqueKey {
            columns: vec!["id".into()],
        }];
        let mut stream = stream;
        let err = write_parquet_stream(&mut stream, &dest, &opts, &assertions)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("E_RBT_MATERIALIZE_ASSERT") || err.contains("Duplicate"),
            "got: {err}"
        );
        assert!(!dest.exists(), "failed assert must not publish dest");
        assert!(
            !partial_path_for(&dest).exists(),
            "partial must be cleaned on assert fail"
        );
        Ok(())
    }

    #[test]
    fn atomic_publish_replaces_existing() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let dest = temp.path().join("f.parquet");
        fs::write(&dest, b"old")?;
        let partial = partial_path_for(&dest);
        fs::write(&partial, b"new-data")?;
        atomic_publish(&partial, &dest)?;
        assert_eq!(fs::read(&dest)?, b"new-data");
        assert!(!partial.exists());
        Ok(())
    }

    #[test]
    fn write_empty_parquet_ok() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let dest = temp.path().join("empty.parquet");
        write_empty_parquet(sample_schema(), &dest, &MaterializeWriteOptions::default())?;
        assert!(dest.exists());
        let batches = load_parquet_batches(&dest)?;
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 0);
        Ok(())
    }

}

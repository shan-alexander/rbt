//! `rbt::materializer`: multi-format writers including filesystem Iceberg-style tables.
//!
//! **Streaming materialize** ([`stream`]) is the default engine path: pull a DataFusion
//! batch stream, write batch-by-batch, drop each batch, atomic-publish the file.

pub mod iceberg_catalog;
pub mod incremental;
pub mod lineage;
pub mod stream;
pub mod wap;

pub use iceberg_catalog::{
    verify_iceberg_catalog_table, write_iceberg_catalog_batches, write_iceberg_catalog_stream,
    IcebergCatalogOptions, IcebergCatalogWriteStats,
};
pub use incremental::{
    clear_incremental_parts, incremental_ref_path, load_manifest,
    materialize_incremental_append_stream, materialize_scoped_replace_stream, parts_dir_for_parquet,
    resolve_part_keys, scope_part_id, uses_parts_directory, IncrementalManifest,
};
pub use lineage::{stamp_batch, stamped_schema, LineageStamp};
pub use stream::{
    atomic_publish, load_parquet_batches, materialize_stream, partial_path_for,
    write_empty_parquet, write_parquet_batches_atomic, write_parquet_stream, MaterializeWriteOptions,
    StreamWriteStats,
};
pub use wap::{new_wap_run_id, wap_publish, WapAuditLog, WapModelPaths, WapPhase};

use crate::core::dag::OutputFormat;
use crate::core::project::MaterializeConfig;
use crate::testing::{Assertion, RecordBatchValidator, ValidationResult};
use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use serde_json::{json, Value};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Status of a Write-Audit-Publish materialization run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WapStatus {
    Staged,
    AuditedSuccess,
    AuditedFailure { errors: Vec<String> },
    Published { rows_written: usize },
    RolledBack,
}

/// Multi-format stream writer (Parquet, JSONL, CSV, filesystem Iceberg layout).
pub struct MultiFormatWriter;

impl MultiFormatWriter {
    /// Writes RecordBatch array to target output format.
    ///
    /// * **Parquet / Jsonl / Csv** — single file at `destination_path`
    /// * **Iceberg** — table directory at `destination_path` with `data/` + `metadata/`
    /// * **ParquetAndIceberg** — flat `.parquet` sibling plus `{stem}.iceberg/` table dir
    /// * **ZeroCopyClone** — currently materializes Parquet (clone semantics later)
    pub fn write_batches(
        batches: &[RecordBatch],
        format: &OutputFormat,
        destination_path: &Path,
    ) -> Result<usize> {
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if batches.is_empty() {
            return Ok(0);
        }

        match format {
            OutputFormat::Parquet | OutputFormat::ZeroCopyClone => {
                write_parquet_file(batches, destination_path)?;
            }
            OutputFormat::Jsonl => {
                if let Some(parent) = destination_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let file = File::create(destination_path)?;
                let mut writer = arrow::json::LineDelimitedWriter::new(file);
                for batch in batches {
                    writer.write(batch)?;
                }
                writer.finish()?;
            }
            OutputFormat::Csv => {
                if let Some(parent) = destination_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let file = File::create(destination_path)?;
                let mut writer = arrow::csv::Writer::new(file);
                for batch in batches {
                    writer.write(batch)?;
                }
            }
            OutputFormat::Iceberg => {
                // Collect path: prefer catalog SoR (P2); FS layout only when configured.
                let opts = MaterializeWriteOptions::from_config(
                    &crate::core::project::MaterializeConfig::default(),
                    true,
                );
                match opts.iceberg_mode {
                    crate::core::project::IcebergWriteMode::Catalog => {
                        write_iceberg_catalog_batches_sync(batches, destination_path, &opts)?;
                    }
                    crate::core::project::IcebergWriteMode::Filesystem => {
                        write_iceberg_fs_table(batches, destination_path)?;
                    }
                }
            }
            OutputFormat::ParquetAndIceberg => {
                let parquet_path =
                    if destination_path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                        destination_path.to_path_buf()
                    } else {
                        destination_path.with_extension("parquet")
                    };
                write_parquet_file(batches, &parquet_path)?;
                write_iceberg_fs_table(batches, &sibling_iceberg_dir(&parquet_path))?;
            }
        }

        Ok(total_rows)
    }
}

fn write_iceberg_catalog_batches_sync(
    batches: &[RecordBatch],
    destination_path: &Path,
    opts: &MaterializeWriteOptions,
) -> Result<()> {
    let cat_opts = IcebergCatalogOptions {
        namespace: opts.iceberg_namespace.clone(),
        warehouse: Some(destination_path.to_path_buf()),
    };
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        // Safe when called from async via spawn_blocking/block_in_place or sync tests.
        tokio::task::block_in_place(|| {
            handle.block_on(write_iceberg_catalog_batches(
                batches,
                destination_path,
                &cat_opts,
            ))
        })?;
    } else {
        let rt = tokio::runtime::Runtime::new()
            .context("E_RBT_ICEBERG_CATALOG: create tokio runtime")?;
        rt.block_on(write_iceberg_catalog_batches(
            batches,
            destination_path,
            &cat_opts,
        ))?;
    }
    Ok(())
}

fn write_parquet_file(batches: &[RecordBatch], path: &Path) -> Result<()> {
    if batches.is_empty() {
        anyhow::bail!(
            "write_parquet_file: empty batch list for {}",
            path.display()
        );
    }
    let schema = batches[0].schema();
    for (i, b) in batches.iter().enumerate().skip(1) {
        if b.schema().as_ref() != schema.as_ref() {
            anyhow::bail!(
                "write_parquet_file: schema mismatch at batch {} for {}",
                i,
                path.display()
            );
        }
    }
    let opts = MaterializeWriteOptions::from_config(&MaterializeConfig::default(), true);
    write_parquet_batches_atomic(batches, path, &opts).map(|_| ())
}

/// Sibling Iceberg table directory for dual-write: `foo.parquet` → `foo.iceberg/`.
pub fn sibling_iceberg_dir(parquet_path: &Path) -> PathBuf {
    let stem = parquet_path.with_extension("");
    PathBuf::from(format!("{}.iceberg", stem.display()))
}

/// Write an Iceberg-style filesystem table (data files + metadata snapshot).
///
/// Layout:
/// ```text
/// table_root/
///   data/part-00000.parquet
///   metadata/
///     v1.metadata.json
///     version-hint.text
/// ```
///
/// This is a **full-refresh** replace of the table root (not multi-snapshot OCC yet).
/// Compatible enough for rbt CLI `--format iceberg` and dual-write demos; full REST
/// catalog commit remains a follow-on.
pub fn write_iceberg_fs_table(batches: &[RecordBatch], table_root: &Path) -> Result<usize> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if batches.is_empty() {
        return Ok(0);
    }

    // Full refresh: clear previous table root if present.
    if table_root.exists() {
        fs::remove_dir_all(table_root)
            .with_context(|| format!("clear iceberg table {}", table_root.display()))?;
    }
    let data_dir = table_root.join("data");
    let meta_dir = table_root.join("metadata");
    fs::create_dir_all(&data_dir)?;
    fs::create_dir_all(&meta_dir)?;

    let data_file_name = "part-00000.parquet";
    let data_path = data_dir.join(data_file_name);
    write_parquet_file(batches, &data_path)?;

    let schema = batches[0].schema();
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
    let snapshot_id = now_ms;
    let location = table_root
        .canonicalize()
        .unwrap_or_else(|_| table_root.to_path_buf());
    let location_uri = format!("file://{}", location.display());

    let metadata = json!({
        "format-version": 2,
        "table-uuid": format!("{:032x}", snapshot_id),
        "location": location_uri,
        "last-sequence-number": 1,
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
            "write.format.default": "parquet"
        },
        "current-snapshot-id": snapshot_id,
        "snapshots": [{
            "snapshot-id": snapshot_id,
            "sequence-number": 1,
            "timestamp-ms": now_ms,
            "summary": {
                "operation": "overwrite",
                "rbt.added-records": total_rows.to_string(),
                "rbt.added-data-files": "1",
                "rbt.data-file": format!("data/{}", data_file_name)
            },
            "schema-id": 0
        }],
        "snapshot-log": [{
            "timestamp-ms": now_ms,
            "snapshot-id": snapshot_id
        }],
        "metadata-log": [],
        "rbt": {
            "note": "Filesystem Iceberg-style table written by rbt (full refresh). Not a full catalog OCC commit.",
            "data_files": [format!("data/{}", data_file_name)],
            "row_count": total_rows
        }
    });

    let meta_path = meta_dir.join("v1.metadata.json");
    let mut meta_file = File::create(&meta_path)?;
    writeln!(meta_file, "{}", serde_json::to_string_pretty(&metadata)?)?;

    let mut hint = File::create(meta_dir.join("version-hint.text"))?;
    writeln!(hint, "1")?;

    // Convenience pointer for tools that look for metadata.json
    fs::copy(&meta_path, meta_dir.join("metadata.json"))?;

    tracing::info!(
        "Iceberg FS table written: {} ({} rows, data/{})",
        table_root.display(),
        total_rows,
        data_file_name
    );
    Ok(total_rows)
}

fn arrow_type_to_iceberg_json(dt: &arrow::datatypes::DataType) -> Value {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Boolean => json!("boolean"),
        DataType::Int32 => json!("int"),
        DataType::Int64 => json!("long"),
        DataType::Float32 => json!("float"),
        DataType::Float64 => json!("double"),
        DataType::Utf8 | DataType::LargeUtf8 => json!("string"),
        DataType::Binary | DataType::LargeBinary => json!("binary"),
        DataType::Date32 | DataType::Date64 => json!("date"),
        DataType::Timestamp(_, _) => json!("timestamptz"),
        other => json!(format!("string /* arrow:{:?} */", other)),
    }
}

/// Write-Audit-Publish (WAP) Materializer — audit helpers (catalog branch publish later).
pub struct WapMaterializer {
    pub target_table: String,
    pub wap_branch: String,
    pub staging_dir: PathBuf,
}

impl WapMaterializer {
    pub fn new(
        target_table: impl Into<String>,
        wap_branch: impl Into<String>,
        staging_dir: impl AsRef<Path>,
    ) -> Self {
        Self {
            target_table: target_table.into(),
            wap_branch: wap_branch.into(),
            staging_dir: staging_dir.as_ref().to_path_buf(),
        }
    }

    pub async fn write_stage(&self, batches: &[RecordBatch]) -> Result<usize> {
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        tracing::info!(
            "WAP WRITE: Staging {} rows into branch '{}' for table '{}'",
            total_rows,
            self.wap_branch,
            self.target_table
        );
        tokio::fs::create_dir_all(&self.staging_dir).await?;
        // Materialize staging snapshot as Iceberg FS table under staging_dir.
        if !batches.is_empty() {
            write_iceberg_fs_table(batches, &self.staging_dir.join("table"))?;
        }
        Ok(total_rows)
    }

    pub fn audit_stage(
        &self,
        batches: &[RecordBatch],
        assertions: &[Assertion],
    ) -> Result<ValidationResult> {
        tracing::info!(
            "WAP AUDIT: Executing {} assertions on branch '{}'",
            assertions.len(),
            self.wap_branch
        );
        let validation = RecordBatchValidator::validate_batches(batches, assertions);
        if validation.failed_assertions > 0 {
            tracing::error!(
                "WAP AUDIT FAILED: {} assertions failed on branch '{}': {:?}",
                validation.failed_assertions,
                self.wap_branch,
                validation.errors
            );
        } else {
            tracing::info!(
                "WAP AUDIT PASSED: All {} assertions passed on branch '{}'",
                validation.passed_assertions,
                self.wap_branch
            );
        }
        Ok(validation)
    }

    pub async fn publish_stage(&self, audit_result: &ValidationResult) -> Result<WapStatus> {
        if audit_result.failed_assertions > 0 {
            return Ok(WapStatus::RolledBack);
        }
        Ok(WapStatus::Published {
            rows_written: audit_result.total_rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

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

    #[test]
    fn test_multi_format_writer() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let batch = sample_batch();

        let parquet_file = temp_dir.path().join("output.parquet");
        let rows = MultiFormatWriter::write_batches(
            std::slice::from_ref(&batch),
            &OutputFormat::Parquet,
            &parquet_file,
        )?;
        assert_eq!(rows, 3);
        assert!(parquet_file.exists());
        assert!(parquet_file.metadata()?.len() > 0);

        let iceberg_dir = temp_dir.path().join("tbl");
        // Explicit FS layout path for dual-compat tests; catalog SoR has its own unit test.
        let irows = write_iceberg_fs_table(std::slice::from_ref(&batch), &iceberg_dir)?;
        assert_eq!(irows, 3);
        assert!(iceberg_dir.join("data/part-00000.parquet").exists());
        assert!(iceberg_dir.join("metadata/v1.metadata.json").exists());
        assert!(iceberg_dir.join("metadata/version-hint.text").exists());
        assert!(iceberg_dir.join("metadata/metadata.json").exists());

        let meta: Value = serde_json::from_str(&fs::read_to_string(
            iceberg_dir.join("metadata/v1.metadata.json"),
        )?)?;
        assert_eq!(meta["format-version"], 2);
        assert_eq!(meta["rbt"]["row_count"], 3);

        let dual = temp_dir.path().join("dual.parquet");
        MultiFormatWriter::write_batches(&[batch], &OutputFormat::ParquetAndIceberg, &dual)?;
        assert!(dual.exists());
        assert!(sibling_iceberg_dir(&dual)
            .join("data/part-00000.parquet")
            .exists());

        Ok(())
    }

    #[test]
    fn iceberg_full_refresh_replaces() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("t");
        let b = sample_batch();
        write_iceberg_fs_table(std::slice::from_ref(&b), &root)?;
        // plant a junk file then rewrite
        fs::write(root.join("data/junk.txt"), b"x")?;
        write_iceberg_fs_table(std::slice::from_ref(&b), &root)?;
        assert!(!root.join("data/junk.txt").exists());
        assert!(root.join("data/part-00000.parquet").exists());
        Ok(())
    }

    #[test]
    fn empty_batches_ok() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let n = MultiFormatWriter::write_batches(
            &[],
            &OutputFormat::Parquet,
            &temp.path().join("empty.parquet"),
        )?;
        assert_eq!(n, 0);
        assert!(!temp.path().join("empty.parquet").exists());
        Ok(())
    }

    #[test]
    fn sibling_iceberg_dir_name() {
        assert_eq!(
            sibling_iceberg_dir(Path::new("/lake/gold/fact.parquet")),
            PathBuf::from("/lake/gold/fact.iceberg")
        );
    }
}

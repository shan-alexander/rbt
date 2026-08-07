//! Declared schema emit (RBT-A6).
//!
//! When bronze is missing (`on_missing: empty`) or SQL returns zero rows, published
//! Parquet and preview batches still expose a **stable physical schema** from
//! frontmatter `columns.*.dtype` (plus `partition_by` keys).
//!
//! ## Supported `columns.*.dtype` values
//!
//! | Logical dtype | Arrow type |
//! |---------------|------------|
//! | `utf8`, `string`, `str`, `varchar`, `text` | Utf8 |
//! | `int64`, `long`, `bigint`, `i64` | Int64 |
//! | `int32`, `int`, `i32` | Int32 |
//! | `int16`, `smallint`, `i16` | Int16 |
//! | `int8`, `tinyint`, `i8` | Int8 |
//! | `uint64`, `u64` / `uint32`, `u32` | UInt64 / UInt32 |
//! | `float64`, `double`, `f64` | Float64 |
//! | `float32`, `float`, `f32` | Float32 |
//! | `bool`, `boolean` | Boolean |
//! | `binary`, `bytes`, `blob` | Binary |
//! | `date`, `date32` | Date32 |
//! | `timestamp`, `timestamp_us`, `timestamptz` | Timestamp(µs) |
//! | `timestamp_ms` / `_ns` / `_s` | Timestamp(ms/ns/s) |
//!
//! See [`parse_logical_dtype`] and [`SUPPORTED_LOGICAL_DTYPES`].

use crate::core::frontmatter::{parse_logical_dtype, StagingFrontmatter};
use anyhow::{Context, Result};
use arrow::array::new_null_array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::sync::Arc;

/// Canonical list of logical dtype tokens accepted by [`parse_logical_dtype`].
///
/// Aliases (e.g. `string` → utf8) are also accepted; this list is the primary set
/// for docs and inventory tests (RBT-A6.1).
pub const SUPPORTED_LOGICAL_DTYPES: &[&str] = &[
    "utf8",
    "int64",
    "int32",
    "int16",
    "int8",
    "uint64",
    "uint32",
    "float64",
    "float32",
    "bool",
    "binary",
    "date32",
    "timestamp",
    "timestamp_ms",
    "timestamp_ns",
    "timestamp_s",
];

/// Soft declared schema for materialize/preview (RBT-A6).
///
/// Includes only `columns` entries that have a `dtype` (docs-only columns without
/// dtype are skipped), plus `partition_by` keys not already present (Utf8), then
/// optional `_source_path`.
///
/// Returns `Ok(None)` when nothing typed is declared. Does **not** fail when a
/// column lacks `dtype` — that is reserved for the strict empty-frame path.
pub fn try_declared_schema(fm: &StagingFrontmatter) -> Result<Option<SchemaRef>> {
    build_declared_schema(fm, /* require_dtype_on_listed */ false)
}

/// Required declared schema for bronze `on_missing: empty`.
///
/// Every listed `columns` entry must have `dtype`. Errors with `E_RBT_EMPTY_SCHEMA`
/// when no typed fields can be built.
pub fn declared_schema_for_frontmatter(fm: &StagingFrontmatter) -> Result<SchemaRef> {
    build_declared_schema(fm, /* require_dtype_on_listed */ true)?.with_context(|| {
        "E_RBT_EMPTY_SCHEMA: on_missing: empty requires columns with dtype \
         and/or partition_by (model scan contract has no schema fields)"
    })
}

fn build_declared_schema(
    fm: &StagingFrontmatter,
    require_dtype_on_listed: bool,
) -> Result<Option<SchemaRef>> {
    let mut fields: Vec<Field> = Vec::new();
    let mut seen = HashSet::new();

    if let Some(cols) = &fm.columns {
        for (name, meta) in cols {
            match meta.dtype.as_deref() {
                Some(dtype) => {
                    let dt = parse_logical_dtype(dtype).with_context(|| {
                        format!("E_RBT_EMPTY_SCHEMA: column '{name}' dtype '{dtype}'")
                    })?;
                    fields.push(Field::new(name, dt, true));
                    seen.insert(name.clone());
                }
                None if require_dtype_on_listed => {
                    anyhow::bail!(
                        "E_RBT_EMPTY_SCHEMA: column '{name}' needs dtype: for on_missing: empty \
                         (e.g. utf8, int64, float64, bool, date32, timestamp)"
                    );
                }
                None => {
                    // Soft path: description-only columns are docs metadata.
                }
            }
        }
    }

    if let Some(parts) = &fm.partition_by {
        for p in parts {
            if seen.insert(p.clone()) {
                fields.push(Field::new(p, DataType::Utf8, true));
            }
        }
    }

    if fm.inject_source_path.unwrap_or(false) && seen.insert("_source_path".into()) {
        fields.push(Field::new("_source_path", DataType::Utf8, true));
    }

    if fields.is_empty() {
        return Ok(None);
    }
    Ok(Some(Arc::new(Schema::new(fields))))
}

/// Zero-row [`RecordBatch`] with the declared frontmatter schema (RBT-A6.2).
///
/// Shared by bronze empty registration and zero-row materialize/preview.
pub fn empty_batch_for_frontmatter(fm: &StagingFrontmatter) -> Result<RecordBatch> {
    let schema = declared_schema_for_frontmatter(fm)?;
    Ok(RecordBatch::new_empty(schema))
}

/// Merge SQL/stream schema with declared contract fields.
///
/// Existing SQL field names and types win; any declared field not in `base` is
/// appended (nullable) so zero-row writers still emit the full contract.
pub fn merge_stream_and_declared(base: &Schema, declared: &Schema) -> SchemaRef {
    let mut fields: Vec<Field> = base.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut seen: HashSet<String> = fields.iter().map(|f| f.name().to_string()).collect();
    for f in declared.fields() {
        if seen.insert(f.name().clone()) {
            fields.push(f.as_ref().clone());
        }
    }
    Arc::new(Schema::new(fields))
}

/// Keep all SQL columns; append null columns for any declared field missing from
/// the batch (RBT-A6.4). Does **not** cast existing SQL types to declared types.
pub fn ensure_declared_columns(batch: &RecordBatch, declared: &Schema) -> Result<RecordBatch> {
    let mut missing: Vec<Field> = Vec::new();
    for f in declared.fields() {
        if batch.schema().index_of(f.name()).is_err() {
            missing.push(f.as_ref().clone());
        }
    }
    if missing.is_empty() {
        return Ok(batch.clone());
    }
    let n = batch.num_rows();
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns = batch.columns().to_vec();
    for f in missing {
        columns.push(new_null_array(f.data_type(), n));
        fields.push(f);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).context("E_RBT_SCHEMA_EMIT: ensure_declared_columns")
}

/// Align a list of batches (or produce one empty batch) to include declared fields.
///
/// When `batches` is empty or all zero-row, returns a single empty batch with
/// `merge_stream_and_declared(sql_schema, declared)` when declared is set,
/// otherwise `RecordBatch::new_empty(sql_schema)`.
pub fn align_batches_to_declared(
    batches: &[RecordBatch],
    sql_schema: &Schema,
    declared: Option<&Schema>,
) -> Result<Vec<RecordBatch>> {
    let Some(decl) = declared else {
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(Arc::new(sql_schema.clone()))]);
        }
        return Ok(batches.to_vec());
    };

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if batches.is_empty() || total_rows == 0 {
        let merged = merge_stream_and_declared(sql_schema, decl);
        return Ok(vec![RecordBatch::new_empty(merged)]);
    }

    batches
        .iter()
        .map(|b| ensure_declared_columns(b, decl))
        .collect()
}

/// Inventory helper for tests: every token in [`SUPPORTED_LOGICAL_DTYPES`] parses.
pub fn parse_supported_dtypes_ok() -> Result<()> {
    for d in SUPPORTED_LOGICAL_DTYPES {
        let _ = parse_logical_dtype(d).with_context(|| format!("dtype inventory failed for {d}"))?;
    }
    Ok(())
}

/// Reject unknown dtype with a stable error prefix.
pub fn unknown_dtype_is_rejected(s: &str) -> bool {
    match parse_logical_dtype(s) {
        Ok(_) => false,
        Err(e) => format!("{e:#}").contains("unknown dtype"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::frontmatter::ColumnMeta;
    use arrow::array::{Array, Int64Array, StringArray};
    use std::collections::BTreeMap;

    fn fm_with_cols(cols: &[(&str, &str)]) -> StagingFrontmatter {
        let mut map = BTreeMap::new();
        for (n, dt) in cols {
            map.insert(
                (*n).into(),
                ColumnMeta {
                    dtype: Some((*dt).into()),
                    ..Default::default()
                },
            );
        }
        StagingFrontmatter {
            columns: Some(map),
            partition_by: Some(vec!["entity".into(), "report_date".into()]),
            ..Default::default()
        }
    }

    #[test]
    fn dtype_inventory_parses() {
        parse_supported_dtypes_ok().unwrap();
        assert!(unknown_dtype_is_rejected("not_a_type"));
        assert_eq!(parse_logical_dtype("string").unwrap(), DataType::Utf8);
        assert_eq!(parse_logical_dtype("date").unwrap(), DataType::Date32);
    }

    #[test]
    fn empty_batch_has_declared_and_partition_cols() {
        let fm = fm_with_cols(&[("id", "int64"), ("name", "utf8"), ("active", "bool")]);
        let batch = empty_batch_for_frontmatter(&fm).unwrap();
        assert_eq!(batch.num_rows(), 0);
        let s = batch.schema();
        assert_eq!(s.field_with_name("id").unwrap().data_type(), &DataType::Int64);
        assert_eq!(s.field_with_name("name").unwrap().data_type(), &DataType::Utf8);
        assert_eq!(
            s.field_with_name("active").unwrap().data_type(),
            &DataType::Boolean
        );
        // partition keys not already in columns
        assert_eq!(
            s.field_with_name("entity").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            s.field_with_name("report_date").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn ensure_adds_missing_declared_keeps_sql() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("extra", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let declared = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let out = ensure_declared_columns(&batch, &declared).unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(out.num_columns(), 3);
        assert!(out.schema().index_of("extra").is_ok());
        assert!(out.schema().index_of("name").is_ok());
        let name = out
            .column(out.schema().index_of("name").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(name.is_null(0) && name.is_null(1));
    }

    #[test]
    fn align_zero_rows_emits_merged_empty() {
        let sql = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
        let declared = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let out = align_batches_to_declared(&[], &sql, Some(&declared)).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 0);
        assert!(out[0].schema().index_of("name").is_ok());
    }

    #[test]
    fn try_declared_none_without_contract() {
        let fm = StagingFrontmatter::default();
        assert!(try_declared_schema(&fm).unwrap().is_none());
        assert!(declared_schema_for_frontmatter(&fm).is_err());
    }
}

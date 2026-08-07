//! Keyed upsert materialization (RBT-A7) — Type-1 entity-grain tables.
//!
//! **Honest scope (v1):** collect SQL + existing Parquet into memory, upsert, atomic
//! full rewrite of a single parquet file. Guarded by [`DEFAULT_UPSERT_MAX_ROWS`].
//!
//! ## Semantics (per incoming row)
//!
//! 1. Key missing in store → **insert** full row  
//! 2. Key present and all `compare_columns` equal (NULL-safe) → **touch** only  
//! 3. Else → **update** all non-key columns from incoming (including touch)
//!
//! Peer keys not present in the incoming batch are **kept**.

use crate::core::frontmatter::StagingFrontmatter;
use crate::materializer::stream::{
    load_parquet_batches, write_parquet_batches_atomic, MaterializeWriteOptions, StreamWriteStats,
};
use crate::testing::{Assertion, RecordBatchValidator, ValidationResult};
use anyhow::{bail, Context, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeStringArray, StringArray, StringViewArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt32Array, UInt64Array,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

/// Default max rows (existing + incoming) before `E_RBT_UPSERT_TOO_LARGE`.
/// Override with env `RBT_UPSERT_MAX_ROWS`.
pub const DEFAULT_UPSERT_MAX_ROWS: usize = 2_000_000;

/// Upsert key / touch / compare configuration (from frontmatter).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertConfig {
    pub unique_key: Vec<String>,
    pub touch_columns: Vec<String>,
    /// When `None`, resolved at execute time to all non-key, non-touch schema columns.
    pub compare_columns: Option<Vec<String>>,
}

impl UpsertConfig {
    pub fn from_frontmatter(fm: &StagingFrontmatter) -> Result<Self> {
        let unique_key: Vec<String> = fm
            .unique_key
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if unique_key.is_empty() {
            bail!(
                "E_RBT_UPSERT_KEY: materialization keyed_upsert requires non-empty unique_key: \
                 (e.g. unique_key: [entity_id])"
            );
        }

        let touch_columns: Vec<String> = fm
            .touch_columns
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let compare_columns = fm.compare_columns.as_ref().map(|cols| {
            cols.iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        });

        let key_set: HashSet<&str> = unique_key.iter().map(String::as_str).collect();
        let touch_set: HashSet<&str> = touch_columns.iter().map(String::as_str).collect();

        for t in &touch_columns {
            if key_set.contains(t.as_str()) {
                bail!(
                    "E_RBT_UPSERT_SCHEMA: touch_columns '{t}' overlaps unique_key — \
                     keys are immutable"
                );
            }
        }

        if let Some(ref cmp) = compare_columns {
            for c in cmp {
                if key_set.contains(c.as_str()) {
                    bail!(
                        "E_RBT_UPSERT_SCHEMA: compare_columns '{c}' overlaps unique_key"
                    );
                }
                if touch_set.contains(c.as_str()) {
                    bail!(
                        "E_RBT_UPSERT_SCHEMA: compare_columns '{c}' overlaps touch_columns \
                         (touch is not compared)"
                    );
                }
            }
        }

        // Dedup keys preserving order
        let mut seen = HashSet::new();
        let unique_key: Vec<String> = unique_key
            .into_iter()
            .filter(|k| seen.insert(k.clone()))
            .collect();

        Ok(Self {
            unique_key,
            touch_columns,
            compare_columns,
        })
    }

    /// Resolve compare columns against a schema (default = all non-key non-touch fields).
    pub fn resolve_compare_columns(&self, schema: &Schema) -> Result<Vec<String>> {
        if let Some(ref explicit) = self.compare_columns {
            for c in explicit {
                if schema.index_of(c).is_err() {
                    bail!(
                        "E_RBT_UPSERT_SCHEMA: compare_columns '{c}' not found in result schema"
                    );
                }
            }
            return Ok(explicit.clone());
        }
        let key: HashSet<&str> = self.unique_key.iter().map(String::as_str).collect();
        let touch: HashSet<&str> = self.touch_columns.iter().map(String::as_str).collect();
        Ok(schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| !key.contains(n.as_str()) && !touch.contains(n.as_str()))
            .collect())
    }

    pub fn validate_schema(&self, schema: &Schema) -> Result<()> {
        for k in &self.unique_key {
            if schema.index_of(k).is_err() {
                bail!(
                    "E_RBT_UPSERT_KEY: unique_key column '{k}' missing from SQL result schema"
                );
            }
        }
        for t in &self.touch_columns {
            if schema.index_of(t).is_err() {
                bail!(
                    "E_RBT_UPSERT_SCHEMA: touch_columns '{t}' missing from SQL result schema"
                );
            }
        }
        let _ = self.resolve_compare_columns(schema)?;
        Ok(())
    }
}

/// Counters for receipt / ops (RBT-A7.6).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpsertStats {
    pub rows_inserted: usize,
    /// Non-key attributes changed (full non-key replace).
    pub rows_updated: usize,
    /// Compare equal → only touch columns applied.
    pub rows_touched: usize,
    /// Existing keys not present in incoming batch (kept as-is).
    pub rows_kept: usize,
    /// Final table row count after upsert.
    pub total_rows: usize,
}

/// Result of a pure in-memory upsert (no IO).
#[derive(Debug, Clone)]
pub struct UpsertResult {
    pub batch: RecordBatch,
    pub stats: UpsertStats,
}

fn upsert_max_rows() -> usize {
    std::env::var("RBT_UPSERT_MAX_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_UPSERT_MAX_ROWS)
}

/// Concat batches; empty input → empty batch with `schema`.
pub fn concat_or_empty(schema: SchemaRef, batches: &[RecordBatch]) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    concat_batches(&schema, batches)
        .map_err(|e| anyhow::anyhow!("E_RBT_UPSERT_SCHEMA: concat batches: {e}"))
}

/// Encode a multi-column key at `row` as a stable byte string (NULL-safe).
fn encode_key(batch: &RecordBatch, row: usize, key_idxs: &[usize]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(64);
    for &idx in key_idxs {
        let col = batch.column(idx);
        out.push(0x1f); // separator
        if col.is_null(row) {
            out.push(b'N');
            continue;
        }
        out.push(b'V');
        encode_scalar(col.as_ref(), row, &mut out)?;
    }
    Ok(out)
}

fn utf8_value(col: &dyn Array, row: usize) -> Result<&str> {
    match col.data_type() {
        DataType::Utf8 => Ok(col
            .as_any()
            .downcast_ref::<StringArray>()
            .context("E_RBT_UPSERT_SCHEMA: utf8 downcast")?
            .value(row)),
        DataType::LargeUtf8 => Ok(col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .context("E_RBT_UPSERT_SCHEMA: large_utf8 downcast")?
            .value(row)),
        DataType::Utf8View => Ok(col
            .as_any()
            .downcast_ref::<StringViewArray>()
            .context("E_RBT_UPSERT_SCHEMA: utf8_view downcast")?
            .value(row)),
        other => bail!("E_RBT_UPSERT_SCHEMA: not a utf8 family type {other:?}"),
    }
}

fn encode_scalar(col: &dyn Array, row: usize, out: &mut Vec<u8>) -> Result<()> {
    match col.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            out.extend_from_slice(utf8_value(col, row)?.as_bytes());
        }
        DataType::Int64 => {
            let a = col.as_any().downcast_ref::<Int64Array>().unwrap();
            out.extend_from_slice(&a.value(row).to_le_bytes());
        }
        DataType::Int32 => {
            let a = col.as_any().downcast_ref::<Int32Array>().unwrap();
            out.extend_from_slice(&a.value(row).to_le_bytes());
        }
        DataType::Int16 => {
            let a = col.as_any().downcast_ref::<Int16Array>().unwrap();
            out.extend_from_slice(&a.value(row).to_le_bytes());
        }
        DataType::Int8 => {
            let a = col.as_any().downcast_ref::<Int8Array>().unwrap();
            out.push(a.value(row) as u8);
        }
        DataType::UInt64 => {
            let a = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            out.extend_from_slice(&a.value(row).to_le_bytes());
        }
        DataType::UInt32 => {
            let a = col.as_any().downcast_ref::<UInt32Array>().unwrap();
            out.extend_from_slice(&a.value(row).to_le_bytes());
        }
        DataType::Float64 => {
            let a = col.as_any().downcast_ref::<Float64Array>().unwrap();
            out.extend_from_slice(&a.value(row).to_bits().to_le_bytes());
        }
        DataType::Float32 => {
            let a = col.as_any().downcast_ref::<Float32Array>().unwrap();
            out.extend_from_slice(&a.value(row).to_bits().to_le_bytes());
        }
        DataType::Boolean => {
            let a = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            out.push(if a.value(row) { 1 } else { 0 });
        }
        DataType::Date32 => {
            let a = col.as_any().downcast_ref::<Date32Array>().unwrap();
            out.extend_from_slice(&a.value(row).to_le_bytes());
        }
        DataType::Timestamp(unit, _) => {
            let v = match unit {
                arrow::datatypes::TimeUnit::Second => col
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .value(row),
                arrow::datatypes::TimeUnit::Millisecond => col
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(row),
                arrow::datatypes::TimeUnit::Microsecond => col
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(row),
                arrow::datatypes::TimeUnit::Nanosecond => col
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(row),
            };
            out.extend_from_slice(&v.to_le_bytes());
        }
        other => bail!(
            "E_RBT_UPSERT_SCHEMA: unique_key / compare column type {other:?} not supported \
             for keyed upsert (use utf8|int*|float*|bool|date32|timestamp)"
        ),
    }
    Ok(())
}

/// NULL-safe equality of one column at two row indices (possibly different batches).
fn scalar_eq(a: &dyn Array, ra: usize, b: &dyn Array, rb: usize) -> Result<bool> {
    let an = a.is_null(ra);
    let bn = b.is_null(rb);
    if an && bn {
        return Ok(true);
    }
    if an || bn {
        return Ok(false);
    }
    if a.data_type() != b.data_type() {
        bail!(
            "E_RBT_UPSERT_SCHEMA: compare type mismatch {:?} vs {:?}",
            a.data_type(),
            b.data_type()
        );
    }
    match a.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Ok(utf8_value(a, ra)? == utf8_value(b, rb)?)
        }
        DataType::Int64 => {
            let aa = a.as_any().downcast_ref::<Int64Array>().unwrap();
            let bb = b.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(aa.value(ra) == bb.value(rb))
        }
        DataType::Int32 => {
            let aa = a.as_any().downcast_ref::<Int32Array>().unwrap();
            let bb = b.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(aa.value(ra) == bb.value(rb))
        }
        DataType::Date32 => {
            let aa = a.as_any().downcast_ref::<Date32Array>().unwrap();
            let bb = b.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(aa.value(ra) == bb.value(rb))
        }
        DataType::Timestamp(unit, _) => {
            let va = match unit {
                arrow::datatypes::TimeUnit::Second => a
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .value(ra),
                arrow::datatypes::TimeUnit::Millisecond => a
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(ra),
                arrow::datatypes::TimeUnit::Microsecond => a
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(ra),
                arrow::datatypes::TimeUnit::Nanosecond => a
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(ra),
            };
            let vb = match unit {
                arrow::datatypes::TimeUnit::Second => b
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .value(rb),
                arrow::datatypes::TimeUnit::Millisecond => b
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(rb),
                arrow::datatypes::TimeUnit::Microsecond => b
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(rb),
                arrow::datatypes::TimeUnit::Nanosecond => b
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(rb),
            };
            Ok(va == vb)
        }
        DataType::Int16 => {
            let aa = a.as_any().downcast_ref::<Int16Array>().unwrap();
            let bb = b.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(aa.value(ra) == bb.value(rb))
        }
        DataType::Int8 => {
            let aa = a.as_any().downcast_ref::<Int8Array>().unwrap();
            let bb = b.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(aa.value(ra) == bb.value(rb))
        }
        DataType::UInt64 => {
            let aa = a.as_any().downcast_ref::<UInt64Array>().unwrap();
            let bb = b.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(aa.value(ra) == bb.value(rb))
        }
        DataType::UInt32 => {
            let aa = a.as_any().downcast_ref::<UInt32Array>().unwrap();
            let bb = b.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(aa.value(ra) == bb.value(rb))
        }
        DataType::Float64 => {
            let aa = a.as_any().downcast_ref::<Float64Array>().unwrap();
            let bb = b.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(aa.value(ra).to_bits() == bb.value(rb).to_bits())
        }
        DataType::Float32 => {
            let aa = a.as_any().downcast_ref::<Float32Array>().unwrap();
            let bb = b.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(aa.value(ra).to_bits() == bb.value(rb).to_bits())
        }
        DataType::Boolean => {
            let aa = a.as_any().downcast_ref::<BooleanArray>().unwrap();
            let bb = b.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(aa.value(ra) == bb.value(rb))
        }
        other => bail!(
            "E_RBT_UPSERT_SCHEMA: compare type {other:?} not supported"
        ),
    }
}

fn columns_equal(
    left: &RecordBatch,
    lr: usize,
    right: &RecordBatch,
    rr: usize,
    col_names: &[String],
) -> Result<bool> {
    for name in col_names {
        let li = left.schema().index_of(name).map_err(|_| {
            anyhow::anyhow!("E_RBT_UPSERT_SCHEMA: left missing compare col '{name}'")
        })?;
        let ri = right.schema().index_of(name).map_err(|_| {
            anyhow::anyhow!("E_RBT_UPSERT_SCHEMA: right missing compare col '{name}'")
        })?;
        if !scalar_eq(left.column(li).as_ref(), lr, right.column(ri).as_ref(), rr)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Merge schemas: existing fields first, then any new fields from incoming.
fn merge_schemas(existing: &Schema, incoming: &Schema) -> SchemaRef {
    let mut fields: Vec<Field> = existing.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut seen: HashSet<String> = fields.iter().map(|f| f.name().to_string()).collect();
    for f in incoming.fields() {
        if seen.insert(f.name().clone()) {
            fields.push(f.as_ref().clone());
        }
    }
    Arc::new(Schema::new(fields))
}

/// Extract one row as a map of column name → scalar array (len 1).
fn extract_row_map(batch: &RecordBatch, row: usize) -> Result<BTreeMap<String, ArrayRef>> {
    let mut m = BTreeMap::new();
    for (i, f) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(i);
        let sliced = col.slice(row, 1);
        m.insert(f.name().clone(), sliced);
    }
    Ok(m)
}

/// Build a row map for touch-only: take `base` row, overlay touch cols from `incoming`.
fn touch_row_map(
    base: &RecordBatch,
    base_row: usize,
    incoming: &RecordBatch,
    in_row: usize,
    touch: &[String],
) -> Result<BTreeMap<String, ArrayRef>> {
    let mut m = extract_row_map(base, base_row)?;
    for t in touch {
        let idx = incoming.schema().index_of(t).map_err(|_| {
            anyhow::anyhow!("E_RBT_UPSERT_SCHEMA: touch col '{t}' missing on incoming")
        })?;
        m.insert(t.clone(), incoming.column(idx).slice(in_row, 1));
    }
    Ok(m)
}

/// Build a row map for full non-key replace: keys from existing (or incoming),
/// all other columns from incoming; keep existing-only columns from base.
fn update_row_map(
    base: &RecordBatch,
    base_row: usize,
    incoming: &RecordBatch,
    in_row: usize,
    unique_key: &[String],
) -> Result<BTreeMap<String, ArrayRef>> {
    let key_set: HashSet<&str> = unique_key.iter().map(String::as_str).collect();
    let mut m = extract_row_map(base, base_row)?;
    for (i, f) in incoming.schema().fields().iter().enumerate() {
        let name = f.name();
        if key_set.contains(name.as_str()) {
            continue; // keys immutable
        }
        m.insert(name.clone(), incoming.column(i).slice(in_row, 1));
    }
    Ok(m)
}

/// Assemble ordered maps into one RecordBatch with `out_schema`.
fn maps_to_batch(
    out_schema: SchemaRef,
    rows: &[BTreeMap<String, ArrayRef>],
) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(out_schema));
    }
    let n = rows.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(out_schema.fields().len());
    for f in out_schema.fields() {
        let name = f.name();
        let mut pieces: Vec<ArrayRef> = Vec::with_capacity(n);
        for row in rows {
            if let Some(a) = row.get(name) {
                pieces.push(a.clone());
            } else {
                pieces.push(arrow::array::new_null_array(f.data_type(), 1));
            }
        }
        let refs: Vec<&dyn Array> = pieces.iter().map(|a| a.as_ref()).collect();
        let col = arrow::compute::concat(&refs)
            .map_err(|e| anyhow::anyhow!("E_RBT_UPSERT_SCHEMA: concat col {name}: {e}"))?;
        columns.push(col);
    }
    RecordBatch::try_new(out_schema, columns)
        .context("E_RBT_UPSERT_SCHEMA: assemble upsert batch")
}

/// Pure in-memory Type-1 upsert (RBT-A7.3).
///
/// `existing` may be empty. `incoming` is the SQL result for this run.
/// Output order: existing key order (stable), then newly inserted keys in
/// incoming encounter order.
pub fn upsert_batches(
    existing: &RecordBatch,
    incoming: &RecordBatch,
    cfg: &UpsertConfig,
) -> Result<UpsertResult> {
    cfg.validate_schema(incoming.schema().as_ref())?;
    // Existing may be empty schema; if non-empty, keys must exist
    if existing.num_rows() > 0 {
        for k in &cfg.unique_key {
            if existing.schema().index_of(k).is_err() {
                bail!(
                    "E_RBT_UPSERT_KEY: unique_key column '{k}' missing from existing table schema"
                );
            }
        }
    }

    let compare = cfg.resolve_compare_columns(incoming.schema().as_ref())?;
    let out_schema = if existing.num_columns() == 0 {
        incoming.schema()
    } else {
        merge_schemas(existing.schema().as_ref(), incoming.schema().as_ref())
    };

    let max = upsert_max_rows();
    let total_in = existing.num_rows().saturating_add(incoming.num_rows());
    if total_in > max {
        bail!(
            "E_RBT_UPSERT_TOO_LARGE: existing {} + incoming {} rows exceeds max {max} \
             (set RBT_UPSERT_MAX_ROWS to raise; v1 is in-memory collect)",
            existing.num_rows(),
            incoming.num_rows()
        );
    }

    let in_key_idxs: Vec<usize> = cfg
        .unique_key
        .iter()
        .map(|k| incoming.schema().index_of(k).unwrap())
        .collect();

    // Store: key → row map. Preserve insertion order of existing keys.
    let mut order: Vec<Vec<u8>> = Vec::new();
    let mut store: HashMap<Vec<u8>, BTreeMap<String, ArrayRef>> = HashMap::new();

    if existing.num_rows() > 0 {
        let ex_key_idxs: Vec<usize> = cfg
            .unique_key
            .iter()
            .map(|k| existing.schema().index_of(k).unwrap())
            .collect();
        for r in 0..existing.num_rows() {
            let key = encode_key(existing, r, &ex_key_idxs)?;
            if store.contains_key(&key) {
                // Last existing wins if duplicate keys in store (shouldn't happen)
                store.insert(key.clone(), extract_row_map(existing, r)?);
            } else {
                order.push(key.clone());
                store.insert(key, extract_row_map(existing, r)?);
            }
        }
    }

    let mut stats = UpsertStats::default();
    let mut seen_incoming: HashSet<Vec<u8>> = HashSet::new();

    for r in 0..incoming.num_rows() {
        let key = encode_key(incoming, r, &in_key_idxs)?;
        // Last incoming row wins for duplicate keys within the batch
        let is_first_in_batch = seen_incoming.insert(key.clone());

        match store.get(&key) {
            None => {
                store.insert(key.clone(), extract_row_map(incoming, r)?);
                order.push(key);
                if is_first_in_batch {
                    stats.rows_inserted += 1;
                }
            }
            Some(_old_map) => {
                // Need base batch row for compare — re-read from existing/incoming store
                // Compare using full batches is easier: find row in existing
                // For simplicity, rebuild a 1-row batch from old map for compare.
                // Actually compare using existing batch if key was from existing.
                // Simpler path: materialize old row as batch once.
                let old_batch = maps_to_batch(out_schema.clone(), &[store.get(&key).unwrap().clone()])?;
                let attrs_equal =
                    columns_equal(&old_batch, 0, incoming, r, &compare)?;
                if attrs_equal {
                    let new_map =
                        touch_row_map(&old_batch, 0, incoming, r, &cfg.touch_columns)?;
                    store.insert(key, new_map);
                    if is_first_in_batch {
                        stats.rows_touched += 1;
                    }
                } else {
                    let new_map =
                        update_row_map(&old_batch, 0, incoming, r, &cfg.unique_key)?;
                    store.insert(key, new_map);
                    if is_first_in_batch {
                        stats.rows_updated += 1;
                    }
                }
            }
        }
    }

    stats.rows_kept = order
        .iter()
        .filter(|k| !seen_incoming.contains(*k))
        .count();

    let rows: Vec<BTreeMap<String, ArrayRef>> = order
        .iter()
        .filter_map(|k| store.get(k).cloned())
        .collect();
    let batch = maps_to_batch(out_schema, &rows)?;
    stats.total_rows = batch.num_rows();
    Ok(UpsertResult { batch, stats })
}

/// Load existing parquet (if any), upsert with SQL batches, atomic write.
pub fn materialize_keyed_upsert(
    dest_parquet: &Path,
    incoming: &[RecordBatch],
    cfg: &UpsertConfig,
    opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
) -> Result<(StreamWriteStats, UpsertStats)> {
    if incoming.is_empty() {
        // No SQL batches — still need schema; fail closed unless empty existing
        if !dest_parquet.exists() {
            bail!(
                "E_RBT_UPSERT_SCHEMA: keyed_upsert produced zero batches and no existing table \
                 at {}",
                dest_parquet.display()
            );
        }
        // Keep existing as-is (no-op run)
        let existing = load_parquet_batches(dest_parquet)?;
        let rows: usize = existing.iter().map(|b| b.num_rows()).sum();
        let validation = if assertions.is_empty() {
            ValidationResult {
                total_rows: rows,
                passed_assertions: 0,
                failed_assertions: 0,
                errors: Vec::new(),
            }
        } else {
            RecordBatchValidator::validate_batches(&existing, assertions)
        };
        let kept = UpsertStats {
            rows_kept: rows,
            total_rows: rows,
            ..Default::default()
        };
        return Ok((
            StreamWriteStats {
                rows,
                batches: existing.len(),
                path: dest_parquet.to_path_buf(),
                bytes_written: dest_parquet
                    .metadata()
                    .map(|m| m.len())
                    .unwrap_or(0),
                validation,
            },
            kept,
        ));
    }

    let in_schema = incoming[0].schema();
    for (i, b) in incoming.iter().enumerate().skip(1) {
        if b.schema().as_ref() != in_schema.as_ref() {
            bail!("E_RBT_UPSERT_SCHEMA: incoming batch {i} schema mismatch");
        }
    }
    let incoming_batch = concat_or_empty(in_schema.clone(), incoming)?;

    let existing_batch = if dest_parquet.is_file() {
        let batches = load_parquet_batches(dest_parquet).with_context(|| {
            format!(
                "E_RBT_UPSERT_SCHEMA: read existing {}",
                dest_parquet.display()
            )
        })?;
        if batches.is_empty() {
            RecordBatch::new_empty(in_schema.clone())
        } else {
            let sch = batches[0].schema();
            concat_or_empty(sch, &batches)?
        }
    } else {
        RecordBatch::new_empty(in_schema.clone())
    };

    let result = upsert_batches(&existing_batch, &incoming_batch, cfg)?;
    let out_batches = vec![result.batch];

    let validation = if assertions.is_empty() {
        ValidationResult {
            total_rows: result.stats.total_rows,
            passed_assertions: 0,
            failed_assertions: 0,
            errors: Vec::new(),
        }
    } else {
        RecordBatchValidator::validate_batches(&out_batches, assertions)
    };

    if validation.failed_assertions > 0 && opts.fail_fast_assertions {
        bail!(
            "E_RBT_MATERIALIZE_ASSERT: keyed_upsert assertions failed: {}",
            validation.errors.join("; ")
        );
    }

    let rows = write_parquet_batches_atomic(&out_batches, dest_parquet, opts)?;
    let bytes = dest_parquet.metadata().map(|m| m.len()).unwrap_or(0);
    Ok((
        StreamWriteStats {
            rows,
            batches: 1,
            path: dest_parquet.to_path_buf(),
            bytes_written: bytes,
            validation,
        },
        result.stats,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch_entities(rows: &[(&str, &str, &str)]) -> RecordBatch {
        // entity_id, attr, last_seen
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, true),
            Field::new("attr", DataType::Utf8, true),
            Field::new("last_seen", DataType::Utf8, true),
        ]));
        let e: Vec<&str> = rows.iter().map(|r| r.0).collect();
        let a: Vec<&str> = rows.iter().map(|r| r.1).collect();
        let t: Vec<&str> = rows.iter().map(|r| r.2).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(e)),
                Arc::new(StringArray::from(a)),
                Arc::new(StringArray::from(t)),
            ],
        )
        .unwrap()
    }

    fn cfg() -> UpsertConfig {
        UpsertConfig {
            unique_key: vec!["entity_id".into()],
            touch_columns: vec!["last_seen".into()],
            compare_columns: Some(vec!["attr".into()]),
        }
    }

    #[test]
    fn insert_new_keys() {
        let existing = RecordBatch::new_empty(batch_entities(&[]).schema());
        let incoming = batch_entities(&[("a", "x", "d1"), ("b", "y", "d1")]);
        let r = upsert_batches(&existing, &incoming, &cfg()).unwrap();
        assert_eq!(r.stats.rows_inserted, 2);
        assert_eq!(r.stats.total_rows, 2);
        assert_eq!(r.batch.num_rows(), 2);
    }

    #[test]
    fn touch_only_when_attrs_equal() {
        let existing = batch_entities(&[("a", "x", "d1"), ("b", "y", "d1")]);
        let incoming = batch_entities(&[("a", "x", "d2")]); // same attr, new touch
        let r = upsert_batches(&existing, &incoming, &cfg()).unwrap();
        assert_eq!(r.stats.rows_touched, 1);
        assert_eq!(r.stats.rows_updated, 0);
        assert_eq!(r.stats.rows_inserted, 0);
        assert_eq!(r.stats.rows_kept, 1);
        assert_eq!(r.stats.total_rows, 2);
        // Find entity a
        let ids = r
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let attrs = r
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let seen = r
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut found = false;
        for i in 0..r.batch.num_rows() {
            if ids.value(i) == "a" {
                assert_eq!(attrs.value(i), "x");
                assert_eq!(seen.value(i), "d2");
                found = true;
            }
            if ids.value(i) == "b" {
                assert_eq!(seen.value(i), "d1");
            }
        }
        assert!(found);
    }

    #[test]
    fn update_when_attrs_change() {
        let existing = batch_entities(&[("a", "x", "d1")]);
        let incoming = batch_entities(&[("a", "z", "d2")]);
        let r = upsert_batches(&existing, &incoming, &cfg()).unwrap();
        assert_eq!(r.stats.rows_updated, 1);
        assert_eq!(r.stats.rows_touched, 0);
        let attrs = r
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(attrs.value(0), "z");
    }

    #[test]
    fn multi_key_unique() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
            Field::new("seen", DataType::Utf8, true),
        ]));
        let existing = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])),
                Arc::new(StringArray::from(vec!["us", "eu"])),
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["d1", "d1"])),
            ],
        )
        .unwrap();
        let incoming = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["us"])),
                Arc::new(Int64Array::from(vec![1])), // same v → touch
                Arc::new(StringArray::from(vec!["d9"])),
            ],
        )
        .unwrap();
        let cfg = UpsertConfig {
            unique_key: vec!["entity".into(), "region".into()],
            touch_columns: vec!["seen".into()],
            compare_columns: Some(vec!["v".into()]),
        };
        let r = upsert_batches(&existing, &incoming, &cfg).unwrap();
        assert_eq!(r.stats.rows_touched, 1);
        assert_eq!(r.stats.total_rows, 2);
    }

    #[test]
    fn missing_unique_key_errors() {
        let fm = StagingFrontmatter {
            materialization: Some("keyed_upsert".into()),
            ..Default::default()
        };
        let err = UpsertConfig::from_frontmatter(&fm).unwrap_err().to_string();
        assert!(err.contains("E_RBT_UPSERT_KEY"));
    }

    #[test]
    fn null_safe_compare() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("attr", DataType::Utf8, true),
            Field::new("seen", DataType::Utf8, true),
        ]));
        let existing = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("a")])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![Some("d1")])),
            ],
        )
        .unwrap();
        let incoming = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a")])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![Some("d2")])),
            ],
        )
        .unwrap();
        let cfg = UpsertConfig {
            unique_key: vec!["id".into()],
            touch_columns: vec!["seen".into()],
            compare_columns: Some(vec!["attr".into()]),
        };
        let r = upsert_batches(&existing, &incoming, &cfg).unwrap();
        assert_eq!(r.stats.rows_touched, 1);
    }
}

//! `rbt::testing`: Zero-copy, streamable data quality assertion & validation engine for Arrow RecordBatches.

use anyhow::{anyhow, Result};
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;

/// Individual column assertion types for dbt-compatible model testing.
#[derive(Debug, Clone)]
pub enum Assertion {
    NotNull {
        column: String,
    },
    /// Single Utf8 column uniqueness (legacy; prefer UniqueKey for multi-type).
    Unique {
        column: String,
    },
    /// Composite uniqueness across one or more columns (global over all batches).
    UniqueKey {
        columns: Vec<String>,
    },
    AcceptedValues {
        column: String,
        values: Vec<String>,
    },
}

/// Validation result summary for a tested model.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub total_rows: usize,
    pub passed_assertions: usize,
    pub failed_assertions: usize,
    pub errors: Vec<String>,
}

/// Streaming data validator for Apache Arrow record batches.
pub struct RecordBatchValidator;

impl RecordBatchValidator {
    /// Validates that a specified column contains no NULL entries in the batch.
    pub fn assert_not_null(batch: &RecordBatch, column: &str) -> Result<()> {
        let schema = batch.schema();
        let column_idx = schema
            .index_of(column)
            .map_err(|_| anyhow!("Column '{}' not found in RecordBatch schema", column))?;

        let array = batch.column(column_idx);
        let null_count = array.null_count();
        if null_count > 0 {
            return Err(anyhow!(
                "Assertion failed: Column '{}' has {} null values",
                column,
                null_count
            ));
        }

        Ok(())
    }

    /// Validates that a string/Utf8 column contains strictly unique entries in the batch.
    pub fn assert_unique(batch: &RecordBatch, column: &str) -> Result<()> {
        let schema = batch.schema();
        let column_idx = schema
            .index_of(column)
            .map_err(|_| anyhow!("Column '{}' not found in RecordBatch schema", column))?;

        let array = batch.column(column_idx);
        let string_array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                anyhow!(
                    "Column '{}' must be Utf8 type for unique validation",
                    column
                )
            })?;

        let mut seen = HashSet::new();
        for i in 0..string_array.len() {
            if string_array.is_valid(i) {
                let val = string_array.value(i);
                if !seen.insert(val) {
                    return Err(anyhow!(
                        "Assertion failed: Duplicate value '{}' found in column '{}'",
                        val,
                        column
                    ));
                }
            }
        }

        Ok(())
    }

    /// Global composite uniqueness across all batches (stringified cell values).
    pub fn assert_unique_key(batches: &[RecordBatch], columns: &[String]) -> Result<()> {
        if columns.is_empty() {
            return Err(anyhow!("unique_key assertion requires at least one column"));
        }
        if batches.is_empty() {
            return Ok(());
        }
        for col in columns {
            if batches[0].schema().index_of(col).is_err() {
                return Err(anyhow!(
                    "Column '{}' not found in RecordBatch schema for unique_key",
                    col
                ));
            }
        }

        let mut seen: HashSet<String> = HashSet::new();
        for batch in batches {
            let idxs: Vec<usize> = columns
                .iter()
                .map(|c| batch.schema().index_of(c).unwrap())
                .collect();
            for row in 0..batch.num_rows() {
                let mut key = String::new();
                for (i, &col_idx) in idxs.iter().enumerate() {
                    if i > 0 {
                        key.push('\u{1f}');
                    }
                    key.push_str(&array_value_to_key(batch.column(col_idx), row));
                }
                if !seen.insert(key.clone()) {
                    return Err(anyhow!(
                        "Assertion failed: Duplicate composite key {:?} on columns {:?}",
                        key.split('\u{1f}').collect::<Vec<_>>(),
                        columns
                    ));
                }
            }
        }
        Ok(())
    }

    /// Validates that all non-null values in a string-like column belong to the `accepted_values` set.
    ///
    /// Accepts Utf8 / LargeUtf8 / Utf8View (Parquet re-read / spill often yields Utf8View).
    pub fn assert_accepted_values(
        batch: &RecordBatch,
        column: &str,
        accepted_values: &[&str],
    ) -> Result<()> {
        let schema = batch.schema();
        let column_idx = schema
            .index_of(column)
            .map_err(|_| anyhow!("Column '{}' not found in RecordBatch schema", column))?;

        let array = batch.column(column_idx);
        let valid_set: HashSet<&str> = accepted_values.iter().copied().collect();
        for i in 0..array.len() {
            if array.is_null(i) {
                continue;
            }
            let val = array_value_to_key(array, i);
            if !valid_set.contains(val.as_str()) {
                return Err(anyhow!(
                    "Assertion failed: Value '{}' in column '{}' is not in accepted list {:?}",
                    val,
                    column,
                    accepted_values
                ));
            }
        }

        Ok(())
    }

    /// Runs a suite of assertions over an incoming array of RecordBatches.
    ///
    /// `Unique` / per-batch not_null still walk batches; `UniqueKey` is global.
    pub fn validate_batches(batches: &[RecordBatch], assertions: &[Assertion]) -> ValidationResult {
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let mut passed = 0;
        let mut failed = 0;
        let mut errors = Vec::new();

        for assertion in assertions {
            let res = match assertion {
                Assertion::NotNull { column } => {
                    let mut ok = Ok(());
                    for batch in batches {
                        if let Err(e) = Self::assert_not_null(batch, column) {
                            ok = Err(e);
                            break;
                        }
                    }
                    ok
                }
                Assertion::Unique { column } => {
                    // Global unique for single column via UniqueKey path
                    Self::assert_unique_key(batches, std::slice::from_ref(column))
                }
                Assertion::UniqueKey { columns } => Self::assert_unique_key(batches, columns),
                Assertion::AcceptedValues { column, values } => {
                    let str_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
                    let mut ok = Ok(());
                    for batch in batches {
                        if let Err(e) = Self::assert_accepted_values(batch, column, &str_refs) {
                            ok = Err(e);
                            break;
                        }
                    }
                    ok
                }
            };

            match res {
                Ok(()) => passed += 1,
                Err(e) => {
                    errors.push(e.to_string());
                    failed += 1;
                }
            }
        }

        ValidationResult {
            total_rows,
            passed_assertions: passed,
            failed_assertions: failed,
            errors,
        }
    }
}

/// Streaming frontmatter assertion runner (per-batch + global unique state).
///
/// Drop each batch after `observe_batch` so peak RAM stays O(unique cardinality)
/// rather than O(full result).
#[derive(Debug, Default)]
pub struct StreamingAssertionRunner {
    total_rows: usize,
    /// (column) not-null checks applied every batch
    not_null: Vec<String>,
    /// (column, accepted values) checks every batch
    accepted: Vec<(String, Vec<String>)>,
    /// Global unique key trackers
    unique_trackers: Vec<UniqueKeyTracker>,
    errors: Vec<String>,
    fail_fast: bool,
}

impl StreamingAssertionRunner {
    pub fn new(assertions: &[Assertion], fail_fast: bool) -> Self {
        let mut not_null = Vec::new();
        let mut accepted = Vec::new();
        let mut unique_trackers = Vec::new();
        for a in assertions {
            match a {
                Assertion::NotNull { column } => not_null.push(column.clone()),
                Assertion::AcceptedValues { column, values } => {
                    accepted.push((column.clone(), values.clone()))
                }
                Assertion::Unique { column } => {
                    unique_trackers.push(UniqueKeyTracker::new(vec![column.clone()]))
                }
                Assertion::UniqueKey { columns } => {
                    unique_trackers.push(UniqueKeyTracker::new(columns.clone()))
                }
            }
        }
        Self {
            total_rows: 0,
            not_null,
            accepted,
            unique_trackers,
            errors: Vec::new(),
            fail_fast,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.not_null.is_empty() && self.accepted.is_empty() && self.unique_trackers.is_empty()
    }

    /// Observe one batch. On fail-fast, returns Err on first violation.
    pub fn observe_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.total_rows += batch.num_rows();
        for col in &self.not_null {
            if let Err(e) = RecordBatchValidator::assert_not_null(batch, col) {
                self.errors.push(e.to_string());
                if self.fail_fast {
                    return Err(anyhow!("{}", self.errors.last().unwrap()));
                }
            }
        }
        for (col, vals) in &self.accepted {
            let refs: Vec<&str> = vals.iter().map(|s| s.as_str()).collect();
            if let Err(e) = RecordBatchValidator::assert_accepted_values(batch, col, &refs) {
                self.errors.push(e.to_string());
                if self.fail_fast {
                    return Err(anyhow!("{}", self.errors.last().unwrap()));
                }
            }
        }
        for tracker in &mut self.unique_trackers {
            if let Err(e) = tracker.observe_batch(batch) {
                self.errors.push(e.to_string());
                if self.fail_fast {
                    return Err(anyhow!("{}", self.errors.last().unwrap()));
                }
            }
        }
        Ok(())
    }

    pub fn finish(self) -> ValidationResult {
        let assertion_count =
            self.not_null.len() + self.accepted.len() + self.unique_trackers.len();
        if self.errors.is_empty() {
            ValidationResult {
                total_rows: self.total_rows,
                passed_assertions: assertion_count,
                failed_assertions: 0,
                errors: Vec::new(),
            }
        } else {
            // Count distinct error messages as failed assertion signals (fail-fast often = 1).
            let failed = self.errors.len().min(assertion_count.max(1));
            ValidationResult {
                total_rows: self.total_rows,
                passed_assertions: assertion_count.saturating_sub(failed),
                failed_assertions: failed,
                errors: self.errors,
            }
        }
    }
}

/// Global composite uniqueness tracker for streaming materialize.
#[derive(Debug)]
pub struct UniqueKeyTracker {
    columns: Vec<String>,
    seen: HashSet<String>,
    column_idxs: Option<Vec<usize>>,
}

impl UniqueKeyTracker {
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
            seen: HashSet::new(),
            column_idxs: None,
        }
    }

    pub fn observe_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.columns.is_empty() {
            return Err(anyhow!("unique_key assertion requires at least one column"));
        }
        if self.column_idxs.is_none() {
            let mut idxs = Vec::with_capacity(self.columns.len());
            for col in &self.columns {
                let idx = batch.schema().index_of(col).map_err(|_| {
                    anyhow!("Column '{col}' not found in RecordBatch schema for unique_key")
                })?;
                idxs.push(idx);
            }
            self.column_idxs = Some(idxs);
        }
        let idxs = self.column_idxs.as_ref().unwrap();
        for row in 0..batch.num_rows() {
            let mut key = String::new();
            for (i, &col_idx) in idxs.iter().enumerate() {
                if i > 0 {
                    key.push('\u{1f}');
                }
                key.push_str(&array_value_to_key(batch.column(col_idx), row));
            }
            if !self.seen.insert(key.clone()) {
                return Err(anyhow!(
                    "Assertion failed: Duplicate composite key {:?} on columns {:?}",
                    key.split('\u{1f}').collect::<Vec<_>>(),
                    self.columns
                ));
            }
        }
        Ok(())
    }

    pub fn cardinality(&self) -> usize {
        self.seen.len()
    }
}

pub(crate) fn array_value_to_key(array: &ArrayRef, row: usize) -> String {
    if array.is_null(row) {
        return "\u{0}".to_string();
    }
    // Common primitives (fast paths)
    if let Some(s) = array.as_any().downcast_ref::<StringArray>() {
        return s.value(row).to_string();
    }
    if let Some(s) = array
        .as_any()
        .downcast_ref::<arrow::array::StringViewArray>()
    {
        return s.value(row).to_string();
    }
    if let Some(s) = array
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
    {
        return s.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        return a.value(row).to_string();
    }
    // Dictionary / timestamps / views / nested: Arrow display formatter
    // (Parquet re-read often yields Utf8View or dictionary-encoded strings.)
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    let opts = FormatOptions::default().with_display_error(true);
    if let Ok(fmt) = ArrayFormatter::try_new(array.as_ref(), &opts) {
        return fmt.value(row).to_string();
    }
    format!("row{row}")
}

/// Build assertions from frontmatter-style test declarations.
pub fn assertions_from_model_tests(
    not_null: Option<&[String]>,
    unique: Option<&[String]>,
    accepted_values: Option<&std::collections::HashMap<String, Vec<String>>>,
) -> Vec<Assertion> {
    let mut out = Vec::new();
    if let Some(cols) = not_null {
        for c in cols {
            out.push(Assertion::NotNull { column: c.clone() });
        }
    }
    if let Some(cols) = unique {
        if !cols.is_empty() {
            out.push(Assertion::UniqueKey {
                columns: cols.to_vec(),
            });
        }
    }
    if let Some(map) = accepted_values {
        for (col, vals) in map {
            out.push(Assertion::AcceptedValues {
                column: col.clone(),
                values: vals.clone(),
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn streaming_unique_tracker_cross_batch() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k", DataType::Utf8, false),
        ]));
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )?;
        let b2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![3, 2])),
                Arc::new(StringArray::from(vec!["c", "b"])),
            ],
        )?;
        let mut runner = StreamingAssertionRunner::new(
            &[Assertion::UniqueKey {
                columns: vec!["id".into()],
            }],
            true,
        );
        runner.observe_batch(&b1)?;
        let err = runner.observe_batch(&b2).unwrap_err().to_string();
        assert!(err.contains("Duplicate"), "got {err}");
        Ok(())
    }

    #[test]
    fn test_record_batch_assertions() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["active", "pending", "active"])),
            ],
        )?;

        RecordBatchValidator::assert_not_null(&batch, "id")?;
        RecordBatchValidator::assert_accepted_values(
            &batch,
            "status",
            &["active", "pending", "completed"],
        )?;

        let assertions = vec![
            Assertion::NotNull {
                column: "id".to_string(),
            },
            Assertion::AcceptedValues {
                column: "status".to_string(),
                values: vec!["active".to_string(), "pending".to_string()],
            },
            Assertion::UniqueKey {
                columns: vec!["id".to_string()],
            },
        ];

        let result = RecordBatchValidator::validate_batches(&[batch], &assertions);
        assert_eq!(result.passed_assertions, 3);
        assert_eq!(result.failed_assertions, 0);

        Ok(())
    }

    #[test]
    fn test_composite_unique_global() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["A", "B"])),
                Arc::new(Int64Array::from(vec![1, 1])),
            ],
        )?;
        let b2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )?;
        let res = RecordBatchValidator::validate_batches(
            &[b1, b2],
            &[Assertion::UniqueKey {
                columns: vec!["symbol".into(), "ts".into()],
            }],
        );
        assert_eq!(res.failed_assertions, 1);
        Ok(())
    }
}

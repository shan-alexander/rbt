//! Lineage stamp columns for gold/silver materialize (P6 / G8).
//!
//! When frontmatter `lineage_stamp: true`, each output batch gains constant Utf8 columns:
//! * `_rbt_run_id` — run id for this invocation
//! * `_rbt_contract_version` — project/run contract version
//! * `_rbt_model` — model name
//! * `_rbt_bronze_fingerprint` — optional bronze fingerprint for the run (when known)

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Values stamped onto every row of a model result.
#[derive(Debug, Clone, Default)]
pub struct LineageStamp {
    pub run_id: String,
    pub contract_version: String,
    pub model: String,
    pub bronze_fingerprint: Option<String>,
}

impl LineageStamp {
    pub fn column_names(&self) -> Vec<&'static str> {
        let mut v = vec!["_rbt_run_id", "_rbt_contract_version", "_rbt_model"];
        if self.bronze_fingerprint.is_some() {
            v.push("_rbt_bronze_fingerprint");
        }
        v
    }
}

/// Append lineage columns to a batch (idempotent if columns already present).
pub fn stamp_batch(batch: &RecordBatch, stamp: &LineageStamp) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let schema = batch.schema();
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    let mut push_col = |name: &str, value: &str| -> Result<()> {
        if schema.index_of(name).is_ok() {
            return Ok(());
        }
        fields.push(Field::new(name, DataType::Utf8, true));
        let arr: ArrayRef = Arc::new(StringArray::from(vec![value.to_string(); n]));
        columns.push(arr);
        Ok(())
    };

    push_col("_rbt_run_id", &stamp.run_id)?;
    push_col("_rbt_contract_version", &stamp.contract_version)?;
    push_col("_rbt_model", &stamp.model)?;
    if let Some(ref fp) = stamp.bronze_fingerprint {
        push_col("_rbt_bronze_fingerprint", fp)?;
    }

    let new_schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(new_schema, columns).with_context(|| {
        format!(
            "E_RBT_LINEAGE: stamp batch for model '{}' (rows={n})",
            stamp.model
        )
    })
}

/// Stamp schema only (for empty-frame / writer init when first batch is empty).
pub fn stamped_schema(
    base: &arrow::datatypes::Schema,
    stamp: &LineageStamp,
) -> Arc<Schema> {
    let mut fields: Vec<Field> = base.fields().iter().map(|f| f.as_ref().clone()).collect();
    let names: std::collections::HashSet<&str> =
        base.fields().iter().map(|f| f.name().as_str()).collect();
    for name in stamp.column_names() {
        if !names.contains(name) {
            fields.push(Field::new(name, DataType::Utf8, true));
        }
    }
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn stamp_adds_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let stamped = stamp_batch(
            &batch,
            &LineageStamp {
                run_id: "r1".into(),
                contract_version: "v1".into(),
                model: "fact_x".into(),
                bronze_fingerprint: Some("fnv1a64:ab".into()),
            },
        )
        .unwrap();
        assert_eq!(stamped.num_columns(), 5);
        assert!(stamped.schema().index_of("_rbt_run_id").is_ok());
        assert!(stamped.schema().index_of("_rbt_bronze_fingerprint").is_ok());
        // idempotent
        let again = stamp_batch(
            &stamped,
            &LineageStamp {
                run_id: "r1".into(),
                contract_version: "v1".into(),
                model: "fact_x".into(),
                bronze_fingerprint: Some("fnv1a64:ab".into()),
            },
        )
        .unwrap();
        assert_eq!(again.num_columns(), 5);
    }
}

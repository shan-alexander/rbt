//! Design B — first-class Rust model nodes (ADR-003).
//!
//! Hosts implement [`RustModel`], register on the engine, and place nodes in the DAG via
//! [`crate::ModelSpec::rust`]. Execution produces Arrow batches that use the same materializer
//! / `ref()` path as SQL models.
//!
//! # Fail-closed codes
//!
//! - `E_RBT_RUST_MODEL` — unknown registry key / missing implementation  
//! - `E_RBT_RUST_SCHEMA` — output batches disagree with declared schema  
//! - `E_RBT_RUST_MAT` — unsupported materialization for Rust v1  

use anyhow::{bail, Context, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::core::run_scope::RunScope;

/// Host-owned whole-node transform (Design B).
///
/// Register with [`crate::TransformationEngine::register_rust_model`] or
/// [`crate::RbtEngineBuilder::with_rust_model`]. The registry key is [`RustModel::name`]
/// and must match the DAG model name.
///
/// `execute` is async so hosts can `session.sql(...).await` without nesting runtimes.
#[async_trait]
pub trait RustModel: Send + Sync {
    /// DAG / registry identity (must equal the `ModelNode.name`).
    fn name(&self) -> &str;

    /// Declared output schema (required — used for zero-row writes and validation).
    fn output_schema(&self) -> SchemaRef;

    /// Run the transform. Upstream `ref` / bronze tables are already on [`RustModelContext::session`].
    async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput>;
}

/// Per-run context passed into [`RustModel::execute`].
pub struct RustModelContext<'a> {
    pub session: &'a SessionContext,
    pub project_dir: &'a Path,
    pub scope: &'a RunScope,
    pub model_name: &'a str,
    pub run_id: &'a str,
    pub contract_version: &'a str,
    pub bronze_fingerprint: Option<&'a str>,
}

/// Output of a Rust model.
pub enum RustModelOutput {
    /// Zero or more record batches (same schema). Empty vec → zero-row table from declared schema.
    Batches(Vec<RecordBatch>),
    /// Streaming path (B5) — preferred for large outputs; schema must match [`RustModel::output_schema`].
    ///
    /// Consumed once by the materializer (table / parts strategies). Keyed upsert still
    /// requires collect (memory-bound) in v1.
    Stream(datafusion::physical_plan::SendableRecordBatchStream),
}

impl std::fmt::Debug for RustModelOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Batches(b) => f.debug_tuple("Batches").field(&b.len()).finish(),
            Self::Stream(_) => f.write_str("Stream(..)"),
        }
    }
}

/// Build a [`SendableRecordBatchStream`] from owned batches (host + engine helper).
pub fn batches_to_stream(
    schema: arrow::datatypes::SchemaRef,
    batches: Vec<RecordBatch>,
) -> datafusion::physical_plan::SendableRecordBatchStream {
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    let stream = futures::stream::iter(
        batches
            .into_iter()
            .map(|b| Ok(b) as datafusion::common::Result<RecordBatch>),
    );
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

/// Process-local map of host Rust models (name → impl).
#[derive(Default)]
pub struct RustModelRegistry {
    models: HashMap<String, Arc<dyn RustModel>>,
}

impl RustModelRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, model: Arc<dyn RustModel>) -> Result<()> {
        let key = model.name().trim().to_string();
        if key.is_empty() {
            bail!("E_RBT_RUST_MODEL: RustModel::name must be non-empty");
        }
        if self.models.contains_key(&key) {
            bail!(
                "E_RBT_RUST_MODEL: rust model '{key}' already registered \
                 (one implementation per name)"
            );
        }
        self.models.insert(key, model);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<Arc<dyn RustModel>> {
        self.models.get(name).cloned().ok_or_else(|| {
            anyhow::anyhow!(
                "E_RBT_RUST_MODEL: no Rust model registered for '{name}'. \
                 Call register_rust_model / RbtEngineBuilder::with_rust_model. \
                 Known: [{}]",
                self.models.keys().cloned().collect::<Vec<_>>().join(", ")
            )
        })
    }

    pub fn contains(&self, name: &str) -> bool {
        self.models.contains_key(name)
    }

    pub fn clear(&mut self) {
        self.models.clear();
    }

    pub fn len(&self) -> usize {
        self.models.len()
    }

    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }
}

/// Validate batches against declared schema (names + data types; nullability is soft).
pub fn validate_batches_schema(batches: &[RecordBatch], expected: &Schema) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    for (bi, batch) in batches.iter().enumerate() {
        let got = batch.schema();
        if got.fields().len() != expected.fields().len() {
            bail!(
                "E_RBT_RUST_SCHEMA: batch {bi} has {} fields, declared schema has {}",
                got.fields().len(),
                expected.fields().len()
            );
        }
        for (i, exp) in expected.fields().iter().enumerate() {
            let g = got.field(i);
            if g.name() != exp.name() {
                bail!(
                    "E_RBT_RUST_SCHEMA: batch {bi} field {i}: name '{}' != declared '{}'",
                    g.name(),
                    exp.name()
                );
            }
            if g.data_type() != exp.data_type() {
                bail!(
                    "E_RBT_RUST_SCHEMA: batch {bi} field '{}': type {:?} != declared {:?}",
                    exp.name(),
                    g.data_type(),
                    exp.data_type()
                );
            }
        }
        // subsequent batches must match first
        if bi > 0 && got.as_ref() != batches[0].schema().as_ref() {
            bail!("E_RBT_RUST_SCHEMA: batch {bi} schema differs from batch 0");
        }
    }
    Ok(())
}

/// Zero-row batch matching `schema` (all columns null / empty arrays).
pub fn empty_batch_for_schema(schema: SchemaRef) -> Result<RecordBatch> {
    let arrays: Result<Vec<ArrayRef>> = schema
        .fields()
        .iter()
        .map(|f| empty_array_for_type(f.data_type()))
        .collect();
    let arrays = arrays.context("E_RBT_RUST_SCHEMA: empty array for schema")?;
    RecordBatch::try_new(schema, arrays).context("E_RBT_RUST_SCHEMA: empty RecordBatch")
}

fn empty_array_for_type(dt: &DataType) -> Result<ArrayRef> {
    use arrow::array::*;
    Ok(match dt {
        DataType::Utf8 => Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(Vec::<Option<&str>>::new())),
        DataType::Int64 => Arc::new(Int64Array::from(Vec::<i64>::new())),
        DataType::Int32 => Arc::new(Int32Array::from(Vec::<i32>::new())),
        DataType::UInt64 => Arc::new(UInt64Array::from(Vec::<u64>::new())),
        DataType::UInt32 => Arc::new(UInt32Array::from(Vec::<u32>::new())),
        DataType::Float64 => Arc::new(Float64Array::from(Vec::<f64>::new())),
        DataType::Float32 => Arc::new(Float32Array::from(Vec::<f32>::new())),
        DataType::Boolean => Arc::new(BooleanArray::from(Vec::<bool>::new())),
        DataType::Binary => Arc::new(BinaryArray::from(Vec::<Option<&[u8]>>::new())),
        other => bail!(
            "E_RBT_RUST_SCHEMA: empty batch unsupported for type {other:?} in Design B v1 \
             (use a non-empty batch or extend empty_array_for_type)"
        ),
    })
}

/// Ensure `Field` list is usable as SchemaRef helper for hosts.
pub fn schema_from_fields(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::Field;

    struct DoubleId;

    #[async_trait]
    impl RustModel for DoubleId {
        fn name(&self) -> &str {
            "tf_double"
        }
        fn output_schema(&self) -> SchemaRef {
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
        }
        async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput> {
            let _ = ctx;
            let schema = self.output_schema();
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
            )?;
            Ok(RustModelOutput::Batches(vec![batch]))
        }
    }

    #[test]
    fn registry_roundtrip() -> Result<()> {
        let mut reg = RustModelRegistry::new();
        reg.register(Arc::new(DoubleId))?;
        assert!(reg.contains("tf_double"));
        let m = reg.get("tf_double")?;
        assert_eq!(m.name(), "tf_double");
        let err = match reg.get("missing") {
            Ok(_) => panic!("expected missing model error"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("E_RBT_RUST_MODEL"));
        Ok(())
    }

    #[test]
    fn schema_validate_ok_and_fail() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1i64]))])?;
        validate_batches_schema(&[batch], schema.as_ref())?;
        let bad = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )?;
        let err = validate_batches_schema(&[bad], schema.as_ref())
            .unwrap_err()
            .to_string();
        assert!(err.contains("E_RBT_RUST_SCHEMA"));
        Ok(())
    }
}

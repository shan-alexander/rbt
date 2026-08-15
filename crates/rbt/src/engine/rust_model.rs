//! # Design B — first-class Rust model nodes (ADR-003)
//!
//! Hosts implement [`RustModel`], register on the engine, and place nodes in the DAG via
//! [`crate::ModelSpec::rust`]. Execution produces Arrow batches (or a stream) that use the
//! **same materializer** and `ref()` path as SQL models.
//!
//! ## Minimal host flow
//!
//! 1. `#[async_trait] impl RustModel for MyNode { … }`
//! 2. `RbtEngineBuilder::new().with_rust_model(MyNode).build().await?`
//! 3. `ModelSpec::rust("my_node").refs(["stg_upstream"]).output_path(…)`
//! 4. Downstream SQL: `SELECT … FROM {{ ref('my_node') }}`
//!
//! ## Materializations
//!
//! `table`, `keyed_upsert`, `scoped_replace`, `incremental_append`, and table+parts
//! (`consolidate: never`). Prefer [`RustModelOutput::Stream`] for large outputs (except
//! keyed_upsert, which still collects). **Do not** use `materialization: alias` on Rust
//! nodes (identity alias is a SQL/file path).
//!
//! ## Large outputs (memory honesty) — default recommendation
//!
//! **Prefer [`RustModelOutput::Stream`]** (or [`batches_to_stream`]) so the materializer can
//! encode Parquet without retaining the full result set. Use [`RustModelOutput::Batches`]
//! only for small dimensions / unit tests.
//!
//! Avoid `df.collect()` over multi‑GB upstreams. Prefer:
//! - `df.execute_stream().await?` → [`RustModelOutput::Stream`]
//! - or **Phase 2** [`RustModel::execute_partition`] so the engine never builds a mega batch
//!
//! ## Phase 2 — partition API (RBT-C B6)
//!
//! For partition-local kernels (per-symbol TA, per-entity features), implement:
//!
//! 1. [`RustModel::parallel_contract`] → [`ParallelContract::PartitionLocal`]
//! 2. [`RustModel::execute_partition`] — process **one** partition’s batches
//! 3. Model frontmatter: `materialization: scoped_replace`, `partition_by` / `part_key`
//! 4. Project: `execution.concurrency` enabled with strategy `partition` or `auto`
//!
//! The engine then:
//! - fans multi-value scope into WorkUnits
//! - opens **only that part’s** upstream data ([`PartitionInput`])
//! - writes one gold/silver part via existing `scoped_replace`
//!
//! Default [`execute_partition`] returns `E_RBT_RUST_PARTITION` “not implemented”;
//! the engine **falls back** to [`execute`] with a narrowed scope (compat).
//!
//! ### Host example (partition-local)
//!
//! ```rust,no_run
//! use rbt::{
//!     async_trait, ParallelContract, PartitionInput, PartitionKey, RustModel,
//!     RustModelContext, RustModelOutput,
//! };
//! use rbt::arrow::datatypes::{DataType, Field, Schema};
//! use std::sync::Arc;
//!
//! struct TfIndicators;
//! #[async_trait]
//! impl RustModel for TfIndicators {
//!     fn name(&self) -> &str { "tf_indicators_1m" }
//!     fn output_schema(&self) -> arrow::datatypes::SchemaRef {
//!         Arc::new(Schema::new(vec![
//!             Field::new("symbol", DataType::Utf8, false),
//!             Field::new("close", DataType::Float64, true),
//!         ]))
//!     }
//!     fn parallel_contract(&self) -> ParallelContract {
//!         ParallelContract::PartitionLocal { keys: vec!["symbol".into()] }
//!     }
//!     async fn execute(&self, ctx: &RustModelContext<'_>) -> anyhow::Result<RustModelOutput> {
//!         // Mega / fallback path (small scopes or concurrency off)
//!         let df = ctx.session.sql(r#"SELECT * FROM "stg_bars_1m""#).await?;
//!         Ok(RustModelOutput::Stream(df.execute_stream().await?))
//!     }
//!     async fn execute_partition(
//!         &self,
//!         _ctx: &RustModelContext<'_>,
//!         part: &PartitionKey,
//!         input: PartitionInput,
//!     ) -> anyhow::Result<RustModelOutput> {
//!         let symbol = part.get("symbol").unwrap_or("?");
//!         let _ = symbol;
//!         // Prefer engine-built stream for this partition only:
//!         if let Some(stream) = input.into_stream() {
//!             // map batches → indicators; return Stream or Batches
//!             return Ok(RustModelOutput::Stream(stream));
//!         }
//!         anyhow::bail!("E_RBT_RUST_PARTITION: empty input for {part:?}");
//!     }
//! }
//! ```
//!
//! ## Fail-closed codes
//!
//! | Code | When |
//! |------|------|
//! | `E_RBT_RUST_MODEL` | Unknown registry key / missing implementation |
//! | `E_RBT_RUST_SCHEMA` | Output batches disagree with declared schema |
//! | `E_RBT_RUST_MAT` | Unsupported materialization / format |
//! | `E_RBT_RUST_PARTITION` | Partition API misuse / not implemented (fallback) |
//! | `E_RBT_ALIAS` | Alias materialization not valid for Design B nodes |
//!
//! ## Related
//!
//! - Planner IR: [`crate::ParallelContract`], [`crate::plan_execution`]
//! - Config: `execution.concurrency` in `rbt_project.yml`
//! - Plan: `docs/plans/design-b-rust-models.md`, RBT-C Phase 2

use anyhow::{bail, Result};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::core::run_scope::RunScope;
use crate::core::work_unit::ParallelContract;

// Re-export for host ergonomics (single import path with RustModel).
pub use crate::core::work_unit::ParallelContract as RustParallelContract;

/// Host-owned whole-node transform (Design B / ADR-003).
///
/// Register with [`crate::TransformationEngine::register_rust_model`] or
/// [`crate::RbtEngineBuilder::with_rust_model`]. The registry key is [`RustModel::name`]
/// and **must** match the DAG model name from [`crate::ModelSpec::rust`].
///
/// `execute` is async so hosts can call `ctx.session.sql(...).await` without nesting runtimes.
/// Use the crate-level [`crate::async_trait`] re-export on implementations.
///
/// # Implementors checklist
///
/// 1. **Always** implement [`name`](Self::name), [`output_schema`](Self::output_schema),
///    [`execute`](Self::execute).
/// 2. For partition-local lakes, also implement [`parallel_contract`](Self::parallel_contract)
///    and [`execute_partition`](Self::execute_partition).
/// 3. Prefer returning [`RustModelOutput::Stream`] from both paths for large data.
/// 4. Do not hold full upstream tables in RAM when a stream or partition input is available.
///
/// # Example (whole-table / mega path)
///
/// ```rust,no_run
/// use rbt::{async_trait, RustModel, RustModelContext, RustModelOutput};
/// use rbt::arrow::datatypes::{DataType, Field, Schema};
/// use std::sync::Arc;
///
/// struct Double;
/// #[async_trait]
/// impl RustModel for Double {
///     fn name(&self) -> &str { "tf_double" }
///     fn output_schema(&self) -> arrow::datatypes::SchemaRef {
///         Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
///     }
///     async fn execute(&self, ctx: &RustModelContext<'_>) -> anyhow::Result<RustModelOutput> {
///         let df = ctx.session.sql(r#"SELECT id * 2 AS id FROM "stg_x""#).await?;
///         // Prefer Stream for large tables:
///         Ok(RustModelOutput::Stream(df.execute_stream().await?))
///     }
/// }
/// ```
#[async_trait]
pub trait RustModel: Send + Sync {
    /// DAG / registry identity (must equal the [`crate::ModelNode`] name).
    fn name(&self) -> &str;

    /// Declared output schema (required for zero-row writes and validation).
    fn output_schema(&self) -> SchemaRef;

    /// Whole-table / mega-plan transform.
    ///
    /// Upstream `ref` / bronze tables are already registered on
    /// [`RustModelContext::session`]. Used when:
    /// - concurrency fan-out is off, or
    /// - the model is not partition-local, or
    /// - [`execute_partition`] is not overridden (engine falls back here).
    async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput>;

    /// Declares parallel safety for the WorkUnit planner and engine (RBT-C Phase 2).
    ///
    /// Default: [`ParallelContract::Unknown`] (mega plan; never forces fan-out from trait alone).
    ///
    /// Return [`ParallelContract::PartitionLocal`] when your kernel is correct on a single
    /// partition value (e.g. one symbol’s bars). Pair with `materialization: scoped_replace`
    /// and `partition_by` / `part_key` on the model node so the planner can expand multi-value
    /// scopes into WorkUnits.
    fn parallel_contract(&self) -> ParallelContract {
        ParallelContract::Unknown
    }

    /// Optional partition-local entry point (RBT-C Phase 2 / Design B B6).
    ///
    /// The engine calls this **once per partition WorkUnit** when:
    /// - the unit is a partition slice (`is_partition_slice`), and
    /// - this method does **not** return the default “not implemented” error
    ///   (or returns success).
    ///
    /// Default implementation returns `E_RBT_RUST_PARTITION` with “not implemented”;
    /// the engine then falls back to [`execute`] with a scope narrowed to this partition.
    ///
    /// # Arguments
    ///
    /// * `ctx` — same session context as [`execute`] (tables registered; prefer `input`)
    /// * `part` — key bindings for this unit (e.g. `symbol=NVDA`)
    /// * `input` — **one partition’s** batches/stream/paths (engine-built when possible)
    ///
    /// # Host obligations
    ///
    /// - Do not scan the full multi-partition table when `input` already isolates the part.
    /// - Prefer [`RustModelOutput::Stream`].
    /// - Output schema must match [`output_schema`](Self::output_schema).
    async fn execute_partition(
        &self,
        ctx: &RustModelContext<'_>,
        part: &PartitionKey,
        input: PartitionInput,
    ) -> Result<RustModelOutput> {
        let _ = (ctx, part, input);
        bail!(
            "E_RBT_RUST_PARTITION: execute_partition not implemented for '{}'. \
             Override RustModel::execute_partition for partition-local kernels, \
             or rely on execute() mega/fallback path.",
            self.name()
        );
    }
}

/// Per-run context passed into [`RustModel::execute`] / [`RustModel::execute_partition`].
///
/// Upstream models materialised earlier in the tier plan are registered on [`Self::session`]
/// under bare table names (when using the default empty [`crate::ModelSpec`] catalog prefix).
///
/// # Concurrent workers
///
/// Under L1/L2 concurrency the session is **private to the worker** (not shared with other
/// units). Do not assume process-global table registration across parallel units.
pub struct RustModelContext<'a> {
    /// DataFusion session for this unit/worker (SQL + registered tables).
    pub session: &'a SessionContext,
    /// Project / work directory for the run.
    pub project_dir: &'a Path,
    /// Partition binds and run flags for **this** unit (already narrowed for slices).
    pub scope: &'a RunScope,
    /// DAG node name (same as [`RustModel::name`] when registered correctly).
    pub model_name: &'a str,
    /// Run identity (receipts / lineage stamps).
    pub run_id: &'a str,
    /// Effective contract version for this scope.
    pub contract_version: &'a str,
    /// Bronze fingerprint when Stage 1 ran; may be a host placeholder on stage re-entry.
    pub bronze_fingerprint: Option<&'a str>,
}

/// Partition key bindings for one WorkUnit (RBT-C Phase 2).
///
/// Example: `{"symbol": "NVDA"}` or `{"entity": "a.com", "report_date": "2026-08-07"}`.
///
/// Keys should match the model’s `part_key` / `partition_by` and
/// [`ParallelContract::PartitionLocal`] keys.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PartitionKey {
    /// Ordered map for stable display and hashing (BTreeMap).
    pub keys: BTreeMap<String, String>,
}

impl PartitionKey {
    /// Build from an iterator of `(key, value)`.
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut keys = BTreeMap::new();
        for (k, v) in pairs {
            keys.insert(k.into(), v.into());
        }
        Self { keys }
    }

    /// Borrowed from WorkUnit / planner bindings.
    pub fn from_map(map: &BTreeMap<String, String>) -> Self {
        Self {
            keys: map.clone(),
        }
    }

    /// Value for a partition column, if present.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.keys.get(key).map(String::as_str)
    }

    /// True when no bindings (mega plan / empty unit).
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

impl std::fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.keys.is_empty() {
            return write!(f, "{{}}");
        }
        write!(f, "{{")?;
        let mut first = true;
        for (k, v) in &self.keys {
            if !first {
                write!(f, ", ")?;
            }
            first = false;
            write!(f, "{k}={v}")?;
        }
        write!(f, "}}")
    }
}

/// Engine-supplied input for [`RustModel::execute_partition`].
///
/// Prefer consuming [`Self::stream`] when present: it is already filtered/opened for
/// **this partition only**. `paths` lists physical parquet part files when known
/// (from upstream manifests); hosts may open them with pure Arrow if they prefer.
///
/// # Building policy (engine)
///
/// 1. Resolve each model `ref()` upstream output path.
/// 2. If upstream is a parts directory, select parts whose `part_meta.keys` match
///    this partition (or open the deterministic `part-{scope_id}` when present).
/// 3. Else register the upstream table and `SELECT * FROM "up" WHERE k1='v1' AND …`
///    then `execute_stream` → [`Self::stream`].
///
/// Hosts should treat empty stream + empty paths as “no rows for this partition”.
pub struct PartitionInput {
    /// Upstream model names used to build this input (for diagnostics).
    pub upstream_models: Vec<String>,
    /// Physical part files when the engine resolved them from manifests.
    pub paths: Vec<PathBuf>,
    /// Stream of batches for this partition only (preferred consumption path).
    pub stream: Option<SendableRecordBatchStream>,
}

impl std::fmt::Debug for PartitionInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionInput")
            .field("upstream_models", &self.upstream_models)
            .field("paths", &self.paths)
            .field("stream", &self.stream.as_ref().map(|_| "Stream(..)"))
            .finish()
    }
}

impl PartitionInput {
    /// Empty input (no upstream or nothing matched).
    pub fn empty() -> Self {
        Self {
            upstream_models: Vec::new(),
            paths: Vec::new(),
            stream: None,
        }
    }

    /// Take the stream if present (consumes `self.stream`).
    pub fn into_stream(mut self) -> Option<SendableRecordBatchStream> {
        self.stream.take()
    }

    /// True when there is neither a stream nor any path.
    pub fn is_empty(&self) -> bool {
        self.stream.is_none() && self.paths.is_empty()
    }
}

/// Output of a Rust model (batches or stream).
///
/// # Recommendation
///
/// | Data size | Prefer |
/// |-----------|--------|
/// | Large facts / OHLCV / events | [`Stream`](Self::Stream) |
/// | Tiny dims / tests | [`Batches`](Self::Batches) |
/// | Keyed upsert | Batches (engine still collects in v1) |
pub enum RustModelOutput {
    /// Zero or more record batches (same schema). Empty vec → zero-row table from declared schema.
    Batches(Vec<RecordBatch>),
    /// Streaming path (B5) — **default recommendation** for large outputs.
    ///
    /// Schema must match [`RustModel::output_schema`]. Consumed once by the materializer.
    /// See [`batches_to_stream`] to wrap owned batches.
    Stream(SendableRecordBatchStream),
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
) -> SendableRecordBatchStream {
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    let stream = futures::stream::iter(
        batches
            .into_iter()
            .map(|b| Ok(b) as datafusion::common::Result<RecordBatch>),
    );
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

/// True when an error is the default “execute_partition not implemented” signal.
pub fn is_partition_not_implemented(err: &anyhow::Error) -> bool {
    let s = format!("{err:#}");
    s.contains("E_RBT_RUST_PARTITION") && s.contains("not implemented")
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

    /// Register a host model. Fails if `name()` is empty or already registered.
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

    /// Lookup by DAG name.
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

    /// Clone all registered models (for L1/L2 private worker engines).
    pub fn snapshot(&self) -> Vec<Arc<dyn RustModel>> {
        self.models.values().cloned().collect()
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
        for (fi, (gf, ef)) in got.fields().iter().zip(expected.fields().iter()).enumerate() {
            if gf.name() != ef.name() {
                bail!(
                    "E_RBT_RUST_SCHEMA: batch {bi} field {fi}: name '{}' != declared '{}'",
                    gf.name(),
                    ef.name()
                );
            }
            if gf.data_type() != ef.data_type() {
                bail!(
                    "E_RBT_RUST_SCHEMA: batch {bi} field '{}': type {:?} != declared {:?}",
                    gf.name(),
                    gf.data_type(),
                    ef.data_type()
                );
            }
        }
    }
    Ok(())
}

/// Zero-row batch matching `schema` (for empty materialize).
pub fn empty_batch_for_schema(schema: SchemaRef) -> Result<RecordBatch> {
    Ok(RecordBatch::new_empty(schema))
}

/// Build a schema from (name, DataType, nullable) triples (host convenience).
pub fn schema_from_fields(fields: Vec<(&str, DataType, bool)>) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .into_iter()
            .map(|(n, t, null)| Field::new(n, t, null))
            .collect::<Vec<_>>(),
    ))
}

/// Build a [`PartitionInput`] for a Design B partition unit (engine helper).
///
/// Opens upstream `ref()` models as a filtered stream and/or part paths.
pub async fn build_partition_input(
    session: &SessionContext,
    dag: &crate::core::dag::ModelDag,
    model: &crate::core::dag::ModelNode,
    part: &PartitionKey,
) -> Result<PartitionInput> {
    use crate::core::parser::DependencyRef;
    use crate::materializer::{
        incremental_ref_path, load_manifest, parts_dir_for_parquet, uses_parts_directory,
    };
    use datafusion::prelude::ParquetReadOptions;

    let mut input = PartitionInput::empty();
    if part.is_empty() {
        return Ok(input);
    }

    for dep in &model.dependencies {
        let DependencyRef::Model(up_name) = dep else {
            continue;
        };
        input.upstream_models.push(up_name.clone());
        let Some(&idx) = dag.node_map.get(up_name) else {
            continue;
        };
        let up = &dag.graph[idx];
        let Some(ref op) = up.output_path else {
            continue;
        };
        let dest = PathBuf::from(op);
        let ref_path = if uses_parts_directory(&up.materialization) {
            incremental_ref_path(&dest)
        } else {
            let parts = parts_dir_for_parquet(&dest);
            if parts.is_dir() {
                parts
            } else {
                dest.clone()
            }
        };

        // Prefer explicit part files from manifest meta matching keys.
        if ref_path.is_dir() {
            if let Ok(man) = load_manifest(&ref_path) {
                for (part_name, meta) in &man.part_meta {
                    if !meta.keys.is_empty()
                        && part.keys.iter().all(|(k, v)| meta.keys.get(k) == Some(v))
                    {
                        let p = ref_path.join(part_name);
                        if p.is_file() {
                            input.paths.push(p);
                        }
                    }
                }
            }
            // Also try listing part-*.parquet if meta empty but only one file? keep paths.
        } else if ref_path.is_file() {
            input.paths.push(ref_path.clone());
        }

        // Always build a filtered SQL stream over the registered upstream table name.
        // Ensure table is registered for filter path.
        let table_name = up_name.as_str();
        let already = session.table_exist(table_name).unwrap_or(false);
        if !already && ref_path.exists() {
            let path_str = ref_path.to_str().unwrap_or_default();
            if let Err(e) = session
                .register_parquet(table_name, path_str, ParquetReadOptions::default())
                .await
            {
                tracing::debug!(
                    upstream = %up_name,
                    error = %e,
                    "partition input: register_parquet failed (filter stream may be empty)"
                );
            }
        }

        if session.table_exist(table_name).unwrap_or(false) && input.stream.is_none() {
            let mut wheres = Vec::new();
            for (k, v) in &part.keys {
                // Escape single quotes in values
                let esc = v.replace('\'', "''");
                wheres.push(format!("\"{k}\" = '{esc}'"));
            }
            if !wheres.is_empty() {
                let sql = format!(
                    "SELECT * FROM \"{table_name}\" WHERE {}",
                    wheres.join(" AND ")
                );
                match session.sql(&sql).await {
                    Ok(df) => match df.execute_stream().await {
                        Ok(stream) => {
                            input.stream = Some(stream);
                        }
                        Err(e) => {
                            tracing::debug!(%sql, error = %e, "partition filter stream failed");
                        }
                    },
                    Err(e) => {
                        tracing::debug!(%sql, error = %e, "partition filter SQL failed");
                    }
                }
            }
        }
    }

    Ok(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_key_display() {
        let p = PartitionKey::from_pairs([("symbol", "AAPL")]);
        assert_eq!(p.get("symbol"), Some("AAPL"));
        assert!(format!("{p}").contains("symbol=AAPL"));
    }

    #[test]
    fn not_implemented_detector() {
        let e = anyhow::anyhow!(
            "E_RBT_RUST_PARTITION: execute_partition not implemented for 'x'"
        );
        assert!(is_partition_not_implemented(&e));
        assert!(!is_partition_not_implemented(&anyhow::anyhow!("other")));
    }
}

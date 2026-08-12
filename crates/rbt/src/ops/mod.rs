//! Lake ops façade for embedders (RBT-L1.4 / ADR-007).
//!
//! Thin composition over receipts, fingerprints, materializers, and [`ModelSpec`].
//! Does **not** introduce a second execution engine — same SoT as CLI/`execute_dag*`.
//!
//! # Patterns
//!
//! Structural **façade**: intentional, stable names for the 80% silver path so hosts
//! avoid inventing skip paths or re-copying engine skip logic.
//!
//! # Example
//!
//! ```rust,no_run
//! use rbt::ops::{plan_skip, stage_model_spec, upsert_registry};
//! use rbt::{ModelDag, RbtProjectConfig, RunScope, SourceFormat, UpsertConfig};
//! use std::path::Path;
//! # fn demo(dag: &ModelDag, cfg: &RbtProjectConfig, batches: &[rbt::arrow::record_batch::RecordBatch]) -> anyhow::Result<()> {
//! let project = Path::new(".");
//! let scope = RunScope::new().with_var("report_date", "2026-08-01");
//! let plan = plan_skip(dag, project, cfg, &scope)?;
//! if plan.should_skip {
//!     return Ok(());
//! }
//! let stg = stage_model_spec(
//!     "stg_events",
//!     "SELECT * FROM {{ source('bronze', 'events') }}",
//!     "bronze/events",
//!     SourceFormat::Parquet,
//!     Some(&["entity_id".into()]),
//! ).output_path("/tmp/stg_events.parquet");
//! let _ = stg;
//! let upsert_cfg = UpsertConfig {
//!     unique_key: vec!["entity_id".into()],
//!     touch_columns: vec!["last_seen".into()],
//!     compare_columns: None,
//! };
//! upsert_registry(Path::new("/tmp/dim.parquet"), batches, &upsert_cfg, None, &[])?;
//! # Ok(())
//! # }
//! ```

use crate::core::dag::{Materialization, ModelDag, ModelLayer};
use crate::core::dag_builder::ModelSpec;
use crate::core::frontmatter::{SourceFormat, StagingFrontmatter};
use crate::core::project::RbtProjectConfig;
use crate::core::receipt::{
    bronze_fingerprint, effective_contract_version, fingerprints_match_for_skip, RunReceipt,
    RunStatus,
};
use crate::core::run_scope::RunScope;
use crate::materializer::stream::{MaterializeWriteOptions, StreamWriteStats};
use crate::materializer::upsert::{materialize_keyed_upsert, UpsertConfig, UpsertStats};
use crate::testing::Assertion;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use std::path::Path;

/// Result of comparing current bronze fingerprint to the latest successful receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkipPlan {
    /// Current bronze fingerprint (path_stat or content mode per project config).
    pub current_fingerprint: String,
    /// Effective contract version used for the compare.
    pub contract_version: String,
    /// Scope key used to load `latest_{scope_key}.json`.
    pub scope_key: String,
    /// Previous successful receipt fingerprint, if any.
    pub previous_fingerprint: Option<String>,
    /// True when host may skip materialize (same identity as engine `--skip-if-match`).
    pub should_skip: bool,
    /// Human-readable reason when `should_skip` or when forced re-run.
    pub reason: String,
}

/// Plan whether a scoped run can skip (library form of CLI `--skip-if-match`).
///
/// Compares:
/// 1. Latest receipt for `scope.scope_key()` with `status == Ok`
/// 2. `fingerprints_match_for_skip(prev, current)`
/// 3. Matching `contract_version`
///
/// Does **not** require `scope.skip_if_fingerprint_match` to be set — the host decides
/// whether to honour `should_skip`.
pub fn plan_skip(
    dag: &ModelDag,
    project_dir: &Path,
    config: &RbtProjectConfig,
    scope: &RunScope,
) -> Result<SkipPlan> {
    let scope_key = scope.scope_key();
    let contract_version = effective_contract_version(config, scope);
    let current_fingerprint = bronze_fingerprint(dag, project_dir, config, scope)?;

    let prev = RunReceipt::load_latest_for_scope(project_dir, &scope_key);
    let previous_fingerprint = prev.as_ref().map(|r| r.bronze_fingerprint.clone());

    let (should_skip, reason) = match prev {
        Some(r)
            if r.status == RunStatus::Ok
                && fingerprints_match_for_skip(&r.bronze_fingerprint, &current_fingerprint)
                && r.contract_version == contract_version =>
        {
            (
                true,
                "bronze fingerprint + contract_version match previous successful receipt".into(),
            )
        }
        Some(r) if r.status != RunStatus::Ok => (
            false,
            format!(
                "previous receipt status was {:?}; re-execute",
                r.status
            ),
        ),
        Some(r) if r.contract_version != contract_version => (
            false,
            format!(
                "contract_version changed ({} → {}); re-execute",
                r.contract_version, contract_version
            ),
        ),
        Some(_) => (
            false,
            "bronze fingerprint changed vs previous successful receipt; re-execute".into(),
        ),
        None => (false, "no previous successful receipt for scope; execute".into()),
    };

    Ok(SkipPlan {
        current_fingerprint,
        contract_version,
        scope_key,
        previous_fingerprint,
        should_skip,
        reason,
    })
}

/// Build a staging [`ModelSpec`] with a bronze scan contract (common silver entry).
///
/// Sets layer `Staging`, materialization `Table`, and frontmatter `scan_path` /
/// `source_format` / optional `grain`. Caller should set `.output_path(...)` before
/// execute when writing Parquet.
pub fn stage_model_spec(
    name: impl Into<String>,
    sql: impl Into<String>,
    scan_path: impl Into<String>,
    source_format: SourceFormat,
    grain: Option<&[String]>,
) -> ModelSpec {
    let mut fm = StagingFrontmatter {
        scan_path: Some(scan_path.into()),
        source_format: Some(source_format),
        materialization: Some("table".into()),
        ..Default::default()
    };
    if let Some(g) = grain {
        if !g.is_empty() {
            fm.grain = Some(g.to_vec());
        }
    }
    ModelSpec::sql(name, sql)
        .layer(ModelLayer::Staging)
        .materialization(Materialization::Table)
        .frontmatter(fm)
}

/// Build a mart [`ModelSpec`] configured for `keyed_upsert` (dim / registry).
///
/// `unique_key` is required (match columns). When empty, frontmatter will still be set
/// but materialize fails closed with `E_RBT_UPSERT_KEY` — same as file projects.
pub fn keyed_upsert_model_spec(
    name: impl Into<String>,
    sql: impl Into<String>,
    unique_key: &[String],
    touch_columns: &[String],
    compare_columns: Option<&[String]>,
) -> ModelSpec {
    let fm = StagingFrontmatter {
        materialization: Some("keyed_upsert".into()),
        unique_key: Some(unique_key.to_vec()),
        touch_columns: Some(touch_columns.to_vec()),
        compare_columns: compare_columns.map(|c| c.to_vec()),
        grain: Some(unique_key.to_vec()),
        ..Default::default()
    };
    ModelSpec::sql(name, sql)
        .layer(ModelLayer::Mart)
        .materialization(Materialization::KeyedUpsert)
        .frontmatter(fm)
}

/// Materialize a Type-1 registry / dim parquet via [`materialize_keyed_upsert`].
///
/// Hosts supply **candidate** batches (already computed in SQL or Rust). Peers not in
/// the candidate set are kept. `write_opts` defaults to project-independent stream options
/// when `None`.
pub fn upsert_registry(
    dest_parquet: &Path,
    candidates: &[RecordBatch],
    cfg: &UpsertConfig,
    write_opts: Option<&MaterializeWriteOptions>,
    assertions: &[Assertion],
) -> Result<(StreamWriteStats, UpsertStats)> {
    let default_opts = MaterializeWriteOptions::default();
    let opts = write_opts.unwrap_or(&default_opts);
    materialize_keyed_upsert(dest_parquet, candidates, cfg, opts, assertions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::dag_builder::DagBuilder;
    use crate::core::project::RbtProjectConfig;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn tiny_dag() -> ModelDag {
        DagBuilder::new()
            .model(
                ModelSpec::sql("stg_a", "SELECT 1 AS id")
                    .output_path("/tmp/stg_a.parquet"),
            )
            .build()
            .unwrap()
    }

    #[test]
    fn plan_skip_no_receipt_executes() {
        let dir = tempdir().unwrap();
        let dag = tiny_dag();
        let cfg = RbtProjectConfig::default();
        let scope = RunScope::new().with_var("d", "1");
        let plan = plan_skip(&dag, dir.path(), &cfg, &scope).unwrap();
        assert!(!plan.should_skip);
        assert!(plan.reason.contains("no previous"));
        assert!(!plan.current_fingerprint.is_empty());
    }

    #[test]
    fn stage_model_spec_sets_scan_and_layer() {
        let spec = stage_model_spec(
            "stg_events",
            "SELECT 1",
            "bronze/events",
            SourceFormat::Parquet,
            Some(&["entity_id".into()]),
        );
        assert_eq!(spec.name, "stg_events");
        assert_eq!(spec.layer, Some(ModelLayer::Staging));
        let fm = spec.frontmatter.as_ref().unwrap();
        assert_eq!(fm.scan_path.as_deref(), Some("bronze/events"));
        assert_eq!(fm.source_format, Some(SourceFormat::Parquet));
        assert_eq!(fm.grain.as_ref().unwrap(), &vec!["entity_id".to_string()]);
    }

    #[test]
    fn keyed_upsert_model_spec_sets_keys() {
        let spec = keyed_upsert_model_spec(
            "dim_entity",
            "SELECT 1 AS entity_id",
            &["entity_id".into()],
            &["last_seen".into()],
            None,
        );
        assert_eq!(spec.materialization, Materialization::KeyedUpsert);
        let fm = spec.frontmatter.unwrap();
        assert_eq!(fm.unique_key.unwrap(), vec!["entity_id"]);
        assert_eq!(fm.touch_columns.unwrap(), vec!["last_seen"]);
    }

    #[test]
    fn upsert_registry_inserts_then_keeps() -> Result<()> {
        let dir = tempdir()?;
        let dest = dir.path().join("dim.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )?;
        let cfg = UpsertConfig {
            unique_key: vec!["entity_id".into()],
            touch_columns: vec![],
            compare_columns: Some(vec!["v".into()]),
        };
        let (w1, s1) = upsert_registry(&dest, &[batch], &cfg, None, &[])?;
        assert!(dest.is_file());
        assert_eq!(s1.total_rows, 2);
        assert_eq!(w1.rows, 2);

        // Second run with only key=1 updated; key=2 must be kept
        let batch2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![11])),
            ],
        )?;
        let (_w2, s2) = upsert_registry(&dest, &[batch2], &cfg, None, &[])?;
        assert_eq!(s2.total_rows, 2);
        Ok(())
    }
}

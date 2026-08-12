//! # rbt
//!
//! **Data-engineering workflow engine** for medallion lakes: bronze files → silver/gold
//! Parquet via SQL DAGs (DataFusion), with run scope, receipts, and optional Iceberg-FS.
//!
//! ## Install
//!
//! **CLI**
//! ```bash
//! cargo install rbt-datalake
//! rbt --help
//! ```
//!
//! **Library**
//! ```toml
//! [dependencies]
//! rbt-datalake = "0.9.0"
//! ```
//!
//! ## Quick start (library)
//!
//! **File project frontend** (CLI / DE layout):
//!
//! ```rust,no_run
//! use rbt::{RbtProjectConfig, RunScope, TransformationEngine};
//! # async fn demo() -> anyhow::Result<()> {
//! let project = std::path::Path::new(".");
//! let config = RbtProjectConfig::load(project)?;
//! let dag = config.build_dag(project, None)?;
//! let engine = TransformationEngine::new();
//! // Scalar + multi-value partition binds (RBT-A1)
//! let scope = RunScope::new()
//!     .with_var("report_date", "2026-07-29")
//!     .with_var_multi("entity", ["a.com", "b.com"])?;
//! let _summary = engine
//!     .execute_dag_with_scope(&dag, project, "./target/output", &config, &scope)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! **Programmatic IR** (embedders / no `models/` directory — RBT-L1.3):
//!
//! ```rust,no_run
//! use rbt::{DagBuilder, Materialization, ModelLayer, ModelSpec, OutputFormat};
//! # fn demo() -> anyhow::Result<()> {
//! let dag = DagBuilder::new()
//!     .model(
//!         ModelSpec::sql("stg_x", "SELECT 1 AS id")
//!             .layer(ModelLayer::Staging)
//!             .materialization(Materialization::Table)
//!             .output_format(OutputFormat::Parquet)
//!             .output_path("/tmp/stg_x.parquet"),
//!     )
//!     .build()?;
//! assert!(dag.node_map.contains_key("stg_x"));
//! # Ok(())
//! # }
//! ```
//!
//! File projects and [`DagBuilder`] share the same [`ModelDag`] execution IR.
//!
//! ## Embed profile
//!
//! ```toml
//! rbt-datalake = { version = "0.9", default-features = false, features = ["sql", "parquet"] }
//! # optional: "iceberg", "jshift", "cli"
//! ```
//!
//! Stack crates are re-exported as [`arrow`], [`parquet`], [`datafusion`] (and
//! `iceberg` when enabled) so hosts share one monomorphic ABI.
//!
//! ## Host UDFs (L1.5 / Design A)
//!
//! Register domain kernels without subclassing the engine:
//!
//! ```rust,no_run
//! use rbt::{RbtEngineBuilder, UdfPack};
//! use rbt::datafusion::prelude::SessionContext;
//! # struct MyPack;
//! # impl UdfPack for MyPack {
//! #   fn register(&self, _ctx: &SessionContext) -> anyhow::Result<()> { Ok(()) }
//! # }
//! # async fn demo() -> anyhow::Result<()> {
//! let engine = RbtEngineBuilder::new()
//!     .with_udf_pack(MyPack)
//!     .build()
//!     .await?;
//! let _ = engine;
//! # Ok(())
//! # }
//! ```
//!
//! Lake helpers: [`ops`] (`plan_skip`, `stage_model_spec`, `upsert_registry`).
//! Bronze adapters: [`BronzeAdapter`], [`adapter_for`], [`read_with_adapter`].
//!
//! ## Run scope (A1)
//!
//! See [`RunScope`] and [`ScopeValue`]: repeated `--var`, `--var-file`, and
//! `with_var_multi` bind hive **IN** filters. Showcase:
//! `examples/a1_multi_value_scope`.

#![doc(html_root_url = "https://docs.rs/rbt-datalake/0.9.0")]

pub mod core;
pub mod engine;
pub mod json;
pub mod materializer;
pub mod measure;
pub mod ops;
pub mod scan;
pub mod testing;

// --- Public API (stable surface for library users) ---------------------------

pub use core::{
    apply_scope_to_frontmatter, bronze_fingerprint, contract_diff_to_bronze_diagnostics,
    effective_contract_version, expand_braced_vars, fingerprints_match_for_skip,
    model_has_test_contract, parse_fingerprint_prefix, parse_logical_dtype, parse_select_spec,
    resolve_scan_path, run_contract_diff, scan_path_exists, strip_contract_prefix,
    try_apply_scope_to_frontmatter, AcceptedValuesEntry, BronzeCheckMode, BronzeDiagnostic,
    BronzeValidationReport, ColumnMeta, ContractDiffColumn, ContractDiffReport, ContractsConfig,
    ConsolidatePolicy, DagBuilder, DependencyRef, DiagnosticSeverity, EnumContract, EnumProbe,
    FingerprintAlgo, FingerprintConfig, FingerprintMode, IcebergConfig, IcebergWriteMode,
    Materialization, MaterializeConfig, MaterializeMode, ModelDag, ModelLayer, ModelNode,
    ModelRole, ModelRunResult, ModelSpec, ModelTests,
    OnMissing, OnNewPolicy, OutputFormat, PathGlobSet, RbtProjectConfig, RbtTemplateEngine,
    RefBackend, RefStrategy, RelationshipTest, RunReceipt, RunScope, RunStatus, ScanConfig,
    ScopeValue, SelectMode, SelectToken, SourceFormat, SqlModelParser, StagingFrontmatter,
    DEFAULT_MAX_ROW_GROUP_BYTES, DEFAULT_MAX_ROW_GROUP_ROWS, DEFAULT_MEMTABLE_MAX_ROWS,
    DEFAULT_MULTI_VAR_LIMIT, DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES, SUPPORTED_LOGICAL_DTYPES,
    align_batches_to_declared, declared_schema_for_frontmatter, empty_batch_for_frontmatter,
    ensure_declared_columns, merge_stream_and_declared, try_declared_schema,
};

pub use engine::{
    register_bronze_for_model, register_bronze_for_model_scoped, register_bronze_sources_for_dag,
    register_bronze_sources_for_dag_scoped, BronzeRegistrationMode, BronzeSourceMeta,
    BronzeTableProvider, DagExecutionSummary, PreviewResult, RbtEngineBuilder,
    TransformationEngine,
};
pub use engine::udf::{
    register_builtin_udfs, register_scalar_udf, register_udf_pack, UdfPack, BUILTIN_UDF_NAMES,
};

pub use materializer::{
    clear_incremental_parts, consolidate_parts_to_parquet, incremental_ref_path,
    materialize_incremental_append_stream, materialize_keyed_upsert,
    materialize_scoped_replace_stream, materialize_table_parts_only_stream, materialize_stream,
    new_wap_run_id, parts_dir_for_parquet, resolve_part_keys, scope_part_id, sibling_iceberg_dir,
    stamp_batch, upsert_batches, uses_parts_directory, wap_publish, write_iceberg_fs_table,
    write_parquet_stream, IncrementalManifest, LineageStamp, MaterializeWriteOptions,
    MultiFormatWriter, StreamWriteStats, UpsertConfig, UpsertResult, UpsertStats, WapAuditLog,
    WapMaterializer, WapModelPaths, WapPhase, WapStatus, DEFAULT_UPSERT_MAX_ROWS,
};
#[cfg(feature = "iceberg")]
pub use materializer::{
    verify_iceberg_catalog_table, write_iceberg_catalog_batches, IcebergCatalogOptions,
    IcebergCatalogWriteStats,
};
pub use scan::parts::{is_parts_directory, list_part_files, PartsManifest};
pub use measure::{
    default_report_path, list_scenarios, run_measure_scenario, write_measure_report, MeasureReport,
    ModeCompare, SCENARIO_COMPLEX_BRONZE, SCENARIO_ENTITY_REGISTRY_UPSERT,
    SCENARIO_INCREMENTAL_APPEND, SCENARIO_SMOKE_PIPELINE, SCENARIO_STREAM_VS_COLLECT,
    SCENARIO_VALIDATE_DX, SCENARIO_WHALE_SYNTHETIC, DEFAULT_WHALE_PARTS, DEFAULT_WHALE_ROWS,
};

pub use json::{JShiftExtractor, JsonExtractSpec};
pub use ops::{
    keyed_upsert_model_spec, plan_skip, stage_model_spec, upsert_registry, SkipPlan,
};
pub use scan::adapter::{
    adapter_for, adapter_for_name, builtin_adapters, clear_host_adapters, read_with_adapter,
    register_host_adapter, register_named_adapter, resolve_for_request, AdapterRegistry,
    BronzeAdapter, NamedBronzeAdapter, ResolvedAdapter,
};
pub use scan::{parse_hive_partitions, LakeScanner, ScanFileOrder, ScanRequest};
pub use engine::stages::{
    expand_model_selection, stage_plan_skip, stage_write_receipt, ExecuteTiersOptions,
    PipelineStage, ReceiptWriteArgs, StageExecuteResult,
};
// parts re-exported above

pub use testing::{
    assertions_from_model_tests, Assertion, RecordBatchValidator, StreamingAssertionRunner,
    UniqueKeyTracker, ValidationResult,
};

/// Package version string.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// --- L1.2 / ADR-005: one Arrow/DataFusion major for the process -------------
// Prefer `rbt::arrow` / `rbt::datafusion` in embedders to avoid dual-linking.
/// Re-export of the [arrow] crate used by this build.
pub use arrow;
/// Re-export of [parquet] aligned with [`arrow`].
pub use parquet;
/// Re-export of [datafusion] (SQL engine).
pub use datafusion;
#[cfg(feature = "iceberg")]
/// Re-export of [iceberg] when feature `iceberg` is enabled.
pub use iceberg;

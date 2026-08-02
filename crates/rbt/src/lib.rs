//! # rbt
//!
//! Medallion SQL DAG engine for lakehouse transforms (bronze → silver → gold).
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
//! rbt-datalake = "0.7.3"
//! ```
//!
//! ## Quick start (library)
//!
//! ```rust,no_run
//! use rbt::{RbtProjectConfig, RunScope, TransformationEngine};
//! # async fn demo() -> anyhow::Result<()> {
//! let project = std::path::Path::new(".");
//! let config = RbtProjectConfig::load(project)?;
//! let dag = config.build_dag(project, None)?;
//! let engine = TransformationEngine::new();
//! let scope = RunScope::new().with_var("report_date", "2026-07-29");
//! let _summary = engine
//!     .execute_dag_with_scope(&dag, project, "./target/output", &config, &scope)
//!     .await?;
//! # Ok(())
//! # }
//! ```

#![doc(html_root_url = "https://docs.rs/rbt-datalake/0.7.3")]

pub mod core;
pub mod engine;
pub mod json;
pub mod materializer;
pub mod measure;
pub mod scan;
pub mod testing;

// --- Public API (stable surface for library users) ---------------------------

pub use core::{
    apply_scope_to_frontmatter, bronze_fingerprint, effective_contract_version, expand_braced_vars,
    model_has_test_contract, parse_logical_dtype, parse_select_spec, resolve_scan_path,
    scan_path_exists, BronzeCheckMode, BronzeDiagnostic, BronzeValidationReport, ColumnMeta,
    DependencyRef, DiagnosticSeverity, IcebergConfig, IcebergWriteMode, Materialization,
    MaterializeConfig, MaterializeMode, ModelDag, ModelLayer, ModelNode, ModelRunResult,
    ModelTests, OnMissing, OutputFormat, PathGlobSet, RbtProjectConfig, RbtTemplateEngine,
    RefBackend, RefStrategy, RelationshipTest, RunReceipt, RunScope, RunStatus, ScanConfig,
    SelectMode, SelectToken, SourceFormat, SqlModelParser, StagingFrontmatter,
    DEFAULT_MAX_ROW_GROUP_BYTES,
    DEFAULT_MAX_ROW_GROUP_ROWS, DEFAULT_MEMTABLE_MAX_ROWS, DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES,
};

pub use engine::{
    register_bronze_for_model, register_bronze_for_model_scoped, register_bronze_sources_for_dag,
    register_bronze_sources_for_dag_scoped, BronzeRegistrationMode, BronzeSourceMeta,
    BronzeTableProvider, DagExecutionSummary, PreviewResult, RbtEngineBuilder,
    TransformationEngine,
};
pub use engine::udf::{register_builtin_udfs, register_scalar_udf, BUILTIN_UDF_NAMES};

pub use materializer::{
    clear_incremental_parts, incremental_ref_path, materialize_incremental_append_stream,
    materialize_stream, new_wap_run_id, sibling_iceberg_dir, stamp_batch, verify_iceberg_catalog_table,
    wap_publish, write_iceberg_catalog_batches, write_iceberg_fs_table, write_parquet_stream,
    IcebergCatalogOptions, IcebergCatalogWriteStats, LineageStamp, MaterializeWriteOptions,
    MultiFormatWriter, StreamWriteStats, WapAuditLog, WapMaterializer, WapModelPaths, WapPhase,
    WapStatus,
};
pub use scan::parts::{is_parts_directory, list_part_files, PartsManifest};
pub use measure::{
    default_report_path, list_scenarios, run_measure_scenario, write_measure_report, MeasureReport,
    ModeCompare, SCENARIO_COMPLEX_BRONZE, SCENARIO_INCREMENTAL_APPEND, SCENARIO_SMOKE_PIPELINE,
    SCENARIO_STREAM_VS_COLLECT, SCENARIO_VALIDATE_DX, SCENARIO_WHALE_SYNTHETIC, DEFAULT_WHALE_PARTS,
    DEFAULT_WHALE_ROWS,
};

pub use json::{JShiftExtractor, JsonExtractSpec};
pub use scan::{parse_hive_partitions, LakeScanner, ScanRequest};
// parts re-exported above

pub use testing::{
    assertions_from_model_tests, Assertion, RecordBatchValidator, StreamingAssertionRunner,
    UniqueKeyTracker, ValidationResult,
};

/// Package version string.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

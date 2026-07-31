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
//! rbt-datalake = "0.4.0"
//! ```
//!
//! ## Quick start (library)
//!
//! ```rust,no_run
//! use rbt::{RbtProjectConfig, TransformationEngine};
//! # async fn demo() -> anyhow::Result<()> {
//! let project = std::path::Path::new(".");
//! let config = RbtProjectConfig::load(project)?;
//! let dag = config.build_dag(project, None)?;
//! let engine = TransformationEngine::new();
//! let _summary = engine.execute_dag(&dag, project, "./target/output").await?;
//! # Ok(())
//! # }
//! ```

#![doc(html_root_url = "https://docs.rs/rbt-datalake/0.4.0")]

pub mod core;
pub mod engine;
pub mod json;
pub mod materializer;
pub mod scan;
pub mod testing;

// --- Public API (stable surface for library users) ---------------------------

pub use core::{
    model_has_test_contract, parse_select_spec, resolve_scan_path, scan_path_exists,
    BronzeCheckMode, BronzeDiagnostic, BronzeValidationReport, ColumnMeta, DependencyRef,
    DiagnosticSeverity, IcebergConfig, IcebergWriteMode, Materialization, MaterializeConfig,
    MaterializeMode, ModelDag, ModelLayer, ModelNode, ModelTests, OutputFormat, PathGlobSet,
    RbtProjectConfig, RbtTemplateEngine, RefBackend, RefStrategy, ScanConfig, SelectMode,
    SelectToken, SourceFormat, SqlModelParser, StagingFrontmatter, DEFAULT_MAX_ROW_GROUP_BYTES,
    DEFAULT_MAX_ROW_GROUP_ROWS, DEFAULT_MEMTABLE_MAX_ROWS, DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES,
};

pub use engine::{
    register_bronze_for_model, register_bronze_sources_for_dag, BronzeRegistrationMode,
    BronzeSourceMeta, BronzeTableProvider, DagExecutionSummary, PreviewResult, RbtEngineBuilder,
    TransformationEngine,
};

pub use materializer::{
    materialize_stream, sibling_iceberg_dir, verify_iceberg_catalog_table,
    write_iceberg_catalog_batches, write_iceberg_fs_table, write_parquet_stream,
    IcebergCatalogOptions, IcebergCatalogWriteStats, MaterializeWriteOptions, MultiFormatWriter,
    StreamWriteStats, WapMaterializer, WapStatus,
};

pub use json::{JShiftExtractor, JsonExtractSpec};
pub use scan::{parse_hive_partitions, LakeScanner, ScanRequest};

pub use testing::{
    assertions_from_model_tests, Assertion, RecordBatchValidator, StreamingAssertionRunner,
    UniqueKeyTracker, ValidationResult,
};

/// Package version string.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

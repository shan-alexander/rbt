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
//! rbt-datalake = "0.0.2"
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

#![doc(html_root_url = "https://docs.rs/rbt/0.0.2")]

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
    DiagnosticSeverity, Materialization, ModelDag, ModelLayer, ModelNode, ModelTests,
    OutputFormat, RbtProjectConfig, RbtTemplateEngine, SelectMode, SelectToken, SourceFormat,
    SqlModelParser, StagingFrontmatter,
};

pub use engine::{
    register_bronze_for_model, register_bronze_sources_for_dag, BronzeRegistrationMode,
    BronzeSourceMeta, BronzeTableProvider, DagExecutionSummary, RbtEngineBuilder,
    TransformationEngine,
};

pub use materializer::{
    sibling_iceberg_dir, write_iceberg_fs_table, MultiFormatWriter, WapMaterializer, WapStatus,
};

pub use scan::{parse_hive_partitions, LakeScanner, ScanRequest};
pub use json::{JShiftExtractor, JsonExtractSpec};

pub use testing::{
    assertions_from_model_tests, Assertion, RecordBatchValidator, ValidationResult,
};

/// Package version string.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

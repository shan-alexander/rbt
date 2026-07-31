//! `rbt::core`: Core pipeline model declarations, AST/Jinja template resolution, and DAG topological compilation.

pub mod dag;
pub mod frontmatter;
pub mod parser;
pub mod paths;
pub mod project;
pub mod select;

pub use dag::{Materialization, ModelDag, ModelLayer, ModelNode, OutputFormat};
pub use frontmatter::{
    resolve_scan_path, scan_path_exists, BronzeCheckMode, BronzeDiagnostic, BronzeValidationReport,
    ColumnMeta, DiagnosticSeverity, ModelTests, SourceFormat, StagingFrontmatter,
};
pub use parser::{DependencyRef, RbtTemplateEngine, SqlModelParser};
pub use paths::PathGlobSet;
pub use paths::{
    expand_roots, path_matches_globs, resolve_configured_path, resolve_project_path,
    validate_glob_patterns,
};
pub use project::{
    MaterializeConfig, RbtProjectConfig, RefBackend, RefStrategy, ScanConfig,
    DEFAULT_MEMTABLE_MAX_ROWS, DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES,
};
pub use select::{model_has_test_contract, parse_select_spec, SelectMode, SelectToken};

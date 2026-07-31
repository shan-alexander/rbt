//! `rbt::core`: Core pipeline model declarations, AST/Jinja template resolution, and DAG topological compilation.

pub mod dag;
pub mod frontmatter;
pub mod parser;
pub mod project;
pub mod select;

pub use dag::{Materialization, ModelDag, ModelLayer, ModelNode, OutputFormat};
pub use frontmatter::{
    resolve_scan_path, scan_path_exists, BronzeCheckMode, BronzeDiagnostic, BronzeValidationReport,
    ColumnMeta, DiagnosticSeverity, ModelTests, SourceFormat, StagingFrontmatter,
};
pub use parser::{DependencyRef, RbtTemplateEngine, SqlModelParser};
pub use project::{
    MaterializeConfig, RbtProjectConfig, RefBackend, RefStrategy, DEFAULT_MEMTABLE_MAX_ROWS,
};
pub use select::{model_has_test_contract, parse_select_spec, SelectMode, SelectToken};

//! `rbt::core`: Core pipeline model declarations, AST/Jinja template resolution, and DAG topological compilation.

pub mod contracts;
pub mod dag;
pub mod dag_builder;
pub mod frontmatter;
pub mod parser;
pub mod paths;
pub mod project;
pub mod receipt;
pub mod run_scope;
pub mod schema_emit;
pub mod select;

pub use contracts::{
    contract_diff_to_bronze_diagnostics, run_contract_diff, strip_contract_prefix, ContractsConfig,
    ContractDiffColumn, ContractDiffReport, EnumContract, EnumProbe, OnNewPolicy,
};
pub use dag::{
    parse_materialization_hint, Materialization, ModelDag, ModelLayer, ModelNode, OutputFormat,
};
pub use dag_builder::{DagBuilder, ModelSpec};
pub use frontmatter::{
    parse_logical_dtype, resolve_scan_path, scan_path_exists, AcceptedValuesEntry, BronzeCheckMode,
    BronzeDiagnostic, BronzeValidationReport, ColumnMeta, DiagnosticSeverity, ModelTests,
    RelationshipTest, SourceFormat, StagingFrontmatter,
};
pub use parser::{DependencyRef, RbtTemplateEngine, SqlModelParser};
pub use paths::PathGlobSet;
pub use paths::{
    expand_roots, path_matches_globs, resolve_configured_path, resolve_project_path,
    validate_glob_patterns,
};
pub use project::{
    ConsolidatePolicy, FingerprintAlgo, FingerprintConfig, FingerprintMode, IcebergConfig,
    IcebergWriteMode, MaterializeConfig, MaterializeMode, RbtProjectConfig, RefBackend, RefStrategy,
    ScanConfig, DEFAULT_MAX_ROW_GROUP_BYTES, DEFAULT_MAX_ROW_GROUP_ROWS, DEFAULT_MEMTABLE_MAX_ROWS,
    DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES,
};
pub use receipt::{
    apply_scope_to_frontmatter, bronze_fingerprint, effective_contract_version,
    fingerprints_match_for_skip, parse_fingerprint_prefix, try_apply_scope_to_frontmatter,
    ModelRunResult, RunReceipt, RunStatus,
};
pub use run_scope::{
    expand_braced_vars, fnv1a64, OnMissing, RunScope, ScopeValue, DEFAULT_MULTI_VAR_LIMIT,
};
pub use schema_emit::{
    align_batches_to_declared, declared_schema_for_frontmatter, empty_batch_for_frontmatter,
    ensure_declared_columns, merge_stream_and_declared, try_declared_schema,
    SUPPORTED_LOGICAL_DTYPES,
};
pub use select::{model_has_test_contract, parse_select_spec, SelectMode, SelectToken};

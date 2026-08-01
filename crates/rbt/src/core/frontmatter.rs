//! Staging SQL frontmatter: bronze scan contract and compile-time path checks.

use crate::core::run_scope::OnMissing;
use anyhow::{bail, Context, Result};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// How `rbt compile` treats missing/unresolvable bronze `scan_path` entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BronzeCheckMode {
    /// Skip filesystem checks (DAG structure only).
    Off,
    /// Emit warnings; compile still succeeds (default for `compile`).
    #[default]
    Warn,
    /// Missing or invalid bronze sources fail compile.
    Fail,
}

impl BronzeCheckMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Warn => "warn",
            Self::Fail => "fail",
        }
    }
}

impl fmt::Display for BronzeCheckMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Supported bronze file formats for staging lake scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceFormat {
    /// Newline-delimited JSON (also accepts alias `ndjson`).
    #[serde(alias = "ndjson")]
    Jsonl,
    /// Single JSON document or JSON array of objects.
    Json,
    Parquet,
    Csv,
    /// Arrow IPC file (random-access footer).
    #[serde(alias = "arrow", alias = "arrow_file", alias = "ipc")]
    ArrowIpc,
    /// Arrow IPC stream (append-friendly / WAL-style).
    #[serde(alias = "arrow_stream", alias = "ipc_stream")]
    ArrowIpcStream,
    /// Line-oriented application / server logs.
    Log,
    /// Line-oriented text (llms.txt, docs dumps, structured line files).
    Txt,
    /// TOML tables / array-of-tables as rows.
    Toml,
    /// Length-delimited or whole-file protobuf blobs (opaque bronze).
    ///
    /// Each file becomes one row: `_source_path` (Utf8) + `payload` (Binary).
    /// Typed decode of domain messages is a later step (Rust models / schema registry).
    #[serde(alias = "pb", alias = "proto")]
    Protobuf,
}

impl SourceFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Json => "json",
            Self::Parquet => "parquet",
            Self::Csv => "csv",
            Self::ArrowIpc => "arrow_ipc",
            Self::ArrowIpcStream => "arrow_ipc_stream",
            Self::Log => "log",
            Self::Txt => "txt",
            Self::Toml => "toml",
            Self::Protobuf => "protobuf",
        }
    }

    /// Prefer DataFusion listing / external table registration when true.
    pub fn prefers_datafusion_listing(self) -> bool {
        matches!(self, Self::Parquet | Self::Csv | Self::Json | Self::Jsonl)
    }

    /// Infer format from a file extension (without the dot).
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_ascii_lowercase().as_str() {
            "jsonl" | "ndjson" => Some(Self::Jsonl),
            "json" => Some(Self::Json),
            "parquet" | "pq" => Some(Self::Parquet),
            "csv" | "tsv" => Some(Self::Csv),
            "arrow" | "arrows" | "ipc" | "feather" => Some(Self::ArrowIpc),
            "arrows_stream" | "ipc_stream" => Some(Self::ArrowIpcStream),
            "log" => Some(Self::Log),
            "txt" | "text" | "md" => Some(Self::Txt),
            "toml" => Some(Self::Toml),
            "pb" | "protobuf" | "protobin" => Some(Self::Protobuf),
            _ => None,
        }
    }

    /// Parse free-form frontmatter / CLI format strings.
    pub fn parse(s: &str) -> Result<Self> {
        let key = s.trim().to_ascii_lowercase().replace('-', "_");
        match key.as_str() {
            "jsonl" | "ndjson" | "json_lines" => Ok(Self::Jsonl),
            "json" => Ok(Self::Json),
            "parquet" | "pq" => Ok(Self::Parquet),
            "csv" | "tsv" => Ok(Self::Csv),
            "arrow_ipc" | "arrow" | "arrow_file" | "ipc" | "feather" => Ok(Self::ArrowIpc),
            "arrow_ipc_stream" | "arrow_stream" | "ipc_stream" => Ok(Self::ArrowIpcStream),
            "log" => Ok(Self::Log),
            "txt" | "text" => Ok(Self::Txt),
            "toml" => Ok(Self::Toml),
            "protobuf" | "pb" | "proto" | "protobin" => Ok(Self::Protobuf),
            other => bail!(
                "Unknown source_format '{}'. Expected one of: jsonl, json, parquet, csv, arrow_ipc, arrow_ipc_stream, log, txt, toml, protobuf",
                other
            ),
        }
    }
}

impl fmt::Display for SourceFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Declared data-quality tests for a model (run after materialization when present).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ModelTests {
    /// Columns that must have zero nulls.
    #[serde(default)]
    pub not_null: Option<Vec<String>>,
    /// Single column unique, or multi-column composite unique when len > 1.
    #[serde(default)]
    pub unique: Option<Vec<String>>,
    /// Map of column → allowed string values.
    #[serde(default)]
    pub accepted_values: Option<std::collections::HashMap<String, Vec<String>>>,
    /// When true (default), failed tests abort `rbt run` for that model.
    #[serde(default)]
    pub fail_on_error: Option<bool>,
}

impl ModelTests {
    pub fn is_empty(&self) -> bool {
        self.not_null.as_ref().map(|v| v.is_empty()).unwrap_or(true)
            && self.unique.as_ref().map(|v| v.is_empty()).unwrap_or(true)
            && self
                .accepted_values
                .as_ref()
                .map(|m| m.is_empty())
                .unwrap_or(true)
    }

    pub fn should_fail_on_error(&self) -> bool {
        self.fail_on_error.unwrap_or(true)
    }
}

/// Per-column documentation for humans and AI agents.
///
/// * `description` — short label (1–2 lines)
/// * `context` — longer intent, units, lineage, caveats (agent-oriented)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ColumnMeta {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub context: Option<String>,
    /// Optional logical type hint (`utf8`, `int64`, `float64`, `timestamp`, …).
    #[serde(default)]
    pub dtype: Option<String>,
    /// Optional unit (`USD`, `shares`, `ratio`, `ns_epoch`, …).
    #[serde(default)]
    pub unit: Option<String>,
}

/// YAML frontmatter embedded in model SQL files (`---` … `---`).
///
/// Used on staging, transforms, and marts. Scan-related fields only apply when
/// a bronze scan contract (`scan_path`) is present.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StagingFrontmatter {
    /// Human-readable model purpose (docs + future catalog).
    #[serde(default)]
    pub description: Option<String>,
    /// Longer model-level context for AI agents (why it exists, consumers, caveats).
    #[serde(default)]
    pub context: Option<String>,
    /// Column-level description + context map (name → meta).
    #[serde(default)]
    pub columns: Option<std::collections::BTreeMap<String, ColumnMeta>>,

    /// Explicit bronze format. If omitted, inferred from `scan_path` extension.
    #[serde(default)]
    pub source_format: Option<SourceFormat>,
    /// File or directory to scan (project-relative, absolute, or `$root/...` template).
    #[serde(default)]
    pub scan_path: Option<String>,
    /// Filename / relative-path glob(s) under `scan_path` (OR semantics).
    ///
    /// Examples: `crawlplan.parquet`, `**/raw_snoop/crawlplan.parquet`, `*.jsonl`.
    /// Empty / omitted = all files matching `source_format`.
    /// Accepts a single string or a YAML list.
    ///
    /// **Pushdown note:** any non-empty `path_glob` forces the scan→MemTable bronze path
    /// (DataFusion directory listing / predicate pushdown is **not** used for that source),
    /// because listing providers cannot apply rbt's filename globs or hive path injection.
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub path_glob: Option<Vec<String>>,
    /// Optional hive-style partition keys (path injection + future pruning).
    #[serde(default)]
    pub partition_by: Option<Vec<String>>,
    /// jshift / projection field paths for selective JSON(L) extract.
    #[serde(default)]
    pub paths: Option<Vec<String>>,
    /// Override catalog/schema name for registration (default: first `source()` name).
    #[serde(default)]
    pub source_name: Option<String>,
    /// Override table name for registration (default: first `source()` table).
    #[serde(default)]
    pub source_table: Option<String>,
    /// TOML: key of the array-of-tables to expand into rows (default: auto-detect).
    #[serde(default)]
    pub toml_rows_key: Option<String>,
    /// When true, use scan→MemTable path even for formats that support DF listing.
    #[serde(default)]
    pub force_scan: Option<bool>,
    /// Only scan hive-partitioned files whose path segments match these values.
    /// Example: `{ timeframe: "1m" }` keeps `.../timeframe=1m/...` and skips `timeframe=1d`.
    #[serde(default)]
    pub require_partitions: Option<std::collections::HashMap<String, String>>,
    /// Inject `_source_path` (Utf8) with the absolute file path for each row.
    /// Enables "latest chunk wins" dedupe via `ORDER BY _source_path DESC`.
    #[serde(default)]
    pub inject_source_path: Option<bool>,
    /// When scan root is missing or filters match no files: `error` (default) | `empty`.
    ///
    /// `empty` registers a zero-row table with a declared schema from `columns.*.dtype`
    /// (plus `partition_by` keys as Utf8). Required for partial multi-artifact bronze.
    #[serde(default)]
    pub on_missing: Option<OnMissing>,
    /// Silver stage policy hint (docs + future engine): `full_refresh` | `latest_only` |
    /// `append` | `mirror_bronze`. Does not change SQL by itself — authors implement
    /// semantics in the model; rbt may use this for materialization defaults later.
    #[serde(default)]
    pub stage_mode: Option<String>,

    /// Logical grain of the model (e.g. `[symbol, timestamp_ns]`).
    #[serde(default)]
    pub grain: Option<Vec<String>>,
    /// Primary uniqueness contract (usually same as grain for staging facts).
    #[serde(default)]
    pub unique_key: Option<Vec<String>>,
    /// Free-form tags for selection / docs.
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    /// Materialization hint: `table` | `view` | `incremental_append` (engine may ignore for now).
    #[serde(default)]
    pub materialization: Option<String>,
    /// Post-materialization assertions.
    #[serde(default)]
    pub tests: Option<ModelTests>,
    /// Opaque metadata for tools and agents (strings, lists, nested maps OK).
    #[serde(default)]
    pub meta: Option<std::collections::BTreeMap<String, serde_yaml::Value>>,
}

impl StagingFrontmatter {
    /// Resolve format from explicit field or path extension.
    pub fn resolve_format(&self) -> Result<SourceFormat> {
        if let Some(fmt) = self.source_format {
            return Ok(fmt);
        }
        let path = self
            .scan_path
            .as_deref()
            .context("frontmatter missing both source_format and scan_path")?;
        // Strip globs for extension sniffing: `foo/*.jsonl` → look at last segment
        let candidate = path.rsplit('/').next().unwrap_or(path);
        let candidate = candidate.trim_matches(|c| c == '*' || c == '?');
        if let Some(ext) = Path::new(candidate).extension().and_then(|e| e.to_str()) {
            if let Some(fmt) = SourceFormat::from_extension(ext) {
                return Ok(fmt);
            }
        }
        // Directory paths: no extension — require explicit format
        bail!(
            "Cannot infer source_format from scan_path '{}'; set source_format explicitly",
            path
        );
    }

    pub fn has_scan_contract(&self) -> bool {
        self.scan_path
            .as_ref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
    }

    pub fn on_missing_policy(&self) -> OnMissing {
        self.on_missing.unwrap_or(OnMissing::Error)
    }

    /// Build Arrow schema for empty bronze frames (`on_missing: empty`).
    ///
    /// Fields: declared `columns` with `dtype`, then any `partition_by` keys not already
    /// present (Utf8), then optional `_source_path`.
    pub fn empty_frame_schema(&self) -> Result<SchemaRef> {
        let mut fields: Vec<Field> = Vec::new();
        let mut seen = std::collections::HashSet::new();

        if let Some(cols) = &self.columns {
            for (name, meta) in cols {
                let dtype = meta
                    .dtype
                    .as_deref()
                    .with_context(|| {
                        format!(
                            "E_RBT_EMPTY_SCHEMA: column '{name}' needs dtype: for on_missing: empty \
                             (e.g. utf8, int64, float64, bool, binary, timestamp)"
                        )
                    })?;
                let dt = parse_logical_dtype(dtype).with_context(|| {
                    format!("E_RBT_EMPTY_SCHEMA: column '{name}' dtype '{dtype}'")
                })?;
                fields.push(Field::new(name, dt, true));
                seen.insert(name.clone());
            }
        }

        if let Some(parts) = &self.partition_by {
            for p in parts {
                if seen.insert(p.clone()) {
                    fields.push(Field::new(p, DataType::Utf8, true));
                }
            }
        }

        if self.inject_source_path.unwrap_or(false) && seen.insert("_source_path".into()) {
            fields.push(Field::new("_source_path", DataType::Utf8, true));
        }

        if fields.is_empty() {
            bail!(
                "E_RBT_EMPTY_SCHEMA: on_missing: empty requires columns with dtype \
                 and/or partition_by (model scan contract has no schema fields)"
            );
        }
        Ok(Arc::new(Schema::new(fields)))
    }
}

/// Parse logical dtype strings used in frontmatter `columns.*.dtype`.
pub fn parse_logical_dtype(s: &str) -> Result<DataType> {
    let key = s.trim().to_ascii_lowercase().replace('-', "_");
    Ok(match key.as_str() {
        "utf8" | "string" | "str" | "varchar" | "text" => DataType::Utf8,
        "int64" | "long" | "bigint" | "i64" => DataType::Int64,
        "int32" | "int" | "i32" => DataType::Int32,
        "int16" | "smallint" | "i16" => DataType::Int16,
        "int8" | "tinyint" | "i8" => DataType::Int8,
        "uint64" | "u64" => DataType::UInt64,
        "uint32" | "u32" => DataType::UInt32,
        "float64" | "double" | "f64" => DataType::Float64,
        "float32" | "float" | "f32" => DataType::Float32,
        "bool" | "boolean" => DataType::Boolean,
        "binary" | "bytes" | "blob" => DataType::Binary,
        "date" | "date32" => DataType::Date32,
        "timestamp" | "timestamp_us" | "timestamptz" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        "timestamp_ms" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "timestamp_ns" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "timestamp_s" => DataType::Timestamp(TimeUnit::Second, None),
        other => bail!(
            "unknown dtype '{other}' (expected utf8|int64|int32|float64|bool|binary|date|timestamp…)"
        ),
    })
}

/// Severity of a bronze compile diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticSeverity {
    Warning,
    Error,
}

/// One compile-time finding about bronze frontmatter / scan paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BronzeDiagnostic {
    pub model: String,
    pub severity: DiagnosticSeverity,
    pub code: &'static str,
    pub message: String,
}

impl fmt::Display for BronzeDiagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level = match self.severity {
            DiagnosticSeverity::Warning => "warning",
            DiagnosticSeverity::Error => "error",
        };
        write!(
            f,
            "{}[{}] model={}: {}",
            level, self.code, self.model, self.message
        )
    }
}

/// Result of bronze path validation during compile.
#[derive(Debug, Clone, Default)]
pub struct BronzeValidationReport {
    pub diagnostics: Vec<BronzeDiagnostic>,
}

impl BronzeValidationReport {
    pub fn warning_count(&self) -> usize {
        self.diagnostics
            .iter()
            .filter(|d| d.severity == DiagnosticSeverity::Warning)
            .count()
    }

    pub fn error_count(&self) -> usize {
        self.diagnostics
            .iter()
            .filter(|d| d.severity == DiagnosticSeverity::Error)
            .count()
    }

    pub fn has_errors(&self) -> bool {
        self.error_count() > 0
    }
}

/// Resolve `scan_path` against the project root (no named roots). Prefer
/// [`crate::core::paths::resolve_project_path`] when `roots:` are in play.
pub fn resolve_scan_path(project_dir: &Path, scan_path: &str) -> PathBuf {
    crate::core::paths::resolve_project_path(project_dir, scan_path, &Default::default())
        .unwrap_or_else(|_| project_dir.to_path_buf())
}

pub use crate::core::paths::is_remote_uri;

/// Deserialize either a single string or a sequence into `Option<Vec<String>>`.
fn deserialize_string_or_vec<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, SeqAccess, Visitor};
    use std::fmt;

    struct StringOrVec;
    impl<'de> Visitor<'de> for StringOrVec {
        type Value = Option<Vec<String>>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a string or list of strings")
        }

        fn visit_none<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Self::Value, E> {
            Ok(Some(vec![v.to_string()]))
        }

        fn visit_string<E: de::Error>(self, v: String) -> std::result::Result<Self::Value, E> {
            Ok(Some(vec![v]))
        }

        fn visit_seq<A: SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut out = Vec::new();
            while let Some(s) = seq.next_element::<String>()? {
                out.push(s);
            }
            Ok(Some(out))
        }
    }

    deserializer.deserialize_any(StringOrVec)
}

/// Whether a resolved local scan path currently exists (file or directory).
/// Remote URIs are treated as "exists" for compile (runtime / object-store later).
pub fn scan_path_exists(project_dir: &Path, scan_path: &str) -> bool {
    scan_path_exists_with_roots(project_dir, scan_path, &std::collections::HashMap::new())
}

/// Like [`scan_path_exists`] but expands `$root` templates from project config.
pub fn scan_path_exists_with_roots(
    project_dir: &Path,
    scan_path: &str,
    roots: &std::collections::HashMap<String, String>,
) -> bool {
    if is_remote_uri(scan_path.trim()) {
        return true;
    }
    let Ok(resolved) = crate::core::paths::resolve_project_path(project_dir, scan_path, roots)
    else {
        return false;
    };
    // Support simple trailing globs: `dir/*.jsonl` → check parent dir
    let check = strip_simple_glob(&resolved);
    check.exists()
}

fn strip_simple_glob(path: &Path) -> PathBuf {
    let s = path.to_string_lossy();
    if s.contains('*') || s.contains('?') {
        if let Some(parent) = path.parent() {
            return parent.to_path_buf();
        }
    }
    path.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_from_extension_and_parse() {
        assert_eq!(
            SourceFormat::from_extension("jsonl"),
            Some(SourceFormat::Jsonl)
        );
        assert_eq!(
            SourceFormat::from_extension("toml"),
            Some(SourceFormat::Toml)
        );
        assert_eq!(SourceFormat::from_extension("log"), Some(SourceFormat::Log));
        assert_eq!(
            SourceFormat::parse("arrow-ipc").unwrap(),
            SourceFormat::ArrowIpc
        );
        assert_eq!(SourceFormat::parse("ndjson").unwrap(), SourceFormat::Jsonl);
    }

    #[test]
    fn resolve_format_from_path() {
        let fm = StagingFrontmatter {
            scan_path: Some("lake/bronze/raw.jsonl".into()),
            ..Default::default()
        };
        assert_eq!(fm.resolve_format().unwrap(), SourceFormat::Jsonl);
    }

    #[test]
    fn remote_uri_exists_for_compile() {
        assert!(scan_path_exists(
            Path::new("/tmp"),
            "s3://bucket/bronze/x.jsonl"
        ));
    }
}

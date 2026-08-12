//! Bronze adapter trait + registry (RBT-A10 / A10.12 host injection).
//!
//! [`LakeScanner`] lists files and enriches batches (hive partitions, `_source_path`,
//! optional `_ingest_seq`). Per-file decode is delegated to a [`BronzeAdapter`] looked up
//! by [`SourceFormat`], or a host [`NamedBronzeAdapter`] via `frontmatter.adapter`.
//!
//! # Adding a builtin format (one PR)
//!
//! 1. Add a [`SourceFormat`] variant + parse/extension hooks.  
//! 2. Implement [`BronzeAdapter`] (or reuse whole-file UTF-8 helpers).  
//! 3. Register in [`builtin_adapters`].  
//! 4. Tests + matrix row in `docs/BRONZE_ADAPTER_MATRIX.md`.
//!
//! # Host adapters (no fork)
//!
//! - **Override** a builtin: [`register_host_adapter`] / [`AdapterRegistry::register_override`].  
//! - **Named** proprietary format: [`register_named_adapter`] + frontmatter `adapter: my_ticks`.  
//!
//! Unknown / unregistered formats fail closed with `E_RBT_SOURCE_FORMAT`.

use super::{
    expand_json_document_to_jsonl, read_arrow_ipc_auto, read_csv, read_json_arrow,
    read_line_oriented, read_parquet, read_protobuf_opaque, read_toml, read_whole_file_utf8,
    utf8_schema_from_paths, ScanRequest,
};
use crate::core::frontmatter::SourceFormat;
use anyhow::{bail, Result};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, OnceLock, RwLock};

/// Decode one bronze file into Arrow batches (no hive / source-path inject).
pub trait BronzeAdapter: Send + Sync {
    fn name(&self) -> &'static str;
    fn format(&self) -> SourceFormat;

    /// File extensions (no dot) this adapter claims under a directory scan.
    fn extensions(&self) -> &'static [&'static str];

    /// When true, bronze registration may use DataFusion listing (no path_glob / inject).
    fn prefers_datafusion_listing(&self) -> bool {
        false
    }

    fn read_file(&self, path: &Path, req: &ScanRequest) -> Result<Vec<RecordBatch>>;
}

/// Host-only bronze decoder keyed by free-form name (`frontmatter.adapter`).
///
/// Does not require a [`SourceFormat`] enum variant — use for proprietary landings without
/// forking rbt. Fail-closed: unregistered names → `E_RBT_SOURCE_FORMAT`.
pub trait NamedBronzeAdapter: Send + Sync {
    /// Registry key (matched case-insensitively to frontmatter `adapter:`).
    fn name(&self) -> &str;

    /// File extensions (no dot) this adapter claims under a directory scan.
    fn extensions(&self) -> &[&str];

    fn read_file(&self, path: &Path, req: &ScanRequest) -> Result<Vec<RecordBatch>>;
}

/// Process-level + testable host adapter map (A10.12).
///
/// Builtins always remain available as fallback for formats without an override.
#[derive(Default)]
pub struct AdapterRegistry {
    overrides: HashMap<SourceFormat, Arc<dyn BronzeAdapter>>,
    named: HashMap<String, Arc<dyn NamedBronzeAdapter>>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a host adapter that **overrides** a builtin [`SourceFormat`].
    ///
    /// Second registration for the same format fails with `E_RBT_ADAPTER_DUP`.
    pub fn register_override(&mut self, adapter: Arc<dyn BronzeAdapter>) -> Result<()> {
        let fmt = adapter.format();
        if self.overrides.contains_key(&fmt) {
            bail!(
                "E_RBT_ADAPTER_DUP: host adapter already registered for source_format='{}'. \
                 clear_host_adapters() first or use a single process registry.",
                fmt.as_str()
            );
        }
        self.overrides.insert(fmt, adapter);
        Ok(())
    }

    /// Register a proprietary format by free-form name (not a builtin `source_format` string).
    pub fn register_named(&mut self, adapter: Arc<dyn NamedBronzeAdapter>) -> Result<()> {
        let key = normalize_adapter_name(adapter.name());
        if key.is_empty() {
            bail!("E_RBT_ADAPTER_DUP: named adapter name must be non-empty");
        }
        if SourceFormat::try_parse(&key).is_some() {
            bail!(
                "E_RBT_ADAPTER_DUP: '{key}' is a builtin source_format name; \
                 implement BronzeAdapter and call register_override / register_host_adapter"
            );
        }
        if self.named.contains_key(&key) {
            bail!(
                "E_RBT_ADAPTER_DUP: named adapter '{key}' already registered"
            );
        }
        self.named.insert(key, adapter);
        Ok(())
    }

    pub fn clear(&mut self) {
        self.overrides.clear();
        self.named.clear();
    }

    pub fn has_override(&self, format: SourceFormat) -> bool {
        self.overrides.contains_key(&format)
    }

    pub fn has_named(&self, name: &str) -> bool {
        self.named.contains_key(&normalize_adapter_name(name))
    }

    fn resolve_format(&self, format: SourceFormat) -> Result<ResolvedAdapter> {
        if let Some(a) = self.overrides.get(&format) {
            return Ok(ResolvedAdapter::Host(a.clone()));
        }
        for a in builtin_adapters() {
            if a.format() == format {
                return Ok(ResolvedAdapter::Builtin(*a));
            }
        }
        bail!(
            "E_RBT_SOURCE_FORMAT: no bronze adapter registered for source_format='{}'. \
             Supported builtins: {}. Host overrides: {}. See docs/BRONZE_ADAPTERS.md.",
            format.as_str(),
            SourceFormat::all_names().join(", "),
            self.overrides
                .keys()
                .map(|f| f.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn resolve_named(&self, name: &str) -> Result<ResolvedAdapter> {
        let key = normalize_adapter_name(name);
        if let Some(a) = self.named.get(&key) {
            return Ok(ResolvedAdapter::Named(a.clone()));
        }
        bail!(
            "E_RBT_SOURCE_FORMAT: no host adapter registered for adapter='{}'. \
             Call register_named_adapter / AdapterRegistry::register_named. \
             Known named adapters: [{}]. Builtins: {}.",
            name,
            self.named.keys().cloned().collect::<Vec<_>>().join(", "),
            SourceFormat::all_names().join(", ")
        )
    }
}

fn normalize_adapter_name(s: &str) -> String {
    s.trim().to_ascii_lowercase().replace('-', "_")
}

/// Resolved decoder for one scan (builtin, host override, or named).
pub enum ResolvedAdapter {
    Builtin(&'static dyn BronzeAdapter),
    Host(Arc<dyn BronzeAdapter>),
    Named(Arc<dyn NamedBronzeAdapter>),
}

impl ResolvedAdapter {
    pub fn name(&self) -> &str {
        match self {
            Self::Builtin(a) => a.name(),
            Self::Host(a) => a.name(),
            Self::Named(a) => a.name(),
        }
    }

    pub fn extensions(&self) -> Vec<&str> {
        match self {
            Self::Builtin(a) => a.extensions().to_vec(),
            Self::Host(a) => a.extensions().to_vec(),
            Self::Named(a) => a.extensions().to_vec(),
        }
    }

    pub fn prefers_datafusion_listing(&self) -> bool {
        match self {
            Self::Builtin(a) => a.prefers_datafusion_listing(),
            Self::Host(a) => a.prefers_datafusion_listing(),
            Self::Named(_) => false,
        }
    }

    pub fn read_file(&self, path: &Path, req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        match self {
            Self::Builtin(a) => a.read_file(path, req),
            Self::Host(a) => a.read_file(path, req),
            Self::Named(a) => a.read_file(path, req),
        }
    }
}

fn process_registry() -> &'static RwLock<AdapterRegistry> {
    static REG: OnceLock<RwLock<AdapterRegistry>> = OnceLock::new();
    REG.get_or_init(|| RwLock::new(AdapterRegistry::new()))
}

/// Install a host override for a builtin [`SourceFormat`] (process-global).
pub fn register_host_adapter(adapter: Arc<dyn BronzeAdapter>) -> Result<()> {
    process_registry()
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .register_override(adapter)
}

/// Install a named proprietary adapter (process-global). Use frontmatter `adapter: <name>`.
pub fn register_named_adapter(adapter: Arc<dyn NamedBronzeAdapter>) -> Result<()> {
    process_registry()
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .register_named(adapter)
}

/// Drop all process-global host adapters (tests / host reconfiguration).
pub fn clear_host_adapters() {
    process_registry()
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
}

/// Resolve adapter for a builtin format (host override wins over builtin).
pub fn adapter_for(format: SourceFormat) -> Result<ResolvedAdapter> {
    process_registry()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .resolve_format(format)
}

/// Resolve by frontmatter `adapter:` name (named host only).
pub fn adapter_for_name(name: &str) -> Result<ResolvedAdapter> {
    process_registry()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .resolve_named(name)
}

/// Resolve from a [`ScanRequest`] (named `custom_adapter` wins).
pub fn resolve_for_request(req: &ScanRequest) -> Result<ResolvedAdapter> {
    if let Some(ref name) = req.custom_adapter {
        return adapter_for_name(name);
    }
    adapter_for(req.format)
}

/// Decode via the registry (public so hosts can unit-test adapters without LakeScanner).
pub fn read_with_adapter(path: &Path, req: &ScanRequest) -> Result<Vec<RecordBatch>> {
    resolve_for_request(req)?.read_file(path, req)
}

/// Built-in adapters (stable order for docs).
pub fn builtin_adapters() -> &'static [&'static dyn BronzeAdapter] {
    &[
        &ParquetAdapter,
        &CsvAdapter,
        &JsonlAdapter,
        &JsonAdapter,
        &ArrowIpcAdapter,
        &ArrowIpcStreamAdapter,
        &LogAdapter,
        &TxtAdapter,
        &TomlAdapter,
        &ProtobufAdapter,
        &HtmlAdapter,
        &XmlAdapter,
        &RobotsAdapter,
    ]
}

// --- adapters ----------------------------------------------------------------

struct ParquetAdapter;
impl BronzeAdapter for ParquetAdapter {
    fn name(&self) -> &'static str {
        "parquet"
    }
    fn format(&self) -> SourceFormat {
        SourceFormat::Parquet
    }
    fn extensions(&self) -> &'static [&'static str] {
        &["parquet", "pq"]
    }
    fn prefers_datafusion_listing(&self) -> bool {
        true
    }
    fn read_file(&self, path: &Path, _req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        read_parquet(path, None)
    }
}

struct CsvAdapter;
impl BronzeAdapter for CsvAdapter {
    fn name(&self) -> &'static str {
        "csv"
    }
    fn format(&self) -> SourceFormat {
        SourceFormat::Csv
    }
    fn extensions(&self) -> &'static [&'static str] {
        &["csv", "tsv"]
    }
    fn prefers_datafusion_listing(&self) -> bool {
        true
    }
    fn read_file(&self, path: &Path, _req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        read_csv(path, None)
    }
}

struct JsonlAdapter;
impl BronzeAdapter for JsonlAdapter {
    fn name(&self) -> &'static str {
        "jsonl"
    }
    fn format(&self) -> SourceFormat {
        SourceFormat::Jsonl
    }
    fn extensions(&self) -> &'static [&'static str] {
        &["jsonl", "ndjson"]
    }
    fn prefers_datafusion_listing(&self) -> bool {
        true
    }
    fn read_file(&self, path: &Path, req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        read_json_family(path, req, SourceFormat::Jsonl)
    }
}

struct JsonAdapter;
impl BronzeAdapter for JsonAdapter {
    fn name(&self) -> &'static str {
        "json"
    }
    fn format(&self) -> SourceFormat {
        SourceFormat::Json
    }
    fn extensions(&self) -> &'static [&'static str] {
        &["json"]
    }
    fn prefers_datafusion_listing(&self) -> bool {
        true
    }
    fn read_file(&self, path: &Path, req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        read_json_family(path, req, SourceFormat::Json)
    }
}

fn read_json_family(
    path: &Path,
    req: &ScanRequest,
    format: SourceFormat,
) -> Result<Vec<RecordBatch>> {
    if !req.paths.is_empty() {
        let schema = utf8_schema_from_paths(&req.paths);
        let bytes = std::fs::read(path)?;
        let extractor = crate::json::JShiftExtractor::new(req.paths.clone());
        if format == SourceFormat::Json {
            let expanded = expand_json_document_to_jsonl(&bytes)?;
            Ok(vec![extractor.extract_jsonl(&expanded, schema)?])
        } else {
            Ok(vec![extractor.extract_jsonl(&bytes, schema)?])
        }
    } else {
        read_json_arrow(path, format)
    }
}

struct ArrowIpcAdapter;
impl BronzeAdapter for ArrowIpcAdapter {
    fn name(&self) -> &'static str {
        "arrow_ipc"
    }
    fn format(&self) -> SourceFormat {
        SourceFormat::ArrowIpc
    }
    fn extensions(&self) -> &'static [&'static str] {
        &["arrow", "arrows", "ipc", "feather"]
    }
    fn read_file(&self, path: &Path, _req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        read_arrow_ipc_auto(path)
    }
}

struct ArrowIpcStreamAdapter;
impl BronzeAdapter for ArrowIpcStreamAdapter {
    fn name(&self) -> &'static str {
        "arrow_ipc_stream"
    }
    fn format(&self) -> SourceFormat {
        SourceFormat::ArrowIpcStream
    }
    fn extensions(&self) -> &'static [&'static str] {
        &["arrow", "arrows", "ipc", "arrows_stream", "ipc_stream"]
    }
    fn read_file(&self, path: &Path, _req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        read_arrow_ipc_auto(path)
    }
}

struct LogAdapter;
impl BronzeAdapter for LogAdapter {
    fn name(&self) -> &'static str {
        "log"
    }
    fn format(&self) -> SourceFormat {
        SourceFormat::Log
    }
    fn extensions(&self) -> &'static [&'static str] {
        &["log"]
    }
    fn read_file(&self, path: &Path, _req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        Ok(vec![read_line_oriented(path)?])
    }
}

struct TxtAdapter;
impl BronzeAdapter for TxtAdapter {
    fn name(&self) -> &'static str {
        "txt"
    }
    fn format(&self) -> SourceFormat {
        SourceFormat::Txt
    }
    fn extensions(&self) -> &'static [&'static str] {
        &["txt", "text", "md"]
    }
    fn read_file(&self, path: &Path, _req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        // Line-oriented for dumps / llms.txt style; use robots/html/xml for whole-file body.
        Ok(vec![read_line_oriented(path)?])
    }
}

struct TomlAdapter;
impl BronzeAdapter for TomlAdapter {
    fn name(&self) -> &'static str {
        "toml"
    }
    fn format(&self) -> SourceFormat {
        SourceFormat::Toml
    }
    fn extensions(&self) -> &'static [&'static str] {
        &["toml"]
    }
    fn read_file(&self, path: &Path, req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        Ok(vec![read_toml(path, req.toml_rows_key.as_deref())?])
    }
}

struct ProtobufAdapter;
impl BronzeAdapter for ProtobufAdapter {
    fn name(&self) -> &'static str {
        "protobuf"
    }
    fn format(&self) -> SourceFormat {
        SourceFormat::Protobuf
    }
    fn extensions(&self) -> &'static [&'static str] {
        &["pb", "protobuf", "protobin"]
    }
    fn read_file(&self, path: &Path, req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        Ok(vec![read_protobuf_opaque(
            path,
            req.protobuf_max_payload_bytes,
        )?])
    }
}

struct HtmlAdapter;
impl BronzeAdapter for HtmlAdapter {
    fn name(&self) -> &'static str {
        "html"
    }
    fn format(&self) -> SourceFormat {
        SourceFormat::Html
    }
    fn extensions(&self) -> &'static [&'static str] {
        &["html", "htm"]
    }
    fn read_file(&self, path: &Path, _req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        Ok(vec![read_whole_file_utf8(path, "html")?])
    }
}

struct XmlAdapter;
impl BronzeAdapter for XmlAdapter {
    fn name(&self) -> &'static str {
        "xml"
    }
    fn format(&self) -> SourceFormat {
        SourceFormat::Xml
    }
    fn extensions(&self) -> &'static [&'static str] {
        &["xml"]
    }
    fn read_file(&self, path: &Path, _req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        // Opaque whole-file rows — SQL/regexp or host pre-normalize to JSONL for structure.
        Ok(vec![read_whole_file_utf8(path, "xml")?])
    }
}

struct RobotsAdapter;
impl BronzeAdapter for RobotsAdapter {
    fn name(&self) -> &'static str {
        "robots"
    }
    fn format(&self) -> SourceFormat {
        SourceFormat::Robots
    }
    fn extensions(&self) -> &'static [&'static str] {
        // robots.txt often has no useful ext when named `robots.txt` → ext "txt"
        // Directory scans match .txt under source_format: robots via explicit format.
        &["txt", "robots"]
    }
    fn read_file(&self, path: &Path, _req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        Ok(vec![read_whole_file_utf8(path, "body")?])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::frontmatter::SourceFormat;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn registry_covers_all_formats() {
        for fmt in SourceFormat::ALL {
            adapter_for(*fmt).unwrap_or_else(|e| panic!("missing adapter for {fmt:?}: {e}"));
        }
        assert_eq!(builtin_adapters().len(), SourceFormat::ALL.len());
    }

    fn test_req(dir: &std::path::Path, format: SourceFormat, scan: &str) -> ScanRequest {
        ScanRequest {
            project_dir: dir.to_path_buf(),
            scan_path: scan.into(),
            format,
            paths: vec![],
            toml_rows_key: None,
            partition_by: vec![],
            require_partitions: Default::default(),
            require_partitions_in: Default::default(),
            path_glob: vec![],
            inject_source_path: false,
            inject_ingest_seq: false,
            inject_source_mtime: false,
            file_order: super::super::ScanFileOrder::Path,
            custom_adapter: None,
            roots: Default::default(),
            protobuf_max_payload_bytes: 1 << 20,
            allow_empty: false,
        }
    }

    #[test]
    fn html_whole_file_row() -> Result<()> {
        let dir = tempdir()?;
        let p = dir.path().join("page.html");
        std::fs::write(&p, b"<html><body>hi</body></html>")?;
        let req = test_req(dir.path(), SourceFormat::Html, &p.to_string_lossy());
        let batches = read_with_adapter(&p, &req)?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert!(batches[0].schema().field_with_name("html").is_ok());
        Ok(())
    }

    #[test]
    fn robots_whole_file() -> Result<()> {
        let dir = tempdir()?;
        let p = dir.path().join("robots.txt");
        let mut f = std::fs::File::create(&p)?;
        writeln!(f, "User-agent: *")?;
        writeln!(f, "Disallow: /private")?;
        let req = test_req(dir.path(), SourceFormat::Robots, &p.to_string_lossy());
        let batches = read_with_adapter(&p, &req)?;
        assert_eq!(batches[0].num_rows(), 1);
        Ok(())
    }

    struct HostTxtTicks;
    impl NamedBronzeAdapter for HostTxtTicks {
        fn name(&self) -> &str {
            "host_ticks"
        }
        fn extensions(&self) -> &[&str] {
            &["tick"]
        }
        fn read_file(&self, path: &Path, _req: &ScanRequest) -> Result<Vec<RecordBatch>> {
            Ok(vec![read_whole_file_utf8(path, "payload")?])
        }
    }

    #[test]
    fn named_host_adapter_roundtrip() -> Result<()> {
        clear_host_adapters();
        register_named_adapter(std::sync::Arc::new(HostTxtTicks))?;
        let dir = tempdir()?;
        let p = dir.path().join("a.tick");
        std::fs::write(&p, b"tick-body")?;
        let mut req = test_req(dir.path(), SourceFormat::Txt, &p.to_string_lossy());
        req.custom_adapter = Some("host_ticks".into());
        let batches = read_with_adapter(&p, &req)?;
        assert_eq!(batches[0].num_rows(), 1);
        assert!(batches[0].schema().field_with_name("payload").is_ok());
        clear_host_adapters();
        let err = read_with_adapter(&p, &req).unwrap_err().to_string();
        assert!(err.contains("E_RBT_SOURCE_FORMAT"), "{err}");
        Ok(())
    }
}

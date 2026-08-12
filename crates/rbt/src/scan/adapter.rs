//! Bronze adapter trait + registry (RBT-A10).
//!
//! [`LakeScanner`] lists files and enriches batches (hive partitions, `_source_path`).
//! Per-file decode is delegated to a [`BronzeAdapter`] looked up by [`SourceFormat`].
//!
//! # Adding a format (one PR)
//!
//! 1. Add a [`SourceFormat`] variant + parse/extension hooks.  
//! 2. Implement [`BronzeAdapter`] (or reuse [`WholeFileUtf8Adapter`]).  
//! 3. Register in [`builtin_adapters`].  
//! 4. Tests + matrix row in `docs/BRONZE_ADAPTER_MATRIX.md`.
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
use std::path::Path;

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

/// Look up the built-in adapter for `format`.
pub fn adapter_for(format: SourceFormat) -> Result<&'static dyn BronzeAdapter> {
    for a in builtin_adapters() {
        if a.format() == format {
            return Ok(*a);
        }
    }
    bail!(
        "E_RBT_SOURCE_FORMAT: no bronze adapter registered for source_format='{}'. \
         Supported: {}. See docs/BRONZE_ADAPTERS.md.",
        format.as_str(),
        SourceFormat::all_names().join(", ")
    )
}

/// Decode via the registry (public so hosts can unit-test adapters without LakeScanner).
pub fn read_with_adapter(path: &Path, req: &ScanRequest) -> Result<Vec<RecordBatch>> {
    adapter_for(req.format)?.read_file(path, req)
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

    #[test]
    fn html_whole_file_row() -> Result<()> {
        let dir = tempdir()?;
        let p = dir.path().join("page.html");
        std::fs::write(&p, b"<html><body>hi</body></html>")?;
        let req = ScanRequest {
            project_dir: dir.path().to_path_buf(),
            scan_path: p.to_string_lossy().into(),
            format: SourceFormat::Html,
            paths: vec![],
            toml_rows_key: None,
            partition_by: vec![],
            require_partitions: Default::default(),
            require_partitions_in: Default::default(),
            path_glob: vec![],
            inject_source_path: false,
            roots: Default::default(),
            protobuf_max_payload_bytes: 1 << 20,
            allow_empty: false,
        };
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
        let req = ScanRequest {
            project_dir: dir.path().to_path_buf(),
            scan_path: p.to_string_lossy().into(),
            format: SourceFormat::Robots,
            paths: vec![],
            toml_rows_key: None,
            partition_by: vec![],
            require_partitions: Default::default(),
            require_partitions_in: Default::default(),
            path_glob: vec![],
            inject_source_path: false,
            roots: Default::default(),
            protobuf_max_payload_bytes: 1 << 20,
            allow_empty: false,
        };
        let batches = read_with_adapter(&p, &req)?;
        assert_eq!(batches[0].num_rows(), 1);
        Ok(())
    }
}

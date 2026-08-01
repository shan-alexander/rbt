//! `rbt::scan`: Multi-format bronze lake scanners producing Arrow `RecordBatch`es.
//!
//! Formats: JSONL (jshift), JSON, Parquet, CSV, Arrow IPC (file/stream), log, txt, TOML.
//! Parts directories: [`parts`] for multi-file parquet tables (P6).

pub mod parts;

use crate::core::frontmatter::{SourceFormat, StagingFrontmatter};
use crate::core::paths::{resolve_project_path, validate_glob_patterns, PathGlobSet};
use crate::core::project::{ScanConfig, DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES};
use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{ArrayRef, BinaryBuilder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Scan request derived from staging frontmatter + project root.
#[derive(Debug, Clone)]
pub struct ScanRequest {
    pub project_dir: PathBuf,
    pub scan_path: String,
    pub format: SourceFormat,
    /// jshift / projection paths (JSONL selective extract).
    pub paths: Vec<String>,
    /// TOML array-of-tables key (optional).
    pub toml_rows_key: Option<String>,
    /// Hive-style partition keys to inject as Utf8 columns from the file path.
    pub partition_by: Vec<String>,
    /// Keep only files whose hive path matches these partition values.
    pub require_partitions: std::collections::HashMap<String, String>,
    /// Filename / relative globs under scan_path (OR). Empty = all format matches.
    pub path_glob: Vec<String>,
    /// Inject `_source_path` column (absolute path of the bronze file).
    pub inject_source_path: bool,
    /// Named roots for `$name` expansion in `scan_path` (from project config).
    pub roots: HashMap<String, String>,
    /// Max bytes for one opaque protobuf file (from `scan.protobuf_max_payload_bytes`).
    pub protobuf_max_payload_bytes: u64,
    /// When true, missing roots / zero files after filters return empty list (no error).
    pub allow_empty: bool,
}

impl ScanRequest {
    pub fn from_frontmatter(
        project_dir: impl AsRef<Path>,
        fm: &StagingFrontmatter,
    ) -> Result<Self> {
        Self::from_frontmatter_with_roots(project_dir, fm, HashMap::new())
    }

    pub fn from_frontmatter_with_roots(
        project_dir: impl AsRef<Path>,
        fm: &StagingFrontmatter,
        roots: HashMap<String, String>,
    ) -> Result<Self> {
        Self::from_frontmatter_with_config(project_dir, fm, roots, &ScanConfig::default())
    }

    pub fn from_frontmatter_with_config(
        project_dir: impl AsRef<Path>,
        fm: &StagingFrontmatter,
        roots: HashMap<String, String>,
        scan_cfg: &ScanConfig,
    ) -> Result<Self> {
        let scan_path = fm
            .scan_path
            .as_ref()
            .context(
                "E_RBT_BRONZE_SCAN_PATH_MISSING: frontmatter.scan_path is required for bronze scan",
            )?
            .clone();
        let format = fm.resolve_format().with_context(|| {
            format!("E_RBT_BRONZE_FORMAT: cannot resolve source_format for scan_path '{scan_path}'")
        })?;
        let path_glob = fm.path_glob.clone().unwrap_or_default();
        validate_glob_patterns(&path_glob).with_context(|| {
            format!("E_RBT_PATH_GLOB_INVALID: bad path_glob on scan_path '{scan_path}'")
        })?;
        Ok(Self {
            project_dir: project_dir.as_ref().to_path_buf(),
            scan_path,
            format,
            paths: fm.paths.clone().unwrap_or_default(),
            toml_rows_key: fm.toml_rows_key.clone(),
            partition_by: fm.partition_by.clone().unwrap_or_default(),
            require_partitions: fm.require_partitions.clone().unwrap_or_default(),
            path_glob,
            inject_source_path: fm.inject_source_path.unwrap_or(false),
            roots,
            protobuf_max_payload_bytes: scan_cfg.protobuf_max_payload_bytes,
            allow_empty: matches!(
                fm.on_missing_policy(),
                crate::core::run_scope::OnMissing::Empty
            ),
        })
    }

    pub fn resolved_path(&self) -> Result<PathBuf> {
        resolve_project_path(&self.project_dir, &self.scan_path, &self.roots)
    }
}

/// Multi-format lake scanner.
pub struct LakeScanner {
    pub paths: Vec<String>,
}

impl LakeScanner {
    pub fn new(paths: Vec<String>) -> Self {
        Self { paths }
    }

    pub fn from_request(req: &ScanRequest) -> Self {
        Self {
            paths: req.paths.clone(),
        }
    }

    /// Resolve scan root and list bronze files after partition/glob filters.
    pub fn list_files(&self, req: &ScanRequest) -> Result<(PathBuf, Vec<PathBuf>)> {
        let root = req.resolved_path().with_context(|| {
            format!(
                "E_RBT_BRONZE_PATH: failed resolving scan_path '{}' \
                 (project_dir={}). Check absolute paths and `roots:` templates.",
                req.scan_path,
                req.project_dir.display()
            )
        })?;
        if !root.exists() {
            if req.allow_empty {
                return Ok((root, Vec::new()));
            }
            bail!(
                "E_RBT_BRONZE_SCAN_PATH_NOT_FOUND: bronze scan_path does not exist: {} \
                 (resolved from '{}'). Hint: verify the lake path and `$root` expansion, \
                 or set on_missing: empty for optional artifact families.",
                root.display(),
                req.scan_path
            );
        }

        let mut files = Vec::new();
        collect_files_for_format(&root, req.format, &mut files)?;
        if !req.require_partitions.is_empty() {
            files.retain(|f| path_matches_require_partitions(f, &root, &req.require_partitions));
        }
        if !req.path_glob.is_empty() {
            let glob_set = PathGlobSet::compile(&req.path_glob)?;
            files.retain(|f| glob_set.matches(f, &root));
        }
        if files.is_empty() {
            if req.allow_empty {
                return Ok((root, Vec::new()));
            }
            bail!(
                "E_RBT_BRONZE_SCAN_EMPTY: no {} files under {} after filters \
                 (require_partitions={:?}, path_glob={:?}). \
                 Hint: path_glob disables DataFusion listing pushdown and uses the \
                 scan→MemTable/spill path; check filename patterns and hive partitions, \
                 or set on_missing: empty for optional artifact families.",
                req.format.as_str(),
                root.display(),
                req.require_partitions,
                req.path_glob
            );
        }
        Ok((root, files))
    }

    /// Scan using a full [`ScanRequest`] (format-aware). Loads all batches into memory.
    ///
    /// When `allow_empty` and no files match, returns an empty `Vec` (caller builds empty schema).
    pub async fn scan(&self, req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        let (root, files) = self.list_files(req)?;
        if files.is_empty() {
            tracing::info!(
                "Bronze scan: 0 file(s) under {} (format={}, allow_empty={})",
                root.display(),
                req.format,
                req.allow_empty
            );
            return Ok(Vec::new());
        }
        tracing::info!(
            "Bronze scan: {} file(s) under {} (format={}, globs={:?})",
            files.len(),
            root.display(),
            req.format,
            req.path_glob
        );

        let mut batches = Vec::new();
        for file_path in files {
            for batch in self.read_enriched(&file_path, &root, req)? {
                batches.push(batch);
            }
        }
        Ok(batches)
    }

    /// Stream bronze files into a single Parquet file (atomic publish).
    ///
    /// Peak memory ≈ one source file's batches + Parquet encoder — not the full hive tree.
    /// Used for Arrow IPC multi-file bronze when `scan.spill_arrow_ipc` is true.
    pub fn scan_spill_to_parquet(
        &self,
        req: &ScanRequest,
        dest: &Path,
        opts: &crate::materializer::MaterializeWriteOptions,
    ) -> Result<crate::materializer::StreamWriteStats> {
        use crate::materializer::{atomic_publish, partial_path_for};
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;
        use std::fs::{self, File};
        use std::io::BufWriter;

        let (root, files) = self.list_files(req)?;
        tracing::info!(
            "Bronze spill→parquet: {} file(s) under {} → {} (format={})",
            files.len(),
            root.display(),
            dest.display(),
            req.format
        );

        let partial = partial_path_for(dest);
        if partial.exists() {
            let _ = fs::remove_file(&partial);
        }
        if let Some(parent) = partial.parent() {
            fs::create_dir_all(parent)?;
        }

        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(opts.max_row_group_rows.max(1)))
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer: Option<ArrowWriter<BufWriter<File>>> = None;
        let mut schema: Option<SchemaRef> = None;
        let mut rows = 0usize;
        let mut batches_n = 0usize;

        let mut write_loop = || -> Result<()> {
            for file_path in &files {
                let file_batches = self.read_enriched(file_path, &root, req).with_context(|| {
                    format!(
                        "E_RBT_BRONZE_SPILL: read {} for spill",
                        file_path.display()
                    )
                })?;
                for batch in file_batches {
                    if batch.num_rows() == 0 && batch.num_columns() == 0 {
                        continue;
                    }
                    if writer.is_none() {
                        schema = Some(batch.schema());
                        let file = File::create(&partial).with_context(|| {
                            format!(
                                "E_RBT_BRONZE_SPILL: create partial {}",
                                partial.display()
                            )
                        })?;
                        let buf = BufWriter::with_capacity(8 * 1024 * 1024, file);
                        writer = Some(
                            ArrowWriter::try_new(buf, batch.schema(), Some(props.clone()))
                                .with_context(|| {
                                    format!(
                                        "E_RBT_BRONZE_SPILL: ArrowWriter for {}",
                                        partial.display()
                                    )
                                })?,
                        );
                    }
                    let w = writer.as_mut().unwrap();
                    w.write(&batch).with_context(|| {
                        format!(
                            "E_RBT_BRONZE_SPILL: write batch from {}",
                            file_path.display()
                        )
                    })?;
                    rows += batch.num_rows();
                    batches_n += 1;
                    if w.in_progress_size() >= opts.max_row_group_bytes {
                        w.flush()?;
                    }
                    // batch dropped
                }
            }
            Ok(())
        };

        if let Err(e) = write_loop() {
            if let Some(w) = writer.take() {
                let _ = w.close();
            }
            let _ = fs::remove_file(&partial);
            return Err(e);
        }

        if let Some(w) = writer.take() {
            w.close().with_context(|| {
                format!("E_RBT_BRONZE_SPILL: close writer {}", partial.display())
            })?;
            atomic_publish(&partial, dest)?;
        } else {
            // Zero rows but known schema is rare; fail if we never saw a batch.
            let _ = schema;
            bail!(
                "E_RBT_BRONZE_SPILL: no batches produced for spill to {} \
                 ({} candidate files)",
                dest.display(),
                files.len()
            );
        }

        let bytes_written = std::fs::metadata(dest).map(|m| m.len()).unwrap_or(0);
        tracing::info!(
            "Bronze spill complete: {} rows, {} batches, {} bytes → {}",
            rows,
            batches_n,
            bytes_written,
            dest.display()
        );
        Ok(crate::materializer::StreamWriteStats {
            rows,
            batches: batches_n,
            path: dest.to_path_buf(),
            bytes_written,
            validation: crate::testing::ValidationResult {
                total_rows: rows,
                passed_assertions: 0,
                failed_assertions: 0,
                errors: Vec::new(),
            },
        })
    }

    fn read_enriched(
        &self,
        file_path: &Path,
        root: &Path,
        req: &ScanRequest,
    ) -> Result<Vec<RecordBatch>> {
        let file_batches = self
            .read_file(file_path, req)
            .with_context(|| format!("Failed reading bronze file {}", file_path.display()))?;
        let mut out = Vec::with_capacity(file_batches.len());
        for batch in file_batches {
            let mut batch = if req.partition_by.is_empty() {
                batch
            } else {
                inject_hive_partitions(batch, file_path, root, &req.partition_by)?
            };
            if req.inject_source_path {
                batch = inject_source_path_column(batch, file_path)?;
            }
            out.push(batch);
        }
        Ok(out)
    }

    /// Legacy helper: recurse path and read by extension with an explicit schema
    /// (used by unit tests and jshift-projected JSONL).
    pub async fn scan_path(
        &self,
        path: impl AsRef<Path>,
        schema: SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        let mut files = Vec::new();
        collect_files(path.as_ref(), &mut files)?;
        let mut batches = Vec::new();

        for file_path in files {
            let ext = file_path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            match ext.as_str() {
                "parquet" => batches.extend(read_parquet(&file_path, Some(schema.clone()))?),
                "json" | "jsonl" | "ndjson" => {
                    let bytes = std::fs::read(&file_path)?;
                    let extractor = crate::json::JShiftExtractor::new(self.paths.clone());
                    batches.push(extractor.extract_jsonl(&bytes, schema.clone())?);
                }
                "csv" | "tsv" => batches.extend(read_csv(&file_path, Some(schema.clone()))?),
                "log" | "txt" | "text" | "md" => {
                    batches.push(read_line_oriented(&file_path)?);
                }
                "toml" => batches.push(read_toml(&file_path, None)?),
                "arrow" | "arrows" | "ipc" | "feather" => {
                    batches.extend(read_arrow_ipc_auto(&file_path)?);
                }
                _ => {
                    tracing::debug!("Skipping unsupported file: {:?}", file_path);
                }
            }
        }
        Ok(batches)
    }

    fn read_file(&self, file_path: &Path, req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        match req.format {
            SourceFormat::Parquet => read_parquet(file_path, None),
            SourceFormat::Protobuf => Ok(vec![read_protobuf_opaque(
                file_path,
                req.protobuf_max_payload_bytes,
            )?]),
            SourceFormat::Csv => read_csv(file_path, None),
            SourceFormat::Jsonl | SourceFormat::Json => {
                if !self.paths.is_empty() {
                    let schema = utf8_schema_from_paths(&self.paths);
                    let bytes = std::fs::read(file_path)?;
                    let extractor = crate::json::JShiftExtractor::new(self.paths.clone());
                    // JSONL extractor works line-wise; for single JSON array, expand first
                    if req.format == SourceFormat::Json {
                        let expanded = expand_json_document_to_jsonl(&bytes)?;
                        Ok(vec![extractor.extract_jsonl(&expanded, schema)?])
                    } else {
                        Ok(vec![extractor.extract_jsonl(&bytes, schema)?])
                    }
                } else {
                    // Schema-free path: use Arrow JSON reader (line-delimited)
                    read_json_arrow(file_path, req.format)
                }
            }
            // Real lakes often write stream IPC with a `.arrow` extension.
            SourceFormat::ArrowIpc | SourceFormat::ArrowIpcStream => read_arrow_ipc_auto(file_path),
            SourceFormat::Log | SourceFormat::Txt => Ok(vec![read_line_oriented(file_path)?]),
            SourceFormat::Toml => Ok(vec![read_toml(file_path, req.toml_rows_key.as_deref())?]),
        }
    }
}

fn utf8_schema_from_paths(paths: &[String]) -> SchemaRef {
    Arc::new(Schema::new(
        paths
            .iter()
            .map(|p| Field::new(p.as_str(), DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ))
}

fn expand_json_document_to_jsonl(bytes: &[u8]) -> Result<Vec<u8>> {
    let v: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| anyhow!("Invalid JSON document: {}", e))?;
    match v {
        serde_json::Value::Array(items) => {
            let mut out = Vec::new();
            for item in items {
                out.extend(serde_json::to_vec(&item)?);
                out.push(b'\n');
            }
            Ok(out)
        }
        other => {
            let mut out = serde_json::to_vec(&other)?;
            out.push(b'\n');
            Ok(out)
        }
    }
}

fn collect_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_file() {
        files.push(path.to_path_buf());
    } else if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            collect_files(&entry.path(), files)?;
        }
    }
    Ok(())
}

fn extension_matches(format: SourceFormat, ext: &str) -> bool {
    let ext = ext.to_ascii_lowercase();
    match format {
        SourceFormat::Jsonl => matches!(ext.as_str(), "jsonl" | "ndjson"),
        SourceFormat::Json => ext == "json",
        SourceFormat::Parquet => matches!(ext.as_str(), "parquet" | "pq"),
        SourceFormat::Csv => matches!(ext.as_str(), "csv" | "tsv"),
        SourceFormat::ArrowIpc => matches!(ext.as_str(), "arrow" | "arrows" | "ipc" | "feather"),
        SourceFormat::ArrowIpcStream => {
            matches!(
                ext.as_str(),
                "arrow" | "arrows" | "ipc" | "arrows_stream" | "ipc_stream"
            )
        }
        SourceFormat::Log => ext == "log",
        SourceFormat::Txt => matches!(ext.as_str(), "txt" | "text" | "md"),
        SourceFormat::Toml => ext == "toml",
        SourceFormat::Protobuf => matches!(ext.as_str(), "pb" | "protobuf" | "protobin"),
    }
}

/// Opaque protobuf bronze: one row per file with path + raw bytes.
///
/// Typed message decode is intentionally deferred (schema registry / Rust models).
/// `max_bytes` defaults to 1 GiB via project `scan.protobuf_max_payload_bytes`.
fn read_protobuf_opaque(path: &Path, max_bytes: u64) -> Result<RecordBatch> {
    let meta = std::fs::metadata(path).with_context(|| {
        format!(
            "E_RBT_PROTOBUF_IO: cannot stat protobuf file {}",
            path.display()
        )
    })?;
    let len = meta.len();
    if len > max_bytes {
        bail!(
            "E_RBT_PROTOBUF_TOO_LARGE: file {} is {len} bytes, exceeds \
             scan.protobuf_max_payload_bytes={max_bytes} (default {} = 1 GiB). \
             Raise `scan.protobuf_max_payload_bytes` in rbt_project.yml only if intentional.",
            path.display(),
            DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES
        );
    }

    let mut file = File::open(path)
        .with_context(|| format!("E_RBT_PROTOBUF_IO: open protobuf file {}", path.display()))?;
    let mut payload = Vec::with_capacity(len as usize);
    file.read_to_end(&mut payload)
        .with_context(|| format!("E_RBT_PROTOBUF_IO: read protobuf file {}", path.display()))?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("_source_path", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("payload_len", DataType::Int64, false),
    ]));

    let mut path_b = StringBuilder::new();
    let mut bin_b = BinaryBuilder::new();
    let mut len_b = Int64Builder::new();
    path_b.append_value(path.to_string_lossy().as_ref());
    bin_b.append_value(&payload);
    len_b.append_value(payload.len() as i64);

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(path_b.finish()),
            Arc::new(bin_b.finish()),
            Arc::new(len_b.finish()),
        ],
    )?)
}

fn collect_files_for_format(
    path: &Path,
    format: SourceFormat,
    files: &mut Vec<PathBuf>,
) -> Result<()> {
    if path.is_file() {
        files.push(path.to_path_buf());
        return Ok(());
    }
    if path.is_dir() {
        let mut all = Vec::new();
        collect_files(path, &mut all)?;
        for f in all {
            let ext = f.extension().and_then(|e| e.to_str()).unwrap_or("");
            if extension_matches(format, ext) {
                files.push(f);
            }
        }
        // If directory has files but none matched extension, and format is Log/Txt,
        // still allow extensionless? Skip for now.
        return Ok(());
    }
    bail!(
        "scan path is neither file nor directory: {}",
        path.display()
    );
}

fn read_parquet(path: &Path, projection: Option<SchemaRef>) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = if let Some(schema) = projection {
        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();
        let arrow_schema = builder.schema();
        let mut indices = Vec::new();
        for field in schema.fields() {
            if let Some((idx, _)) =
                parquet::arrow::parquet_column(&schema_descr, arrow_schema, field.name())
            {
                indices.push(idx);
            }
        }
        let mask = parquet::arrow::ProjectionMask::leaves(&schema_descr, indices);
        builder.with_projection(mask).build()?
    } else {
        builder.build()?
    };
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

fn read_csv(path: &Path, schema: Option<SchemaRef>) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let mut batches = Vec::new();
    if let Some(schema) = schema {
        let reader = arrow::csv::ReaderBuilder::new(schema)
            .with_header(true)
            .build(file)?;
        for batch in reader {
            batches.push(batch?);
        }
    } else {
        // Infer schema from first rows
        let format = arrow::csv::reader::Format::default().with_header(true);
        let (inferred, _) = format.infer_schema(File::open(path)?, Some(1024))?;
        let schema = Arc::new(inferred);
        let file = File::open(path)?;
        let reader = arrow::csv::ReaderBuilder::new(schema)
            .with_header(true)
            .build(file)?;
        for batch in reader {
            batches.push(batch?);
        }
    }
    Ok(batches)
}

fn read_json_arrow(path: &Path, format: SourceFormat) -> Result<Vec<RecordBatch>> {
    let data = std::fs::read(path)?;
    let data = if format == SourceFormat::Json {
        expand_json_document_to_jsonl(&data)?
    } else {
        data
    };
    read_json_infer_from_bytes(&data)
}

fn read_json_infer_from_bytes(data: &[u8]) -> Result<Vec<RecordBatch>> {
    use arrow::json::reader::infer_json_schema_from_seekable;
    let mut cursor = std::io::Cursor::new(data);
    let (schema, _n) = infer_json_schema_from_seekable(&mut cursor, Some(1024))?;
    cursor.set_position(0);
    let schema = Arc::new(schema);
    let reader = arrow::json::ReaderBuilder::new(schema).build(cursor)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

fn read_arrow_ipc_file(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let reader = arrow::ipc::reader::FileReader::try_new(file, None)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

fn read_arrow_ipc_stream(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let reader = arrow::ipc::reader::StreamReader::try_new(file, None)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Prefer random-access IPC file; fall back to stream (common for `.arrow` lake dumps).
fn read_arrow_ipc_auto(path: &Path) -> Result<Vec<RecordBatch>> {
    match read_arrow_ipc_file(path) {
        Ok(batches) => Ok(batches),
        Err(file_err) => read_arrow_ipc_stream(path).with_context(|| {
            format!(
                "Arrow IPC file and stream readers both failed for {} (file error: {file_err})",
                path.display()
            )
        }),
    }
}

/// Parse `key=value` segments from `file` relative to `root`.
pub fn parse_hive_partitions(
    file: &Path,
    root: &Path,
) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    let rel = file.strip_prefix(root).unwrap_or(file);
    for comp in rel.components() {
        if let std::path::Component::Normal(os) = comp {
            let s = os.to_string_lossy();
            if let Some((k, v)) = s.split_once('=') {
                if !k.is_empty() {
                    out.insert(k.to_string(), v.to_string());
                }
            }
        }
    }
    out
}

fn path_matches_require_partitions(
    file: &Path,
    root: &Path,
    require: &std::collections::HashMap<String, String>,
) -> bool {
    if require.is_empty() {
        return true;
    }
    let parts = parse_hive_partitions(file, root);
    require
        .iter()
        .all(|(k, v)| parts.get(k).map(|pv| pv == v).unwrap_or(false))
}

/// Append hive partition columns (Utf8) for keys in `partition_by` that are not already present.
fn inject_hive_partitions(
    batch: RecordBatch,
    file: &Path,
    root: &Path,
    partition_by: &[String],
) -> Result<RecordBatch> {
    if partition_by.is_empty() {
        return Ok(batch);
    }
    let parts = parse_hive_partitions(file, root);
    let n = batch.num_rows();
    let mut fields: Vec<arrow::datatypes::Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    for key in partition_by {
        if batch.schema().index_of(key).is_ok() {
            // Column already in payload (e.g. symbol); keep file data, do not overwrite.
            continue;
        }
        let val = parts.get(key).map(|s| s.as_str());
        let mut b = StringBuilder::with_capacity(n, n * 8);
        for _ in 0..n {
            match val {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
        fields.push(Field::new(key.as_str(), DataType::Utf8, true));
        columns.push(Arc::new(b.finish()) as ArrayRef);
    }

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Append `_source_path` Utf8 with the absolute bronze file path (for latest-wins dedupe).
fn inject_source_path_column(batch: RecordBatch, file: &Path) -> Result<RecordBatch> {
    if batch.schema().index_of("_source_path").is_ok() {
        return Ok(batch);
    }
    let n = batch.num_rows();
    let path_str = file.to_string_lossy();
    let mut b = StringBuilder::with_capacity(n, n * path_str.len().max(16));
    for _ in 0..n {
        b.append_value(path_str.as_ref());
    }
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new("_source_path", DataType::Utf8, false));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(b.finish()) as ArrayRef);
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// Line-oriented bronze: `.log`, `.txt`, llms.txt-style docs.
/// Schema: `line_no` Int64, `content` Utf8.
fn read_line_oriented(path: &Path) -> Result<RecordBatch> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut line_nos = Int64Builder::new();
    let mut contents = StringBuilder::new();
    let mut n: i64 = 0;
    for line in reader.lines() {
        let line = line?;
        n += 1;
        line_nos.append_value(n);
        contents.append_value(line);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("line_no", DataType::Int64, false),
        Field::new("content", DataType::Utf8, false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(line_nos.finish()) as ArrayRef,
            Arc::new(contents.finish()) as ArrayRef,
        ],
    )?)
}

fn read_toml(path: &Path, rows_key: Option<&str>) -> Result<RecordBatch> {
    let text = std::fs::read_to_string(path)?;
    let value: toml::Value = text
        .parse()
        .with_context(|| format!("Invalid TOML: {}", path.display()))?;

    let rows: Vec<toml::map::Map<String, toml::Value>> = match &value {
        toml::Value::Table(table) => {
            if let Some(key) = rows_key {
                match table.get(key) {
                    Some(toml::Value::Array(arr)) => array_of_tables(arr)?,
                    other => bail!(
                        "toml_rows_key '{}' is not an array of tables (got {:?})",
                        key,
                        other.map(|v| v.type_str())
                    ),
                }
            } else if let Some((_k, toml::Value::Array(arr))) = table
                .iter()
                .find(|(_, v)| matches!(v, toml::Value::Array(a) if a.iter().all(|x| x.is_table())))
            {
                array_of_tables(arr)?
            } else {
                // Single-row: top-level scalars / nested stringified
                vec![table.clone()]
            }
        }
        toml::Value::Array(arr) => array_of_tables(arr)?,
        other => bail!("Unsupported top-level TOML type: {}", other.type_str()),
    };

    if rows.is_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("empty", DataType::Utf8, true)]));
        return Ok(RecordBatch::try_new(
            schema,
            vec![Arc::new(StringBuilder::new().finish()) as ArrayRef],
        )?);
    }

    // Column union
    let mut col_names: Vec<String> = Vec::new();
    for row in &rows {
        for k in row.keys() {
            if !col_names.iter().any(|c| c == k) {
                col_names.push(k.clone());
            }
        }
    }

    let mut builders: Vec<StringBuilder> = col_names
        .iter()
        .map(|_| StringBuilder::with_capacity(rows.len(), rows.len() * 16))
        .collect();

    for row in &rows {
        for (i, col) in col_names.iter().enumerate() {
            match row.get(col) {
                Some(v) => builders[i].append_value(toml_value_to_string(v)),
                None => builders[i].append_null(),
            }
        }
    }

    let fields: Vec<Field> = col_names
        .iter()
        .map(|c| Field::new(c.as_str(), DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let arrays: Vec<ArrayRef> = builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as ArrayRef)
        .collect();
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn array_of_tables(arr: &[toml::Value]) -> Result<Vec<toml::map::Map<String, toml::Value>>> {
    let mut rows = Vec::with_capacity(arr.len());
    for (i, item) in arr.iter().enumerate() {
        match item {
            toml::Value::Table(t) => rows.push(t.clone()),
            other => bail!(
                "TOML array element {} is not a table (got {})",
                i,
                other.type_str()
            ),
        }
    }
    Ok(rows)
}

fn toml_value_to_string(v: &toml::Value) -> String {
    match v {
        toml::Value::String(s) => s.clone(),
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        toml::Value::Datetime(d) => d.to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_lake_scanner_multi_format() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let dir_path = temp_dir.path();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        std::fs::write(
            dir_path.join("file1.jsonl"),
            b"{\"id\": 1, \"name\": \"Alice\"}\n{\"id\": 2, \"name\": \"Bob\"}\n",
        )?;
        std::fs::write(dir_path.join("file2.csv"), b"id,name\n3,Charlie\n4,Dave\n")?;

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![5, 6])),
                Arc::new(StringArray::from(vec!["Eve", "Frank"])),
            ],
        )?;
        let file = File::create(dir_path.join("file3.parquet"))?;
        let mut writer =
            parquet::arrow::arrow_writer::ArrowWriter::try_new(file, schema.clone(), None)?;
        writer.write(&batch)?;
        writer.close()?;

        std::fs::write(dir_path.join("app.log"), "info boot\nwarn disk\n")?;
        std::fs::write(
            dir_path.join("meta.toml"),
            r#"
[[records]]
id = "1"
name = "toml_a"
[[records]]
id = "2"
name = "toml_b"
"#,
        )?;

        let scanner = LakeScanner::new(vec!["id".to_string(), "name".to_string()]);
        let batches = scanner.scan_path(dir_path, schema.clone()).await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 2 jsonl + 2 csv + 2 parquet + 2 log + 2 toml = 10
        assert_eq!(total_rows, 10);
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_request_jsonl() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("trades.jsonl");
        std::fs::write(
            &path,
            r#"{"ticker":"NVDA","price":1.0}
{"ticker":"AAPL","price":2.0}
"#,
        )?;

        let fm = StagingFrontmatter {
            source_format: Some(SourceFormat::Jsonl),
            scan_path: Some(path.file_name().unwrap().to_string_lossy().into()),
            ..Default::default()
        };
        let req = ScanRequest::from_frontmatter(temp.path(), &fm)?;
        let scanner = LakeScanner::from_request(&req);
        let batches = scanner.scan(&req).await?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_hive_partition_inject_and_filter() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path();
        let dir = root.join("symbol=NVDA").join("timeframe=1m");
        std::fs::create_dir_all(&dir)?;
        // minimal IPC stream with one utf8 column
        let schema = Arc::new(Schema::new(vec![Field::new(
            "close",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0])) as ArrayRef],
        )?;
        let path = dir.join("chunk.arrow");
        {
            let file = File::create(&path)?;
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(file, &batch.schema())?;
            writer.write(&batch)?;
            writer.finish()?;
        }
        // noise partition that should be filtered out
        let other = root.join("symbol=AAPL").join("timeframe=1d");
        std::fs::create_dir_all(&other)?;
        {
            let file = File::create(other.join("chunk.arrow"))?;
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(file, &batch.schema())?;
            writer.write(&batch)?;
            writer.finish()?;
        }

        let mut require = std::collections::HashMap::new();
        require.insert("timeframe".into(), "1m".into());
        let fm = StagingFrontmatter {
            source_format: Some(SourceFormat::ArrowIpcStream),
            scan_path: Some(".".into()),
            partition_by: Some(vec!["symbol".into(), "timeframe".into()]),
            require_partitions: Some(require),
            ..Default::default()
        };
        let req = ScanRequest::from_frontmatter(root, &fm)?;
        let scanner = LakeScanner::from_request(&req);
        let batches = scanner.scan(&req).await?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
        let schema = batches[0].schema();
        assert!(schema.index_of("timeframe").is_ok());
        assert!(schema.index_of("symbol").is_ok());
        Ok(())
    }

    #[test]
    fn test_line_oriented_and_toml() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let txt = temp.path().join("llms.txt");
        std::fs::write(&txt, "# Title\n\nSome doc line\n")?;
        let batch = read_line_oriented(&txt)?;
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().field(0).name(), "line_no");
        assert_eq!(batch.schema().field(1).name(), "content");

        let toml_path = temp.path().join("cfg.toml");
        std::fs::write(
            &toml_path,
            r#"
[[items]]
k = "a"
[[items]]
k = "b"
"#,
        )?;
        let tbatch = read_toml(&toml_path, Some("items"))?;
        assert_eq!(tbatch.num_rows(), 2);
        let col = tbatch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(col.value(0) == "a" || col.value(1) == "a");
        Ok(())
    }

    #[tokio::test]
    async fn test_path_glob_filters_artifacts() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path();
        // Multi-artifact hive (same layout pattern as multi-domain landing zones)
        let d1 = root
            .join("domain=x.com")
            .join("report_date=2026-07-29")
            .join("run_id=r1")
            .join("raw_snoop");
        std::fs::create_dir_all(&d1)?;
        // two different parquet artifact types
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        for (name, ids) in [
            ("crawlplan.parquet", vec![1i64]),
            ("other.parquet", vec![9i64]),
        ] {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))])?;
            let f = File::create(d1.join(name))?;
            let mut w =
                parquet::arrow::arrow_writer::ArrowWriter::try_new(f, schema.clone(), None)?;
            w.write(&batch)?;
            w.close()?;
        }

        let fm = StagingFrontmatter {
            source_format: Some(SourceFormat::Parquet),
            scan_path: Some(".".into()),
            path_glob: Some(vec!["**/crawlplan.parquet".into()]),
            partition_by: Some(vec!["domain".into()]),
            inject_source_path: Some(true),
            ..Default::default()
        };
        let req = ScanRequest::from_frontmatter(root, &fm)?;
        let scanner = LakeScanner::from_request(&req);
        let batches = scanner.scan(&req).await?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "only crawlplan.parquet rows");
        assert!(batches[0].schema().index_of("domain").is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_protobuf_opaque_scan() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let pb = temp.path().join("msg.pb");
        std::fs::write(&pb, b"\x08\x96\x01")?; // arbitrary bytes
        let fm = StagingFrontmatter {
            source_format: Some(SourceFormat::Protobuf),
            scan_path: Some(pb.file_name().unwrap().to_string_lossy().into()),
            ..Default::default()
        };
        let req = ScanRequest::from_frontmatter(temp.path(), &fm)?;
        assert_eq!(
            req.protobuf_max_payload_bytes,
            DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES
        );
        let scanner = LakeScanner::from_request(&req);
        let batches = scanner.scan(&req).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "_source_path");
        assert_eq!(batches[0].schema().field(1).name(), "payload");
        Ok(())
    }

    #[tokio::test]
    async fn test_protobuf_respects_max_payload_bytes() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let pb = temp.path().join("big.pb");
        std::fs::write(&pb, vec![0u8; 64])?;
        let fm = StagingFrontmatter {
            source_format: Some(SourceFormat::Protobuf),
            scan_path: Some("big.pb".into()),
            ..Default::default()
        };
        let scan_cfg = ScanConfig {
            protobuf_max_payload_bytes: 16,
            ..Default::default()
        };
        let req = ScanRequest::from_frontmatter_with_config(
            temp.path(),
            &fm,
            HashMap::new(),
            &scan_cfg,
        )?;
        let scanner = LakeScanner::from_request(&req);
        let err = scanner.scan(&req).await.unwrap_err();
        // anyhow chains: outer "Failed reading bronze file …" + inner E_RBT_PROTOBUF_TOO_LARGE
        let full = format!("{err:#}");
        assert!(
            full.contains("E_RBT_PROTOBUF_TOO_LARGE"),
            "expected size-cap error in chain, got: {full}"
        );
        assert!(full.contains("protobuf_max_payload_bytes"));
        Ok(())
    }

    #[tokio::test]
    async fn test_path_glob_single_star_is_not_recursive() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path();
        let deep = root.join("a").join("b").join("raw");
        std::fs::create_dir_all(&deep)?;
        let shallow = root.join("raw");
        std::fs::create_dir_all(&shallow)?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        for (dir, id) in [(&deep, 1i64), (&shallow, 2i64)] {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![id]))])?;
            let f = File::create(dir.join("crawlplan.parquet"))?;
            let mut w =
                parquet::arrow::arrow_writer::ArrowWriter::try_new(f, schema.clone(), None)?;
            w.write(&batch)?;
            w.close()?;
        }
        let fm = StagingFrontmatter {
            source_format: Some(SourceFormat::Parquet),
            scan_path: Some(".".into()),
            // one segment only — should hit raw/crawlplan, not a/b/raw/crawlplan
            path_glob: Some(vec!["*/crawlplan.parquet".into()]),
            ..Default::default()
        };
        let req = ScanRequest::from_frontmatter(root, &fm)?;
        let scanner = LakeScanner::from_request(&req);
        let batches = scanner.scan(&req).await?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "single-star glob must not match deep hive path");
        Ok(())
    }

    #[tokio::test]
    async fn test_absolute_scan_path() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let abs = temp.path().join("abs.jsonl");
        std::fs::write(&abs, b"{\"a\":1}\n{\"a\":2}\n")?;
        let fm = StagingFrontmatter {
            source_format: Some(SourceFormat::Jsonl),
            scan_path: Some(abs.to_string_lossy().into()),
            ..Default::default()
        };
        // project_dir is different from parent of abs
        let proj = tempfile::tempdir()?;
        let req = ScanRequest::from_frontmatter(proj.path(), &fm)?;
        let scanner = LakeScanner::from_request(&req);
        let batches = scanner.scan(&req).await?;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        Ok(())
    }
}

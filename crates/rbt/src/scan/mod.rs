//! `rbt::scan`: Multi-format bronze lake scanners producing Arrow `RecordBatch`es.
//!
//! Formats: JSONL (jshift), JSON, Parquet, CSV, Arrow IPC (file/stream), log, txt, TOML.

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use crate::core::frontmatter::{resolve_scan_path, SourceFormat, StagingFrontmatter};
use std::fs::File;
use std::io::{BufRead, BufReader};
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
    /// Inject `_source_path` column (absolute path of the bronze file).
    pub inject_source_path: bool,
}

impl ScanRequest {
    pub fn from_frontmatter(
        project_dir: impl AsRef<Path>,
        fm: &StagingFrontmatter,
    ) -> Result<Self> {
        let scan_path = fm
            .scan_path
            .as_ref()
            .context("frontmatter.scan_path is required for bronze scan")?
            .clone();
        let format = fm.resolve_format()?;
        Ok(Self {
            project_dir: project_dir.as_ref().to_path_buf(),
            scan_path,
            format,
            paths: fm.paths.clone().unwrap_or_default(),
            toml_rows_key: fm.toml_rows_key.clone(),
            partition_by: fm.partition_by.clone().unwrap_or_default(),
            require_partitions: fm.require_partitions.clone().unwrap_or_default(),
            inject_source_path: fm.inject_source_path.unwrap_or(false),
        })
    }

    pub fn resolved_path(&self) -> PathBuf {
        resolve_scan_path(&self.project_dir, &self.scan_path)
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

    /// Scan using a full [`ScanRequest`] (format-aware).
    pub async fn scan(&self, req: &ScanRequest) -> Result<Vec<RecordBatch>> {
        let root = req.resolved_path();
        if !root.exists() {
            bail!(
                "Bronze scan_path does not exist: {} (resolved from '{}')",
                root.display(),
                req.scan_path
            );
        }

        let mut files = Vec::new();
        collect_files_for_format(&root, req.format, &mut files)?;
        if !req.require_partitions.is_empty() {
            files.retain(|f| path_matches_require_partitions(f, &root, &req.require_partitions));
        }
        if files.is_empty() {
            bail!(
                "No {} files found under {} (after partition filters)",
                req.format.as_str(),
                root.display()
            );
        }

        tracing::info!(
            "Bronze scan: {} file(s) under {} (format={})",
            files.len(),
            root.display(),
            req.format
        );

        let mut batches = Vec::new();
        for file_path in files {
            let file_batches = self
                .read_file(&file_path, req)
                .with_context(|| format!("Failed reading bronze file {}", file_path.display()))?;
            for batch in file_batches {
                let mut batch = if req.partition_by.is_empty() {
                    batch
                } else {
                    inject_hive_partitions(batch, &file_path, &root, &req.partition_by)?
                };
                if req.inject_source_path {
                    batch = inject_source_path_column(batch, &file_path)?;
                }
                batches.push(batch);
            }
        }
        Ok(batches)
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
            SourceFormat::ArrowIpc | SourceFormat::ArrowIpcStream => {
                read_arrow_ipc_auto(file_path)
            }
            SourceFormat::Log | SourceFormat::Txt => Ok(vec![read_line_oriented(file_path)?]),
            SourceFormat::Toml => Ok(vec![read_toml(
                file_path,
                req.toml_rows_key.as_deref(),
            )?]),
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
    let v: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|e| anyhow!("Invalid JSON document: {}", e))?;
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
            matches!(ext.as_str(), "arrow" | "arrows" | "ipc" | "arrows_stream" | "ipc_stream")
        }
        SourceFormat::Log => ext == "log",
        SourceFormat::Txt => matches!(ext.as_str(), "txt" | "text" | "md"),
        SourceFormat::Toml => ext == "toml",
    }
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
    bail!("scan path is neither file nor directory: {}", path.display());
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
pub fn parse_hive_partitions(file: &Path, root: &Path) -> std::collections::HashMap<String, String> {
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
    require.iter().all(|(k, v)| parts.get(k).map(|pv| pv == v).unwrap_or(false))
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
    let mut fields: Vec<arrow::datatypes::Field> =
        batch.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
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
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
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
    let value: toml::Value = text.parse().with_context(|| format!("Invalid TOML: {}", path.display()))?;

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
        return Ok(RecordBatch::try_new(schema, vec![Arc::new(StringBuilder::new().finish()) as ArrayRef])?);
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

    let mut builders: Vec<StringBuilder> =
        col_names.iter().map(|_| StringBuilder::with_capacity(rows.len(), rows.len() * 16)).collect();

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
            other => bail!("TOML array element {} is not a table (got {})", i, other.type_str()),
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
        let schema = Arc::new(Schema::new(vec![Field::new("close", DataType::Float64, true)]));
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
}

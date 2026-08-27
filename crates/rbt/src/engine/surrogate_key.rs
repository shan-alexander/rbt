//! Deterministic surrogate-key kernel (ADR-009 / RBT-A16).
//!
//! Pure hashing + Arrow batch stamp used by SQL UDFs and frontmatter materialize.
//! MIISK ([`SkAlgo::Integer`]) assigns via [`crate::engine::sk_registry`] (durable
//! NK→SK parquet under `.rbt/sk_registry/`).
//!
//! Upsert matching stays on natural grain — never on SK.
//!
//! # Quick reference
//!
//! | Algo | Type | Notes |
//! |------|------|--------|
//! | [`SkAlgo::Balanced`] | `FixedSizeBinary(16)` | Default blake3_128 |
//! | [`SkAlgo::Integer`] | `Int64` | MIISK registry; stamp-only |
//! | [`SkAlgo::Fast64`] | `Int64` | xxh3 hash; bound distinct N |
//! | [`SkAlgo::Safe256`] | `FixedSizeBinary(32)` | Full blake3 |
//! | [`SkAlgo::CompatMd5`] | `FixedSizeBinary(16)` | dbt parity |
//!
//! Unknown member sentinel is **all zeros** (`0i64` / zero digest) for every algo.

use anyhow::{bail, Context, Result};
use arrow::array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int64Array,
    StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use md5::{Digest, Md5};
use std::path::PathBuf;
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64;

use super::sk_registry::{assign_miisk_column, default_registry_path};

/// Domain / null sentinel — frozen for `v1` (see ADR-009).
pub const SK_DOMAIN_PREFIX: &str = "rbt_sk_v1";
pub const SK_NULL_TOKEN: &str = "_rbt_sk_null_";

/// Supported algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SkAlgo {
    /// BLAKE3 truncated to 128 bits → [`DataType::FixedSizeBinary`](16). Default.
    Balanced,
    /// xxHash3-64 → [`DataType::Int64`]. Opt-in for bounded N.
    Fast64,
    /// Full BLAKE3-256 → [`DataType::FixedSizeBinary`](32).
    Safe256,
    /// MD5-128 for dbt parity → [`DataType::FixedSizeBinary`](16).
    CompatMd5,
    /// Durable MIISK Int64 via NK→SK registry (materialize stamp only; not a pure SQL UDF).
    Integer,
}

impl SkAlgo {
    /// Parse case-insensitive name / alias.
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "balanced" | "blake3_128" | "blake3" => Ok(Self::Balanced),
            "fast64" | "xxh3_64" | "xxhash64" | "xxh3" => Ok(Self::Fast64),
            "safe256" | "blake3_256" => Ok(Self::Safe256),
            "compat_md5" | "md5" => Ok(Self::CompatMd5),
            "integer" | "miisk" | "seq" | "sequential" => Ok(Self::Integer),
            other => bail!(
                "E_RBT_SK: unknown surrogate_key_algo '{other}' \
                 (expected balanced|fast64|safe256|compat_md5|integer)"
            ),
        }
    }

    /// Canonical name baked into the hash domain (hash algos only).
    pub fn canonical(self) -> &'static str {
        match self {
            Self::Balanced => "blake3_128",
            Self::Fast64 => "xxh3_64",
            Self::Safe256 => "blake3_256",
            Self::CompatMd5 => "md5",
            Self::Integer => "integer",
        }
    }

    pub fn is_miisk(self) -> bool {
        matches!(self, Self::Integer)
    }

    pub fn arrow_type(self, encoding: SkEncoding) -> DataType {
        match (self, encoding) {
            (Self::Fast64 | Self::Integer, _) => DataType::Int64,
            (_, SkEncoding::Hex) => DataType::Utf8,
            (Self::Balanced | Self::CompatMd5, SkEncoding::Binary) => DataType::FixedSizeBinary(16),
            (Self::Safe256, SkEncoding::Binary) => DataType::FixedSizeBinary(32),
        }
    }

    pub fn digest_len(self) -> usize {
        match self {
            Self::Fast64 | Self::Integer => 8,
            Self::Balanced | Self::CompatMd5 => 16,
            Self::Safe256 => 32,
        }
    }
}

/// Output encoding for digests (ignored for `fast64`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SkEncoding {
    #[default]
    Binary,
    Hex,
}

impl SkEncoding {
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "binary" | "bin" => Ok(Self::Binary),
            "hex" | "hexadecimal" => Ok(Self::Hex),
            other => bail!("E_RBT_SK: unknown surrogate_key_encoding '{other}' (binary|hex)"),
        }
    }
}

/// Resolved frontmatter / stamp config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurrogateKeyConfig {
    pub column: String,
    pub algo: SkAlgo,
    pub encoding: SkEncoding,
    pub grain_cols: Vec<String>,
    pub unknown_member: bool,
    /// Required for [`SkAlgo::Integer`] — durable NK→SK parquet registry path.
    pub registry_path: Option<PathBuf>,
}

impl SurrogateKeyConfig {
    pub fn arrow_type(&self) -> DataType {
        self.algo.arrow_type(self.encoding)
    }

    /// Resolve optional SK stamp from model frontmatter (ADR-009).
    ///
    /// `Ok(None)` when `surrogate_key` unset; error if set without non-empty `grain`.
    /// Call [`Self::with_registry_for_model`] for `integer` algo before stamping.
    pub fn from_frontmatter(
        fm: &crate::core::frontmatter::StagingFrontmatter,
    ) -> Result<Option<Self>> {
        let Some(column) = fm
            .surrogate_key
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let grain = fm
            .grain
            .as_ref()
            .filter(|g| !g.is_empty())
            .with_context(|| {
                format!(
                    "E_RBT_SK: surrogate_key '{column}' requires non-empty frontmatter grain: […] \
                     (ADR-009)"
                )
            })?;
        let algo = match fm.surrogate_key_algo.as_deref() {
            Some(s) => SkAlgo::parse(s)?,
            None => SkAlgo::Balanced,
        };
        let encoding = match fm.surrogate_key_encoding.as_deref() {
            Some(s) => SkEncoding::parse(s)?,
            None => SkEncoding::Binary,
        };
        if algo.is_miisk() && encoding == SkEncoding::Hex {
            bail!("E_RBT_SK: surrogate_key_encoding hex is not valid for algo integer/MIISK");
        }
        Ok(Some(Self {
            column,
            algo,
            encoding,
            grain_cols: grain.clone(),
            unknown_member: fm.unknown_member.unwrap_or(false),
            registry_path: None,
        }))
    }

    /// Bind durable registry path for MIISK (`{project}/.rbt/sk_registry/{model}.parquet`).
    pub fn with_registry_for_model(mut self, project_dir: &std::path::Path, model: &str) -> Self {
        if self.algo.is_miisk() {
            self.registry_path = Some(default_registry_path(project_dir, model));
        }
        self
    }
}

/// Hash one grain row (pre-stringified field values; use [`SK_NULL_TOKEN`] for nulls).
pub fn hash_grain_fields(algo: SkAlgo, fields: &[&str]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64 + fields.iter().map(|f| f.len() + 1).sum::<usize>());
    buf.extend_from_slice(SK_DOMAIN_PREFIX.as_bytes());
    buf.push(b'|');
    buf.extend_from_slice(algo.canonical().as_bytes());
    buf.push(0);
    for f in fields {
        buf.extend_from_slice(f.as_bytes());
        buf.push(0);
    }
    match algo {
        SkAlgo::Balanced => {
            let hash = blake3::hash(&buf);
            hash.as_bytes()[..16].to_vec()
        }
        SkAlgo::Safe256 => blake3::hash(&buf).as_bytes().to_vec(),
        SkAlgo::Fast64 => xxh3_64(&buf).to_le_bytes().to_vec(),
        SkAlgo::CompatMd5 => Md5::digest(&buf).to_vec(),
        SkAlgo::Integer => {
            // Not used for MIISK (registry assigner); keep deterministic stub for tests.
            xxh3_64(&buf).to_le_bytes().to_vec()
        }
    }
}

/// All-zero unknown sentinel bytes for `algo` (raw digest width; not hex).
pub fn unknown_digest(algo: SkAlgo) -> Vec<u8> {
    vec![0u8; algo.digest_len()]
}

/// Unknown as Arrow scalar-friendly value for the configured output type.
pub fn unknown_array(algo: SkAlgo, encoding: SkEncoding, n: usize) -> Result<ArrayRef> {
    let dig = unknown_digest(algo);
    match (algo, encoding) {
        (SkAlgo::Fast64 | SkAlgo::Integer, _) => {
            Ok(Arc::new(Int64Array::from(vec![0i64; n])) as ArrayRef)
        }
        (_, SkEncoding::Hex) => {
            let hex = hex_encode(&dig);
            Ok(Arc::new(StringArray::from(vec![hex; n])) as ArrayRef)
        }
        (SkAlgo::Balanced | SkAlgo::CompatMd5, SkEncoding::Binary) => {
            fixed_binary_const(16, &dig, n)
        }
        (SkAlgo::Safe256, SkEncoding::Binary) => fixed_binary_const(32, &dig, n),
    }
}

fn fixed_binary_const(width: i32, dig: &[u8], n: usize) -> Result<ArrayRef> {
    let mut b = FixedSizeBinaryBuilder::with_capacity(n, width);
    for _ in 0..n {
        b.append_value(dig)
            .map_err(|e| anyhow::anyhow!("E_RBT_SK: fixed binary: {e}"))?;
    }
    Ok(Arc::new(b.finish()) as ArrayRef)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

/// Stringify one Arrow cell for the SK domain (null → [`SK_NULL_TOKEN`]).
pub fn cell_to_sk_str(arr: &dyn Array, row: usize) -> Result<String> {
    if arr.is_null(row) {
        return Ok(SK_NULL_TOKEN.to_string());
    }
    // Fast paths
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr
        .as_any()
        .downcast_ref::<arrow::array::StringViewArray>()
    {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::LargeStringArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Date32Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::TimestampNanosecondArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<BinaryArray>() {
        return Ok(hex_encode(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        return Ok(hex_encode(a.value(row)));
    }
    // Fallback: ScalarValue Display
    let sv = datafusion::common::ScalarValue::try_from_array(arr, row)
        .map_err(|e| anyhow::anyhow!("E_RBT_SK: stringify cell: {e}"))?;
    Ok(sv.to_string())
}

/// Hash grain columns of a batch into an Arrow array of the configured type.
pub fn hash_batch_columns(
    columns: &[&dyn Array],
    algo: SkAlgo,
    encoding: SkEncoding,
) -> Result<ArrayRef> {
    if columns.is_empty() {
        bail!("E_RBT_SK: hash_batch_columns requires ≥1 grain column");
    }
    let n = columns[0].len();
    for c in columns.iter().skip(1) {
        if c.len() != n {
            bail!("E_RBT_SK: grain column length mismatch");
        }
    }

    match (algo, encoding) {
        (SkAlgo::Integer, _) => bail!(
            "E_RBT_SK: algo integer/MIISK is materialize-stamp only (durable registry); \
             use frontmatter surrogate_key_algo: integer — not hash_batch_columns / SQL UDF"
        ),
        (SkAlgo::Fast64, _) => {
            let mut vals = Vec::with_capacity(n);
            let mut field_bufs = vec![String::new(); columns.len()];
            for row in 0..n {
                for (i, col) in columns.iter().enumerate() {
                    field_bufs[i] = cell_to_sk_str(*col, row)?;
                }
                let refs: Vec<&str> = field_bufs.iter().map(|s| s.as_str()).collect();
                let dig = hash_grain_fields(algo, &refs);
                let mut le = [0u8; 8];
                le.copy_from_slice(&dig);
                vals.push(Some(i64::from_le_bytes(le)));
            }
            Ok(Arc::new(Int64Array::from(vals)) as ArrayRef)
        }
        (_, SkEncoding::Hex) => {
            let mut b = StringBuilder::with_capacity(n, n * algo.digest_len() * 2);
            let mut field_bufs = vec![String::new(); columns.len()];
            for row in 0..n {
                for (i, col) in columns.iter().enumerate() {
                    field_bufs[i] = cell_to_sk_str(*col, row)?;
                }
                let refs: Vec<&str> = field_bufs.iter().map(|s| s.as_str()).collect();
                let dig = hash_grain_fields(algo, &refs);
                b.append_value(hex_encode(&dig));
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        (SkAlgo::Balanced | SkAlgo::CompatMd5, SkEncoding::Binary) => {
            build_fixed(16, columns, algo)
        }
        (SkAlgo::Safe256, SkEncoding::Binary) => build_fixed(32, columns, algo),
    }
}

fn build_fixed(width: i32, columns: &[&dyn Array], algo: SkAlgo) -> Result<ArrayRef> {
    let n = columns[0].len();
    let mut b = FixedSizeBinaryBuilder::with_capacity(n, width);
    let mut field_bufs = vec![String::new(); columns.len()];
    for row in 0..n {
        for (i, col) in columns.iter().enumerate() {
            field_bufs[i] = cell_to_sk_str(*col, row)?;
        }
        let refs: Vec<&str> = field_bufs.iter().map(|s| s.as_str()).collect();
        let dig = hash_grain_fields(algo, &refs);
        b.append_value(&dig)
            .map_err(|e| anyhow::anyhow!("E_RBT_SK: fixed binary append: {e}"))?;
    }
    Ok(Arc::new(b.finish()) as ArrayRef)
}

/// Append or replace SK column from grain.
///
/// - **Hash algos:** idempotent if column already present (SQL already selected SK).
/// - **MIISK (`integer`):** always (re)assigns from durable registry; requires
///   [`SurrogateKeyConfig::registry_path`].
pub fn stamp_batch_sk(batch: &RecordBatch, cfg: &SurrogateKeyConfig) -> Result<RecordBatch> {
    let schema = batch.schema();
    if !cfg.algo.is_miisk() && schema.index_of(&cfg.column).is_ok() {
        return Ok(batch.clone());
    }

    let sk_arr: ArrayRef = if cfg.algo.is_miisk() {
        let path = cfg.registry_path.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "E_RBT_SK: algo integer requires registry_path \
                 (engine should call with_registry_for_model)"
            )
        })?;
        let (arr, _) = assign_miisk_column(batch, &cfg.grain_cols, path)?;
        arr as ArrayRef
    } else {
        let mut col_refs: Vec<&dyn Array> = Vec::with_capacity(cfg.grain_cols.len());
        let mut owned: Vec<ArrayRef> = Vec::new();
        for name in &cfg.grain_cols {
            let idx = schema.index_of(name).with_context(|| {
                format!(
                    "E_RBT_SK: grain column '{name}' missing from batch when stamping '{}'",
                    cfg.column
                )
            })?;
            owned.push(batch.column(idx).clone());
        }
        for c in &owned {
            col_refs.push(c.as_ref());
        }
        hash_batch_columns(&col_refs, cfg.algo, cfg.encoding)?
    };

    replace_or_append_column(batch, &cfg.column, cfg.arrow_type(), sk_arr).with_context(|| {
        format!(
            "E_RBT_SK: stamp column '{}' (algo={})",
            cfg.column,
            cfg.algo.canonical()
        )
    })
}

fn replace_or_append_column(
    batch: &RecordBatch,
    name: &str,
    dtype: DataType,
    arr: ArrayRef,
) -> Result<RecordBatch> {
    let schema = batch.schema();
    if let Ok(idx) = schema.index_of(name) {
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        fields[idx] = Field::new(name, dtype, true);
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns[idx] = arr;
        return RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .context("E_RBT_SK: replace SK column");
    }
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(name, dtype, true));
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(arr);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).context("E_RBT_SK: append SK")
}

/// Ensure a single Unknown member row (SK = 0) exists; append if missing.
pub fn ensure_unknown_member(batch: &RecordBatch, cfg: &SurrogateKeyConfig) -> Result<RecordBatch> {
    if !cfg.unknown_member {
        return Ok(batch.clone());
    }
    // Require SK column present
    let sk_idx = match batch.schema().index_of(&cfg.column) {
        Ok(i) => i,
        Err(_) => {
            let stamped = stamp_batch_sk(batch, cfg)?;
            return ensure_unknown_member(&stamped, cfg);
        }
    };
    let sk_col = batch.column(sk_idx);
    let has_zero = match cfg.algo {
        SkAlgo::Fast64 | SkAlgo::Integer => sk_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| (0..a.len()).any(|i| !a.is_null(i) && a.value(i) == 0))
            .unwrap_or(false),
        SkAlgo::Balanced | SkAlgo::CompatMd5 | SkAlgo::Safe256 if cfg.encoding == SkEncoding::Binary => {
            sk_col
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .map(|a| {
                    let zero = vec![0u8; a.value_length() as usize];
                    (0..a.len()).any(|i| !a.is_null(i) && a.value(i) == zero.as_slice())
                })
                .unwrap_or(false)
        }
        _ => false, // hex: skip auto-detect; still append if asked
    };
    if has_zero {
        return Ok(batch.clone());
    }
    let unk = unknown_member_batch(cfg, batch.schema().as_ref())?;
    // Align unk schema to batch (unknown_member_batch may differ column order)
    let unk = project_to_schema(&unk, batch.schema().as_ref())?;
    let out = arrow::compute::concat_batches(&batch.schema(), [batch, &unk])
        .context("E_RBT_SK: concat unknown_member")?;
    Ok(out)
}

fn project_to_schema(batch: &RecordBatch, target: &Schema) -> Result<RecordBatch> {
    let mut cols = Vec::with_capacity(target.fields().len());
    for f in target.fields() {
        match batch.schema().index_of(f.name()) {
            Ok(i) => cols.push(batch.column(i).clone()),
            Err(_) => cols.push(arrow::array::new_null_array(f.data_type(), batch.num_rows())),
        }
    }
    RecordBatch::try_new(Arc::new(target.clone()), cols).context("E_RBT_SK: project unknown row")
}

/// Apply SK stamp (+ optional Unknown row) — shared by stream + keyed_upsert.
pub fn apply_surrogate_key(batch: &RecordBatch, cfg: &SurrogateKeyConfig) -> Result<RecordBatch> {
    let stamped = stamp_batch_sk(batch, cfg)?;
    ensure_unknown_member(&stamped, cfg)
}

/// Schema after SK stamp (for empty writers).
pub fn sk_stamped_schema(base: &Schema, cfg: &SurrogateKeyConfig) -> Arc<Schema> {
    if base.index_of(&cfg.column).is_ok() {
        return Arc::new(base.clone());
    }
    let mut fields: Vec<Field> = base.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(&cfg.column, cfg.arrow_type(), true));
    Arc::new(Schema::new(fields))
}

/// Build a one-row Unknown member batch (all grain nulls + zero SK).
pub fn unknown_member_batch(cfg: &SurrogateKeyConfig, base: &Schema) -> Result<RecordBatch> {
    let mut fields: Vec<Field> = base.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len() + 1);
    for f in base.fields() {
        columns.push(arrow::array::new_null_array(f.data_type(), 1));
    }
    if base.index_of(&cfg.column).is_err() {
        fields.push(Field::new(&cfg.column, cfg.arrow_type(), true));
    }
    // Replace or append SK column with zeros
    let sk = unknown_array(cfg.algo, cfg.encoding, 1)?;
    if let Ok(idx) = base.index_of(&cfg.column) {
        columns[idx] = sk;
    } else {
        columns.push(sk);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .context("E_RBT_SK: unknown_member batch")
}

/// Expand bare `sk()` / `surrogate_key('algo')` using frontmatter grain columns.
///
/// See ADR-009 §7. Does not touch `sk_unknown()` or calls that already pass args.
pub fn expand_sk_shorthands(sql: &str, grain: &[String]) -> Result<String> {
    if grain.is_empty() {
        // Detect bare calls that would need grain
        if bare_sk_call_present(sql) {
            bail!(
                "E_RBT_SK: bare sk()/surrogate_key(algo) requires frontmatter grain: […] \
                 (or pass grain columns explicitly)"
            );
        }
        return Ok(sql.to_string());
    }
    let cols = grain
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    let mut out = sql.to_string();

    // surrogate_key('algo') / surrogate_key("algo") / rbt_surrogate_key(...)
    // only-algo form → add grain cols (no backrefs — rust regex lacks them)
    let algo_single = regex::Regex::new(
        r#"(?i)\b((?:rbt_)?surrogate_key)\s*\(\s*'([^']+)'\s*\)"#,
    )
    .expect("sk algo single-quote regex");
    out = algo_single
        .replace_all(&out, |caps: &regex::Captures| {
            format!("{}('{}', {cols})", &caps[1], &caps[2])
        })
        .into_owned();
    let algo_double = regex::Regex::new(
        r#"(?i)\b((?:rbt_)?surrogate_key)\s*\(\s*"([^"]+)"\s*\)"#,
    )
    .expect("sk algo double-quote regex");
    out = algo_double
        .replace_all(&out, |caps: &regex::Captures| {
            format!("{}(\"{}\", {cols})", &caps[1], &caps[2])
        })
        .into_owned();

    // sk() / rbt_sk() with empty arg list — negative lookbehind-ish: not sk_unknown
    // Match word-boundary sk( ) but not sk_unknown via explicit name.
    let sk_empty =
        regex::Regex::new(r"(?i)\b((?:rbt_)?sk)\s*\(\s*\)").expect("sk empty regex");
    out = sk_empty
        .replace_all(&out, |caps: &regex::Captures| {
            format!("{}({cols})", &caps[1])
        })
        .into_owned();

    Ok(out)
}

fn bare_sk_call_present(sql: &str) -> bool {
    let sk_empty = regex::Regex::new(r"(?i)\b(?:rbt_)?sk\s*\(\s*\)").unwrap();
    let algo_single =
        regex::Regex::new(r#"(?i)\b(?:rbt_)?surrogate_key\s*\(\s*'[^']+'\s*\)"#).unwrap();
    let algo_double =
        regex::Regex::new(r#"(?i)\b(?:rbt_)?surrogate_key\s*\(\s*"[^"]+"\s*\)"#).unwrap();
    sk_empty.is_match(sql) || algo_single.is_match(sql) || algo_double.is_match(sql)
}

fn quote_ident(name: &str) -> String {
    // Simple pass-through for typical snake_case; quote if needed
    if name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name
            .chars()
            .next()
            .map(|c| c.is_ascii_alphabetic() || c == '_')
            .unwrap_or(false)
    {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn stable_hash_and_null_token() {
        let a = hash_grain_fields(SkAlgo::Balanced, &["AAPL", "2024-01-01"]);
        let b = hash_grain_fields(SkAlgo::Balanced, &["AAPL", "2024-01-01"]);
        assert_eq!(a, b);
        assert_eq!(a.len(), 16);
        let n = hash_grain_fields(SkAlgo::Balanced, &[SK_NULL_TOKEN, "x"]);
        let n2 = hash_grain_fields(SkAlgo::Balanced, &[SK_NULL_TOKEN, "x"]);
        assert_eq!(n, n2);
        assert_ne!(a, n);
    }

    #[test]
    fn fast64_int_and_unknown_zero() {
        let d = hash_grain_fields(SkAlgo::Fast64, &["sym"]);
        assert_eq!(d.len(), 8);
        assert_eq!(unknown_digest(SkAlgo::Fast64), vec![0u8; 8]);
        assert_eq!(unknown_digest(SkAlgo::Balanced), vec![0u8; 16]);
    }

    #[test]
    fn expand_sk_from_grain() {
        let grain = vec!["symbol".into(), "as_of".into()];
        let sql = "SELECT symbol, sk() AS sk, surrogate_key('fast64') AS sk2 FROM t";
        let out = expand_sk_shorthands(sql, &grain).unwrap();
        assert!(out.contains("sk(symbol, as_of)"));
        assert!(out.contains("surrogate_key('fast64', symbol, as_of)"));
        assert!(!out.contains("sk()"));
    }

    #[test]
    fn expand_does_not_touch_unknown_or_explicit() {
        let grain = vec!["a".into()];
        let sql = "SELECT sk_unknown(), sk(a, b) FROM t";
        let out = expand_sk_shorthands(sql, &grain).unwrap();
        assert!(out.contains("sk_unknown()"));
        assert!(out.contains("sk(a, b)"));
    }

    #[test]
    fn bare_sk_without_grain_errors() {
        let err = expand_sk_shorthands("SELECT sk() FROM t", &[]).unwrap_err();
        assert!(err.to_string().contains("E_RBT_SK"));
    }

    #[test]
    fn stamp_idempotent() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("entity_sk", DataType::FixedSizeBinary(16), true),
        ]));
        let dig = hash_grain_fields(SkAlgo::Balanced, &["x"]);
        let mut b = FixedSizeBinaryBuilder::with_capacity(1, 16);
        b.append_value(&dig).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(b.finish()),
            ],
        )
        .unwrap();
        let cfg = SurrogateKeyConfig {
            column: "entity_sk".into(),
            algo: SkAlgo::Balanced,
            encoding: SkEncoding::Binary,
            grain_cols: vec!["id".into()],
            unknown_member: false,
            registry_path: None,
        };
        let stamped = stamp_batch_sk(&batch, &cfg).unwrap();
        assert_eq!(stamped.num_columns(), 2);
    }

    #[test]
    fn stamp_adds_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let cfg = SurrogateKeyConfig {
            column: "id_sk".into(),
            algo: SkAlgo::Fast64,
            encoding: SkEncoding::Binary,
            grain_cols: vec!["id".into()],
            unknown_member: false,
            registry_path: None,
        };
        let stamped = stamp_batch_sk(&batch, &cfg).unwrap();
        assert_eq!(stamped.num_columns(), 2);
        assert_eq!(stamped.schema().field(1).name(), "id_sk");
        assert_eq!(stamped.schema().field(1).data_type(), &DataType::Int64);
    }
}

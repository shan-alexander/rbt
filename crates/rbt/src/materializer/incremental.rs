//! # Incremental + scoped_replace + parts-only publish
//!
//! **Honest scope:** part files under `{model}.parts/`, not row-level MERGE.
//!
//! | Strategy | Behavior |
//! |----------|----------|
//! | `incremental_append` | Always add a new `part-NNNN.parquet` |
//! | `scoped_replace` (A2) | Write/replace `part-{scope_id}.parquet` for the active run scope |
//! | `table` + `consolidate: never` (A5) | Full refresh as single `part-full.parquet` (no monolith) |
//! | `consolidate_parts_to_parquet` (A5 ops) | Rebuild monolith from parts; parts stay authoritative |
//!
//! ## Layout
//!
//! ```text
//! lake/silver/stg_events.parts/
//!   part-0000000000001.parquet          # append
//!   part-a1b2c3d4e5f60708.parquet       # scoped_replace (hex scope_id)
//!   part-full.parquet                   # table + consolidate: never
//!   _rbt_manifest.json                  # parts list + part_meta (RBT-C)
//!   _rbt_manifest.lock                  # exclusive merge lock (concurrent writers)
//! ```
//!
//! Downstream `ref()` registers the **parts directory** as a multi-file parquet table.
//! With `materialize.consolidate: always`, parts strategies also rebuild `{model}.parquet`.
//!
//! ## RBT-C concurrent writers
//!
//! Different WorkUnits write **disjoint** `part-{scope_id}.parquet` files. Only the
//! manifest is shared: [`merge_manifest_upsert_part`] takes a lockfile, loads, upserts
//! one part entry (rows, `content_fp`, `source_fp`, keys), recomputes `table_fp`, and
//! atomically publishes. See `docs/plans/partition-work-units-and-concurrent-scheduler.md`.
//!
//! ## Dirty-part skip
//!
//! When `execution.concurrency.dirty_part_skip` is true, the engine skips a unit if
//! [`part_is_clean`] finds a matching `source_fp` (bronze fingerprint for that scalar
//! scope) on an existing part.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::core::run_scope::{fnv1a64, RunScope};
use crate::materializer::stream::{atomic_publish, MaterializeWriteOptions, StreamWriteStats};
use crate::testing::Assertion;
use datafusion::physical_plan::SendableRecordBatchStream;

/// Physical layout for multi-part tables (RBT-C Phase 3).
///
/// | Layout | Path fashion | Best for |
/// |--------|--------------|----------|
/// | [`Parts`](Self::Parts) | `model.parts/part-{id}.parquet` | rbt concurrent writers |
/// | [`Hive`](Self::Hive) | `model/symbol=AAPL/data.parquet` | Spark/Trino/external tools |
///
/// `rbt consolidate` remains the path to a single monolith file for humans/BI.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartsLayout {
    /// Flat part files under `{stem}.parts/` (default).
    #[default]
    Parts,
    /// Hive-style directories under `{stem}/` (logical dest without `.parquet`).
    Hive,
}

impl PartsLayout {
    /// Parse frontmatter / project string (`parts` | `hive`).
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "parts" | "part" | "flat" | "rbt" => Ok(Self::Parts),
            "hive" | "hive_dir" | "hive_dirs" | "directory" => Ok(Self::Hive),
            other => bail!(
                "E_RBT_LAYOUT: unknown parts_layout '{other}' (parts | hive)"
            ),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Parts => "parts",
            Self::Hive => "hive",
        }
    }
}

/// Column-level min/max (and optional null count) for prune hints (Phase 3).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ColumnStats {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_count: Option<u64>,
}

/// Per-part metadata (RBT-C — hierarchical fingerprints / dirty skip / stats).
///
/// Manifest schema v2 stores these under `part_meta`. Legacy v1 manifests only
/// have `parts` + `part_rows` and still load.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct PartMeta {
    /// Relative path under the layout root (e.g. `part-abc.parquet` or `symbol=AAPL/data.parquet`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    /// Content fingerprint of the part file (`fnv1a64:…` or `blake3:…`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_fp: Option<String>,
    /// Bronze/source fingerprint of the scalar scope that produced this part.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_fp: Option<String>,
    /// Partition key bindings for this part.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub keys: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    /// Number of parquet row groups (when footer was inspected).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_groups: Option<usize>,
    /// Optional per-column min/max from parquet footer (Phase 3).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub stats: BTreeMap<String, ColumnStats>,
}

/// Manifest describing incremental / scoped parts for a model.
///
/// # Schema versions
///
/// | Version | Fields |
/// |---------|--------|
/// | 1 (legacy) | `strategy`, `parts`, `part_rows`, `total_rows`, `updated_at_ms` |
/// | 2 | + `part_meta`, `table_fp`, `parallel_safe` |
/// | 2+ (Phase 3) | + `model`, `grain`, `partition_by`, `part_key`, `sort_within_part`, `layout` |
///
/// Readers always accept older manifests (serde defaults). Writers emit version **2**
/// with Phase 3 fields when known.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalManifest {
    pub strategy: String,
    /// Schema version (1 = legacy; 2 = part_meta + content fps + Phase 3 table contract).
    #[serde(default = "default_manifest_schema")]
    pub schema_version: u32,
    /// Logical model name when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Physical layout of parts under the table root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<PartsLayout>,
    /// Declared grain (from frontmatter) — documentation / optimizer index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grain: Option<Vec<String>>,
    /// Partition columns for this table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_by: Option<Vec<String>>,
    /// Part identity keys (subset of partition_by used in scope_id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub part_key: Option<Vec<String>>,
    /// Sort contract within each part (e.g. `timestamp_ns`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_within_part: Option<Vec<String>>,
    /// Part file names **or** hive-relative paths (relative to the layout root).
    pub parts: Vec<String>,
    /// Rows contributed by each part file (A2 needs this to recompute totals on replace).
    #[serde(default)]
    pub part_rows: BTreeMap<String, u64>,
    /// Rich per-part stats / fingerprints (RBT-C).
    #[serde(default)]
    pub part_meta: BTreeMap<String, PartMeta>,
    pub total_rows: u64,
    pub updated_at_ms: u64,
    /// Optional table-level fingerprint over sorted part content_fps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_fp: Option<String>,
    /// Whether concurrent partition writers are safe for this table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_safe: Option<bool>,
}

fn default_manifest_schema() -> u32 {
    1
}

/// Parts directory sibling to a flat parquet path: `foo.parquet` → `foo.parts/`.
///
/// Prefer [`table_layout_root`] when layout may be hive.
pub fn parts_dir_for_parquet(dest_parquet: &Path) -> PathBuf {
    let stem = dest_parquet.with_extension("");
    PathBuf::from(format!("{}.parts", stem.display()))
}

/// Table root directory for a given layout (Phase 3).
///
/// - **Parts:** `{stem}.parts/`
/// - **Hive:** `{stem}/` (directory; logical dest may still be `{stem}.parquet`)
pub fn table_layout_root(dest_parquet: &Path, layout: PartsLayout) -> PathBuf {
    match layout {
        PartsLayout::Parts => parts_dir_for_parquet(dest_parquet),
        PartsLayout::Hive => dest_parquet.with_extension(""),
    }
}

/// Relative path of one hive partition file under the hive root.
///
/// Example keys `{symbol: AAPL, report_date: 2026-01-01}` →
/// `symbol=AAPL/report_date=2026-01-01/data.parquet` (keys sorted by name).
pub fn hive_part_rel_path(keys: &BTreeMap<String, String>) -> String {
    let mut segs: Vec<String> = keys.iter().map(|(k, v)| format!("{k}={v}")).collect();
    segs.sort(); // defensive; BTreeMap already sorted
    if segs.is_empty() {
        return "data.parquet".into();
    }
    format!("{}/data.parquet", segs.join("/"))
}

/// Resolve layout from model frontmatter + project default.
pub fn resolve_parts_layout(
    model_layout: Option<&str>,
    project_default: Option<&str>,
) -> PartsLayout {
    if let Some(s) = model_layout {
        if let Ok(l) = PartsLayout::parse(s) {
            return l;
        }
    }
    if let Some(s) = project_default {
        if let Ok(l) = PartsLayout::parse(s) {
            return l;
        }
    }
    PartsLayout::Parts
}

/// Relative part path for scoped_replace under a layout.
pub fn scoped_part_rel_path(
    layout: PartsLayout,
    scope_id: &str,
    keys: &BTreeMap<String, String>,
) -> String {
    match layout {
        PartsLayout::Parts => format!("part-{scope_id}.parquet"),
        PartsLayout::Hive => {
            if keys.is_empty() {
                format!("part-{scope_id}/data.parquet")
            } else {
                hive_part_rel_path(keys)
            }
        }
    }
}

pub fn manifest_path(parts_dir: &Path) -> PathBuf {
    parts_dir.join("_rbt_manifest.json")
}

pub fn load_manifest(parts_dir: &Path) -> Result<IncrementalManifest> {
    let p = manifest_path(parts_dir);
    if !p.exists() {
        return Ok(empty_manifest("incremental_append"));
    }
    let s = fs::read_to_string(&p)
        .with_context(|| format!("E_RBT_INCREMENTAL: read manifest {}", p.display()))?;
    serde_json::from_str(&s)
        .with_context(|| format!("E_RBT_INCREMENTAL: parse manifest {}", p.display()))
}

fn empty_manifest(strategy: &str) -> IncrementalManifest {
    IncrementalManifest {
        strategy: strategy.into(),
        schema_version: 2,
        model: None,
        layout: Some(PartsLayout::Parts),
        grain: None,
        partition_by: None,
        part_key: None,
        sort_within_part: None,
        parts: Vec::new(),
        part_rows: BTreeMap::new(),
        part_meta: BTreeMap::new(),
        total_rows: 0,
        updated_at_ms: 0,
        table_fp: None,
        parallel_safe: None,
    }
}

/// FNV-1a content fingerprint of a file (`fnv1a64:hex`).
pub fn file_content_fp(path: &Path) -> Result<String> {
    let bytes = fs::read(path)
        .with_context(|| format!("E_RBT_PART_FP: read {}", path.display()))?;
    Ok(format!("fnv1a64:{:016x}", fnv1a64(&bytes)))
}

/// Inspect parquet footer for row-group count and column min/max (best-effort).
///
/// Used to populate [`PartMeta::stats`] / [`PartMeta::row_groups`]. Failures are
/// non-fatal at the call site (stats remain empty).
pub fn parquet_footer_stats(path: &Path) -> Result<(usize, BTreeMap<String, ColumnStats>)> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let file = File::open(path)
        .with_context(|| format!("E_RBT_PART_STATS: open {}", path.display()))?;
    let reader = SerializedFileReader::new(file)
        .with_context(|| format!("E_RBT_PART_STATS: read footer {}", path.display()))?;
    let meta = reader.metadata();
    let n_rg = meta.num_row_groups();
    let mut stats: BTreeMap<String, ColumnStats> = BTreeMap::new();

    for rg_i in 0..n_rg {
        let rg = meta.row_group(rg_i);
        for col_i in 0..rg.num_columns() {
            let col = rg.column(col_i);
            let name = col.column_path().string();
            let Some(st) = col.statistics() else {
                continue;
            };
            let entry = stats.entry(name).or_default();
            let (min_s, max_s) = stats_to_strings(st);
            if let Some(m) = min_s {
                match &entry.min {
                    None => entry.min = Some(m),
                    Some(cur) if m < *cur => entry.min = Some(m),
                    _ => {}
                }
            }
            if let Some(m) = max_s {
                match &entry.max {
                    None => entry.max = Some(m),
                    Some(cur) if m > *cur => entry.max = Some(m),
                    _ => {}
                }
            }
            if let Some(nc) = st.null_count_opt() {
                entry.null_count = Some(entry.null_count.unwrap_or(0) + nc);
            }
        }
    }
    Ok((n_rg, stats))
}

fn stats_to_strings(st: &parquet::file::statistics::Statistics) -> (Option<String>, Option<String>) {
    use parquet::file::statistics::Statistics;
    // parquet 58: min/max via typed accessors; absent stats → None
    match st {
        Statistics::Boolean(s) => (
            s.min_opt().map(|v| v.to_string()),
            s.max_opt().map(|v| v.to_string()),
        ),
        Statistics::Int32(s) => (
            s.min_opt().map(|v| v.to_string()),
            s.max_opt().map(|v| v.to_string()),
        ),
        Statistics::Int64(s) => (
            s.min_opt().map(|v| v.to_string()),
            s.max_opt().map(|v| v.to_string()),
        ),
        Statistics::Int96(s) => (
            s.min_opt().map(|v| format!("{v:?}")),
            s.max_opt().map(|v| format!("{v:?}")),
        ),
        Statistics::Float(s) => (
            s.min_opt().map(|v| v.to_string()),
            s.max_opt().map(|v| v.to_string()),
        ),
        Statistics::Double(s) => (
            s.min_opt().map(|v| v.to_string()),
            s.max_opt().map(|v| v.to_string()),
        ),
        Statistics::ByteArray(s) => (
            s.min_opt()
                .and_then(|v| String::from_utf8(v.data().to_vec()).ok()),
            s.max_opt()
                .and_then(|v| String::from_utf8(v.data().to_vec()).ok()),
        ),
        Statistics::FixedLenByteArray(s) => (
            s.min_opt()
                .and_then(|v| String::from_utf8(v.data().to_vec()).ok()),
            s.max_opt()
                .and_then(|v| String::from_utf8(v.data().to_vec()).ok()),
        ),
    }
}

/// Whether a part can be skipped: file exists and stored source_fp matches current.
pub fn part_is_clean(
    layout_root: &Path,
    part_rel: &str,
    current_source_fp: &str,
) -> bool {
    if current_source_fp.is_empty() {
        return false;
    }
    let Ok(man) = load_manifest(layout_root) else {
        return false;
    };
    let Some(meta) = man.part_meta.get(part_rel) else {
        return false;
    };
    let Some(ref sfp) = meta.source_fp else {
        return false;
    };
    if sfp != current_source_fp {
        return false;
    }
    let rel = meta.path.as_deref().unwrap_or(part_rel);
    layout_root.join(rel).is_file()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn next_part_name(manifest: &IncrementalManifest) -> String {
    let n = manifest.parts.len() as u64 + 1;
    format!("part-{n:013}.parquet")
}

/// Stream-write a new part and update the manifest (append-only).
pub async fn materialize_incremental_append_stream(
    stream: SendableRecordBatchStream,
    dest_parquet: &Path,
    opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
) -> Result<StreamWriteStats> {
    let parts_dir = parts_dir_for_parquet(dest_parquet);
    fs::create_dir_all(&parts_dir).with_context(|| {
        format!(
            "E_RBT_INCREMENTAL: mkdir parts {}",
            parts_dir.display()
        )
    })?;
    let mut manifest = load_manifest(&parts_dir)?;
    let part_name = next_part_name(&manifest);
    let part_path = parts_dir.join(&part_name);

    let mut stream = stream;
    let stats = crate::materializer::stream::write_parquet_stream(
        &mut stream,
        &part_path,
        opts,
        assertions,
    )
    .await
    .with_context(|| {
        format!(
            "E_RBT_INCREMENTAL: write part {}",
            part_path.display()
        )
    })?;

    if stats.rows == 0 {
        // Empty increment: remove empty part if created, leave manifest unchanged.
        let _ = fs::remove_file(&part_path);
        return Ok(StreamWriteStats {
            rows: 0,
            batches: stats.batches,
            path: parts_dir,
            bytes_written: 0,
            validation: stats.validation,
        });
    }

    manifest.parts.push(part_name.clone());
    manifest.part_rows.insert(part_name, stats.rows as u64);
    manifest.total_rows = recompute_total_rows(&manifest);
    manifest.updated_at_ms = now_ms();
    manifest.strategy = "incremental_append".into();
    write_manifest(&parts_dir, &manifest)?;

    // Optional convenience: also refresh a single-file view for tools that expect dest_parquet.
    // We do **not** rewrite the full union here (would defeat incremental). Point dest at parts via symlink if supported.
    write_parts_pointer(dest_parquet, &parts_dir, "incremental_append")?;

    Ok(StreamWriteStats {
        rows: stats.rows,
        batches: stats.batches,
        path: parts_dir,
        bytes_written: stats.bytes_written,
        validation: stats.validation,
    })
}

fn recompute_total_rows(manifest: &IncrementalManifest) -> u64 {
    if !manifest.part_rows.is_empty() {
        return manifest.part_rows.values().copied().sum();
    }
    manifest.total_rows
}

/// Stable scope id for A2 (16 hex chars of FNV-1a over canonical key material).
///
/// Includes model name, contract version, and sorted `part_key` vars (multi sets
/// use their canonical `[a,b]` form so one multi-run is one part).
pub fn scope_part_id(
    model: &str,
    contract_version: &str,
    part_keys: &[String],
    scope: &RunScope,
) -> Result<String> {
    if part_keys.is_empty() {
        bail!(
            "E_RBT_PART_KEY: scoped_replace requires part_key or partition_by keys present in run scope"
        );
    }
    let mut pairs: Vec<(String, String)> = Vec::new();
    for k in part_keys {
        let Some(sv) = scope.vars.get(k) else {
            bail!(
                "E_RBT_PART_KEY: part_key '{k}' not present in run scope vars for model '{model}'"
            );
        };
        pairs.push((k.clone(), sv.canonical()));
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let mut body = format!("model={model}&contract={contract_version}");
    for (k, v) in pairs {
        body.push('&');
        body.push_str(&k);
        body.push('=');
        body.push_str(&v);
    }
    Ok(format!("{:016x}", fnv1a64(body.as_bytes())))
}

/// Resolve `part_key` list: explicit frontmatter, else partition_by ∩ scope vars.
pub fn resolve_part_keys(
    explicit: Option<&[String]>,
    partition_by: Option<&[String]>,
    scope: &RunScope,
) -> Vec<String> {
    if let Some(keys) = explicit {
        return keys.to_vec();
    }
    let mut keys = Vec::new();
    if let Some(pb) = partition_by {
        for k in pb {
            if scope.vars.contains_key(k) {
                keys.push(k.clone());
            }
        }
    }
    if keys.is_empty() {
        // Fall back to all scalar+multi scope vars (stable sorted)
        keys = scope.vars.keys().cloned().collect();
        keys.sort();
    }
    keys
}

/// Options for scoped_replace part publish (RBT-C).
#[derive(Debug, Clone)]
pub struct ScopedPartPublish {
    /// Partition key bindings recorded in part_meta.
    pub keys: BTreeMap<String, String>,
    /// Bronze fingerprint for the scalar scope that produced this part.
    pub source_fp: Option<String>,
    /// Physical layout (default parts).
    pub layout: PartsLayout,
    /// Model name for manifest table contract.
    pub model: Option<String>,
    pub grain: Option<Vec<String>>,
    pub partition_by: Option<Vec<String>>,
    pub part_key: Option<Vec<String>>,
    pub sort_within_part: Option<Vec<String>>,
    pub parallel_safe: Option<bool>,
    /// When true (default), read parquet footer for min/max stats.
    pub collect_stats: bool,
}

impl Default for ScopedPartPublish {
    fn default() -> Self {
        Self {
            keys: BTreeMap::new(),
            source_fp: None,
            layout: PartsLayout::Parts,
            model: None,
            grain: None,
            partition_by: None,
            part_key: None,
            sort_within_part: None,
            parallel_safe: None,
            collect_stats: true,
        }
    }
}

/// Stream-write/replace the part for this scope_id; peer parts for other scopes remain.
pub async fn materialize_scoped_replace_stream(
    stream: SendableRecordBatchStream,
    dest_parquet: &Path,
    opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
    scope_id: &str,
) -> Result<StreamWriteStats> {
    materialize_scoped_replace_stream_with(
        stream,
        dest_parquet,
        opts,
        assertions,
        scope_id,
        &ScopedPartPublish::default(),
    )
    .await
}

/// Like [`materialize_scoped_replace_stream`] with part meta / concurrent-safe merge / hive.
pub async fn materialize_scoped_replace_stream_with(
    stream: SendableRecordBatchStream,
    dest_parquet: &Path,
    opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
    scope_id: &str,
    publish: &ScopedPartPublish,
) -> Result<StreamWriteStats> {
    if scope_id.is_empty() || scope_id.contains("..") {
        bail!("E_RBT_PART_KEY: invalid scope_id '{scope_id}'");
    }
    if publish.layout == PartsLayout::Parts && scope_id.contains('/') {
        bail!("E_RBT_PART_KEY: invalid scope_id '{scope_id}'");
    }

    let layout_root = table_layout_root(dest_parquet, publish.layout);
    fs::create_dir_all(&layout_root).with_context(|| {
        format!(
            "E_RBT_INCREMENTAL: mkdir layout root {}",
            layout_root.display()
        )
    })?;

    let part_rel = scoped_part_rel_path(publish.layout, scope_id, &publish.keys);
    let part_path = layout_root.join(&part_rel);
    if let Some(parent) = part_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut stream = stream;
    let stats = crate::materializer::stream::write_parquet_stream(
        &mut stream,
        &part_path,
        opts,
        assertions,
    )
    .await
    .with_context(|| {
        format!(
            "E_RBT_INCREMENTAL: write scoped part {}",
            part_path.display()
        )
    })?;

    if stats.rows == 0 {
        let _ = fs::remove_file(&part_path);
        merge_manifest_remove_part(&layout_root, &part_rel)?;
    } else {
        let content_fp = file_content_fp(&part_path).ok();
        let bytes = part_path.metadata().map(|m| m.len()).ok();
        let (row_groups, col_stats) = if publish.collect_stats {
            parquet_footer_stats(&part_path).unwrap_or((0, BTreeMap::new()))
        } else {
            (0, BTreeMap::new())
        };
        let meta = PartMeta {
            path: Some(part_rel.clone()),
            rows: Some(stats.rows as u64),
            content_fp,
            source_fp: publish.source_fp.clone(),
            keys: publish.keys.clone(),
            bytes,
            row_groups: if row_groups > 0 {
                Some(row_groups)
            } else {
                None
            },
            stats: col_stats,
        };
        merge_manifest_upsert_part_full(
            &layout_root,
            &part_rel,
            stats.rows as u64,
            meta,
            "scoped_replace",
            publish,
        )?;
    }
    write_parts_pointer(dest_parquet, &layout_root, "scoped_replace")?;

    Ok(StreamWriteStats {
        rows: stats.rows,
        batches: stats.batches,
        path: layout_root,
        bytes_written: if stats.rows == 0 {
            0
        } else {
            stats.bytes_written
        },
        validation: stats.validation,
    })
}

/// Atomic manifest merge under exclusive lock (C1.6) — concurrent workers safe.
pub fn merge_manifest_upsert_part(
    parts_dir: &Path,
    part_name: &str,
    rows: u64,
    meta: PartMeta,
    strategy: &str,
) -> Result<()> {
    merge_manifest_upsert_part_full(
        parts_dir,
        part_name,
        rows,
        meta,
        strategy,
        &ScopedPartPublish::default(),
    )
}

fn merge_manifest_upsert_part_full(
    layout_root: &Path,
    part_name: &str,
    rows: u64,
    meta: PartMeta,
    strategy: &str,
    publish: &ScopedPartPublish,
) -> Result<()> {
    with_manifest_lock(layout_root, |manifest| {
        if !manifest.parts.iter().any(|p| p == part_name) {
            manifest.parts.push(part_name.to_string());
            manifest.parts.sort();
        }
        manifest.part_rows.insert(part_name.to_string(), rows);
        manifest.part_meta.insert(part_name.to_string(), meta);
        manifest.total_rows = recompute_total_rows(manifest);
        manifest.updated_at_ms = now_ms();
        manifest.strategy = strategy.into();
        manifest.schema_version = 2;
        manifest.table_fp = Some(table_fp_from_parts(manifest));
        // Phase 3 table contract fields
        if publish.model.is_some() {
            manifest.model = publish.model.clone();
        }
        manifest.layout = Some(publish.layout);
        if publish.grain.is_some() {
            manifest.grain = publish.grain.clone();
        }
        if publish.partition_by.is_some() {
            manifest.partition_by = publish.partition_by.clone();
        }
        if publish.part_key.is_some() {
            manifest.part_key = publish.part_key.clone();
        }
        if publish.sort_within_part.is_some() {
            manifest.sort_within_part = publish.sort_within_part.clone();
        }
        if publish.parallel_safe.is_some() {
            manifest.parallel_safe = publish.parallel_safe;
        }
        Ok(())
    })
}

fn merge_manifest_remove_part(layout_root: &Path, part_name: &str) -> Result<()> {
    with_manifest_lock(layout_root, |manifest| {
        manifest.parts.retain(|p| p != part_name);
        manifest.part_rows.remove(part_name);
        manifest.part_meta.remove(part_name);
        manifest.total_rows = recompute_total_rows(manifest);
        manifest.updated_at_ms = now_ms();
        manifest.schema_version = 2;
        manifest.table_fp = Some(table_fp_from_parts(manifest));
        Ok(())
    })
}

fn table_fp_from_parts(manifest: &IncrementalManifest) -> String {
    let mut fps: Vec<&str> = manifest
        .part_meta
        .iter()
        .filter_map(|(name, m)| {
            if manifest.parts.iter().any(|p| p == name) {
                m.content_fp.as_deref()
            } else {
                None
            }
        })
        .collect();
    fps.sort_unstable();
    let joined = fps.join("|");
    format!("fnv1a64:{:016x}", fnv1a64(joined.as_bytes()))
}

/// Exclusive lockfile around read-modify-write of `_rbt_manifest.json`.
fn with_manifest_lock(
    parts_dir: &Path,
    f: impl FnOnce(&mut IncrementalManifest) -> Result<()>,
) -> Result<()> {
    fs::create_dir_all(parts_dir)?;
    let lock_path = parts_dir.join("_rbt_manifest.lock");
    let mut attempts = 0u32;
    let lock_file = loop {
        attempts += 1;
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
        {
            Ok(f) => break f,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if attempts > 200 {
                    bail!(
                        "E_RBT_MANIFEST_MERGE: lock timeout on {}",
                        lock_path.display()
                    );
                }
                std::thread::sleep(std::time::Duration::from_millis(5 + (attempts % 10) as u64));
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("E_RBT_MANIFEST_MERGE: open lock {}", lock_path.display())
                });
            }
        }
    };
    let result = (|| {
        let mut manifest = load_manifest(parts_dir)?;
        f(&mut manifest)?;
        write_manifest(parts_dir, &manifest)?;
        Ok(())
    })();
    drop(lock_file);
    let _ = fs::remove_file(&lock_path);
    result
}

fn write_manifest(parts_dir: &Path, manifest: &IncrementalManifest) -> Result<()> {
    let p = manifest_path(parts_dir);
    let partial = p.with_extension("json.partial");
    {
        let mut f = File::create(&partial)
            .with_context(|| format!("E_RBT_INCREMENTAL: create {}", partial.display()))?;
        writeln!(f, "{}", serde_json::to_string_pretty(manifest)?)?;
    }
    atomic_publish(&partial, &p)?;
    Ok(())
}

/// Write a tiny pointer file next to the logical model path so operators know this is incremental.
fn write_parts_pointer(dest_parquet: &Path, parts_dir: &Path, strategy: &str) -> Result<()> {
    let pointer = dest_parquet.with_extension("rbt_incremental.json");
    let body = serde_json::json!({
        "strategy": strategy,
        "parts_dir": parts_dir.file_name().and_then(|s| s.to_str()).unwrap_or("parts"),
        "note": "ref() registers the .parts directory; single-file dest is not rewritten"
    });
    fs::write(&pointer, serde_json::to_vec_pretty(&body)?)
        .with_context(|| format!("E_RBT_INCREMENTAL: write pointer {}", pointer.display()))?;
    Ok(())
}

/// Path to register for `ref()` when incremental: the parts or hive layout root.
///
/// Checks `{stem}.parts/` first (default layout), then hive root `{stem}/` if it
/// looks like a multi-part table (manifest present).
pub fn incremental_ref_path(dest_parquet: &Path) -> PathBuf {
    let parts = parts_dir_for_parquet(dest_parquet);
    if parts.is_dir() {
        return parts;
    }
    let hive = dest_parquet.with_extension("");
    if hive.is_dir() && manifest_path(&hive).is_file() {
        return hive;
    }
    dest_parquet.to_path_buf()
}

/// Full-refresh wipe of incremental parts (when model switches back to table, or explicit).
pub fn clear_incremental_parts(dest_parquet: &Path) -> Result<()> {
    let parts = parts_dir_for_parquet(dest_parquet);
    if parts.exists() {
        fs::remove_dir_all(&parts).with_context(|| {
            format!(
                "E_RBT_INCREMENTAL: clear parts {}",
                parts.display()
            )
        })?;
    }
    let pointer = dest_parquet.with_extension("rbt_incremental.json");
    if pointer.exists() {
        let _ = fs::remove_file(pointer);
    }
    Ok(())
}

/// Validate incremental frontmatter hints.
pub fn parse_incremental_strategy(s: &str) -> Result<&'static str> {
    match s.trim().to_ascii_lowercase().as_str() {
        "incremental_append" | "append" | "incremental" => Ok("incremental_append"),
        "scoped_replace" | "incremental_replace" | "replace_scope" => Ok("scoped_replace"),
        "table" | "full_refresh" | "full-refresh" => Ok("table"),
        other => bail!(
            "E_RBT_INCREMENTAL: unknown materialization '{other}' \
             (supported: table, incremental_append, scoped_replace)"
        ),
    }
}

/// Whether this materialization publishes a multi-file `.parts` directory for `ref()`.
pub fn uses_parts_directory(m: &crate::core::dag::Materialization) -> bool {
    matches!(
        m,
        crate::core::dag::Materialization::IncrementalAppend
            | crate::core::dag::Materialization::ScopedReplace
    )
}

/// Full-refresh write of a single part under `.parts/` (RBT-A5 `consolidate: never` for table).
///
/// Clears existing parts, writes `part-full.parquet`, updates manifest. No monolith file.
pub async fn materialize_table_parts_only_stream(
    stream: SendableRecordBatchStream,
    dest_parquet: &Path,
    opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
) -> Result<StreamWriteStats> {
    let parts_dir = parts_dir_for_parquet(dest_parquet);
    if parts_dir.exists() {
        fs::remove_dir_all(&parts_dir).with_context(|| {
            format!(
                "E_RBT_CONSOLIDATE: clear parts {}",
                parts_dir.display()
            )
        })?;
    }
    fs::create_dir_all(&parts_dir)?;
    let part_name = "part-full.parquet";
    let part_path = parts_dir.join(part_name);
    let mut stream = stream;
    let stats = crate::materializer::stream::write_parquet_stream(
        &mut stream,
        &part_path,
        opts,
        assertions,
    )
    .await?;
    let mut manifest = empty_manifest("table_parts_only");
    manifest.updated_at_ms = now_ms();
    if stats.rows > 0 {
        manifest.parts.push(part_name.into());
        manifest.part_rows.insert(part_name.into(), stats.rows as u64);
        manifest.total_rows = stats.rows as u64;
        if let Ok(fp) = file_content_fp(&part_path) {
            let (rg, col_stats) = parquet_footer_stats(&part_path).unwrap_or((0, BTreeMap::new()));
            manifest.part_meta.insert(
                part_name.into(),
                PartMeta {
                    path: Some(part_name.into()),
                    rows: Some(stats.rows as u64),
                    content_fp: Some(fp),
                    source_fp: None,
                    keys: BTreeMap::new(),
                    bytes: part_path.metadata().map(|m| m.len()).ok(),
                    row_groups: if rg > 0 { Some(rg) } else { None },
                    stats: col_stats,
                },
            );
        }
        manifest.table_fp = Some(table_fp_from_parts(&manifest));
    } else {
        let _ = fs::remove_file(&part_path);
    }
    write_manifest(&parts_dir, &manifest)?;
    write_parts_pointer(dest_parquet, &parts_dir, "table_parts_only")?;
    // Do not leave a stale monolith if one existed
    if dest_parquet.exists() {
        let _ = fs::remove_file(dest_parquet);
    }
    Ok(StreamWriteStats {
        rows: stats.rows,
        batches: stats.batches,
        path: parts_dir,
        bytes_written: stats.bytes_written,
        validation: stats.validation,
    })
}

/// Merge all parquet parts into a single `dest_parquet` file (RBT-A5 consolidate).
///
/// Uses DataFusion listing over the parts directory. Does **not** delete the parts dir.
pub async fn consolidate_parts_to_parquet(
    dest_parquet: &Path,
    opts: &MaterializeWriteOptions,
) -> Result<StreamWriteStats> {
    use datafusion::prelude::{ParquetReadOptions, SessionContext};

    let parts_dir = parts_dir_for_parquet(dest_parquet);
    if !parts_dir.is_dir() {
        bail!(
            "E_RBT_CONSOLIDATE: no parts directory at {} — run a parts materialization first",
            parts_dir.display()
        );
    }
    let part_files = crate::scan::parts::list_part_files(&parts_dir)?;
    if part_files.is_empty() {
        bail!(
            "E_RBT_CONSOLIDATE: parts directory {} has no parquet parts",
            parts_dir.display()
        );
    }

    let ctx = SessionContext::new();
    // Register directory as multi-file parquet table
    ctx.register_parquet(
        "parts",
        parts_dir
            .to_str()
            .context("E_RBT_CONSOLIDATE: parts path not utf-8")?,
        ParquetReadOptions::default(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("E_RBT_CONSOLIDATE: register parts: {e}"))?;

    let df = ctx
        .sql("SELECT * FROM parts")
        .await
        .map_err(|e| anyhow::anyhow!("E_RBT_CONSOLIDATE: select parts: {e}"))?;
    let stream = df
        .execute_stream()
        .await
        .map_err(|e| anyhow::anyhow!("E_RBT_CONSOLIDATE: execute stream: {e}"))?;

    if let Some(parent) = dest_parquet.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut stream = stream;
    let stats = crate::materializer::stream::write_parquet_stream(
        &mut stream,
        dest_parquet,
        opts,
        &[],
    )
    .await
    .context("E_RBT_CONSOLIDATE: write monolith parquet")?;

    // Update pointer note
    let pointer = dest_parquet.with_extension("rbt_incremental.json");
    let body = serde_json::json!({
        "strategy": "consolidated",
        "parts_dir": parts_dir.file_name().and_then(|s| s.to_str()),
        "monolith": dest_parquet.file_name().and_then(|s| s.to_str()),
        "rows": stats.rows,
        "note": "parts remain authoritative; monolith is a convenience rebuild"
    });
    let _ = fs::write(&pointer, serde_json::to_vec_pretty(&body)?);

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hive_rel_path_sorted_keys() {
        let mut keys = BTreeMap::new();
        keys.insert("symbol".into(), "AAPL".into());
        keys.insert("report_date".into(), "2026-01-01".into());
        assert_eq!(
            hive_part_rel_path(&keys),
            "report_date=2026-01-01/symbol=AAPL/data.parquet"
        );
    }

    #[test]
    fn resolve_layout_parse() {
        assert_eq!(PartsLayout::parse("hive").unwrap(), PartsLayout::Hive);
        assert_eq!(
            resolve_parts_layout(Some("hive"), None),
            PartsLayout::Hive
        );
        assert_eq!(
            resolve_parts_layout(None, Some("parts")),
            PartsLayout::Parts
        );
    }

    #[tokio::test]
    async fn scoped_replace_hive_layout_writes_hive_path() -> Result<()> {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use std::sync::Arc;

        let dir = tempfile::tempdir()?;
        let dest = dir.path().join("stg_hive.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("n", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a.com"])),
                Arc::new(Int64Array::from(vec![1i64])),
            ],
        )?;
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(vec![Ok(batch) as datafusion::common::Result<_>]),
        ));
        let mut keys = BTreeMap::new();
        keys.insert("entity".into(), "a.com".into());
        let stats = materialize_scoped_replace_stream_with(
            stream,
            &dest,
            &MaterializeWriteOptions::default(),
            &[],
            "abc123",
            &ScopedPartPublish {
                keys: keys.clone(),
                source_fp: Some("fnv1a64:00".into()),
                layout: PartsLayout::Hive,
                model: Some("stg_hive".into()),
                grain: Some(vec!["entity".into()]),
                partition_by: Some(vec!["entity".into()]),
                part_key: Some(vec!["entity".into()]),
                sort_within_part: None,
                parallel_safe: Some(true),
                collect_stats: true,
            },
        )
        .await?;
        assert_eq!(stats.rows, 1);
        let root = table_layout_root(&dest, PartsLayout::Hive);
        assert!(root.is_dir());
        let hive_file = root.join("entity=a.com/data.parquet");
        assert!(hive_file.is_file(), "missing {}", hive_file.display());
        let man = load_manifest(&root)?;
        assert_eq!(man.layout, Some(PartsLayout::Hive));
        assert_eq!(man.model.as_deref(), Some("stg_hive"));
        assert_eq!(man.parallel_safe, Some(true));
        assert!(man.parts.iter().any(|p| p.contains("entity=a.com")));
        assert!(man.part_meta.values().any(|m| m.content_fp.is_some()));
        Ok(())
    }
}

#[cfg(test)]
mod tests_legacy {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn incremental_appends_two_parts() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let dest = temp.path().join("stg_x.parquet");
        let opts = MaterializeWriteOptions::default();
        let ctx = SessionContext::new();

        let df1 = ctx.sql("SELECT 1 AS id UNION ALL SELECT 2").await?;
        let s1 = df1.execute_stream().await?;
        let st1 =
            materialize_incremental_append_stream(s1, &dest, &opts, &[]).await?;
        assert_eq!(st1.rows, 2);

        let df2 = ctx.sql("SELECT 3 AS id").await?;
        let s2 = df2.execute_stream().await?;
        let st2 =
            materialize_incremental_append_stream(s2, &dest, &opts, &[]).await?;
        assert_eq!(st2.rows, 1);

        let parts = parts_dir_for_parquet(&dest);
        let m = load_manifest(&parts)?;
        assert_eq!(m.parts.len(), 2);
        assert_eq!(m.total_rows, 3);
        assert!(parts.join(&m.parts[0]).exists());
        assert!(parts.join(&m.parts[1]).exists());
        Ok(())
    }

    #[tokio::test]
    async fn scoped_replace_replaces_same_scope_only() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let dest = temp.path().join("stg_x.parquet");
        let opts = MaterializeWriteOptions::default();
        let ctx = SessionContext::new();

        let st1 = materialize_scoped_replace_stream(
            ctx.sql("SELECT 1 AS id UNION ALL SELECT 2")
                .await?
                .execute_stream()
                .await?,
            &dest,
            &opts,
            &[],
            "scope_aaa",
        )
        .await?;
        assert_eq!(st1.rows, 2);

        let st2 = materialize_scoped_replace_stream(
            ctx.sql("SELECT 10 AS id")
                .await?
                .execute_stream()
                .await?,
            &dest,
            &opts,
            &[],
            "scope_bbb",
        )
        .await?;
        assert_eq!(st2.rows, 1);

        // Re-run scope_aaa with 3 rows — peer bbb intact
        let st3 = materialize_scoped_replace_stream(
            ctx.sql("SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
                .await?
                .execute_stream()
                .await?,
            &dest,
            &opts,
            &[],
            "scope_aaa",
        )
        .await?;
        assert_eq!(st3.rows, 3);

        let parts = parts_dir_for_parquet(&dest);
        let m = load_manifest(&parts)?;
        assert_eq!(m.parts.len(), 2);
        assert_eq!(m.total_rows, 4); // 3 + 1
        assert!(parts.join("part-scope_aaa.parquet").exists());
        assert!(parts.join("part-scope_bbb.parquet").exists());
        assert_eq!(m.part_rows.get("part-scope_aaa.parquet"), Some(&3));
        assert_eq!(m.part_rows.get("part-scope_bbb.parquet"), Some(&1));

        // scope_id stability
        let mut scope = RunScope::new().with_var("entity", "a.com");
        scope = scope.with_var("report_date", "2026-08-07");
        let id1 = scope_part_id("stg_x", "1", &["entity".into(), "report_date".into()], &scope)?;
        let id2 = scope_part_id("stg_x", "1", &["report_date".into(), "entity".into()], &scope)?;
        assert_eq!(id1, id2);
        assert_eq!(id1.len(), 16);

        // Multi-value part key hashes as one canonical part (A1 ∩ A2)
        let multi = RunScope::new()
            .with_var_multi("entity", ["a.com", "b.com"])?
            .with_var("report_date", "2026-08-07");
        let mid = scope_part_id(
            "stg_x",
            "1",
            &["entity".into(), "report_date".into()],
            &multi,
        )?;
        assert_ne!(mid, id1);
        assert_eq!(mid.len(), 16);

        // Empty scope after replace removes part
        let st_empty = materialize_scoped_replace_stream(
            ctx.sql("SELECT 1 AS id WHERE false")
                .await?
                .execute_stream()
                .await?,
            &dest,
            &opts,
            &[],
            "scope_aaa",
        )
        .await?;
        assert_eq!(st_empty.rows, 0);
        let m2 = load_manifest(&parts)?;
        assert!(!m2.parts.iter().any(|p| p == "part-scope_aaa.parquet"));
        assert_eq!(m2.total_rows, 1); // only bbb left
        Ok(())
    }

    #[tokio::test]
    async fn table_parts_only_and_consolidate() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let dest = temp.path().join("stg_x.parquet");
        let opts = MaterializeWriteOptions::default();
        let ctx = SessionContext::new();

        // Seed a stale monolith that should be removed
        std::fs::write(&dest, b"stale")?;

        let stats = materialize_table_parts_only_stream(
            ctx.sql("SELECT 1 AS id UNION ALL SELECT 2")
                .await?
                .execute_stream()
                .await?,
            &dest,
            &opts,
            &[],
        )
        .await?;
        assert_eq!(stats.rows, 2);
        assert!(!dest.exists(), "parts-only must not leave monolith");
        let parts = parts_dir_for_parquet(&dest);
        assert!(parts.join("part-full.parquet").exists());
        let m = load_manifest(&parts)?;
        assert_eq!(m.strategy, "table_parts_only");
        assert_eq!(m.total_rows, 2);

        // Ops consolidate rebuilds single file; parts remain
        let c = consolidate_parts_to_parquet(&dest, &opts).await?;
        assert_eq!(c.rows, 2);
        assert!(dest.exists());
        assert!(parts.join("part-full.parquet").exists());
        Ok(())
    }

    #[test]
    fn resolve_part_keys_defaults() {
        let scope = RunScope::new()
            .with_var("entity", "a.com")
            .with_var("report_date", "2026-08-07")
            .with_var("noise", "x");
        let keys = resolve_part_keys(
            None,
            Some(&["entity".into(), "report_date".into(), "run_id".into()]),
            &scope,
        );
        assert_eq!(keys, vec!["entity", "report_date"]);

        let explicit = resolve_part_keys(
            Some(&["entity".into()]),
            Some(&["entity".into(), "report_date".into()]),
            &scope,
        );
        assert_eq!(explicit, vec!["entity"]);
    }
}

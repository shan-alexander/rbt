//! Incremental + **scoped_replace** + **parts-only publish** (RBT-A5) for parquet models.
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
//! Layout:
//! ```text
//! lake/silver/stg_events.parts/
//!   part-0000000000001.parquet          # append
//!   part-a1b2c3d4e5f60708.parquet       # scoped_replace (hex scope_id)
//!   part-full.parquet                   # table + consolidate: never
//!   _rbt_manifest.json
//! ```
//!
//! Downstream `ref()` registers the **parts directory** as a multi-file parquet table.
//! With `materialize.consolidate: always`, parts strategies also rebuild `{model}.parquet`.

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

/// Manifest describing incremental / scoped parts for a model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalManifest {
    pub strategy: String,
    /// Part file names (relative to the parts directory).
    pub parts: Vec<String>,
    /// Rows contributed by each part file (A2 needs this to recompute totals on replace).
    #[serde(default)]
    pub part_rows: BTreeMap<String, u64>,
    pub total_rows: u64,
    pub updated_at_ms: u64,
}

/// Parts directory sibling to a flat parquet path: `foo.parquet` → `foo.parts/`.
pub fn parts_dir_for_parquet(dest_parquet: &Path) -> PathBuf {
    let stem = dest_parquet.with_extension("");
    PathBuf::from(format!("{}.parts", stem.display()))
}

pub fn manifest_path(parts_dir: &Path) -> PathBuf {
    parts_dir.join("_rbt_manifest.json")
}

pub fn load_manifest(parts_dir: &Path) -> Result<IncrementalManifest> {
    let p = manifest_path(parts_dir);
    if !p.exists() {
        return Ok(IncrementalManifest {
            strategy: "incremental_append".into(),
            parts: Vec::new(),
            part_rows: BTreeMap::new(),
            total_rows: 0,
            updated_at_ms: 0,
        });
    }
    let s = fs::read_to_string(&p)
        .with_context(|| format!("E_RBT_INCREMENTAL: read manifest {}", p.display()))?;
    serde_json::from_str(&s)
        .with_context(|| format!("E_RBT_INCREMENTAL: parse manifest {}", p.display()))
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

/// Stream-write/replace the part for this scope_id; peer parts for other scopes remain.
pub async fn materialize_scoped_replace_stream(
    stream: SendableRecordBatchStream,
    dest_parquet: &Path,
    opts: &MaterializeWriteOptions,
    assertions: &[Assertion],
    scope_id: &str,
) -> Result<StreamWriteStats> {
    if scope_id.is_empty() || scope_id.contains('/') || scope_id.contains("..") {
        bail!("E_RBT_PART_KEY: invalid scope_id '{scope_id}'");
    }
    let parts_dir = parts_dir_for_parquet(dest_parquet);
    fs::create_dir_all(&parts_dir).with_context(|| {
        format!(
            "E_RBT_INCREMENTAL: mkdir parts {}",
            parts_dir.display()
        )
    })?;
    let mut manifest = load_manifest(&parts_dir)?;
    let part_name = format!("part-{scope_id}.parquet");
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
            "E_RBT_INCREMENTAL: write scoped part {}",
            part_path.display()
        )
    })?;

    if stats.rows == 0 {
        // Empty result for this scope: drop part so peer scopes dominate the ref().
        let _ = fs::remove_file(&part_path);
        manifest.parts.retain(|p| p != &part_name);
        manifest.part_rows.remove(&part_name);
    } else {
        if !manifest.parts.iter().any(|p| p == &part_name) {
            manifest.parts.push(part_name.clone());
            manifest.parts.sort();
        }
        manifest.part_rows.insert(part_name, stats.rows as u64);
    }
    // Authoritative total = sum of tracked part_rows (legacy manifests without
    // part_rows keep prior total_rows via recompute_total_rows fallback).
    manifest.total_rows = recompute_total_rows(&manifest);
    manifest.updated_at_ms = now_ms();
    manifest.strategy = "scoped_replace".into();
    write_manifest(&parts_dir, &manifest)?;
    write_parts_pointer(dest_parquet, &parts_dir, "scoped_replace")?;

    Ok(StreamWriteStats {
        rows: stats.rows,
        batches: stats.batches,
        path: parts_dir,
        bytes_written: if stats.rows == 0 {
            0
        } else {
            stats.bytes_written
        },
        validation: stats.validation,
    })
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

/// Path to register for `ref()` when incremental: the parts directory.
pub fn incremental_ref_path(dest_parquet: &Path) -> PathBuf {
    let parts = parts_dir_for_parquet(dest_parquet);
    if parts.is_dir() {
        parts
    } else {
        dest_parquet.to_path_buf()
    }
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
    let mut manifest = IncrementalManifest {
        strategy: "table_parts_only".into(),
        parts: Vec::new(),
        part_rows: BTreeMap::new(),
        total_rows: 0,
        updated_at_ms: now_ms(),
    };
    if stats.rows > 0 {
        manifest.parts.push(part_name.into());
        manifest.part_rows.insert(part_name.into(), stats.rows as u64);
        manifest.total_rows = stats.rows as u64;
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

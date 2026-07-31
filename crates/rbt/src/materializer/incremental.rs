//! Incremental append materialization for parquet models.
//!
//! **Honest scope:** append-only **part files** under `{model}.parts/`, not row-level MERGE.
//! Full-refresh `table` materialization still overwrites a single file.
//!
//! Layout:
//! ```text
//! lake/silver/stg_events.parts/
//!   part-0000000000001.parquet
//!   part-0000000000002.parquet
//!   _rbt_manifest.json
//! ```
//!
//! Downstream `ref()` registers the **parts directory** as a multi-file parquet table.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::materializer::stream::{atomic_publish, MaterializeWriteOptions, StreamWriteStats};
use crate::testing::Assertion;
use datafusion::physical_plan::SendableRecordBatchStream;

/// Manifest describing incremental parts for a model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalManifest {
    pub strategy: String,
    pub parts: Vec<String>,
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

    manifest.parts.push(part_name);
    manifest.total_rows = manifest.total_rows.saturating_add(stats.rows as u64);
    manifest.updated_at_ms = now_ms();
    manifest.strategy = "incremental_append".into();
    write_manifest(&parts_dir, &manifest)?;

    // Optional convenience: also refresh a single-file view for tools that expect dest_parquet.
    // We do **not** rewrite the full union here (would defeat incremental). Point dest at parts via symlink if supported.
    write_parts_pointer(dest_parquet, &parts_dir)?;

    Ok(StreamWriteStats {
        rows: stats.rows,
        batches: stats.batches,
        path: parts_dir,
        bytes_written: stats.bytes_written,
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
fn write_parts_pointer(dest_parquet: &Path, parts_dir: &Path) -> Result<()> {
    let pointer = dest_parquet.with_extension("rbt_incremental.json");
    let body = serde_json::json!({
        "strategy": "incremental_append",
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
        "table" | "full_refresh" | "full-refresh" => Ok("table"),
        other => bail!(
            "E_RBT_INCREMENTAL: unknown materialization '{other}' \
             (supported: table, incremental_append)"
        ),
    }
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
}

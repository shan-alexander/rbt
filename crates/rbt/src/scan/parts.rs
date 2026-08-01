//! Parquet **parts directories** for incremental / multi-part silver→gold (P6 / G1).
//!
//! Layout (rbt incremental_append or compatible host):
//! ```text
//! model.parts/
//!   part-0000000000001.parquet
//!   part-0000000000002.parquet
//!   _rbt_manifest.json   # optional; when present, file list is authoritative
//! ```
//!
//! External hosts may publish the same layout; rbt consumes either the manifest
//! or all `*.parquet` files under the directory (excluding `_` prefix names).

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// Compatible with [`crate::materializer::incremental::IncrementalManifest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartsManifest {
    #[serde(default)]
    pub strategy: String,
    pub parts: Vec<String>,
    #[serde(default)]
    pub total_rows: u64,
    #[serde(default)]
    pub updated_at_ms: u64,
}

pub fn manifest_path(parts_dir: &Path) -> PathBuf {
    parts_dir.join("_rbt_manifest.json")
}

/// True when path looks like a multi-part parquet table directory.
pub fn is_parts_directory(path: &Path) -> bool {
    if !path.is_dir() {
        return false;
    }
    if manifest_path(path).is_file() {
        return true;
    }
    // Heuristic: directory named `*.parts` or containing part-*.parquet
    if path
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.ends_with(".parts"))
        .unwrap_or(false)
    {
        return true;
    }
    fs::read_dir(path)
        .map(|rd| {
            rd.filter_map(|e| e.ok()).any(|e| {
                let n = e.file_name();
                let s = n.to_string_lossy();
                s.starts_with("part-") && s.ends_with(".parquet")
            })
        })
        .unwrap_or(false)
}

/// List parquet part files in stable order (manifest order, else sorted names).
pub fn list_part_files(parts_dir: &Path) -> Result<Vec<PathBuf>> {
    if !parts_dir.is_dir() {
        bail!(
            "E_RBT_PARTS: not a directory: {}",
            parts_dir.display()
        );
    }
    let man_path = manifest_path(parts_dir);
    if man_path.is_file() {
        let raw = fs::read_to_string(&man_path).with_context(|| {
            format!("E_RBT_PARTS: read manifest {}", man_path.display())
        })?;
        let man: PartsManifest = serde_json::from_str(&raw).with_context(|| {
            format!("E_RBT_PARTS: parse manifest {}", man_path.display())
        })?;
        let mut out = Vec::with_capacity(man.parts.len());
        for name in &man.parts {
            let p = parts_dir.join(name);
            if !p.is_file() {
                bail!(
                    "E_RBT_PARTS: manifest lists missing part '{}' under {}",
                    name,
                    parts_dir.display()
                );
            }
            out.push(p);
        }
        return Ok(out);
    }

    let mut files: Vec<PathBuf> = fs::read_dir(parts_dir)
        .with_context(|| format!("E_RBT_PARTS: readdir {}", parts_dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.extension().and_then(|e| e.to_str()) == Some("parquet")
                && !p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with('_'))
                    .unwrap_or(false)
        })
        .collect();
    files.sort();
    if files.is_empty() {
        bail!(
            "E_RBT_PARTS: no parquet parts under {} (no _rbt_manifest.json and no *.parquet)",
            parts_dir.display()
        );
    }
    Ok(files)
}

/// Load total_rows from manifest if present.
pub fn manifest_total_rows(parts_dir: &Path) -> Option<u64> {
    let p = manifest_path(parts_dir);
    let raw = fs::read_to_string(p).ok()?;
    let man: PartsManifest = serde_json::from_str(&raw).ok()?;
    Some(man.total_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
use std::io::Write;

    #[test]
    fn list_from_manifest_order() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("part-b.parquet");
        let p2 = dir.path().join("part-a.parquet");
        File::create(&p1).unwrap();
        File::create(&p2).unwrap();
        // empty files ok for list test
        let man = PartsManifest {
            strategy: "incremental_append".into(),
            parts: vec!["part-b.parquet".into(), "part-a.parquet".into()],
            total_rows: 10,
            updated_at_ms: 1,
        };
        let mut f = File::create(manifest_path(dir.path())).unwrap();
        f.write_all(serde_json::to_string(&man).unwrap().as_bytes())
            .unwrap();
        let list = list_part_files(dir.path()).unwrap();
        assert_eq!(list[0].file_name().unwrap(), "part-b.parquet");
        assert_eq!(list[1].file_name().unwrap(), "part-a.parquet");
    }
}

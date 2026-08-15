//! # Alias / zero-copy materialization (RBT-C Phase 0)
//!
//! Identity marts that are pure pass-throughs of an upstream model should **not**
//! re-encode multi‑GB Parquet files. This module productizes that path.
//!
//! ## Frontmatter
//!
//! ```yaml
//! ---
//! materialization: alias   # aliases: zero_copy_ref, zero_copy_clone, clone, zero_copy
//! alias_of: tf_indicators_1m   # optional if the model has exactly one {{ ref('…') }}
//! ---
//! SELECT * FROM {{ ref('tf_indicators_1m') }}
//! ```
//!
//! ## Publish modes (preference order)
//!
//! 1. **hardlink** — same inode when same filesystem (zero extra bytes)
//! 2. **symlink** — when hardlink fails (cross-device / unsupported)
//! 3. **pointer sidecar only** — write `*_rbt_alias.json`; `ref()` may resolve via
//!    `source_path` when the dest link could not be created
//!
//! ## Discovery
//!
//! Sidecar JSON records `upstream_model`, `source_path`, `mode`, optional `rows`.
//! `rbt doctor` warns (`W_RBT_ALIAS_CANDIDATE`) when SQL looks like `SELECT *` identity
//! but still uses `materialization: table`.
//!
//! ## Fail-closed
//!
//! - `lineage_stamp` / tests on alias models → `E_RBT_ALIAS` (would require rewrite)
//! - Design B Rust nodes cannot use alias
//! - Multi-ref without `alias_of` → `E_RBT_ALIAS`

use crate::core::dag::{Materialization, ModelNode};
use crate::core::parser::DependencyRef;
use crate::materializer::incremental::{incremental_ref_path, parts_dir_for_parquet};
use crate::materializer::stream::StreamWriteStats;
use crate::testing::ValidationResult;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// How the alias was published.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AliasPublishMode {
    Hardlink,
    Symlink,
    /// Sidecar only; dest file may not exist — `ref()` must resolve via sidecar.
    Pointer,
}

/// Sidecar written beside the alias destination (or under `.parts` parent).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasSidecar {
    pub strategy: String,
    pub upstream_model: String,
    pub source_path: String,
    pub dest_path: String,
    pub mode: AliasPublishMode,
    #[serde(default)]
    pub parts: bool,
    #[serde(default)]
    pub rows: Option<u64>,
}

/// Resolve which upstream model an alias model points at.
pub fn resolve_alias_upstream(model: &ModelNode) -> Result<String> {
    if let Some(ref explicit) = model
        .frontmatter
        .as_ref()
        .and_then(|f| f.alias_of.as_ref())
    {
        let name = explicit.trim();
        if name.is_empty() {
            bail!(
                "E_RBT_ALIAS: model '{}': alias_of is empty",
                model.name
            );
        }
        return Ok(name.to_string());
    }

    let model_deps: Vec<&str> = model
        .dependencies
        .iter()
        .filter_map(|d| match d {
            DependencyRef::Model(n) => Some(n.as_str()),
            _ => None,
        })
        .collect();

    match model_deps.as_slice() {
        [one] => Ok((*one).to_string()),
        [] => bail!(
            "E_RBT_ALIAS: model '{}': materialization alias requires alias_of: or exactly one \
             {{{{ ref('…') }}}} dependency",
            model.name
        ),
        many => bail!(
            "E_RBT_ALIAS: model '{}': alias has {} ref() deps {:?}; set alias_of: to pick one \
             (or split models)",
            model.name,
            many.len(),
            many
        ),
    }
}

/// Whether compiled/raw SQL looks like a pure identity projection of one table.
///
/// Conservative: strips simple comments, requires single-table `SELECT *` (optional AS alias).
/// Also accepts dbt-style `SELECT * FROM {{ ref('name') }}`.
pub fn looks_like_identity_sql(sql: &str) -> Option<String> {
    let stripped = strip_sql_noise(sql);
    // {{ ref('name') }} / {{ ref("name") }}
    let ref_re = regex::Regex::new(
        r#"(?is)^\s*select\s+\*\s+from\s+\{\{\s*ref\s*\(\s*['"]([^'"]+)['"]\s*\)\s*\}\}\s*;?\s*$"#,
    )
    .ok()?;
    if let Some(caps) = ref_re.captures(&stripped) {
        let name = caps.get(1)?.as_str();
        if !name.is_empty() {
            return Some(name.to_string());
        }
    }
    // SELECT * FROM name  or  SELECT * FROM "name"  or  `name`
    let re = regex::Regex::new(
        r#"(?is)^\s*select\s+\*\s+from\s+(?:(?:"([^"]+)")|(?:'([^']+)')|(?:`([^`]+)`)|([A-Za-z_][A-Za-z0-9_]*))\s*(?:as\s+\w+)?\s*;?\s*$"#,
    )
    .ok()?;
    let caps = re.captures(&stripped)?;
    let name = caps
        .get(1)
        .or_else(|| caps.get(2))
        .or_else(|| caps.get(3))
        .or_else(|| caps.get(4))?
        .as_str();
    if name.is_empty() {
        return None;
    }
    Some(name.to_string())
}

fn strip_sql_noise(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    for line in sql.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with("--") {
            continue;
        }
        // drop block comments crudely
        if t.starts_with("/*") {
            continue;
        }
        out.push_str(t);
        out.push(' ');
    }
    out
}

/// Physical path used for `ref()` of an upstream model (monolith file or `.parts` dir).
pub fn upstream_lake_path(output_path: &Path) -> PathBuf {
    let parts = parts_dir_for_parquet(output_path);
    if parts.is_dir() {
        return parts;
    }
    // incremental_ref_path also prefers parts when present
    let alt = incremental_ref_path(output_path);
    if alt.is_dir() {
        return alt;
    }
    output_path.to_path_buf()
}

fn alias_sidecar_path(dest: &Path, parts: bool) -> PathBuf {
    if parts {
        dest.join("_rbt_alias.json")
    } else if let Some(parent) = dest.parent() {
        let stem = dest
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("model");
        parent.join(format!("{stem}._rbt_alias.json"))
    } else {
        PathBuf::from("_rbt_alias.json")
    }
}

/// Publish `dest` as an alias of `source` without re-encoding data.
pub fn materialize_alias(
    dest: &Path,
    source: &Path,
    upstream_model: &str,
) -> Result<StreamWriteStats> {
    if !source.exists() {
        bail!(
            "E_RBT_ALIAS: upstream path does not exist: {} (model '{upstream_model}'). \
             Materialize the upstream model first.",
            source.display()
        );
    }

    let parts = source.is_dir();
    if parts {
        // Destination for parts tables is the `.parts` directory beside dest parquet path.
        let dest_parts = if dest
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with(".parts"))
            .unwrap_or(false)
        {
            dest.to_path_buf()
        } else {
            parts_dir_for_parquet(dest)
        };
        publish_link_or_pointer(&dest_parts, source, upstream_model, true)
    } else {
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "E_RBT_ALIAS: create parent for {}",
                    dest.display()
                )
            })?;
        }
        // Remove previous dest (file or broken link) so hardlink can succeed.
        if dest.exists() || dest.symlink_metadata().is_ok() {
            let meta = dest.symlink_metadata().ok();
            if meta.as_ref().map(|m| m.file_type().is_dir()).unwrap_or(false) {
                fs::remove_dir_all(dest).ok();
            } else {
                fs::remove_file(dest).ok();
            }
        }
        publish_link_or_pointer(dest, source, upstream_model, false)
    }
}

fn publish_link_or_pointer(
    dest: &Path,
    source: &Path,
    upstream_model: &str,
    parts: bool,
) -> Result<StreamWriteStats> {
    if parts {
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        if dest.exists() || dest.symlink_metadata().is_ok() {
            if dest.is_dir() && !dest.is_symlink() {
                // Existing real parts dir from a prior non-alias run — remove so we can link.
                fs::remove_dir_all(dest).with_context(|| {
                    format!("E_RBT_ALIAS: clear previous parts dir {}", dest.display())
                })?;
            } else {
                fs::remove_file(dest).ok();
            }
        }
    }

    let mut mode = AliasPublishMode::Pointer;
    let mut link_ok = false;

    // Prefer hardlink for files; for dirs, prefer symlink (hardlink dirs are non-portable).
    if !parts {
        if fs::hard_link(source, dest).is_ok() {
            mode = AliasPublishMode::Hardlink;
            link_ok = true;
        }
    }

    if !link_ok {
        #[cfg(unix)]
        {
            if std::os::unix::fs::symlink(source, dest).is_ok() {
                mode = AliasPublishMode::Symlink;
                link_ok = true;
            }
        }
        #[cfg(windows)]
        {
            let res = if parts || source.is_dir() {
                std::os::windows::fs::symlink_dir(source, dest)
            } else {
                std::os::windows::fs::symlink_file(source, dest)
            };
            if res.is_ok() {
                mode = AliasPublishMode::Symlink;
                link_ok = true;
            }
        }
    }

    if !link_ok {
        mode = AliasPublishMode::Pointer;
        tracing::warn!(
            dest = %dest.display(),
            source = %source.display(),
            "E_RBT_ALIAS: hardlink/symlink failed; writing pointer sidecar only \
             (ref() registration uses source_path)"
        );
    }

    let rows = estimate_rows(source, parts);
    let sidecar = AliasSidecar {
        strategy: "alias".into(),
        upstream_model: upstream_model.into(),
        source_path: source.display().to_string(),
        dest_path: dest.display().to_string(),
        mode,
        parts,
        rows,
    };
    let side_path = alias_sidecar_path(dest, parts);
    if let Some(parent) = side_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec_pretty(&sidecar)
        .context("E_RBT_ALIAS: serialize sidecar")?;
    fs::write(&side_path, body).with_context(|| {
        format!("E_RBT_ALIAS: write sidecar {}", side_path.display())
    })?;

    let bytes = if link_ok && dest.exists() {
        if dest.is_file() {
            dest.metadata().map(|m| m.len()).unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };

    let row_count = rows.unwrap_or(0) as usize;
    tracing::info!(
        upstream = %upstream_model,
        source = %source.display(),
        dest = %dest.display(),
        ?mode,
        parts,
        rows = row_count,
        "alias materialize (zero-copy)"
    );

    Ok(StreamWriteStats {
        rows: row_count,
        batches: 0,
        path: if link_ok {
            dest.to_path_buf()
        } else {
            source.to_path_buf()
        },
        bytes_written: bytes,
        validation: ValidationResult {
            total_rows: row_count,
            passed_assertions: 0,
            failed_assertions: 0,
            errors: Vec::new(),
        },
    })
}

fn estimate_rows(source: &Path, parts: bool) -> Option<u64> {
    if parts {
        return crate::scan::parts::manifest_total_rows(source);
    }
    // Best-effort: metadata only would need parquet crate footer parse; skip for Phase 0.
    None
}

/// Path to register for `ref()` after alias publish.
pub fn alias_ref_path(dest: &Path, stats_path: &Path, parts: bool) -> PathBuf {
    if parts {
        // Prefer dest parts dir if link succeeded.
        let p = if dest
            .extension()
            .and_then(|e| e.to_str()) == Some("parquet")
        {
            parts_dir_for_parquet(dest)
        } else {
            dest.to_path_buf()
        };
        if p.exists() {
            return p;
        }
        return stats_path.to_path_buf();
    }
    if dest.exists() || dest.symlink_metadata().is_ok() {
        return dest.to_path_buf();
    }
    stats_path.to_path_buf()
}

/// Load alias sidecar if present next to a model output.
pub fn read_alias_sidecar(dest: &Path) -> Option<AliasSidecar> {
    let candidates = [
        alias_sidecar_path(dest, false),
        alias_sidecar_path(&parts_dir_for_parquet(dest), true),
        dest.join("_rbt_alias.json"),
    ];
    for p in candidates {
        if p.is_file() {
            if let Ok(raw) = fs::read_to_string(&p) {
                if let Ok(s) = serde_json::from_str(&raw) {
                    return Some(s);
                }
            }
        }
    }
    None
}

/// True when materialization is alias / zero-copy.
pub fn is_alias_materialization(m: &Materialization) -> bool {
    m.is_alias()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::dag::{ModelKind, ModelLayer, OutputFormat};
    use crate::core::frontmatter::StagingFrontmatter;
    use std::io::Write;
    use tempfile::tempdir;

    fn node(name: &str, sql: &str, mat: Materialization, deps: Vec<DependencyRef>) -> ModelNode {
        ModelNode {
            name: name.into(),
            description: None,
            kind: ModelKind::Sql,
            raw_sql: sql.into(),
            compiled_sql: sql.into(),
            materialization: mat,
            output_format: OutputFormat::Parquet,
            output_path: None,
            dependencies: deps,
            layer: ModelLayer::Mart,
            frontmatter: None,
        }
    }

    #[test]
    fn identity_sql_detects_select_star() {
        assert_eq!(
            looks_like_identity_sql("SELECT * FROM tf_indicators_1m"),
            Some("tf_indicators_1m".into())
        );
        assert_eq!(
            looks_like_identity_sql("select * from \"stg_x\";"),
            Some("stg_x".into())
        );
        assert_eq!(
            looks_like_identity_sql("SELECT * FROM {{ ref('tf_x') }}"),
            Some("tf_x".into())
        );
        assert!(looks_like_identity_sql("SELECT a FROM stg_x").is_none());
        assert!(looks_like_identity_sql("SELECT * FROM a JOIN b ON 1=1").is_none());
    }

    #[test]
    fn resolve_upstream_from_single_ref() {
        let m = node(
            "obt_x",
            "SELECT * FROM {{ ref('tf_x') }}",
            Materialization::ZeroCopyClone,
            vec![DependencyRef::Model("tf_x".into())],
        );
        assert_eq!(resolve_alias_upstream(&m).unwrap(), "tf_x");
    }

    #[test]
    fn resolve_upstream_explicit_alias_of() {
        let mut m = node(
            "obt_x",
            "SELECT * FROM {{ ref('tf_a') }}",
            Materialization::ZeroCopyClone,
            vec![
                DependencyRef::Model("tf_a".into()),
                DependencyRef::Model("tf_b".into()),
            ],
        );
        m.frontmatter = Some(StagingFrontmatter {
            alias_of: Some("tf_b".into()),
            ..Default::default()
        });
        assert_eq!(resolve_alias_upstream(&m).unwrap(), "tf_b");
    }

    #[test]
    fn hardlink_or_symlink_file() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("up.parquet");
        let dst = dir.path().join("out.parquet");
        {
            let mut f = fs::File::create(&src).unwrap();
            f.write_all(b"PAR1fake").unwrap();
        }
        let stats = materialize_alias(&dst, &src, "tf_up").unwrap();
        assert!(matches!(
            stats.path.file_name().and_then(|n| n.to_str()),
            Some("out.parquet") | Some("up.parquet")
        ));
        // Sidecar exists
        assert!(read_alias_sidecar(&dst).is_some());
        // Dest is readable as source bytes when link succeeded
        if dst.exists() {
            let got = fs::read(&dst).unwrap();
            assert_eq!(got, b"PAR1fake");
        }
    }

    #[test]
    fn parse_alias_synonyms() {
        use crate::core::dag::parse_materialization_hint;
        for s in ["alias", "zero_copy_ref", "zero_copy_clone", "clone", "zero_copy"] {
            assert!(
                parse_materialization_hint(s).unwrap().is_alias(),
                "{s}"
            );
        }
        assert_eq!(Materialization::ZeroCopyClone.as_str(), "alias");
    }
}

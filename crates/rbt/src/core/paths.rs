//! Project path resolution: relative, absolute, and named multi-root expansion.
//!
//! # Roots
//!
//! `rbt_project.yml` may declare:
//!
//! ```yaml
//! roots:
//!   nonprod_lake: /mnt/datalake/kinnalake/nonprod/lake_us/lake
//!   prod_lake: /mnt/datalake/kinnalake/prod/lake_us/lake
//! layers:
//!   staging:
//!     target_path: $nonprod_lake/silver/stage_rbt
//! ```
//!
//! Templates use `$name` or `${name}`. Expansion happens before absolute/relative join.
//! Absolute paths (after expansion) are used as-is — they never hang under `project_dir`.

use anyhow::{bail, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Expand `$root` / `${root}` placeholders, then resolve against `project_dir`.
///
/// * Remote URIs (`s3://`, …) are returned as [`PathBuf`] of the raw string (unchecked).
/// * Absolute local paths are returned unchanged (after expansion).
/// * Relative paths join to `project_dir`.
pub fn resolve_project_path(
    project_dir: &Path,
    configured: &str,
    roots: &HashMap<String, String>,
) -> Result<PathBuf> {
    let expanded = expand_roots(configured.trim(), roots)?;
    if expanded.is_empty() {
        return Ok(project_dir.to_path_buf());
    }
    if is_remote_uri(&expanded) {
        return Ok(PathBuf::from(expanded));
    }
    let p = Path::new(&expanded);
    if p.is_absolute() {
        Ok(p.to_path_buf())
    } else {
        Ok(project_dir.join(p))
    }
}

/// Same as [`resolve_project_path`] for [`Path`] configs (e.g. layer `target_path`).
pub fn resolve_configured_path(
    project_dir: &Path,
    configured: &Path,
    roots: &HashMap<String, String>,
) -> Result<PathBuf> {
    let s = configured.to_string_lossy();
    resolve_project_path(project_dir, &s, roots)
}

/// Replace `${name}` then `$name` with values from `roots`.
///
/// Unknown `$identifiers` that are not in `roots` leave a hard error so typos fail fast.
pub fn expand_roots(input: &str, roots: &HashMap<String, String>) -> Result<String> {
    if !input.contains('$') {
        return Ok(input.to_string());
    }
    let mut out = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '$' {
            if i + 1 < chars.len() && chars[i + 1] == '{' {
                // ${name}
                if let Some(end) = chars[i + 2..].iter().position(|&c| c == '}') {
                    let name: String = chars[i + 2..i + 2 + end].iter().collect();
                    let val = roots.get(&name).ok_or_else(|| {
                        anyhow::anyhow!(
                            "E_RBT_ROOT_UNKNOWN: path template references unknown root '${{{}}}'; known: {}",
                            name,
                            root_keys(roots)
                        )
                    })?;
                    out.push_str(val);
                    i = i + 3 + end; // past }
                    continue;
                }
                bail!("E_RBT_ROOT_TEMPLATE: unclosed '${{...' in path '{}'", input);
            }
            // $name — identifier chars
            let start = i + 1;
            let mut j = start;
            while j < chars.len() && (chars[j].is_ascii_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            if j == start {
                // lone `$` or `$/` — keep literally
                out.push('$');
                i += 1;
                continue;
            }
            let name: String = chars[start..j].iter().collect();
            let val = roots.get(&name).ok_or_else(|| {
                anyhow::anyhow!(
                    "E_RBT_ROOT_UNKNOWN: path template references unknown root '${}'; known: {}",
                    name,
                    root_keys(roots)
                )
            })?;
            out.push_str(val);
            i = j;
            continue;
        }
        out.push(chars[i]);
        i += 1;
    }
    Ok(out)
}

fn root_keys(roots: &HashMap<String, String>) -> String {
    let mut keys: Vec<_> = roots.keys().cloned().collect();
    keys.sort();
    if keys.is_empty() {
        "(none — define `roots:` in rbt_project.yml)".into()
    } else {
        keys.join(", ")
    }
}

pub fn is_remote_uri(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.starts_with("s3://")
        || lower.starts_with("s3a://")
        || lower.starts_with("gs://")
        || lower.starts_with("gcs://")
        || lower.starts_with("az://")
        || lower.starts_with("abfs://")
        || lower.starts_with("abfss://")
        || lower.starts_with("http://")
        || lower.starts_with("https://")
        || lower.starts_with("file://")
}

/// Match a file path against one or more globs relative to `scan_root` (or basename).
///
/// Patterns use the [`glob`] crate syntax (`*`, `?`, `**` via `glob::Pattern` — note:
/// `**` is treated as multiple `*` segments by walking; we match against relative path
/// string with `/` separators and against the file name alone).
pub fn path_matches_globs(file: &Path, scan_root: &Path, globs: &[String]) -> bool {
    if globs.is_empty() {
        return true;
    }
    let rel = file
        .strip_prefix(scan_root)
        .unwrap_or(file)
        .to_string_lossy()
        .replace('\\', "/");
    let name = file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_string();

    globs.iter().any(|pat| match glob::Pattern::new(pat) {
        Ok(p) => p.matches(&rel) || p.matches(&name) || p.matches(file.to_string_lossy().as_ref()),
        Err(_) => false,
    })
}

/// Compile-time validation of glob patterns.
pub fn validate_glob_patterns(globs: &[String]) -> Result<()> {
    for g in globs {
        glob::Pattern::new(g)
            .map_err(|e| anyhow::anyhow!("E_RBT_PATH_GLOB_INVALID: pattern '{}': {}", g, e))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn absolute_path_not_joined_under_project() {
        let roots = HashMap::new();
        let p = resolve_project_path(
            Path::new("/home/proj"),
            "/mnt/datalake/kinnalake/nonprod/lake_us/lake/silver",
            &roots,
        )
        .unwrap();
        assert_eq!(
            p,
            PathBuf::from("/mnt/datalake/kinnalake/nonprod/lake_us/lake/silver")
        );
    }

    #[test]
    fn relative_path_joins_project() {
        let roots = HashMap::new();
        let p = resolve_project_path(Path::new("/home/proj"), "lake/silver", &roots).unwrap();
        assert_eq!(p, PathBuf::from("/home/proj/lake/silver"));
    }

    #[test]
    fn root_template_dollar_and_braces() {
        let mut roots = HashMap::new();
        roots.insert(
            "nonprod_lake".into(),
            "/mnt/datalake/kinnalake/nonprod/lake_us/lake".into(),
        );
        let a = expand_roots("$nonprod_lake/lz/kinnaruns", &roots).unwrap();
        assert_eq!(
            a,
            "/mnt/datalake/kinnalake/nonprod/lake_us/lake/lz/kinnaruns"
        );
        let b = expand_roots("${nonprod_lake}/silver/stage", &roots).unwrap();
        assert_eq!(
            b,
            "/mnt/datalake/kinnalake/nonprod/lake_us/lake/silver/stage"
        );
    }

    #[test]
    fn unknown_root_errors() {
        let roots = HashMap::new();
        let err = expand_roots("$missing/x", &roots).unwrap_err().to_string();
        assert!(err.contains("E_RBT_ROOT_UNKNOWN"));
    }

    #[test]
    fn path_glob_filename_and_relative() {
        let root = Path::new("/lake/lz/kinnaruns");
        let file = Path::new(
            "/lake/lz/kinnaruns/domain=x.com/report_date=2026-07-29/run_id=r1/raw_snoop/crawlplan.parquet",
        );
        assert!(path_matches_globs(
            file,
            root,
            &["**/crawlplan.parquet".into()]
        ));
        assert!(path_matches_globs(
            file,
            root,
            &["crawlplan.parquet".into()]
        ));
        assert!(!path_matches_globs(
            file,
            root,
            &["**/enriched_scrape.parquet".into()]
        ));
        assert!(path_matches_globs(file, root, &[])); // empty = match all
    }

    #[test]
    fn invalid_glob_rejected() {
        assert!(validate_glob_patterns(&["**/ok.parquet".into()]).is_ok());
        // unclosed bracket is invalid in glob
        assert!(validate_glob_patterns(&["file[".into()]).is_err());
    }
}

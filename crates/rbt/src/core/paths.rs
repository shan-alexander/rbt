//! Project path resolution: relative, absolute, and named multi-root expansion.
//!
//! # Roots
//!
//! `rbt_project.yml` may declare:
//!
//! ```yaml
//! roots:
//!   nonprod_lake: /mnt/datalake/acme/nonprod/lake_us/lake
//!   prod_lake: /mnt/datalake/acme/prod/lake_us/lake
//! layers:
//!   staging:
//!     target_path: $nonprod_lake/silver/stage
//! ```
//!
//! Templates use `$name` or `${name}`. Expansion happens before absolute/relative join.
//! Absolute paths (after expansion) are used as-is — they never hang under `project_dir`.
//!
//! # Path globs
//!
//! Bronze `path_glob` uses [`globset`](https://docs.rs/globset) with **literal path
//! separators**: `*` / `?` never cross `/`; use `**` for recursive directory match
//! (gitignore-style). When any `path_glob` is set, bronze registration **always** uses
//! the scan→MemTable path so filters apply; DataFusion listing pushdown is disabled
//! for that source.

use anyhow::{bail, Context, Result};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
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
    let raw = configured.trim();
    if raw.is_empty() {
        return Ok(project_dir.to_path_buf());
    }
    let expanded = expand_roots(raw, roots).with_context(|| {
        format!(
            "E_RBT_PATH_EXPAND: failed expanding path template '{raw}' \
             (project_dir={})",
            project_dir.display()
        )
    })?;
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
    resolve_project_path(project_dir, &s, roots).with_context(|| {
        format!(
            "E_RBT_LAYER_PATH: failed resolving configured path '{}' \
             (project_dir={}). Hint: use an absolute path, a path relative to the \
             project, or a `$root` / `${{root}}` template defined under `roots:` in \
             rbt_project.yml.",
            configured.display(),
            project_dir.display()
        )
    })
}

/// Replace `${name}` / `$name` with values from `roots`.
///
/// Unknown `$identifiers` that are not in `roots` error with `E_RBT_ROOT_UNKNOWN`.
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
                    if name.is_empty() {
                        bail!("E_RBT_ROOT_TEMPLATE: empty root name '${{}}' in path '{input}'");
                    }
                    let val = roots.get(&name).ok_or_else(|| {
                        anyhow::anyhow!(
                            "E_RBT_ROOT_UNKNOWN: path template references unknown root \
                             '${{{name}}}'. Known roots: {}. \
                             Define it under `roots:` in rbt_project.yml, e.g. \
                             `roots: {{ {name}: /absolute/lake/path }}`.",
                            root_keys(roots),
                        )
                    })?;
                    out.push_str(val);
                    i = i + 3 + end; // past }
                    continue;
                }
                bail!(
                    "E_RBT_ROOT_TEMPLATE: unclosed '${{...' in path '{input}'. \
                     Expected `${{root_name}}`."
                );
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
                    "E_RBT_ROOT_UNKNOWN: path template references unknown root \
                     '${name}'. Known roots: {}. \
                     Define it under `roots:` in rbt_project.yml.",
                    root_keys(roots),
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

/// Compiled set of path globs (gitignore-style `**` via globset).
///
/// # Semantics
///
/// Patterns are compiled with **literal path separators** (`GlobBuilder::literal_separator(true)`):
/// - `*` and `?` match within a single path segment only
/// - `**` matches across directories (including zero segments)
/// - character classes (`[abc]`) work as in globset/gitignore
///
/// Empty pattern list matches **everything** (no filter).
///
/// # Match candidates
///
/// A file matches if **any** pattern matches **any** applicable candidate (OR/OR):
///
/// 1. **Relative path** — path of `file` relative to `scan_root`, POSIX `/` separators
///    (always tried).
/// 2. **Basename** — file name only, tried when the set contains at least one pattern
///    with **no** `/` (e.g. `crawlplan.parquet`), so bare filenames match at any depth.
/// 3. **Absolute path** — full path with `/` separators, tried when the set contains at
///    least one pattern starting with `/`.
///
/// Path-shaped patterns (`*/…`, `**/…`) never fall back to basename-only matching in a
/// way that would let a shallow `*` over-match deep trees — `*` cannot cross `/`.
#[derive(Debug, Clone)]
pub struct PathGlobSet {
    set: GlobSet,
    /// True when any pattern has no `/` (basename-style, e.g. `crawlplan.parquet`).
    match_basename: bool,
    /// True when any pattern starts with `/` (absolute path match).
    match_absolute: bool,
    /// Original patterns (for diagnostics).
    pub patterns: Vec<String>,
}

impl PathGlobSet {
    /// Compile patterns. Empty input matches everything.
    pub fn compile(globs: &[String]) -> Result<Self> {
        if globs.is_empty() {
            return Ok(Self {
                set: GlobSet::empty(),
                match_basename: false,
                match_absolute: false,
                patterns: Vec::new(),
            });
        }
        let mut builder = GlobSetBuilder::new();
        let mut match_basename = false;
        let mut match_absolute = false;
        for (i, g) in globs.iter().enumerate() {
            let trimmed = g.trim();
            if trimmed.is_empty() {
                bail!(
                    "E_RBT_PATH_GLOB_INVALID: path_glob[{i}] is empty. \
                     Use a filename (e.g. `crawlplan.parquet`) or a relative pattern \
                     (e.g. `**/raw_snoop/crawlplan.parquet`). \
                     Hint: `*` does not cross `/`; use `**` for recursive match."
                );
            }
            if trimmed.contains('/') {
                if trimmed.starts_with('/') {
                    match_absolute = true;
                }
            } else {
                match_basename = true;
            }
            // literal_separator: `*` / `?` never match `/` — only `**` is recursive.
            let glob = GlobBuilder::new(trimmed)
                .literal_separator(true)
                .build()
                .map_err(|e| {
                    anyhow::anyhow!(
                        "E_RBT_PATH_GLOB_INVALID: path_glob[{i}] pattern '{trimmed}' is not valid: {e}. \
                         Hint: use globset/gitignore syntax with literal separators \
                         (`*` single segment, `**` recursive, `?`, `[abc]`)."
                    )
                })?;
            builder.add(glob);
        }
        let set = builder.build().map_err(|e| {
            anyhow::anyhow!(
                "E_RBT_PATH_GLOB_INVALID: failed building glob set from patterns {globs:?}: {e}"
            )
        })?;
        Ok(Self {
            set,
            match_basename,
            match_absolute,
            patterns: globs.iter().map(|s| s.trim().to_string()).collect(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    /// Match `file` under `scan_root` against any compiled pattern (OR).
    pub fn matches(&self, file: &Path, scan_root: &Path) -> bool {
        if self.patterns.is_empty() {
            return true;
        }
        let rel = file
            .strip_prefix(scan_root)
            .unwrap_or(file)
            .to_string_lossy()
            .replace('\\', "/");
        if self.set.is_match(rel.as_str()) {
            return true;
        }
        if self.match_basename {
            if let Some(name) = file.file_name().and_then(|n| n.to_str()) {
                if self.set.is_match(name) {
                    return true;
                }
            }
        }
        if self.match_absolute {
            let abs = file.to_string_lossy().replace('\\', "/");
            if self.set.is_match(abs.as_str()) {
                return true;
            }
        }
        false
    }
}

/// Match a file path against one or more globs relative to `scan_root` (or basename).
pub fn path_matches_globs(file: &Path, scan_root: &Path, globs: &[String]) -> bool {
    match PathGlobSet::compile(globs) {
        Ok(set) => set.matches(file, scan_root),
        Err(_) => false,
    }
}

/// Compile-time validation of glob patterns (fail fast with clear codes).
pub fn validate_glob_patterns(globs: &[String]) -> Result<()> {
    PathGlobSet::compile(globs).map(|_| ())
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
            "/mnt/datalake/acme/nonprod/lake_us/lake/silver",
            &roots,
        )
        .unwrap();
        assert_eq!(
            p,
            PathBuf::from("/mnt/datalake/acme/nonprod/lake_us/lake/silver")
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
            "/mnt/datalake/acme/nonprod/lake_us/lake".into(),
        );
        let a = expand_roots("$nonprod_lake/lz/runs", &roots).unwrap();
        assert_eq!(a, "/mnt/datalake/acme/nonprod/lake_us/lake/lz/runs");
        let b = expand_roots("${nonprod_lake}/silver/stage", &roots).unwrap();
        assert_eq!(b, "/mnt/datalake/acme/nonprod/lake_us/lake/silver/stage");
    }

    #[test]
    fn unknown_root_errors_with_hint() {
        let roots = HashMap::new();
        let err = expand_roots("$missing/x", &roots).unwrap_err().to_string();
        assert!(err.contains("E_RBT_ROOT_UNKNOWN"));
        assert!(err.contains("roots:"));
    }

    #[test]
    fn path_glob_double_star_and_basename() {
        let root = Path::new("/lake/lz/runs");
        let file = Path::new(
            "/lake/lz/runs/domain=x.com/report_date=2026-07-29/run_id=r1/raw_snoop/crawlplan.parquet",
        );
        let set = PathGlobSet::compile(&["**/crawlplan.parquet".into()]).unwrap();
        assert!(set.matches(file, root));
        let set2 = PathGlobSet::compile(&["crawlplan.parquet".into()]).unwrap();
        assert!(set2.matches(file, root));
        let set3 = PathGlobSet::compile(&["**/enriched_scrape.parquet".into()]).unwrap();
        assert!(!set3.matches(file, root));
        assert!(PathGlobSet::compile(&[]).unwrap().matches(file, root));
    }

    #[test]
    fn path_glob_or_semantics() {
        let root = Path::new("/lake");
        let a = Path::new("/lake/a/foo.parquet");
        let b = Path::new("/lake/b/bar.parquet");
        let set =
            PathGlobSet::compile(&["**/foo.parquet".into(), "**/bar.parquet".into()]).unwrap();
        assert!(set.matches(a, root));
        assert!(set.matches(b, root));
    }

    #[test]
    fn path_glob_nested_single_star() {
        let root = Path::new("/lake/lz");
        let file =
            Path::new("/lake/lz/domain=x/report_date=d/run_id=r/raw_snoop/crawlplan.parquet");
        // single-segment * does not cross / (literal_separator)
        let shallow = PathGlobSet::compile(&["*/crawlplan.parquet".into()]).unwrap();
        assert!(
            !shallow.matches(file, root),
            "*/crawlplan.parquet must not match a 5-segment relative path"
        );
        let one_level = Path::new("/lake/lz/raw_snoop/crawlplan.parquet");
        assert!(shallow.matches(one_level, root));
        let deep = PathGlobSet::compile(&["*/*/*/*/crawlplan.parquet".into()]).unwrap();
        assert!(deep.matches(file, root));
        // double-star still reaches deep trees
        let any = PathGlobSet::compile(&["**/crawlplan.parquet".into()]).unwrap();
        assert!(any.matches(file, root));
    }

    #[test]
    fn path_glob_basename_only_matches_any_depth() {
        let root = Path::new("/lake/lz");
        let deep = Path::new("/lake/lz/a/b/c/crawlplan.parquet");
        let set = PathGlobSet::compile(&["crawlplan.parquet".into()]).unwrap();
        assert!(set.matches(deep, root));
        assert!(!PathGlobSet::compile(&["other.parquet".into()])
            .unwrap()
            .matches(deep, root));
    }

    #[test]
    fn path_glob_absolute_pattern() {
        let root = Path::new("/lake");
        let file = Path::new("/mnt/datalake/acme/lz/runs/x.pb");
        let set = PathGlobSet::compile(&["/mnt/datalake/**/*.pb".into()]).unwrap();
        assert!(set.matches(file, root));
        assert!(!PathGlobSet::compile(&["/other/**/*.pb".into()])
            .unwrap()
            .matches(file, root));
    }

    #[test]
    fn invalid_glob_rejected() {
        assert!(validate_glob_patterns(&["**/ok.parquet".into()]).is_ok());
        assert!(validate_glob_patterns(&["file[".into()]).is_err());
        assert!(validate_glob_patterns(&["".into()]).is_err());
    }
}

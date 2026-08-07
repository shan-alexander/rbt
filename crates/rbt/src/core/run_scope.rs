//! Run-scoped variables and partition binds (P5a + **RBT-A1** multi-value).
//!
//! # What this module is for
//!
//! Hosts and the CLI bind free-form keys for one pipeline invocation. Those binds:
//!
//! 1. Expand `{name}` / `${name}` in frontmatter paths (**scalar only**).
//! 2. Merge into hive filters for keys listed in model `partition_by`:
//!    - scalar → equality (`require_partitions`)
//!    - multi → **IN** set (`require_partitions_in`)
//!
//! Project `roots:` still use `$root` / `${root}` via [`crate::core::paths`] (separate
//! from run vars).
//!
//! # Multi-value (A1) — when to use it
//!
//! Prefer multi vars when one run should process **several partition values**
//! (entities, domains, dates) without forking the process:
//!
//! ```text
//! rbt run -p proj --var entity=a.com --var entity=b.com --var report_date=2026-08-07
//! rbt run -p proj --var-file entity=list.txt --var report_date=2026-08-07
//! rbt run -p proj --var 'entity:=["a.com","b.com"]' --var report_date=2026-08-07
//! ```
//!
//! Do **not** put multi keys in path templates (`scan_path: lake/{entity}/…`) —
//! that yields `E_RBT_VAR_MULTI`. Use hive directories + `partition_by` instead.
//!
//! # Library
//!
//! ```rust,no_run
//! use rbt::RunScope;
//!
//! let scope = RunScope::new()
//!     .with_var("report_date", "2026-08-07")
//!     .with_var_multi("entity", ["a.com", "b.com"])
//!     .expect("multi");
//! assert!(scope.vars.get("entity").unwrap().is_multi());
//! ```
//!
//! Showcase fixture: `examples/a1_multi_value_scope`.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::fs;
use std::path::Path;

/// Default max distinct values for one multi-value var (A1).
pub const DEFAULT_MULTI_VAR_LIMIT: usize = 100_000;

/// How a bronze scan behaves when the scan root is missing or filters match no files.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnMissing {
    /// Fail with `E_RBT_BRONZE_*` (default — fail-closed).
    #[default]
    Error,
    /// Register an empty typed table using declared column dtypes (+ partition keys).
    Empty,
}

impl OnMissing {
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" | "fail" | "strict" => Ok(Self::Error),
            "empty" | "empty_frame" | "optional" => Ok(Self::Empty),
            other => bail!(
                "E_RBT_ON_MISSING: unknown on_missing '{other}' (expected error|empty)"
            ),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Empty => "empty",
        }
    }
}

impl fmt::Display for OnMissing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One run variable: scalar or multi-value set (**RBT-A1**).
///
/// # Serialization (receipts / JSON)
///
/// Untagged: a JSON **string** deserializes as [`ScopeValue::Single`]; a JSON
/// **array of strings** as [`ScopeValue::Multi`] (values are sorted + deduped on
/// construction via the helpers, not on deserialize).
///
/// # Limits
///
/// Construction helpers enforce [`DEFAULT_MULTI_VAR_LIMIT`] (override via
/// [`RunScope::multi_var_limit`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScopeValue {
    /// Single partition / free-form bind.
    Single(String),
    /// Sorted distinct values for hive **IN** filtering.
    Multi(Vec<String>),
}

impl ScopeValue {
    pub fn single(v: impl Into<String>) -> Self {
        Self::Single(v.into())
    }

    /// Build multi from iterator; dedup + sort; empty rejected.
    pub fn multi_from_iter<I, S>(iter: I, limit: usize) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut set = BTreeSet::new();
        for s in iter {
            let s = s.into();
            let t = s.trim();
            if t.is_empty() {
                continue;
            }
            set.insert(t.to_string());
            if set.len() > limit {
                bail!(
                    "E_RBT_VAR_LIMIT: multi-value var exceeds limit of {limit} distinct values"
                );
            }
        }
        if set.is_empty() {
            bail!("E_RBT_VAR_MULTI: multi-value set is empty after trim");
        }
        if set.len() == 1 {
            return Ok(Self::Single(set.into_iter().next().unwrap()));
        }
        Ok(Self::Multi(set.into_iter().collect()))
    }

    pub fn is_multi(&self) -> bool {
        matches!(self, Self::Multi(_))
    }

    /// Scalar view: Single, or Multi of length 1 (should not happen after normalize).
    pub fn as_single(&self) -> Option<&str> {
        match self {
            Self::Single(s) => Some(s.as_str()),
            Self::Multi(v) if v.len() == 1 => Some(v[0].as_str()),
            Self::Multi(_) => None,
        }
    }

    /// All values (1 or many), sorted for multi.
    pub fn values(&self) -> Vec<&str> {
        match self {
            Self::Single(s) => vec![s.as_str()],
            Self::Multi(v) => v.iter().map(String::as_str).collect(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Single(_) => 1,
            Self::Multi(v) => v.len(),
        }
    }

    /// Stable display for scope_key / fingerprints.
    pub fn canonical(&self) -> String {
        match self {
            Self::Single(s) => s.clone(),
            Self::Multi(v) => format!("[{}]", v.join(",")),
        }
    }

    /// Insert another scalar; promote Single→Multi when values differ.
    pub fn insert_scalar(&mut self, value: &str, limit: usize) -> Result<()> {
        let value = value.trim();
        if value.is_empty() {
            bail!("E_RBT_VAR: empty value");
        }
        match self {
            Self::Single(existing) => {
                if existing == value {
                    return Ok(());
                }
                let mut set = BTreeSet::new();
                set.insert(existing.clone());
                set.insert(value.to_string());
                if set.len() > limit {
                    bail!(
                        "E_RBT_VAR_LIMIT: multi-value var exceeds limit of {limit} distinct values"
                    );
                }
                *self = Self::Multi(set.into_iter().collect());
            }
            Self::Multi(v) => {
                if v.iter().any(|x| x == value) {
                    return Ok(());
                }
                if v.len() >= limit {
                    bail!(
                        "E_RBT_VAR_LIMIT: multi-value var exceeds limit of {limit} distinct values"
                    );
                }
                v.push(value.to_string());
                v.sort();
                v.dedup();
            }
        }
        Ok(())
    }
}

impl fmt::Display for ScopeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical())
    }
}

/// Variables and policy for one pipeline invocation (CLI / library / orchestrator).
///
/// Built by CLI (`--var`, `--var-file`) or library builders ([`RunScope::with_var`],
/// [`RunScope::with_var_multi`], [`RunScope::with_var_file`]). Consumed by bronze
/// registration, fingerprinting, and receipts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunScope {
    /// Free-form and partition binds (`report_date`, `run_id`, `entity`, …).
    /// Values may be single or multi (A1). See [`ScopeValue`].
    #[serde(default)]
    pub vars: BTreeMap<String, ScopeValue>,
    /// Optional contract version stamped into fingerprints / receipts.
    /// Overrides `rbt_project.yml` `contract_version` when set.
    #[serde(default)]
    pub contract_version: Option<String>,
    /// When true, skip materialize if bronze fingerprint matches last successful receipt.
    #[serde(default)]
    pub skip_if_fingerprint_match: bool,
    /// Write `.rbt/runs/{run_id}.json` receipt after run (default true for CLI run).
    #[serde(default = "default_true")]
    pub write_receipt: bool,
    /// Explicit run id; generated when empty.
    #[serde(default)]
    pub run_id: Option<String>,
    /// Max distinct values per multi var (default [`DEFAULT_MULTI_VAR_LIMIT`]).
    #[serde(default = "default_multi_limit", skip_serializing_if = "is_default_limit")]
    pub multi_var_limit: usize,
}

fn default_true() -> bool {
    true
}

fn default_multi_limit() -> usize {
    DEFAULT_MULTI_VAR_LIMIT
}

fn is_default_limit(n: &usize) -> bool {
    *n == DEFAULT_MULTI_VAR_LIMIT
}

impl RunScope {
    pub fn new() -> Self {
        Self {
            write_receipt: true,
            multi_var_limit: DEFAULT_MULTI_VAR_LIMIT,
            ..Default::default()
        }
    }

    pub fn with_var(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.vars
            .insert(key.into(), ScopeValue::single(value.into()));
        self
    }

    /// Bind a multi-value set (deduped, sorted). Degenerates to Single if one value.
    pub fn with_var_multi<I, S>(mut self, key: impl Into<String>, values: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let key = key.into();
        let sv = ScopeValue::multi_from_iter(values, self.multi_var_limit)?;
        self.vars.insert(key, sv);
        Ok(self)
    }

    /// Load values from a UTF-8 file (one value per line; `#` comments; blank lines skipped).
    pub fn with_var_file(mut self, key: impl Into<String>, path: impl AsRef<Path>) -> Result<Self> {
        self.insert_var_file(key, path)?;
        Ok(self)
    }

    pub fn insert_var_file(&mut self, key: impl Into<String>, path: impl AsRef<Path>) -> Result<()> {
        let key = key.into();
        let path = path.as_ref();
        let raw = fs::read_to_string(path).with_context(|| {
            format!(
                "E_RBT_VAR_FILE: cannot read var file {} for key '{key}'",
                path.display()
            )
        })?;
        let values: Vec<String> = raw
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(|s| s.to_string())
            .collect();
        if values.is_empty() {
            bail!(
                "E_RBT_VAR_FILE: no values in {} for key '{key}'",
                path.display()
            );
        }
        let sv = ScopeValue::multi_from_iter(values, self.multi_var_limit)?;
        // Merge with existing if present
        if let Some(existing) = self.vars.get_mut(&key) {
            for v in sv.values() {
                existing.insert_scalar(v, self.multi_var_limit)?;
            }
        } else {
            self.vars.insert(key, sv);
        }
        Ok(())
    }

    /// Parse repeated `key=value` tokens (CLI `--var`).
    ///
    /// Repeated keys with different values promote to multi.
    /// `key:=["a","b"]` accepts a JSON string array.
    pub fn extend_from_kv_pairs<'a, I>(&mut self, pairs: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a str>,
    {
        for raw in pairs {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            // key:=[...] JSON array form
            if let Some((k, rest)) = raw.split_once(":=") {
                let k = k.trim();
                if k.is_empty() {
                    bail!("E_RBT_VAR: empty key in '{raw}'");
                }
                let v = rest.trim();
                let parsed: Vec<String> = serde_json::from_str(v).with_context(|| {
                    format!(
                        "E_RBT_VAR: expected JSON string array after ':=', got '{v}'. \
                         Example: --var entity:=[\"a.com\",\"b.com\"]"
                    )
                })?;
                let sv = ScopeValue::multi_from_iter(parsed, self.multi_var_limit)?;
                if let Some(existing) = self.vars.get_mut(k) {
                    for x in sv.values() {
                        existing.insert_scalar(x, self.multi_var_limit)?;
                    }
                } else {
                    self.vars.insert(k.to_string(), sv);
                }
                continue;
            }

            let (k, v) = raw.split_once('=').with_context(|| {
                format!(
                    "E_RBT_VAR: expected key=value, got '{raw}'. Example: --var report_date=2026-07-29"
                )
            })?;
            let k = k.trim();
            if k.is_empty() {
                bail!("E_RBT_VAR: empty key in '{raw}'");
            }
            let v = v.trim();
            if let Some(existing) = self.vars.get_mut(k) {
                existing.insert_scalar(v, self.multi_var_limit)?;
            } else {
                self.vars
                    .insert(k.to_string(), ScopeValue::single(v.to_string()));
            }
        }
        Ok(())
    }

    /// Parse `key=path` tokens for `--var-file`.
    pub fn extend_from_var_files<'a, I>(&mut self, pairs: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a str>,
    {
        for raw in pairs {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            let (k, path) = raw.split_once('=').with_context(|| {
                format!(
                    "E_RBT_VAR_FILE: expected key=path, got '{raw}'. Example: --var-file entity=entities.txt"
                )
            })?;
            let k = k.trim();
            if k.is_empty() {
                bail!("E_RBT_VAR_FILE: empty key in '{raw}'");
            }
            self.insert_var_file(k, path.trim())?;
        }
        Ok(())
    }

    /// Load `RBT_VAR_<KEY>` env vars (e.g. `RBT_VAR_REPORT_DATE=2026-07-29` → `report_date`).
    pub fn extend_from_env(&mut self) {
        for (key, val) in std::env::vars() {
            let Some(rest) = key.strip_prefix("RBT_VAR_") else {
                continue;
            };
            if rest.is_empty() {
                continue;
            }
            let var_name = rest.to_ascii_lowercase();
            self.vars
                .entry(var_name)
                .or_insert_with(|| ScopeValue::single(val));
        }
        if let Ok(csv) = std::env::var("RBT_VARS") {
            let _ = self.extend_from_kv_pairs(
                csv.split(',').map(str::trim).filter(|s| !s.is_empty()),
            );
        }
    }

    /// Expand `{name}` and `${name}` using **scalar** run vars only.
    ///
    /// Multi-value keys used in a template → `E_RBT_VAR_MULTI`.
    pub fn expand_template(&self, input: &str) -> Result<String> {
        expand_braced_vars(input, &self.vars)
    }

    /// Scalar map for callers that only need singles (templates already validated).
    pub fn scalar_vars(&self) -> BTreeMap<String, String> {
        self.vars
            .iter()
            .filter_map(|(k, v)| v.as_single().map(|s| (k.clone(), s.to_string())))
            .collect()
    }

    /// Merge frontmatter `require_partitions` with run vars for keys in `partition_by`.
    ///
    /// Returns (equality filters, IN-set filters). Multi vars become IN filters.
    /// Run vars win over static frontmatter for the same key.
    pub fn effective_partition_filters(
        &self,
        partition_by: &[String],
        frontmatter: &HashMap<String, String>,
    ) -> Result<(HashMap<String, String>, HashMap<String, Vec<String>>)> {
        let mut eq: HashMap<String, String> = HashMap::new();
        let mut inset: HashMap<String, Vec<String>> = HashMap::new();

        for (k, v) in frontmatter {
            // Expand templates in static frontmatter values (scalar only)
            eq.insert(k.clone(), self.expand_template(v)?);
        }

        for key in partition_by {
            if let Some(sv) = self.vars.get(key) {
                match sv {
                    ScopeValue::Single(s) => {
                        inset.remove(key);
                        eq.insert(key.clone(), s.clone());
                    }
                    ScopeValue::Multi(vs) => {
                        eq.remove(key);
                        inset.insert(key.clone(), vs.clone());
                    }
                }
            }
        }
        Ok((eq, inset))
    }

    /// Backward-compatible: equality-only map (multi keys omitted — use
    /// [`effective_partition_filters`] for full A1).
    pub fn effective_require_partitions(
        &self,
        partition_by: &[String],
        frontmatter: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        self.effective_partition_filters(partition_by, frontmatter)
            .map(|(eq, _)| eq)
            .unwrap_or_default()
    }

    /// Stable short key for receipts scoped by vars (not including run_id).
    pub fn scope_key(&self) -> String {
        let mut parts: Vec<String> = self
            .vars
            .iter()
            .map(|(k, v)| format!("{k}={}", v.canonical()))
            .collect();
        parts.sort();
        let body = parts.join("&");
        format!("s{:016x}", fnv1a64(body.as_bytes()))
    }

    pub fn resolve_run_id(&self) -> String {
        if let Some(id) = &self.run_id {
            if !id.trim().is_empty() {
                return id.trim().to_string();
            }
        }
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        format!(
            "run_{ts}_{:08x}",
            fnv1a64(self.scope_key().as_bytes()) as u32
        )
    }
}

/// Expand `{key}` and `${key}` placeholders from scope vars.
///
/// Unknown keys left unchanged. Multi-value keys → `E_RBT_VAR_MULTI`.
pub fn expand_braced_vars(input: &str, vars: &BTreeMap<String, ScopeValue>) -> Result<String> {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            if let Some(end) = input[i + 2..].find('}') {
                let key = &input[i + 2..i + 2 + end];
                if let Some(v) = vars.get(key) {
                    let s = v.as_single().ok_or_else(|| {
                        anyhow::anyhow!(
                            "E_RBT_VAR_MULTI: cannot expand multi-value '{{{key}}}' in path template \
                             (values={}). Use partition_by IN filter instead of path templates.",
                            v.canonical()
                        )
                    })?;
                    out.push_str(s);
                } else {
                    out.push_str(&input[i..i + 3 + end]);
                }
                i += 3 + end;
                continue;
            }
        }
        if bytes[i] == b'{' {
            if let Some(end) = input[i + 1..].find('}') {
                let key = &input[i + 1..i + 1 + end];
                if is_simple_ident(key) {
                    if let Some(v) = vars.get(key) {
                        let s = v.as_single().ok_or_else(|| {
                            anyhow::anyhow!(
                                "E_RBT_VAR_MULTI: cannot expand multi-value '{{{key}}}' in path template \
                                 (values={}). Use partition_by IN filter instead of path templates.",
                                v.canonical()
                            )
                        })?;
                        out.push_str(s);
                        i += 2 + end;
                        continue;
                    }
                }
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    Ok(out)
}

fn is_simple_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Stable 64-bit FNV-1a (not crypto; fine for fingerprint / scope keys).
pub fn fnv1a64(data: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for b in data {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_braced_and_dollar() {
        let mut vars = BTreeMap::new();
        vars.insert("report_date".into(), ScopeValue::single("2026-07-29"));
        vars.insert("domain".into(), ScopeValue::single("acme.com"));
        assert_eq!(
            expand_braced_vars("d={domain}/dt={report_date}", &vars).unwrap(),
            "d=acme.com/dt=2026-07-29"
        );
        assert_eq!(
            expand_braced_vars("x=${report_date}/y", &vars).unwrap(),
            "x=2026-07-29/y"
        );
    }

    #[test]
    fn expand_multi_errors() {
        let mut vars = BTreeMap::new();
        vars.insert(
            "domain".into(),
            ScopeValue::multi_from_iter(["a.com", "b.com"], 100).unwrap(),
        );
        let err = expand_braced_vars("d={domain}", &vars).unwrap_err().to_string();
        assert!(err.contains("E_RBT_VAR_MULTI"), "{err}");
    }

    #[test]
    fn merge_partitions_run_wins() {
        let scope = RunScope::new()
            .with_var("report_date", "2026-07-29")
            .with_var("domain", "acme.com");
        let mut fm = HashMap::new();
        fm.insert("report_date".into(), "static".into());
        let pb = vec!["domain".into(), "report_date".into(), "run_id".into()];
        let (eq, ins) = scope.effective_partition_filters(&pb, &fm).unwrap();
        assert_eq!(eq.get("report_date").map(String::as_str), Some("2026-07-29"));
        assert_eq!(eq.get("domain").map(String::as_str), Some("acme.com"));
        assert!(!eq.contains_key("run_id"));
        assert!(ins.is_empty());
    }

    #[test]
    fn multi_partition_in_filter() {
        let scope = RunScope::new()
            .with_var_multi("entity", ["a.com", "b.com"])
            .unwrap()
            .with_var("report_date", "2026-08-07");
        let pb = vec!["entity".into(), "report_date".into()];
        let (eq, ins) = scope
            .effective_partition_filters(&pb, &HashMap::new())
            .unwrap();
        assert_eq!(eq.get("report_date").map(String::as_str), Some("2026-08-07"));
        assert!(!eq.contains_key("entity"));
        assert_eq!(
            ins.get("entity").map(|v| v.as_slice()),
            Some(["a.com".to_string(), "b.com".to_string()].as_slice())
        );
    }

    #[test]
    fn parse_kv_pairs_promote_multi() {
        let mut s = RunScope::new();
        s.extend_from_kv_pairs(["entity=a.com", "entity=b.com", "run_id=r1"])
            .unwrap();
        assert_eq!(s.vars.get("run_id").and_then(|v| v.as_single()), Some("r1"));
        assert!(s.vars.get("entity").unwrap().is_multi());
        assert_eq!(s.vars.get("entity").unwrap().values(), vec!["a.com", "b.com"]);
    }

    #[test]
    fn parse_json_array_form() {
        let mut s = RunScope::new();
        s.extend_from_kv_pairs([r#"entity:=["x.com","y.com"]"#]).unwrap();
        assert_eq!(s.vars.get("entity").unwrap().values(), vec!["x.com", "y.com"]);
    }

    #[test]
    fn var_file_load() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ents.txt");
        fs::write(&p, "a.com\n# comment\nb.com\n\n").unwrap();
        let s = RunScope::new().with_var_file("entity", &p).unwrap();
        assert_eq!(s.vars.get("entity").unwrap().values(), vec!["a.com", "b.com"]);
    }

    #[test]
    fn one_value_multi_degenerates() {
        let sv = ScopeValue::multi_from_iter(["only"], 10).unwrap();
        assert_eq!(sv.as_single(), Some("only"));
        assert!(!sv.is_multi());
    }
}

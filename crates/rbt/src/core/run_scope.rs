//! Run-scoped variables and partition binds (P5a).
//!
//! Orchestrators and CLI pass key/value pairs that:
//! 1. Expand `{name}` / `${name}` templates in frontmatter paths and partition values
//! 2. Merge into effective `require_partitions` for keys listed in `partition_by`
//!
//! Named project `roots:` still use `$root` / `${root}` expansion via [`crate::core::paths`].

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

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

/// Variables and policy for one pipeline invocation (CLI / library / orchestrator).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunScope {
    /// Free-form and partition binds (`report_date`, `run_id`, `domain`, …).
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
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
}

fn default_true() -> bool {
    true
}

impl RunScope {
    pub fn new() -> Self {
        Self {
            write_receipt: true,
            ..Default::default()
        }
    }

    pub fn with_var(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.vars.insert(key.into(), value.into());
        self
    }

    /// Parse repeated `key=value` tokens (CLI `--var`).
    pub fn extend_from_kv_pairs<'a, I>(&mut self, pairs: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a str>,
    {
        for raw in pairs {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            let (k, v) = raw
                .split_once('=')
                .with_context(|| {
                    format!(
                        "E_RBT_VAR: expected key=value, got '{raw}'. Example: --var report_date=2026-07-29"
                    )
                })?;
            let k = k.trim();
            if k.is_empty() {
                bail!("E_RBT_VAR: empty key in '{raw}'");
            }
            self.vars.insert(k.to_string(), v.trim().to_string());
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
            self.vars.entry(var_name).or_insert(val);
        }
        if let Ok(csv) = std::env::var("RBT_VARS") {
            let _ = self.extend_from_kv_pairs(csv.split(',').map(str::trim).filter(|s| !s.is_empty()));
        }
    }

    /// Expand `{name}` and `${name}` using run vars (not project `$roots`).
    pub fn expand_template(&self, input: &str) -> String {
        expand_braced_vars(input, &self.vars)
    }

    /// Merge frontmatter `require_partitions` with run vars for keys in `partition_by`.
    ///
    /// Run vars win over static frontmatter values for the same key.
    /// Values are template-expanded.
    pub fn effective_require_partitions(
        &self,
        partition_by: &[String],
        frontmatter: &std::collections::HashMap<String, String>,
    ) -> std::collections::HashMap<String, String> {
        let mut out = std::collections::HashMap::new();
        for (k, v) in frontmatter {
            out.insert(k.clone(), self.expand_template(v));
        }
        for key in partition_by {
            if let Some(v) = self.vars.get(key) {
                out.insert(key.clone(), v.clone());
            }
        }
        // Also apply any var that was explicitly put only in require-style names
        // already handled via partition_by + frontmatter.
        out
    }

    /// Stable short key for receipts scoped by vars (not including run_id).
    pub fn scope_key(&self) -> String {
        let mut parts: Vec<String> = self
            .vars
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
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
        format!("run_{ts}_{:08x}", fnv1a64(self.scope_key().as_bytes()) as u32)
    }
}

/// Expand `{key}` and `${key}` placeholders from `vars`. Unknown keys left unchanged.
pub fn expand_braced_vars(input: &str, vars: &BTreeMap<String, String>) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            if let Some(end) = input[i + 2..].find('}') {
                let key = &input[i + 2..i + 2 + end];
                if let Some(v) = vars.get(key) {
                    out.push_str(v);
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
                // Avoid matching JSON-like braces with spaces / operators
                if is_simple_ident(key) {
                    if let Some(v) = vars.get(key) {
                        out.push_str(v);
                        i += 2 + end;
                        continue;
                    }
                }
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
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
        vars.insert("report_date".into(), "2026-07-29".into());
        vars.insert("domain".into(), "acme.com".into());
        assert_eq!(
            expand_braced_vars("d={domain}/dt={report_date}", &vars),
            "d=acme.com/dt=2026-07-29"
        );
        assert_eq!(
            expand_braced_vars("x=${report_date}/y", &vars),
            "x=2026-07-29/y"
        );
    }

    #[test]
    fn merge_partitions_run_wins() {
        let scope = RunScope::new()
            .with_var("report_date", "2026-07-29")
            .with_var("domain", "acme.com");
        let mut fm = std::collections::HashMap::new();
        fm.insert("report_date".into(), "static".into());
        let pb = vec!["domain".into(), "report_date".into(), "run_id".into()];
        let eff = scope.effective_require_partitions(&pb, &fm);
        assert_eq!(eff.get("report_date").map(String::as_str), Some("2026-07-29"));
        assert_eq!(eff.get("domain").map(String::as_str), Some("acme.com"));
        assert!(!eff.contains_key("run_id"));
    }

    #[test]
    fn parse_kv_pairs() {
        let mut s = RunScope::new();
        s.extend_from_kv_pairs(["report_date=2026-07-29", "run_id=r1"])
            .unwrap();
        assert_eq!(s.vars.get("run_id").map(String::as_str), Some("r1"));
    }
}

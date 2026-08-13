//! Structured, agent-friendly diagnostics (v0.10.1).
//!
//! Error messages follow a stable shape:
//! - `error[E_RBT_CODE] short summary`
//! - `What failed:` / `Context:` / `How to fix:`
//!
//! Optional JSON: [`ErrorReport`] via CLI `--error-json`.

use serde::Serialize;
use std::path::{Path, PathBuf};

/// Machine-readable error payload for orchestrators / agents.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorReport {
    pub code: String,
    pub summary: String,
    pub what_failed: String,
    pub context: Vec<ContextField>,
    pub how_to_fix: Vec<String>,
    /// Full multi-line human message (same as [`Display`]).
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContextField {
    pub key: String,
    pub value: String,
}

impl ErrorReport {
    pub fn new(code: impl Into<String>, summary: impl Into<String>) -> Self {
        let code = code.into();
        let summary = summary.into();
        Self {
            code,
            summary,
            what_failed: String::new(),
            context: Vec::new(),
            how_to_fix: Vec::new(),
            message: String::new(),
        }
    }

    pub fn what(mut self, s: impl Into<String>) -> Self {
        self.what_failed = s.into();
        self
    }

    pub fn ctx(mut self, key: impl Into<String>, value: impl std::fmt::Display) -> Self {
        self.context.push(ContextField {
            key: key.into(),
            value: value.to_string(),
        });
        self
    }

    pub fn fix(mut self, s: impl Into<String>) -> Self {
        self.how_to_fix.push(s.into());
        self
    }

    /// Finalize multi-line human message.
    pub fn finish(mut self) -> Self {
        self.message = self.render();
        self
    }

    pub fn render(&self) -> String {
        let mut out = format!("error[{}] {}", self.code, self.summary);
        if !self.what_failed.is_empty() {
            out.push_str("\n\nWhat failed:\n  ");
            out.push_str(&self.what_failed.replace('\n', "\n  "));
        }
        if !self.context.is_empty() {
            out.push_str("\n\nContext:");
            for f in &self.context {
                out.push_str(&format!("\n  {}: {}", f.key, f.value));
            }
        }
        if !self.how_to_fix.is_empty() {
            out.push_str("\n\nHow to fix (try in order):");
            for (i, step) in self.how_to_fix.iter().enumerate() {
                out.push_str(&format!("\n  {}. {}", i + 1, step));
            }
        }
        out
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| {
            format!(
                "{{\"code\":\"{}\",\"summary\":\"{}\"}}",
                self.code, self.summary
            )
        })
    }

    /// Convert to anyhow error after finish().
    pub fn into_error(self) -> anyhow::Error {
        let finished = if self.message.is_empty() {
            self.finish()
        } else {
            self
        };
        anyhow::anyhow!(finished.message)
    }
}

/// List sibling names under a parent directory (capped).
pub fn list_siblings(parent: &Path, limit: usize) -> Vec<String> {
    let mut names = Vec::new();
    let Ok(rd) = std::fs::read_dir(parent) else {
        return names;
    };
    for ent in rd.flatten() {
        if names.len() >= limit {
            names.push("…".into());
            break;
        }
        names.push(ent.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    names
}

/// Format available model names for error context.
pub fn format_model_list(names: &[String], limit: usize) -> String {
    if names.is_empty() {
        return "(none)".into();
    }
    let mut v: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    v.sort();
    if v.len() <= limit {
        return v.join(", ");
    }
    format!(
        "{} … (+{} more)",
        v[..limit].join(", "),
        v.len().saturating_sub(limit)
    )
}

/// Detect DataFusion-style missing table errors.
pub fn is_table_not_found_error(err: &str) -> bool {
    let s = err.to_ascii_lowercase();
    (s.contains("table") && s.contains("not found"))
        || s.contains("table or view not found")
        || (s.contains("error during planning") && s.contains("not found"))
}

/// Best-effort extract of a table name from DF error text.
pub fn extract_missing_table_hint(err: &str) -> Option<String> {
    // Patterns: table 'foo' not found / table "foo" / table `datafusion.public.foo`
    for needle in ["table '", "table \"", "Table '", "Table \""] {
        if let Some(i) = err.find(needle) {
            let rest = &err[i + needle.len()..];
            let end = rest
                .find(['\'', '"', ' ', '\n'])
                .unwrap_or(rest.len().min(80));
            let name = rest[..end].trim();
            if !name.is_empty() {
                // strip catalog.schema. prefix for display
                let short = name.rsplit('.').next().unwrap_or(name);
                return Some(short.to_string());
            }
        }
    }
    None
}

/// Project config missing diagnostic.
pub fn project_missing_report(project_dir: &Path) -> ErrorReport {
    let yml = project_dir.join("rbt_project.yml");
    let models = project_dir.join("models");
    let r = ErrorReport::new(
        "E_RBT_PROJECT_MISSING",
        format!(
            "no rbt_project.yml under project_dir={}",
            project_dir.display()
        ),
    )
    .what(
        "Expected project config file is absent. Without it, roots:/layers:/materialize: \
         are not loaded and $lake (and other) path templates resolve incorrectly.",
    )
    .ctx("project_dir", project_dir.display())
    .ctx("looked_for", yml.display())
    .ctx("models_dir_present", models.is_dir())
    .ctx(
        "allow_default_env",
        "set RBT_ALLOW_DEFAULT_PROJECT=1 only for library/tests that intentionally use defaults",
    )
    .fix("Restore the file: git checkout -- rbt_project.yml (or copy from a known-good project).")
    .fix(
        "Confirm -p / --project-dir points at the project root (directory containing models/ and rbt_project.yml).",
    )
    .fix(
        "If the project moved disks, recreate rbt_project.yml with updated roots: and layer paths \
         (and materialize.wap_root on the lake volume when wap: true).",
    )
    .fix("Library embeds may build config in code; CLI runs require rbt_project.yml unless RBT_ALLOW_DEFAULT_PROJECT=1.")
    .finish();
    r
}

/// Dependency missing from DAG.
pub fn dep_missing_report(
    failing_model: &str,
    missing_dep: &str,
    available_models: &[String],
    models_dir: Option<&Path>,
) -> ErrorReport {
    let mut r = ErrorReport::new(
        "E_RBT_DEP_MISSING",
        format!(
            "model '{failing_model}' refs '{missing_dep}' but that model is not in the DAG"
        ),
    )
    .what(format!(
        "Graph build cannot wire {{{{ ref('{missing_dep}') }}}} from model '{failing_model}'."
    ))
    .ctx("failing_model", failing_model)
    .ctx("missing_dep", missing_dep)
    .ctx(
        "models_in_dag",
        format_model_list(available_models, 40),
    )
    .fix(format!(
        "Add the missing model SQL under models/ (e.g. models/transforms/{missing_dep}.sql or models/staging/…) \
         or register it via DagBuilder::model(ModelSpec::…)."
    ))
    .fix(format!(
        "Rebuild the chain that includes both models: rbt run -p <proj> --select {failing_model} \
         (Execute mode includes ancestors when they exist)."
    ))
    .fix("Validate early: rbt compile -p <proj>  and  rbt doctor -p <proj>");
    if let Some(dir) = models_dir {
        r = r.ctx("models_dir", dir.display());
    }
    r.finish()
}

/// Lake artifact missing for ref registration / re-read.
pub fn ref_missing_report(
    upstream_model: &str,
    expected_path: &Path,
    current_model: Option<&str>,
    layer_hint: Option<&str>,
) -> ErrorReport {
    let parent = expected_path.parent();
    let parent_exists = parent.map(|p| p.exists()).unwrap_or(false);
    let siblings = parent
        .filter(|p| p.is_dir())
        .map(|p| list_siblings(p, 12).join(", "))
        .unwrap_or_else(|| "(n/a)".into());

    let mut r = ErrorReport::new(
        "E_RBT_REF_MISSING",
        format!(
            "lake file missing for ref('{upstream_model}'): expected {}",
            expected_path.display()
        ),
    )
    .what(format!(
        "Cannot load lake artifact for upstream model '{upstream_model}'{}.",
        current_model
            .map(|c| format!(" (needed while running '{c}')"))
            .unwrap_or_default()
    ))
    .ctx("upstream_model", upstream_model)
    .ctx("expected_path", expected_path.display())
    .ctx("path_exists", expected_path.exists())
    .ctx(
        "parent_dir",
        parent
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "(none)".into()),
    )
    .ctx("parent_exists", parent_exists)
    .ctx("siblings_in_parent", siblings)
    .fix(format!(
        "Rebuild upstream: rbt run -p <proj> --select {upstream_model}"
    ))
    .fix(format!(
        "Or rebuild the full chain to the consumer: rbt run -p <proj> --select {}",
        current_model.unwrap_or(upstream_model)
    ))
    .fix(
        "If the lake moved volumes, update layers:/roots: in rbt_project.yml and re-run staging \
         (stale absolute paths on another drive will not appear at the expected location).",
    )
    .fix(
        "If materialize.wap: true and publish failed, check materialize.wap_root staging + \
         *.audit.json; prefer wap_root on the same volume as lake outputs.",
    )
    .fix(format!(
        "Inspect contract: rbt explain -s {upstream_model} -p <proj>"
    ));
    if let Some(c) = current_model {
        r = r.ctx("current_model", c);
    }
    if let Some(l) = layer_hint {
        r = r.ctx("upstream_layer_hint", l);
    }
    r.finish()
}

/// SQL planning/exec table missing.
pub fn sql_table_report(
    model: &str,
    df_error: &str,
    missing_table: Option<&str>,
    registered_tables: &[String],
    compiled_sql_snippet: Option<&str>,
) -> ErrorReport {
    let table = missing_table.unwrap_or("(unknown)");
    let mut r = ErrorReport::new(
        "E_RBT_SQL_TABLE",
        format!("model '{model}': DataFusion could not resolve table '{table}'"),
    )
    .what(
        "Compiled SQL references a session table that is not registered for this run \
         (upstream not materialised, wrong catalog_prefix, or ref() name mismatch).",
    )
    .ctx("model", model)
    .ctx("missing_table_hint", table)
    .ctx(
        "registered_tables",
        if registered_tables.is_empty() {
            "(none listed)".into()
        } else {
            format_model_list(
                &registered_tables
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>(),
                30,
            )
        },
    )
    .ctx("datafusion_error", truncate(df_error, 400))
    .fix(format!(
        "Ensure upstream models run in this DAG (default --select includes ancestors): \
         rbt run -p <proj> --select {model}"
    ))
    .fix(
        "Library DAGs: keep ModelSpec catalog_prefix empty (default since 0.10) so ref('x') \
         matches bare table registration.",
    )
    .fix(format!(
        "Rebuild likely upstream explicitly: rbt run -p <proj> --select {table}"
    ))
    .fix("rbt doctor -p <proj>  # config, roots, layer dirs, sample outputs");
    if let Some(sql) = compiled_sql_snippet {
        r = r.ctx("compiled_sql_snippet", truncate(sql, 240));
    }
    r.finish()
}

fn truncate(s: &str, max: usize) -> String {
    let t = s.trim();
    if t.len() <= max {
        t.to_string()
    } else {
        format!("{}…", &t[..max])
    }
}

/// Doctor finding severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorSeverity {
    Ok,
    Warn,
    Error,
}

#[derive(Debug, Clone, Serialize)]
pub struct DoctorFinding {
    pub severity: DoctorSeverity,
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DoctorReport {
    pub project_dir: String,
    pub ok: bool,
    pub findings: Vec<DoctorFinding>,
}

impl DoctorReport {
    pub fn push(
        &mut self,
        severity: DoctorSeverity,
        code: impl Into<String>,
        message: String,
        path: Option<PathBuf>,
    ) {
        if matches!(severity, DoctorSeverity::Error) {
            self.ok = false;
        }
        self.findings.push(DoctorFinding {
            severity,
            code: code.into(),
            message,
            path: path.map(|p| p.display().to_string()),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_missing_has_code_and_fixes() {
        let r = project_missing_report(Path::new("/tmp/proj"));
        assert_eq!(r.code, "E_RBT_PROJECT_MISSING");
        assert!(r.message.contains("How to fix"));
        assert!(r.to_json().contains("E_RBT_PROJECT_MISSING"));
    }

    #[test]
    fn table_not_found_detect() {
        assert!(is_table_not_found_error(
            "Error during planning: table 'datafusion.public.stg_x' not found"
        ));
        assert_eq!(
            extract_missing_table_hint(
                "Error during planning: table 'datafusion.public.stg_x' not found"
            )
            .as_deref(),
            Some("stg_x")
        );
    }
}

use anyhow::{bail, Context, Result};
use minijinja::value::Kwargs;
use minijinja::Environment;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

pub use super::frontmatter::{
    resolve_scan_path, scan_path_exists, BronzeCheckMode, BronzeDiagnostic, BronzeValidationReport,
    DiagnosticSeverity, SourceFormat, StagingFrontmatter,
};

/// Parsed dependency reference from a Jinja-style model query.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DependencyRef {
    Model(String),
    Source {
        source_name: String,
        table_name: String,
    },
}

/// Fast-path SQL Model template parser extracting `{{ ref(...) }}` and `{{ source(...) }}` references.
pub struct SqlModelParser;

impl SqlModelParser {
    /// Extracts YAML frontmatter and pure SQL content from raw model text.
    ///
    /// If a `---` block is present, YAML must parse successfully or this returns an error
    /// (no silent fallback that leaves `---` in the SQL body).
    pub fn parse_frontmatter(raw: &str) -> Result<(Option<StagingFrontmatter>, String)> {
        let trimmed = raw.trim_start();
        if !trimmed.starts_with("---") {
            return Ok((None, raw.to_string()));
        }

        // Opening fence: line starting with --- (optional trailing spaces).
        let after_open = match trimmed.strip_prefix("---") {
            Some(rest) => rest.strip_prefix('\r').unwrap_or(rest),
            None => return Ok((None, raw.to_string())),
        };
        let after_open = after_open
            .strip_prefix('\n')
            .or_else(|| after_open.strip_prefix("\r\n"))
            .unwrap_or(after_open);

        // Closing fence must be its own line (`---`), not a substring of comments
        // like `# --------------------------------`.
        let Some((yaml_str, sql_content)) = split_closing_frontmatter_fence(after_open) else {
            bail!("Unclosed frontmatter block: found opening '---' but no closing '---' line");
        };

        // Empty YAML between fences is allowed → default frontmatter
        if yaml_str.trim().is_empty() {
            return Ok((Some(StagingFrontmatter::default()), sql_content));
        }

        let frontmatter: StagingFrontmatter =
            serde_yaml::from_str(yaml_str).with_context(|| {
                format!(
                    "Invalid frontmatter YAML (between --- delimiters):\n{}",
                    yaml_str.trim()
                )
            })?;
        Ok((Some(frontmatter), sql_content))
    }

    /// Fast-path extraction of all model and source dependencies from raw model SQL.
    pub fn extract_dependencies(sql: &str) -> Result<Vec<DependencyRef>> {
        let mut deps = Vec::new();
        let mut seen = HashSet::new();

        let ref_re = Regex::new(r#"\{\{\s*ref\s*\(\s*['"]([^'"]+)['"]\s*\)\s*\}\}"#)?;
        for cap in ref_re.captures_iter(sql) {
            let model_name = cap[1].trim().to_string();
            if seen.insert(DependencyRef::Model(model_name.clone())) {
                deps.push(DependencyRef::Model(model_name));
            }
        }

        let source_re = Regex::new(
            r#"\{\{\s*source\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*\}\}"#,
        )?;
        for cap in source_re.captures_iter(sql) {
            let source_name = cap[1].trim().to_string();
            let table_name = cap[2].trim().to_string();
            let dep = DependencyRef::Source {
                source_name,
                table_name,
            };
            if seen.insert(dep.clone()) {
                deps.push(dep);
            }
        }

        Ok(deps)
    }

    /// Compiles raw SQL model by resolving `{{ ref(...) }}` and `{{ source(...) }}`.
    ///
    /// - `ref('m')` → `m` when `catalog_prefix` is empty, else `{prefix}.m`
    /// - `source('s','t')` → `s.t` when prefix empty, else `{prefix}.s.t`
    pub fn compile_sql(sql: &str, catalog_prefix: &str) -> Result<String> {
        let ref_re = Regex::new(r#"\{\{\s*ref\s*\(\s*['"]([^'"]+)['"]\s*\)\s*\}\}"#)?;
        let compiled_refs = ref_re.replace_all(sql, |caps: &regex::Captures| {
            if catalog_prefix.is_empty() {
                caps[1].to_string()
            } else {
                format!("{}.{}", catalog_prefix, &caps[1])
            }
        });

        let source_re = Regex::new(
            r#"\{\{\s*source\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*\}\}"#,
        )?;
        let compiled_sources = source_re.replace_all(&compiled_refs, |caps: &regex::Captures| {
            if catalog_prefix.is_empty() {
                format!("{}.{}", &caps[1], &caps[2])
            } else {
                format!("{}.{}.{}", catalog_prefix, &caps[1], &caps[2])
            }
        });

        Ok(compiled_sources.to_string())
    }
}

/// Split body after the opening `---` into (yaml, sql) at a closing fence line.
fn split_closing_frontmatter_fence(after_open: &str) -> Option<(&str, String)> {
    let mut offset = 0usize;
    for line in after_open.split_inclusive('\n') {
        let line_body = line.trim_end_matches(['\n', '\r']);
        if line_body.trim() == "---" {
            let yaml_str = &after_open[..offset];
            let sql = after_open[offset + line.len()..].trim_start().to_string();
            return Some((yaml_str, sql));
        }
        offset += line.len();
    }
    None
}

/// Advanced Jinja-compatible templating engine backed by `minijinja`.
pub struct RbtTemplateEngine {
    catalog_prefix: String,
}

impl RbtTemplateEngine {
    pub fn new(catalog_prefix: impl Into<String>) -> Self {
        Self {
            catalog_prefix: catalog_prefix.into(),
        }
    }

    /// Compiles a SQL model template using full Jinja2 evaluation with dbt-compatible macros.
    pub fn render(&self, template_name: &str, template_source: &str) -> Result<String> {
        let mut env = Environment::new();
        let prefix = Arc::new(self.catalog_prefix.clone());

        let prefix_ref = prefix.clone();
        env.add_function("ref", move |model_name: &str| -> String {
            if prefix_ref.is_empty() {
                model_name.to_string()
            } else {
                format!("{}.{}", prefix_ref, model_name)
            }
        });

        let prefix_src = prefix.clone();
        env.add_function(
            "source",
            move |source_name: &str, table_name: &str| -> String {
                if prefix_src.is_empty() {
                    format!("{}.{}", source_name, table_name)
                } else {
                    format!("{}.{}.{}", prefix_src, source_name, table_name)
                }
            },
        );

        env.add_function("config", |_kwargs: Kwargs| -> String { String::new() });

        env.add_template(template_name, template_source)?;
        let tmpl = env.get_template(template_name)?;
        let rendered = tmpl.render(())?;
        Ok(rendered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_dependencies() -> Result<()> {
        let sql = r#"
            SELECT
                o.order_id,
                u.user_name,
                p.product_name
            FROM {{ ref('stg_orders') }} o
            JOIN {{ ref('stg_users') }} u ON o.user_id = u.user_id
            JOIN {{ source('raw_store', 'products') }} p ON o.product_id = p.id
        "#;

        let deps = SqlModelParser::extract_dependencies(sql)?;
        assert_eq!(deps.len(), 3);
        assert!(deps.contains(&DependencyRef::Model("stg_orders".to_string())));
        assert!(deps.contains(&DependencyRef::Model("stg_users".to_string())));
        assert!(deps.contains(&DependencyRef::Source {
            source_name: "raw_store".to_string(),
            table_name: "products".to_string()
        }));

        Ok(())
    }

    #[test]
    fn test_compile_sql() -> Result<()> {
        let sql = "SELECT * FROM {{ ref('stg_orders') }} JOIN {{ source('raw', 'users') }}";
        let compiled = SqlModelParser::compile_sql(sql, "iceberg_db")?;
        assert_eq!(
            compiled,
            "SELECT * FROM iceberg_db.stg_orders JOIN iceberg_db.raw.users"
        );

        let compiled_local = SqlModelParser::compile_sql(sql, "")?;
        assert_eq!(compiled_local, "SELECT * FROM stg_orders JOIN raw.users");
        Ok(())
    }

    #[test]
    fn test_minijinja_template_engine() -> Result<()> {
        let engine = RbtTemplateEngine::new("prod_lake");
        let template = r#"
            {{ config(materialized="table") }}
            SELECT * FROM {{ ref('stg_events') }}
            WHERE event_type = 'click'
            {% if true %}
                AND source = '{{ source("telemetry", "clicks") }}'
            {% endif %}
        "#;

        let rendered = engine.render("my_model.sql", template)?;
        assert!(rendered.contains("FROM prod_lake.stg_events"));
        assert!(rendered.contains("AND source = 'prod_lake.telemetry.clicks'"));
        Ok(())
    }

    #[test]
    fn test_parse_frontmatter() -> Result<()> {
        let raw_sql = r#"---
source_format: parquet
scan_path: "s3://lake/events/*/*.parquet"
partition_by: ["year", "month"]
paths: [id, tenant_id]
---
SELECT * FROM {{ source('raw', 'events') }}
"#;
        let (frontmatter, sql) = SqlModelParser::parse_frontmatter(raw_sql)?;
        assert!(frontmatter.is_some());
        let fm = frontmatter.unwrap();
        assert_eq!(fm.source_format, Some(SourceFormat::Parquet));
        assert_eq!(
            fm.scan_path.as_deref(),
            Some("s3://lake/events/*/*.parquet")
        );
        assert_eq!(
            fm.partition_by,
            Some(vec!["year".to_string(), "month".to_string()])
        );
        assert_eq!(
            fm.paths,
            Some(vec!["id".to_string(), "tenant_id".to_string()])
        );
        assert!(sql.contains("SELECT * FROM {{ source('raw', 'events') }}"));
        Ok(())
    }

    #[test]
    fn test_invalid_frontmatter_is_error() {
        let raw = "---\nsource_format: [not, valid, for, enum\n---\nSELECT 1";
        let err = SqlModelParser::parse_frontmatter(raw).unwrap_err();
        assert!(
            err.to_string().contains("Invalid frontmatter") || err.to_string().contains("YAML")
        );
    }

    #[test]
    fn test_unclosed_frontmatter_is_error() {
        let raw = "---\nsource_format: jsonl\nSELECT 1";
        let err = SqlModelParser::parse_frontmatter(raw).unwrap_err();
        assert!(err.to_string().contains("Unclosed frontmatter"));
    }

    #[test]
    fn test_frontmatter_ignores_triple_dash_inside_comments() -> Result<()> {
        let raw = r#"---
# --------------------------------
# decorative banner with many dashes
# --------------------------------
source_format: arrow_ipc
scan_path: "lake/bronze"
---
SELECT 1
"#;
        let (fm, sql) = SqlModelParser::parse_frontmatter(raw)?;
        let fm = fm.expect("frontmatter");
        assert_eq!(fm.scan_path.as_deref(), Some("lake/bronze"));
        assert!(sql.contains("SELECT 1"));
        Ok(())
    }
}

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use super::dag::{Materialization, ModelDag, ModelLayer, OutputFormat};

/// Layer-specific target storage & path configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LayerConfig {
    pub path: PathBuf,
    pub target_path: PathBuf,
    pub default_format: Option<String>,
}

/// Project-wide `rbt_project.yml` configuration schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RbtProjectConfig {
    pub name: String,
    pub version: String,
    pub models_dir: PathBuf,
    pub target_path: PathBuf,
    #[serde(default)]
    pub layers: HashMap<String, LayerConfig>,
}

impl Default for RbtProjectConfig {
    fn default() -> Self {
        let mut layers = HashMap::new();
        layers.insert(
            "staging".to_string(),
            LayerConfig {
                path: PathBuf::from("models/staging"),
                target_path: PathBuf::from("lake/silver"),
                default_format: Some("parquet".to_string()),
            },
        );
        layers.insert(
            "transforms".to_string(),
            LayerConfig {
                path: PathBuf::from("models/transforms"),
                target_path: PathBuf::from("lake/gold"),
                default_format: Some("parquet".to_string()),
            },
        );
        layers.insert(
            "marts".to_string(),
            LayerConfig {
                path: PathBuf::from("models/marts"),
                target_path: PathBuf::from("lake/gold"),
                default_format: Some("parquet_and_iceberg".to_string()),
            },
        );

        Self {
            name: "rbt_project".to_string(),
            version: "1.0.0".to_string(),
            models_dir: PathBuf::from("models"),
            target_path: PathBuf::from("lake/gold"),
            layers,
        }
    }
}

impl RbtProjectConfig {
    /// Loads `rbt_project.yml` from project directory or returns default configuration.
    pub fn load(project_dir: &Path) -> Result<Self> {
        let project_file = project_dir.join("rbt_project.yml");
        if project_file.exists() {
            let content = fs::read_to_string(&project_file)?;
            let mut config: RbtProjectConfig = serde_yaml::from_str(&content)
                .with_context(|| format!("Failed to parse {}", project_file.display()))?;

            let defaults = Self::default();
            for (key, val) in defaults.layers {
                config.layers.entry(key).or_insert(val);
            }
            Ok(config)
        } else {
            Ok(Self::default())
        }
    }

    /// Resolves destination output directory path for a model based on its layer configuration.
    pub fn resolve_model_target_path(
        &self,
        project_dir: &Path,
        model_name: &str,
        layer: ModelLayer,
        ext: &str,
    ) -> PathBuf {
        let layer_key = match layer {
            ModelLayer::Staging => "staging",
            ModelLayer::Transform => "transforms",
            ModelLayer::Mart => "marts",
        };

        let target_dir = if let Some(layer_cfg) = self.layers.get(layer_key) {
            project_dir.join(&layer_cfg.target_path)
        } else {
            project_dir.join(&self.target_path)
        };

        target_dir.join(format!("{}.{}", model_name, ext))
    }

    /// Directory target for Iceberg-style table roots (no file extension).
    pub fn resolve_model_target_dir(
        &self,
        project_dir: &Path,
        model_name: &str,
        layer: ModelLayer,
    ) -> PathBuf {
        let layer_key = match layer {
            ModelLayer::Staging => "staging",
            ModelLayer::Transform => "transforms",
            ModelLayer::Mart => "marts",
        };
        let target_dir = if let Some(layer_cfg) = self.layers.get(layer_key) {
            project_dir.join(&layer_cfg.target_path)
        } else {
            project_dir.join(&self.target_path)
        };
        target_dir.join(model_name)
    }

    /// Discovers all `.sql` models under `models/` directory, resolves layer target paths, and constructs `ModelDag`.
    pub fn build_dag(
        &self,
        project_dir: &Path,
        cli_format_override: Option<OutputFormat>,
    ) -> Result<ModelDag> {
        let models_dir = project_dir.join(&self.models_dir);
        let mut dag = ModelDag::new();

        if !models_dir.exists() {
            let default_fmt = cli_format_override.unwrap_or(OutputFormat::Parquet);
            dag.add_model_with_format(
                "stg_users",
                "SELECT 1 AS id, 'Alice' AS name, 'admin' AS role",
                Materialization::Table,
                default_fmt,
                None,
                "",
            )?;
            dag.build_graph()?;
            return Ok(dag);
        }

        let mut model_count = 0;
        for entry in WalkDir::new(&models_dir).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == "sql") {
                let stem = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .context("Invalid file stem")?;
                let raw_sql = fs::read_to_string(path)?;

                let layer = ModelLayer::from_name(stem);
                let format = cli_format_override.clone().unwrap_or_else(|| {
                    let layer_key = match layer {
                        ModelLayer::Staging => "staging",
                        ModelLayer::Transform => "transforms",
                        ModelLayer::Mart => "marts",
                    };
                    if let Some(l_cfg) = self.layers.get(layer_key) {
                        match l_cfg.default_format.as_deref() {
                            Some("parquet") => OutputFormat::Parquet,
                            Some("jsonl") => OutputFormat::Jsonl,
                            Some("csv") => OutputFormat::Csv,
                            Some("iceberg") => OutputFormat::Iceberg,
                            Some("parquet_and_iceberg") => OutputFormat::ParquetAndIceberg,
                            _ => OutputFormat::Parquet,
                        }
                    } else {
                        OutputFormat::Parquet
                    }
                });

                let target_file_path = match format {
                    OutputFormat::Iceberg => {
                        // Directory table root: lake/.../model_name/
                        self.resolve_model_target_dir(project_dir, stem, layer)
                    }
                    OutputFormat::Parquet
                    | OutputFormat::ParquetAndIceberg
                    | OutputFormat::ZeroCopyClone => {
                        self.resolve_model_target_path(project_dir, stem, layer, "parquet")
                    }
                    OutputFormat::Jsonl => {
                        self.resolve_model_target_path(project_dir, stem, layer, "jsonl")
                    }
                    OutputFormat::Csv => {
                        self.resolve_model_target_path(project_dir, stem, layer, "csv")
                    }
                };

                dag.add_model_with_format(
                    stem,
                    &raw_sql,
                    Materialization::Table,
                    format,
                    Some(target_file_path.to_string_lossy().to_string()),
                    "",
                )?;
                model_count += 1;
            }
        }

        if model_count == 0 {
            let default_fmt = cli_format_override.unwrap_or(OutputFormat::Parquet);
            dag.add_model_with_format(
                "stg_users",
                "SELECT 1 AS id, 'Alice' AS name, 'admin' AS role",
                Materialization::Table,
                default_fmt,
                None,
                "",
            )?;
        }

        dag.build_graph()?;
        Ok(dag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layer_target_path_resolution() -> Result<()> {
        let config = RbtProjectConfig::default();
        let project_dir = Path::new("/tmp/test_project");

        let stg_path = config.resolve_model_target_path(
            project_dir,
            "stg_trades",
            ModelLayer::Staging,
            "parquet",
        );
        assert_eq!(stg_path, project_dir.join("lake/silver/stg_trades.parquet"));

        let tf_path = config.resolve_model_target_path(
            project_dir,
            "tf_1m_bars",
            ModelLayer::Transform,
            "parquet",
        );
        assert_eq!(tf_path, project_dir.join("lake/gold/tf_1m_bars.parquet"));

        let mart_path = config.resolve_model_target_path(
            project_dir,
            "fact_1d_bars",
            ModelLayer::Mart,
            "parquet",
        );
        assert_eq!(
            mart_path,
            project_dir.join("lake/gold/fact_1d_bars.parquet")
        );

        Ok(())
    }
}

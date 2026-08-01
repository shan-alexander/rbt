use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use super::dag::{Materialization, ModelDag, ModelLayer, OutputFormat};
use super::paths::{resolve_configured_path, resolve_project_path};

/// Default MemTable row cutoff when `ref_strategy: memtable` and max rows omitted.
pub const DEFAULT_MEMTABLE_MAX_ROWS: usize = 50_000;

/// How completed models are exposed to downstream `{{ ref() }}` in the same run.
///
/// Default is lake-as-truth **parquet / file re-read** (no long-lived MemTable).
/// Opt into MemTable via `materialize.ref_strategy: memtable` in `rbt_project.yml`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefStrategy {
    /// Always re-read the written lake file for `ref()` (default).
    #[default]
    #[serde(alias = "parquet_reread", alias = "lake", alias = "file")]
    Parquet,
    /// Keep an in-memory `MemTable` when `row_count < memtable_max_rows`; else re-read file.
    #[serde(alias = "mem_table", alias = "memory", alias = "arc")]
    Memtable,
}

/// Chosen backend after applying strategy + row cutoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefBackend {
    /// DataFusion `MemTable` holding Arrow batches (Arc-shared).
    MemTable,
    /// Re-read model output from the lake path (`register_parquet` / json / csv).
    LakeFile,
}

/// How model SQL results are written to the lake.
///
/// Default is **`stream`**: `execute_stream` → batch write → drop batch (no full
/// `Vec<RecordBatch>` retention). Use **`collect`** only for debugging / emergency.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializeMode {
    /// Pull DataFusion stream batch-by-batch; atomic publish; bounded peak RAM.
    #[default]
    #[serde(alias = "streaming")]
    Stream,
    /// `DataFrame::collect` then write (legacy; holds full result in RAM).
    #[serde(alias = "batch", alias = "legacy")]
    Collect,
}

/// Default Parquet max row-group row count for streaming writers.
pub const DEFAULT_MAX_ROW_GROUP_ROWS: usize = 1_000_000;
/// Default Parquet in-progress size threshold before `flush()` (128 MiB).
pub const DEFAULT_MAX_ROW_GROUP_BYTES: usize = 128 * 1024 * 1024;

/// How `OutputFormat::Iceberg` is written.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IcebergWriteMode {
    /// Official `iceberg` crate: create table → DataFileWriter → fast_append commit.
    #[default]
    #[serde(alias = "catalog_commit", alias = "sor")]
    Catalog,
    /// Hand-rolled FS layout (`data/` + `metadata/vN.json`) — demos / dual-write sidecar.
    #[serde(alias = "fs", alias = "layout")]
    Filesystem,
}

/// Iceberg-specific materialize options (`materialize.iceberg:`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergConfig {
    /// `catalog` (default, P2 SoR) | `filesystem` (legacy layout)
    #[serde(default)]
    pub mode: IcebergWriteMode,
    /// Catalog namespace (MemoryCatalog single level). Default: `rbt`.
    #[serde(default = "default_iceberg_namespace")]
    pub namespace: String,
}

fn default_iceberg_namespace() -> String {
    "rbt".into()
}

impl Default for IcebergConfig {
    fn default() -> Self {
        Self {
            mode: IcebergWriteMode::Catalog,
            namespace: default_iceberg_namespace(),
        }
    }
}

/// Optional materialization / `ref()` registration policy (`materialize:` in yml).
///
/// All fields are optional; omitting the whole block keeps lake-as-truth Parquet re-read
/// and **stream** write mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializeConfig {
    /// `stream` (default) | `collect`
    #[serde(default)]
    pub mode: MaterializeMode,
    /// `parquet` (default) | `memtable`
    #[serde(default)]
    pub ref_strategy: RefStrategy,
    /// Used only when `ref_strategy: memtable`. Defaults to [`DEFAULT_MEMTABLE_MAX_ROWS`].
    #[serde(default = "default_memtable_max_rows")]
    pub memtable_max_rows: usize,
    /// Parquet `WriterProperties` max rows per row group (stream + collect writers).
    #[serde(default = "default_max_row_group_rows")]
    pub max_row_group_rows: usize,
    /// Soft flush threshold for Parquet `in_progress_size` (bytes).
    #[serde(default = "default_max_row_group_bytes")]
    pub max_row_group_bytes: usize,
    /// Iceberg catalog vs filesystem layout.
    #[serde(default)]
    pub iceberg: IcebergConfig,
    /// Write-Audit-Publish: stage under `.wap/{run_id}/`, audit, then atomic publish.
    /// Default false (stream still uses partial→rename atomicity without WAP dirs).
    #[serde(default)]
    pub wap: bool,
}

fn default_memtable_max_rows() -> usize {
    DEFAULT_MEMTABLE_MAX_ROWS
}

fn default_max_row_group_rows() -> usize {
    DEFAULT_MAX_ROW_GROUP_ROWS
}

fn default_max_row_group_bytes() -> usize {
    DEFAULT_MAX_ROW_GROUP_BYTES
}

impl Default for MaterializeConfig {
    fn default() -> Self {
        Self {
            mode: MaterializeMode::Stream,
            ref_strategy: RefStrategy::Parquet,
            memtable_max_rows: DEFAULT_MEMTABLE_MAX_ROWS,
            max_row_group_rows: DEFAULT_MAX_ROW_GROUP_ROWS,
            max_row_group_bytes: DEFAULT_MAX_ROW_GROUP_BYTES,
            iceberg: IcebergConfig::default(),
            wap: false,
        }
    }
}

/// Default max size for a single opaque protobuf bronze file (1 GiB).
pub const DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES: u64 = 1024 * 1024 * 1024;

/// Optional scan / bronze ingest limits (`scan:` in yml).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanConfig {
    /// Max bytes for one `source_format: protobuf` file. Default: 1 GiB.
    ///
    /// Override to raise/lower the safety cap for opaque `payload` columns.
    #[serde(default = "default_protobuf_max_payload_bytes")]
    pub protobuf_max_payload_bytes: u64,
    /// Spill Arrow IPC bronze (hive / multi-file) to a single Parquet cache, then
    /// register via DataFusion listing — avoids holding every IPC partition in a MemTable.
    ///
    /// Default **true**. Set `false` only for tiny trees or debugging MemTable path.
    #[serde(default = "default_spill_arrow_ipc")]
    pub spill_arrow_ipc: bool,
    /// Directory (project-relative or absolute / `$root`) for bronze spill files.
    /// Default: `.rbt/bronze_spill`.
    #[serde(default = "default_spill_dir")]
    pub spill_dir: String,
}

fn default_protobuf_max_payload_bytes() -> u64 {
    DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES
}

fn default_spill_arrow_ipc() -> bool {
    true
}

fn default_spill_dir() -> String {
    ".rbt/bronze_spill".into()
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            protobuf_max_payload_bytes: DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES,
            spill_arrow_ipc: true,
            spill_dir: default_spill_dir(),
        }
    }
}

impl MaterializeConfig {
    /// Resolve write mode: env `RBT_MATERIALIZE_MODE=stream|collect` overrides yml.
    pub fn effective_mode(&self) -> MaterializeMode {
        match std::env::var("RBT_MATERIALIZE_MODE")
            .or_else(|_| std::env::var("RBT_STREAM_MATERIALIZE"))
            .ok()
            .as_deref()
        {
            // RBT_STREAM_MATERIALIZE=1 / true → stream; 0 / false → collect
            Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("on") => {
                MaterializeMode::Stream
            }
            Some("0") | Some("false") | Some("FALSE") | Some("no") | Some("off") => {
                MaterializeMode::Collect
            }
            Some(s) if s.eq_ignore_ascii_case("stream") || s.eq_ignore_ascii_case("streaming") => {
                MaterializeMode::Stream
            }
            Some(s) if s.eq_ignore_ascii_case("collect") || s.eq_ignore_ascii_case("batch") => {
                MaterializeMode::Collect
            }
            _ => self.mode,
        }
    }

    /// Decide MemTable vs lake file for a model that produced `row_count` rows.
    pub fn choose_ref_backend(&self, row_count: usize) -> RefBackend {
        match self.ref_strategy {
            RefStrategy::Parquet => RefBackend::LakeFile,
            RefStrategy::Memtable if row_count < self.memtable_max_rows => RefBackend::MemTable,
            RefStrategy::Memtable => RefBackend::LakeFile,
        }
    }
}

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
    /// Logical contract / model-pack version for fingerprints and skip-if-match (P5b).
    /// Bump when silver SQL or bronze column contracts change meaning.
    #[serde(default)]
    pub contract_version: Option<String>,
    #[serde(default)]
    pub layers: HashMap<String, LayerConfig>,
    /// Optional; defaults to lake-as-truth Parquet re-read for `ref()`.
    #[serde(default)]
    pub materialize: MaterializeConfig,
    /// Optional bronze scan limits (e.g. protobuf payload cap).
    #[serde(default)]
    pub scan: ScanConfig,
    /// Named absolute (or relative) roots for multi-root lakes.
    ///
    /// Referenced in paths as `$name` or `${name}` (e.g. `$nonprod_lake/lz/runs`).
    #[serde(default)]
    pub roots: HashMap<String, String>,
}

impl Default for RbtProjectConfig {
    fn default() -> Self {
        let mut layers = HashMap::new();
        layers.insert(
            "staging".to_string(),
            LayerConfig {
                path: PathBuf::from("models/staging"),
                // Silver endpoints (stg_*)
                target_path: PathBuf::from("lake/silver/stage"),
                default_format: Some("parquet".to_string()),
            },
        );
        layers.insert(
            "transforms".to_string(),
            LayerConfig {
                path: PathBuf::from("models/transforms"),
                // Gold transforms (ref stg_* only)
                target_path: PathBuf::from("lake/gold/tf"),
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
            contract_version: None,
            layers,
            materialize: MaterializeConfig::default(),
            scan: ScanConfig::default(),
            roots: HashMap::new(),
        }
    }
}

impl RbtProjectConfig {
    /// Loads `rbt_project.yml` from project directory or returns default configuration.
    pub fn load(project_dir: &Path) -> Result<Self> {
        let project_file = project_dir.join("rbt_project.yml");
        if project_file.exists() {
            let content = fs::read_to_string(&project_file).with_context(|| {
                format!(
                    "E_RBT_PROJECT_LOAD: cannot read project file {}",
                    project_file.display()
                )
            })?;
            let mut config: RbtProjectConfig = serde_yaml::from_str(&content).with_context(|| {
                format!(
                    "E_RBT_PROJECT_LOAD: failed to parse {}. \
                     Check required keys (name, version, models_dir, target_path) and \
                     optional materialize:/scan:/roots:/layers blocks.",
                    project_file.display()
                )
            })?;

            let defaults = Self::default();
            for (key, val) in defaults.layers {
                config.layers.entry(key).or_insert(val);
            }
            Ok(config)
        } else {
            Ok(Self::default())
        }
    }

    /// Resolve a configured path (absolute, relative, or `$root/...`) against the project.
    pub fn resolve_path(&self, project_dir: &Path, configured: &str) -> Result<PathBuf> {
        resolve_project_path(project_dir, configured, &self.roots)
    }

    /// Layer output directory (file parent for flat parquet, or table root parent).
    pub fn resolve_layer_target_dir(
        &self,
        project_dir: &Path,
        layer: ModelLayer,
    ) -> Result<PathBuf> {
        let layer_key = match layer {
            ModelLayer::Staging => "staging",
            ModelLayer::Transform => "transforms",
            ModelLayer::Mart => "marts",
        };
        if let Some(layer_cfg) = self.layers.get(layer_key) {
            resolve_configured_path(project_dir, &layer_cfg.target_path, &self.roots)
        } else {
            resolve_configured_path(project_dir, &self.target_path, &self.roots)
        }
    }

    /// Resolves destination output file path for a model based on its layer configuration.
    ///
    /// Supports absolute `target_path` and `$root` templates — never nests an absolute
    /// lake path under `project_dir`.
    pub fn resolve_model_target_path(
        &self,
        project_dir: &Path,
        model_name: &str,
        layer: ModelLayer,
        ext: &str,
    ) -> Result<PathBuf> {
        let dir = self
            .resolve_layer_target_dir(project_dir, layer)
            .with_context(|| {
                format!(
                    "E_RBT_MODEL_TARGET: cannot resolve output directory for model '{model_name}' \
                 (layer={layer:?}). Check `layers.*.target_path`, top-level `target_path`, and \
                 `roots:` in rbt_project.yml."
                )
            })?;
        Ok(dir.join(format!("{model_name}.{ext}")))
    }

    /// Directory target for Iceberg-style table roots (no file extension).
    pub fn resolve_model_target_dir(
        &self,
        project_dir: &Path,
        model_name: &str,
        layer: ModelLayer,
    ) -> Result<PathBuf> {
        let dir = self
            .resolve_layer_target_dir(project_dir, layer)
            .with_context(|| {
                format!(
                    "E_RBT_MODEL_TARGET: cannot resolve table directory for model '{model_name}' \
                 (layer={layer:?}). Check layer target_path and roots:."
                )
            })?;
        Ok(dir.join(model_name))
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
                let stem = path.file_stem().and_then(|s| s.to_str()).with_context(|| {
                    format!(
                        "E_RBT_MODEL_NAME: invalid file stem for model path {}",
                        path.display()
                    )
                })?;
                let raw_sql = fs::read_to_string(path).with_context(|| {
                    format!(
                        "E_RBT_MODEL_IO: failed reading model SQL {}",
                        path.display()
                    )
                })?;

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
                    OutputFormat::Iceberg => self
                        .resolve_model_target_dir(project_dir, stem, layer)
                        .with_context(|| {
                            format!(
                                "E_RBT_MODEL_TARGET: model '{stem}' (Iceberg) — \
                                 failed resolving layer target. \
                                 layer={layer:?}; project={}",
                                project_dir.display()
                            )
                        })?,
                    OutputFormat::Parquet
                    | OutputFormat::ParquetAndIceberg
                    | OutputFormat::ZeroCopyClone => self
                        .resolve_model_target_path(project_dir, stem, layer, "parquet")
                        .with_context(|| {
                            format!(
                                "E_RBT_MODEL_TARGET: model '{stem}' (parquet) — \
                                 failed resolving layer target. \
                                 layer={layer:?}; project={}",
                                project_dir.display()
                            )
                        })?,
                    OutputFormat::Jsonl => self
                        .resolve_model_target_path(project_dir, stem, layer, "jsonl")
                        .with_context(|| {
                            format!(
                                "E_RBT_MODEL_TARGET: model '{stem}' (jsonl) — \
                                 failed resolving layer target"
                            )
                        })?,
                    OutputFormat::Csv => self
                        .resolve_model_target_path(project_dir, stem, layer, "csv")
                        .with_context(|| {
                            format!(
                                "E_RBT_MODEL_TARGET: model '{stem}' (csv) — \
                                 failed resolving layer target"
                            )
                        })?,
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
        )?;
        assert_eq!(
            stg_path,
            project_dir.join("lake/silver/stage/stg_trades.parquet")
        );

        let tf_path = config.resolve_model_target_path(
            project_dir,
            "tf_1m_bars",
            ModelLayer::Transform,
            "parquet",
        )?;
        assert_eq!(
            tf_path,
            project_dir.join("lake/gold/tf/tf_1m_bars.parquet")
        );

        let mart_path = config.resolve_model_target_path(
            project_dir,
            "fact_1d_bars",
            ModelLayer::Mart,
            "parquet",
        )?;
        assert_eq!(
            mart_path,
            project_dir.join("lake/gold/fact_1d_bars.parquet")
        );

        Ok(())
    }

    #[test]
    fn materialize_defaults_to_parquet_reread() {
        let cfg = MaterializeConfig::default();
        assert_eq!(cfg.ref_strategy, RefStrategy::Parquet);
        assert_eq!(cfg.mode, MaterializeMode::Stream);
        assert_eq!(cfg.memtable_max_rows, DEFAULT_MEMTABLE_MAX_ROWS);
        assert_eq!(cfg.max_row_group_rows, DEFAULT_MAX_ROW_GROUP_ROWS);
        assert_eq!(cfg.choose_ref_backend(0), RefBackend::LakeFile);
        assert_eq!(cfg.choose_ref_backend(1_000_000), RefBackend::LakeFile);
    }

    #[test]
    fn materialize_mode_from_yaml() -> Result<()> {
        let yml = r#"
name: t
version: "1"
models_dir: models
target_path: lake/gold
materialize:
  mode: collect
  max_row_group_rows: 1000
"#;
        let cfg: RbtProjectConfig = serde_yaml::from_str(yml)?;
        assert_eq!(cfg.materialize.mode, MaterializeMode::Collect);
        assert_eq!(cfg.materialize.max_row_group_rows, 1000);
        Ok(())
    }

    #[test]
    fn materialize_memtable_respects_cutoff() {
        let cfg = MaterializeConfig {
            ref_strategy: RefStrategy::Memtable,
            memtable_max_rows: 50_000,
            ..Default::default()
        };
        assert_eq!(cfg.choose_ref_backend(49_999), RefBackend::MemTable);
        assert_eq!(cfg.choose_ref_backend(50_000), RefBackend::LakeFile);
        assert_eq!(cfg.choose_ref_backend(50_001), RefBackend::LakeFile);
    }

    #[test]
    fn parse_materialize_block_from_yaml() -> Result<()> {
        let yml = r#"
name: t
version: "1"
models_dir: models
target_path: lake/gold
materialize:
  ref_strategy: memtable
  memtable_max_rows: 10000
"#;
        let cfg: RbtProjectConfig = serde_yaml::from_str(yml)?;
        assert_eq!(cfg.materialize.ref_strategy, RefStrategy::Memtable);
        assert_eq!(cfg.materialize.memtable_max_rows, 10_000);
        assert_eq!(
            cfg.materialize.choose_ref_backend(9_999),
            RefBackend::MemTable
        );
        assert_eq!(
            cfg.materialize.choose_ref_backend(10_000),
            RefBackend::LakeFile
        );
        Ok(())
    }

    #[test]
    fn parse_project_without_materialize_uses_defaults() -> Result<()> {
        let yml = r#"
name: t
version: "1"
models_dir: models
target_path: lake/gold
"#;
        let cfg: RbtProjectConfig = serde_yaml::from_str(yml)?;
        assert_eq!(cfg.materialize, MaterializeConfig::default());
        Ok(())
    }

    #[test]
    fn parse_memtable_without_max_rows_defaults_cutoff() -> Result<()> {
        let yml = r#"
name: t
version: "1"
models_dir: models
target_path: lake/gold
materialize:
  ref_strategy: memtable
"#;
        let cfg: RbtProjectConfig = serde_yaml::from_str(yml)?;
        assert_eq!(cfg.materialize.ref_strategy, RefStrategy::Memtable);
        assert_eq!(cfg.materialize.memtable_max_rows, DEFAULT_MEMTABLE_MAX_ROWS);
        Ok(())
    }

    #[test]
    fn absolute_layer_target_not_nested_under_project() -> Result<()> {
        let yml = r#"
name: multi_root_demo
version: "1"
models_dir: models
target_path: /mnt/datalake/acme/nonprod/lake_us/lake/gold
layers:
  staging:
    path: models/staging
    target_path: /mnt/datalake/acme/nonprod/lake_us/lake/silver/stage
    default_format: parquet
"#;
        let cfg: RbtProjectConfig = serde_yaml::from_str(yml)?;
        let project = Path::new("/home/dev/rbt_projects/demo");
        let stg =
            cfg.resolve_model_target_path(project, "stg_events", ModelLayer::Staging, "parquet")?;
        assert_eq!(
            stg,
            PathBuf::from(
                "/mnt/datalake/acme/nonprod/lake_us/lake/silver/stage/stg_events.parquet"
            )
        );
        assert!(!stg.starts_with(project));
        Ok(())
    }

    #[test]
    fn multi_root_template_in_layer_target() -> Result<()> {
        let yml = r#"
name: multi_root_demo
version: "1"
models_dir: models
target_path: $nonprod_lake/gold
roots:
  nonprod_lake: /mnt/datalake/acme/nonprod/lake_us/lake
layers:
  staging:
    path: models/staging
    target_path: $nonprod_lake/silver/stage
    default_format: parquet
"#;
        let cfg: RbtProjectConfig = serde_yaml::from_str(yml)?;
        let project = Path::new("/home/dev/proj");
        let dir = cfg.resolve_layer_target_dir(project, ModelLayer::Staging)?;
        assert_eq!(
            dir,
            PathBuf::from("/mnt/datalake/acme/nonprod/lake_us/lake/silver/stage")
        );
        Ok(())
    }

    #[test]
    fn bad_root_in_layer_target_is_error() {
        let yml = r#"
name: t
version: "1"
models_dir: models
target_path: lake/gold
layers:
  staging:
    path: models/staging
    target_path: $missing_root/silver
    default_format: parquet
"#;
        let cfg: RbtProjectConfig = serde_yaml::from_str(yml).unwrap();
        let err = cfg
            .resolve_layer_target_dir(Path::new("/proj"), ModelLayer::Staging)
            .unwrap_err()
            .to_string();
        assert!(err.contains("E_RBT_ROOT_UNKNOWN") || err.contains("E_RBT_LAYER_PATH"));
    }

    #[test]
    fn scan_config_defaults_protobuf_cap() {
        let cfg = ScanConfig::default();
        assert_eq!(
            cfg.protobuf_max_payload_bytes,
            DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES
        );
        assert_eq!(DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES, 1024 * 1024 * 1024);
    }

    #[test]
    fn scan_config_override_from_yml() -> Result<()> {
        let yml = r#"
name: t
version: "1"
models_dir: models
target_path: lake/gold
scan:
  protobuf_max_payload_bytes: 4096
"#;
        let cfg: RbtProjectConfig = serde_yaml::from_str(yml)?;
        assert_eq!(cfg.scan.protobuf_max_payload_bytes, 4096);
        // omit scan: → default 1 GiB
        let yml2 = r#"
name: t
version: "1"
models_dir: models
target_path: lake/gold
"#;
        let cfg2: RbtProjectConfig = serde_yaml::from_str(yml2)?;
        assert_eq!(
            cfg2.scan.protobuf_max_payload_bytes,
            DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES
        );
        Ok(())
    }

    /// Workspace examples stay loadable / 0.3.7-shaped (roots + defaults).
    #[test]
    fn load_workspace_example_projects() -> Result<()> {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo = manifest.join("../..");
        for (rel, name, expect_root) in [
            ("examples/smoke_fixture", "smoke_fixture", "lake"),
            ("examples/full_e2e_rbt_example", "market_bars", "lake"),
        ] {
            let dir = repo.join(rel);
            if !dir.join("rbt_project.yml").is_file() {
                // crates.io source package may omit large e2e bronze; skip if missing
                continue;
            }
            let cfg = RbtProjectConfig::load(&dir)?;
            assert_eq!(cfg.name, name, "example {rel}");
            assert_eq!(
                cfg.roots.get("lake").map(String::as_str),
                Some(expect_root),
                "example {rel} should declare roots.lake"
            );
            assert_eq!(cfg.materialize.ref_strategy, RefStrategy::Parquet);
            assert_eq!(
                cfg.scan.protobuf_max_payload_bytes,
                DEFAULT_PROTOBUF_MAX_PAYLOAD_BYTES
            );
            let silver = cfg.resolve_layer_target_dir(&dir, ModelLayer::Staging)?;
            let silver_s = silver.to_string_lossy();
            assert!(
                silver_s.contains("lake/silver") || silver_s.contains("lake\\silver"),
                "staging target for {rel}: {silver_s}"
            );
            // DAG builds for smoke always; e2e only if models present
            if dir.join("models").is_dir() {
                let dag = cfg.build_dag(&dir, None)?;
                assert!(
                    dag.graph.node_count() >= 3,
                    "example {rel} expected ≥3 models, got {}",
                    dag.graph.node_count()
                );
            }
        }
        Ok(())
    }
}

//! `rbt doctor` — preflight project health (config, roots, layers, models).

use super::diagnostics::{DoctorReport, DoctorSeverity};
use super::project::RbtProjectConfig;
use anyhow::Result;
use std::path::{Path, PathBuf};

/// Run project preflight checks. Does not execute the DAG.
pub fn run_doctor(project_dir: &Path) -> Result<DoctorReport> {
    let mut report = DoctorReport {
        project_dir: project_dir.display().to_string(),
        ok: true,
        findings: Vec::new(),
    };

    if !project_dir.exists() {
        report.push(
            DoctorSeverity::Error,
            "E_RBT_PROJECT_DIR",
            format!("project_dir does not exist: {}", project_dir.display()),
            Some(project_dir.to_path_buf()),
        );
        return Ok(report);
    }

    let yml = project_dir.join("rbt_project.yml");
    if !yml.is_file() {
        report.push(
            DoctorSeverity::Error,
            "E_RBT_PROJECT_MISSING",
            String::from(
                "rbt_project.yml not found (CLI requires it unless RBT_ALLOW_DEFAULT_PROJECT=1)",
            ),
            Some(yml.clone()),
        );
    } else {
        report.push(
            DoctorSeverity::Ok,
            "OK_PROJECT_YML",
            String::from("rbt_project.yml present"),
            Some(yml.clone()),
        );
        match RbtProjectConfig::load(project_dir) {
            Ok(cfg) => {
                report.push(
                    DoctorSeverity::Ok,
                    "OK_PROJECT_PARSE",
                    format!("parsed project name='{}' version='{}'", cfg.name, cfg.version),
                    None,
                );
                inspect_config(project_dir, &cfg, &mut report);
                inspect_models(project_dir, &cfg, &mut report);
            }
            Err(e) => {
                report.push(
                    DoctorSeverity::Error,
                    "E_RBT_PROJECT_LOAD",
                    format!("failed to load/parse rbt_project.yml: {e:#}"),
                    Some(yml),
                );
            }
        }
    }

    Ok(report)
}

fn inspect_config(project_dir: &Path, cfg: &RbtProjectConfig, report: &mut DoctorReport) {
    if cfg.roots.is_empty() {
        report.push(
            DoctorSeverity::Warn,
            "W_RBT_ROOTS_EMPTY",
            "roots: is empty — $name path templates will not expand".into(),
            None,
        );
    } else {
        for (name, raw) in &cfg.roots {
            match cfg.resolve_path(project_dir, raw) {
                Ok(p) => {
                    if p.exists() {
                        report.push(
                            DoctorSeverity::Ok,
                            "OK_ROOT",
                            format!("root '{name}' → {}", p.display()),
                            Some(p),
                        );
                    } else {
                        report.push(
                            DoctorSeverity::Warn,
                            "W_RBT_ROOT_MISSING",
                            format!(
                                "root '{name}' resolves to {} but path does not exist yet",
                                p.display()
                            ),
                            Some(p),
                        );
                    }
                }
                Err(e) => report.push(
                    DoctorSeverity::Error,
                    "E_RBT_ROOT",
                    format!("root '{name}' ('{raw}') failed to resolve: {e}"),
                    None,
                ),
            }
        }
    }

    for key in ["staging", "transforms", "marts"] {
        if let Some(layer) = cfg.layers.get(key) {
            let layer_raw = layer.target_path.to_string_lossy();
            match cfg.resolve_path(project_dir, layer_raw.as_ref()) {
                Ok(p) => {
                    if p.exists() {
                        report.push(
                            DoctorSeverity::Ok,
                            "OK_LAYER",
                            format!("layer '{key}' target exists: {}", p.display()),
                            Some(p),
                        );
                    } else {
                        report.push(
                            DoctorSeverity::Warn,
                            "W_RBT_LAYER_DIR",
                            format!(
                                "layer '{key}' target {} does not exist yet (will be created on run)",
                                p.display()
                            ),
                            Some(p),
                        );
                    }
                }
                Err(e) => report.push(
                    DoctorSeverity::Error,
                    "E_RBT_LAYER_PATH",
                    format!("layer '{key}' path '{}': {e}", layer.target_path.display()),
                    None,
                ),
            }
        }
    }

    if cfg.materialize.wap {
        let wap = cfg
            .materialize
            .wap_root
            .as_deref()
            .unwrap_or(".wap");
        match cfg.resolve_path(project_dir, wap) {
            Ok(p) => report.push(
                DoctorSeverity::Ok,
                "OK_WAP_ROOT",
                format!(
                    "wap: true, wap_root → {} (prefer same volume as lake outputs)",
                    p.display()
                ),
                Some(p),
            ),
            Err(e) => report.push(
                DoctorSeverity::Error,
                "E_RBT_WAP",
                format!("wap_root '{wap}': {e}"),
                None,
            ),
        }
    }
}

fn inspect_models(project_dir: &Path, cfg: &RbtProjectConfig, report: &mut DoctorReport) {
    match cfg.build_dag(project_dir, None) {
        Ok(dag) => {
            let mut names: Vec<String> = dag.node_map.keys().cloned().collect();
            names.sort();
            report.push(
                DoctorSeverity::Ok,
                "OK_DAG",
                format!("DAG builds with {} model(s)", names.len()),
                None,
            );
            // Sample a few staging model output paths
            let mut checked = 0usize;
            for name in &names {
                if checked >= 5 {
                    break;
                }
                let Some(&idx) = dag.node_map.get(name) else {
                    continue;
                };
                let node = &dag.graph[idx];
                if let Some(ref op) = node.output_path {
                    let p = Path::new(op);
                    let exists = p.exists();
                    report.push(
                        if exists {
                            DoctorSeverity::Ok
                        } else {
                            DoctorSeverity::Warn
                        },
                        if exists {
                            "OK_OUTPUT"
                        } else {
                            "W_RBT_OUTPUT_MISSING"
                        },
                        format!(
                            "model '{name}' output_path {} {}",
                            p.display(),
                            if exists { "exists" } else { "missing (run model to materialize)" }
                        ),
                        Some(p.to_path_buf()),
                    );
                    checked += 1;
                }
            }
            // C0.6: identity SELECT * marts still on table rewrite → suggest alias
            inspect_identity_marts(&dag, report);
            // C3.7: concurrency config + parallel_safe + parts manifests
            inspect_execution_and_parts(project_dir, cfg, &dag, report);
        }
        Err(e) => {
            report.push(
                DoctorSeverity::Error,
                "E_RBT_DAG",
                format!("build_dag failed: {e:#}"),
                None,
            );
        }
    }
}

/// Phase 3: surface concurrency strategy, parallel_safe, and part-manifest health.
fn inspect_execution_and_parts(
    project_dir: &Path,
    cfg: &RbtProjectConfig,
    dag: &crate::core::dag::ModelDag,
    report: &mut DoctorReport,
) {
    use crate::core::work_unit::classify_parallel_contract;
    use crate::materializer::{
        load_manifest, resolve_parts_layout, table_layout_root, uses_parts_directory,
    };

    let conc = &cfg.execution.concurrency;
    report.push(
        DoctorSeverity::Ok,
        "OK_EXECUTION",
        format!(
            "execution.concurrency enabled={} strategy={} max_workers={} fanout_threshold={} \
             dirty_part_skip={} large_parts_first={} max_inflight_bytes={:?}",
            conc.enabled,
            conc.strategy.as_str(),
            conc.max_workers,
            conc.multi_value_fanout_threshold,
            conc.dirty_part_skip,
            conc.large_parts_first,
            conc.max_inflight_bytes
        ),
        None,
    );
    if conc.enabled && conc.max_workers > 1 {
        report.push(
            DoctorSeverity::Ok,
            "OK_CONCURRENT",
            "concurrency enabled: workers use private SessionContext; manifests merge under lock"
                .into(),
            None,
        );
    }

    let mut checked = 0usize;
    for (name, &idx) in &dag.node_map {
        if checked >= 12 {
            break;
        }
        let node = &dag.graph[idx];
        let contract = classify_parallel_contract(node);
        if matches!(
            node.materialization,
            crate::core::dag::Materialization::ScopedReplace
        ) || node
            .frontmatter
            .as_ref()
            .and_then(|f| f.parallel_safe)
            .is_some()
        {
            report.push(
                DoctorSeverity::Ok,
                "OK_PARALLEL_CONTRACT",
                format!(
                    "model '{name}' parallel_contract={} mat={} parts_layout={:?}",
                    contract.as_str(),
                    node.materialization.as_str(),
                    node.frontmatter
                        .as_ref()
                        .and_then(|f| f.parts_layout.as_ref())
                ),
                None,
            );
            checked += 1;
        }

        if !uses_parts_directory(&node.materialization) {
            continue;
        }
        let Some(ref op) = node.output_path else {
            continue;
        };
        let dest = Path::new(op);
        let layout = resolve_parts_layout(
            node.frontmatter
                .as_ref()
                .and_then(|f| f.parts_layout.as_deref()),
            cfg.materialize.default_parts_layout.as_deref(),
        );
        let root = table_layout_root(dest, layout);
        if !root.is_dir() {
            continue;
        }
        match load_manifest(&root) {
            Ok(man) => {
                let with_fp = man
                    .part_meta
                    .values()
                    .filter(|m| m.content_fp.is_some())
                    .count();
                let with_stats = man
                    .part_meta
                    .values()
                    .filter(|m| !m.stats.is_empty())
                    .count();
                report.push(
                    DoctorSeverity::Ok,
                    "OK_PARTS_MANIFEST",
                    format!(
                        "model '{name}' layout={} parts={} schema_v{} content_fp={with_fp} \
                         col_stats={with_stats} parallel_safe={:?} sort_within_part={:?}",
                        layout.as_str(),
                        man.parts.len(),
                        man.schema_version,
                        man.parallel_safe,
                        man.sort_within_part
                    ),
                    Some(root),
                );
            }
            Err(e) => report.push(
                DoctorSeverity::Warn,
                "W_RBT_MANIFEST",
                format!("model '{name}' parts root {} manifest: {e}", root.display()),
                Some(root),
            ),
        }
        checked += 1;
    }
    let _ = project_dir;
}

/// Warn when SQL looks like pure identity but materialization rewrites bytes.
fn inspect_identity_marts(dag: &crate::core::dag::ModelDag, report: &mut DoctorReport) {
    use crate::core::dag::Materialization;
    use crate::materializer::looks_like_identity_sql;

    for (name, &idx) in &dag.node_map {
        let node = &dag.graph[idx];
        if node.materialization.is_alias() {
            report.push(
                DoctorSeverity::Ok,
                "OK_ALIAS",
                format!(
                    "model '{name}' uses materialization=alias (zero-copy identity)"
                ),
                None,
            );
            continue;
        }
        // Only suggest for rewrite strategies that would re-encode parquet.
        if !matches!(
            node.materialization,
            Materialization::Table | Materialization::View
        ) {
            continue;
        }
        let sql = if !node.raw_sql.trim().is_empty() {
            node.raw_sql.as_str()
        } else {
            node.compiled_sql.as_str()
        };
        // Prefer raw (still has {{ ref }}); identity pattern on compiled table name also OK.
        if let Some(from) = looks_like_identity_sql(sql) {
            report.push(
                DoctorSeverity::Warn,
                "W_RBT_ALIAS_CANDIDATE",
                format!(
                    "model '{name}' looks like SELECT * identity of '{from}' but materialization is \
                     {:?} — consider `materialization: alias` (and alias_of: {from}) to avoid \
                     rewriting multi-GB files (RBT-C Phase 0)",
                    node.materialization
                ),
                node.output_path.as_ref().map(PathBuf::from),
            );
        } else if looks_like_identity_sql(&node.compiled_sql).is_some()
            && matches!(node.materialization, Materialization::Table)
        {
            report.push(
                DoctorSeverity::Warn,
                "W_RBT_ALIAS_CANDIDATE",
                format!(
                    "model '{name}' compiled SQL is SELECT * from a single table with \
                     materialization=table — consider `materialization: alias` to skip rewrite"
                ),
                node.output_path.as_ref().map(PathBuf::from),
            );
        }
    }
}

//! `rbt doctor` — preflight project health (config, roots, layers, models).

use super::diagnostics::{DoctorReport, DoctorSeverity};
use super::project::RbtProjectConfig;
use anyhow::Result;
use std::path::Path;

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

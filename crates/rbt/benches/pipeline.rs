//! Criterion benchmarks for rbt pipeline stages.
//!
//! ```bash
//! # from workspace root (uses examples/ relative to repo)
//! cargo bench -p rbt-datalake --bench pipeline
//!
//! # full 9-model e2e DAG only (slow; ~minutes)
//! RBT_BENCH_FULL=1 cargo bench -p rbt-datalake --bench pipeline -- full_e2e_dag
//! ```
//!
//! Heavy e2e groups **skip** if `examples/full_e2e_rbt_example/lake/bronze` is missing
//! (e.g. crates.io source package without lake data).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rbt::{
    LakeScanner, OutputFormat, RbtProjectConfig, ScanRequest, SelectMode, SourceFormat,
    TransformationEngine,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::runtime::Runtime;

fn workspace_root() -> PathBuf {
    // crates/rbt → repo root
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn smoke_project() -> PathBuf {
    workspace_root().join("examples/smoke_fixture")
}

fn e2e_project() -> Option<PathBuf> {
    let p = workspace_root().join("examples/full_e2e_rbt_example");
    if p.join("lake/bronze").is_dir() {
        Some(p)
    } else {
        None
    }
}

fn rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn clean_outputs(project: &Path) {
    let _ = std::fs::remove_dir_all(project.join("lake/silver"));
    let _ = std::fs::remove_dir_all(project.join("lake/gold"));
}

/// Project load + DAG compile (no IO scan of bronze files beyond path existence checks).
fn bench_compile(c: &mut Criterion) {
    let mut g = c.benchmark_group("compile");
    g.sample_size(40);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(5));

    let smoke = smoke_project();
    g.bench_function("smoke_fixture", |b| {
        b.iter(|| {
            let config = RbtProjectConfig::load(&smoke).expect("load smoke");
            let dag = config
                .build_dag(&smoke, Some(OutputFormat::Parquet))
                .expect("dag");
            black_box(dag.node_map.len())
        });
    });

    if let Some(e2e) = e2e_project() {
        g.bench_function("full_e2e_project", |b| {
            b.iter(|| {
                let config = RbtProjectConfig::load(&e2e).expect("load e2e");
                let dag = config
                    .build_dag(&e2e, Some(OutputFormat::Parquet))
                    .expect("dag");
                black_box(dag.node_map.len())
            });
        });
    }

    g.finish();
}

/// End-to-end `execute_dag` on the tiny smoke fixture (always available in repo).
fn bench_run_smoke(c: &mut Criterion) {
    let runtime = rt();
    let smoke = smoke_project();
    let mut g = c.benchmark_group("run_smoke");
    g.sample_size(25);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(8));
    g.throughput(Throughput::Elements(3)); // ~3 staging rows

    g.bench_function("full_dag_parquet", |b| {
        b.iter(|| {
            clean_outputs(&smoke);
            runtime.block_on(async {
                let config = RbtProjectConfig::load(&smoke).unwrap();
                let dag = config
                    .build_dag(&smoke, Some(OutputFormat::Parquet))
                    .unwrap();
                let engine = TransformationEngine::new();
                let summary = engine
                    .execute_dag(&dag, &smoke, smoke.join("target/bench_out"))
                    .await
                    .expect("smoke run");
                black_box(summary.total_rows_produced)
            })
        });
    });

    g.finish();
}

/// Bronze Arrow IPC scan only (1d partition — smaller than 1m).
fn bench_bronze_scan(c: &mut Criterion) {
    let Some(e2e) = e2e_project() else {
        eprintln!("[bench] skip bronze_scan: full_e2e bronze not present");
        return;
    };
    let runtime = rt();
    let mut g = c.benchmark_group("bronze_scan");
    g.sample_size(10);
    g.warm_up_time(Duration::from_secs(2));
    g.measurement_time(Duration::from_secs(20));

    g.bench_function("arrow_ipc_1d_all_files", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let mut require = HashMap::new();
                require.insert("timeframe".into(), "1d".into());
                let req = ScanRequest {
                    project_dir: e2e.clone(),
                    scan_path: "lake/bronze".into(),
                    format: SourceFormat::ArrowIpc,
                    paths: vec![],
                    toml_rows_key: None,
                    partition_by: vec!["symbol".into(), "timeframe".into()],
                    require_partitions: require,
                    inject_source_path: true,
                };
                let scanner = LakeScanner::from_request(&req);
                let batches = scanner.scan(&req).await.expect("scan 1d");
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                black_box((batches.len(), rows))
            })
        });
    });

    // 1m is large (~3.1M rows) — keep Criterion min sample_size (10)
    g.sample_size(10);
    g.measurement_time(Duration::from_secs(120));
    g.bench_function("arrow_ipc_1m_all_files", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let mut require = HashMap::new();
                require.insert("timeframe".into(), "1m".into());
                let req = ScanRequest {
                    project_dir: e2e.clone(),
                    scan_path: "lake/bronze".into(),
                    format: SourceFormat::ArrowIpc,
                    paths: vec![],
                    toml_rows_key: None,
                    partition_by: vec!["symbol".into(), "timeframe".into()],
                    require_partitions: require,
                    inject_source_path: true,
                };
                let scanner = LakeScanner::from_request(&req);
                let batches = scanner.scan(&req).await.expect("scan 1m");
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                black_box((batches.len(), rows))
            })
        });
    });

    g.finish();
}

/// Selected e2e subgraphs (1d-only path avoids pulling full 1m dim lineage when possible).
fn bench_run_e2e_select(c: &mut Criterion) {
    let Some(e2e) = e2e_project() else {
        eprintln!("[bench] skip run_e2e_select: full_e2e bronze not present");
        return;
    };
    let runtime = rt();
    let mut g = c.benchmark_group("run_e2e_select");
    g.sample_size(10);
    g.warm_up_time(Duration::from_secs(3));
    g.measurement_time(Duration::from_secs(60));

    for (id, select) in [
        ("stg_ohlcv_1d", "stg_ohlcv_1d"),
        ("tf_bar_metrics_1d", "tf_bar_metrics_1d"), // + ancestors → stg_1d
    ] {
        g.bench_with_input(BenchmarkId::new("parquet", id), &select, |b, sel| {
            b.iter(|| {
                clean_outputs(&e2e);
                runtime.block_on(async {
                    let config = RbtProjectConfig::load(&e2e).unwrap();
                    let full = config
                        .build_dag(&e2e, Some(OutputFormat::Parquet))
                        .unwrap();
                    let dag = full
                        .apply_select(Some(sel), SelectMode::Execute)
                        .expect("select");
                    let engine = TransformationEngine::new();
                    let summary = engine
                        .execute_dag(&dag, &e2e, e2e.join("target/bench_out"))
                        .await
                        .expect("e2e select run");
                    black_box((summary.models_executed, summary.total_rows_produced))
                })
            });
        });
    }

    g.finish();
}

/// Full 9-model e2e DAG — expensive. Enabled when bronze exists.
/// Use `cargo bench -p rbt-datalake --bench pipeline -- full_e2e_dag` to filter.
fn bench_run_e2e_full(c: &mut Criterion) {
    let Some(e2e) = e2e_project() else {
        eprintln!("[bench] skip run_e2e_full: full_e2e bronze not present");
        return;
    };
    // Default: run a few samples. Set RBT_BENCH_FULL=0 to skip.
    if std::env::var("RBT_BENCH_FULL").ok().as_deref() == Some("0") {
        eprintln!("[bench] skip run_e2e_full: RBT_BENCH_FULL=0");
        return;
    }

    let runtime = rt();
    let mut g = c.benchmark_group("run_e2e_full");
    // Criterion requires sample_size >= 10
    g.sample_size(10);
    g.warm_up_time(Duration::from_secs(5));
    g.measurement_time(Duration::from_secs(300));
    g.throughput(Throughput::Elements(3_110_044)); // 1m fact grain (approx)

    g.bench_function("full_e2e_dag_parquet", |b| {
        b.iter(|| {
            clean_outputs(&e2e);
            runtime.block_on(async {
                let config = RbtProjectConfig::load(&e2e).unwrap();
                let dag = config
                    .build_dag(&e2e, Some(OutputFormat::Parquet))
                    .unwrap();
                let engine = TransformationEngine::new();
                let summary = engine
                    .execute_dag(&dag, &e2e, e2e.join("target/bench_out"))
                    .await
                    .expect("full e2e");
                black_box((
                    summary.models_executed,
                    summary.total_rows_produced,
                    summary.bronze_sources_registered,
                ))
            })
        });
    });

    g.finish();
}

/// In-process materialize of synthetic batches (no bronze) — isolates Parquet writer.
fn bench_materialize_synth(c: &mut Criterion) {
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rbt::MultiFormatWriter;
    use std::sync::Arc;
    use tempfile::tempdir;

    let mut g = c.benchmark_group("materialize_synth");
    g.sample_size(20);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(10));

    for n_rows in [10_000usize, 100_000, 500_000] {
        g.throughput(Throughput::Elements(n_rows as u64));
        g.bench_with_input(
            BenchmarkId::new("parquet_write", n_rows),
            &n_rows,
            |b, &n| {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("sym", DataType::Utf8, false),
                    Field::new("px", DataType::Float64, false),
                ]));
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from_iter_values(0..n as i64)),
                        Arc::new(StringArray::from(
                            (0..n).map(|i| format!("S{}", i % 100)).collect::<Vec<_>>(),
                        )),
                        Arc::new(Float64Array::from_iter_values(
                            (0..n).map(|i| i as f64 * 0.01),
                        )),
                    ],
                )
                .unwrap();

                b.iter(|| {
                    let dir = tempdir().unwrap();
                    let path = dir.path().join("out.parquet");
                    let rows = MultiFormatWriter::write_batches(
                        black_box(std::slice::from_ref(&batch)),
                        &OutputFormat::Parquet,
                        &path,
                    )
                    .expect("write");
                    black_box(rows)
                });
            },
        );
    }

    g.finish();
}

criterion_group!(
    benches,
    bench_compile,
    bench_run_smoke,
    bench_materialize_synth,
    bench_bronze_scan,
    bench_run_e2e_select,
    bench_run_e2e_full,
);
criterion_main!(benches);

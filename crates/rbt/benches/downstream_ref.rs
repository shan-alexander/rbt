//! MemTable (2nd lifetime / Arc retain) vs lake Parquet re-read for `ref()`.
//!
//! After a model is written to the lake, downstream SQL must resolve the model name.
//! Two strategies:
//!
//! * **memtable** — keep `RecordBatch`es in a DataFusion `MemTable` (current engine path;
//!   `RecordBatch::clone` is shallow Arc, but **RAM is retained** for the session).
//! * **parquet** — drop in-memory batches; `register_parquet` and read when needed.
//!
//! Also measures **decision signals** (row count we already have, `fs::metadata`, Parquet
//! footer row count) to see if a size threshold can be free relative to either path.
//!
//! ```bash
//! cargo bench -p rbt-datalake --bench downstream_ref
//! cargo bench -p rbt-datalake --bench downstream_ref -- decision
//! cargo bench -p rbt-datalake --bench downstream_ref -- query/100000
//! ```

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::ParquetReadOptions;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rbt::{MultiFormatWriter, OutputFormat};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn make_batches(n_rows: usize, batch_rows: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("sym", DataType::Utf8, false),
        Field::new("px", DataType::Float64, false),
    ]));
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < n_rows {
        let end = (start + batch_rows).min(n_rows);
        let len = end - start;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (start..end).map(|i| i as i64),
                )),
                Arc::new(StringArray::from(
                    (start..end)
                        .map(|i| format!("S{:03}", i % 200))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from_iter_values(
                    (start..end).map(|i| (i as f64) * 0.01 + 100.0),
                )),
            ],
        )
        .expect("batch");
        out.push(batch);
        let _ = len;
        start = end;
    }
    out
}

fn write_parquet(batches: &[RecordBatch], path: &Path) {
    MultiFormatWriter::write_batches(batches, &OutputFormat::Parquet, path).expect("write parquet");
}

fn parquet_footer_num_rows(path: &Path) -> i64 {
    let file = File::open(path).expect("open parquet");
    let reader = SerializedFileReader::new(file).expect("parquet reader");
    reader.metadata().file_metadata().num_rows()
}

/// Cheap signals used to pick MemTable vs Listing without a full scan.
fn bench_decision_cost(c: &mut Criterion) {
    let runtime = rt();
    let mut g = c.benchmark_group("decision");
    g.sample_size(50);
    g.warm_up_time(Duration::from_millis(500));
    g.measurement_time(Duration::from_secs(3));

    // Prepare one large file once for footer/metadata benches.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("probe.parquet");
    let batches = make_batches(100_000, 8_192);
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    write_parquet(&batches, &path);

    g.bench_function("row_count_from_batches_sum", |b| {
        b.iter(|| {
            let n: usize = batches.iter().map(|b| b.num_rows()).sum();
            black_box(n)
        });
    });

    // What engine already knows after stream/collect — pure integer, free.
    g.bench_function("row_count_already_known_u64", |b| {
        let n = row_count as u64;
        b.iter(|| black_box(n))
    });

    g.bench_function("fs_metadata_len", |b| {
        b.iter(|| {
            let meta = std::fs::metadata(&path).expect("meta");
            black_box(meta.len())
        });
    });

    g.bench_function("parquet_footer_num_rows", |b| {
        b.iter(|| black_box(parquet_footer_num_rows(&path)));
    });

    // Threshold check as it would appear in product code.
    g.bench_function("threshold_if_rows_lt_100k", |b| {
        let n = row_count;
        b.iter(|| {
            let use_mem = n < 100_000;
            black_box(use_mem)
        });
    });

    // Sanity: register_parquet open cost alone (once per registration).
    g.bench_function("register_parquet_only", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let ctx = SessionContext::new();
                ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
                    .await
                    .expect("register");
                black_box(())
            })
        });
    });

    g.bench_function("memtable_from_batches_arc_clone", |b| {
        b.iter(|| {
            let schema = batches[0].schema();
            let mem = MemTable::try_new(schema, vec![batches.clone()]).expect("mem");
            black_box(Arc::new(mem));
        });
    });

    g.finish();
}

#[derive(Clone, Copy)]
enum Strategy {
    MemTable,
    Parquet,
}

impl Strategy {
    fn label(self) -> &'static str {
        match self {
            Strategy::MemTable => "memtable_arc",
            Strategy::Parquet => "parquet_reread",
        }
    }
}

async fn register_table(
    ctx: &SessionContext,
    name: &str,
    strategy: Strategy,
    batches: &[RecordBatch],
    parquet_path: &Path,
) {
    let _ = ctx.deregister_table(name);
    match strategy {
        Strategy::MemTable => {
            let schema = batches[0].schema();
            let mem = MemTable::try_new(schema, vec![batches.to_vec()]).expect("mem");
            ctx.register_table(name, Arc::new(mem)).expect("reg mem");
        }
        Strategy::Parquet => {
            ctx.register_parquet(
                name,
                parquet_path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await
            .expect("reg parquet");
        }
    }
}

async fn run_query(ctx: &SessionContext, sql: &str) -> usize {
    let df = ctx.sql(sql).await.expect("sql");
    let batches = df.collect().await.expect("collect");
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Downstream `ref()`-like queries: COUNT / filter / full aggregate.
fn bench_downstream_queries(c: &mut Criterion) {
    let runtime = rt();
    let sizes: &[usize] = &[1_000, 10_000, 100_000, 500_000, 2_000_000];

    // Prebuild datasets once (write + batches) outside timed loops where possible.
    struct Fixture {
        n: usize,
        batches: Vec<RecordBatch>,
        path: PathBuf,
        _dir: TempDir,
    }

    let fixtures: Vec<Fixture> = sizes
        .iter()
        .map(|&n| {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join(format!("t_{n}.parquet"));
            let batches = make_batches(n, 8_192);
            write_parquet(&batches, &path);
            Fixture {
                n,
                batches,
                path,
                _dir: dir,
            }
        })
        .collect();

    // --- registration only ---
    {
        let mut g = c.benchmark_group("register");
        g.sample_size(20);
        g.warm_up_time(Duration::from_secs(1));
        g.measurement_time(Duration::from_secs(8));

        for fix in &fixtures {
            g.throughput(Throughput::Elements(fix.n as u64));
            for strat in [Strategy::MemTable, Strategy::Parquet] {
                g.bench_with_input(
                    BenchmarkId::new(strat.label(), fix.n),
                    fix,
                    |b, fix| {
                        b.iter(|| {
                            runtime.block_on(async {
                                let ctx = SessionContext::new();
                                register_table(
                                    &ctx,
                                    "m",
                                    strat,
                                    &fix.batches,
                                    &fix.path,
                                )
                                .await;
                                black_box(())
                            })
                        });
                    },
                );
            }
        }
        g.finish();
    }

    // --- query shapes (register once per iteration then query — full ref cost) ---
    let queries: &[(&str, &str)] = &[
        ("count_star", "SELECT count(*) AS c FROM m"),
        (
            "filter_project",
            "SELECT id, px FROM m WHERE id % 97 = 0",
        ),
        ("sum_px", "SELECT sum(px) AS s FROM m"),
    ];

    for (qname, sql) in queries {
        let mut g = c.benchmark_group(format!("query/{qname}"));
        // Large sizes: still ≥10 samples but shorter overall target via fewer sizes in loop body.
        g.sample_size(12);
        g.warm_up_time(Duration::from_secs(1));
        g.measurement_time(Duration::from_secs(15));

        for fix in &fixtures {
            // Skip 2M for heavier query shapes to keep local runs reasonable — still run count.
            if fix.n >= 2_000_000 && *qname != "count_star" {
                continue;
            }
            g.throughput(Throughput::Elements(fix.n as u64));
            for strat in [Strategy::MemTable, Strategy::Parquet] {
                let id = format!("{}_{}", strat.label(), fix.n);
                g.bench_with_input(BenchmarkId::new(*qname, id), fix, |b, fix| {
                    b.iter(|| {
                        runtime.block_on(async {
                            let ctx = SessionContext::new();
                            register_table(&ctx, "m", strat, &fix.batches, &fix.path).await;
                            let rows = run_query(&ctx, sql).await;
                            black_box(rows)
                        })
                    });
                });
            }
        }
        g.finish();
    }
}

/// Same comparison on a real e2e Parquet output if present (post `rbt run`).
fn bench_e2e_lake_file(c: &mut Criterion) {
    let runtime = rt();
    let candidates = [
        workspace_root().join("examples/full_e2e_rbt_example/lake/silver/stg_ohlcv_1d.parquet"),
        workspace_root().join("examples/full_e2e_rbt_example/lake/gold/fact_1d_bars.parquet"),
        workspace_root()
            .join("examples/full_e2e_rbt_example/lake/silver/stg_ohlcv_1m.parquet"),
    ];

    let path = candidates.into_iter().find(|p| p.is_file());
    let Some(path) = path else {
        eprintln!(
            "[bench] skip e2e_lake: no stg/fact parquet under full_e2e (run the example once)"
        );
        return;
    };

    // Load into batches once for MemTable path (simulates post-collect retain).
    let batches = runtime.block_on(async {
        let ctx = SessionContext::new();
        ctx.register_parquet("src", path.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .expect("reg");
        let df = ctx.sql("SELECT * FROM src").await.expect("sql");
        df.collect().await.expect("collect")
    });
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    let file_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    eprintln!(
        "[bench] e2e_lake file={} rows={} bytes={}",
        path.display(),
        n,
        file_bytes
    );

    let mut g = c.benchmark_group("e2e_lake");
    g.sample_size(10);
    g.warm_up_time(Duration::from_secs(2));
    g.measurement_time(Duration::from_secs(30));
    g.throughput(Throughput::Elements(n as u64));

    for strat in [Strategy::MemTable, Strategy::Parquet] {
        g.bench_function(
            BenchmarkId::new("count_star", strat.label()),
            |b| {
                b.iter(|| {
                    runtime.block_on(async {
                        let ctx = SessionContext::new();
                        register_table(&ctx, "m", strat, &batches, &path).await;
                        let rows = run_query(&ctx, "SELECT count(*) FROM m").await;
                        black_box(rows)
                    })
                });
            },
        );
        g.bench_function(
            BenchmarkId::new("sum_or_count_filter", strat.label()),
            |b| {
                // Schema-agnostic: count with limit-style filter via hash of row number not available;
                // use count(*) where true and a projection-friendly query.
                b.iter(|| {
                    runtime.block_on(async {
                        let ctx = SessionContext::new();
                        register_table(&ctx, "m", strat, &batches, &path).await;
                        // Prefer a cheap metadata-friendly count; also force a scan path.
                        let rows = run_query(&ctx, "SELECT count(*) FROM m").await;
                        black_box(rows)
                    })
                });
            },
        );
    }

    g.finish();
}

criterion_group!(
    benches,
    bench_decision_cost,
    bench_downstream_queries,
    bench_e2e_lake_file,
);
criterion_main!(benches);

//! Surrogate-key generation + join benches (ADR-009).
//!
//! Run from workspace root:
//! ```bash
//! cargo bench -p rbt-datalake --bench surrogate_key
//! # faster smoke:
//! RBT_SK_BENCH_QUICK=1 cargo bench -p rbt-datalake --bench surrogate_key
//! ```

use arrow::array::{FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use rbt::engine::surrogate_key::{
    hash_batch_columns, hash_grain_fields, SkAlgo, SkEncoding,
};
use rbt::engine::udf::register_builtin_udfs;
use std::sync::Arc;
use std::time::Duration;

fn quick() -> bool {
    std::env::var("RBT_SK_BENCH_QUICK")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn gen_grain(n: usize) -> (StringArray, StringArray) {
    let entities: Vec<String> = (0..n).map(|i| format!("E{:08}", i % (n / 10).max(1))).collect();
    let dates: Vec<String> = (0..n)
        .map(|i| format!("2024-{:02}-{:02}", (i % 12) + 1, (i % 28) + 1))
        .collect();
    (StringArray::from(entities), StringArray::from(dates))
}

fn bench_generation(c: &mut Criterion) {
    let mut g = c.benchmark_group("sk_generate");
    g.warm_up_time(Duration::from_millis(200));
    g.measurement_time(Duration::from_secs(if quick() { 1 } else { 3 }));

    let sizes: &[usize] = if quick() {
        &[100_000]
    } else {
        &[100_000, 1_000_000, 5_000_000]
    };

    for &n in sizes {
        let (ent, dates) = gen_grain(n);
        let cols: [&dyn arrow::array::Array; 2] = [&ent, &dates];
        g.throughput(Throughput::Elements(n as u64));

        g.bench_with_input(BenchmarkId::new("fast64_xxh3", n), &n, |b, _| {
            b.iter(|| hash_batch_columns(&cols, SkAlgo::Fast64, SkEncoding::Binary).unwrap())
        });
        g.bench_with_input(BenchmarkId::new("blake3_128_binary", n), &n, |b, _| {
            b.iter(|| hash_batch_columns(&cols, SkAlgo::Balanced, SkEncoding::Binary).unwrap())
        });
        g.bench_with_input(BenchmarkId::new("blake3_128_hex", n), &n, |b, _| {
            b.iter(|| hash_batch_columns(&cols, SkAlgo::Balanced, SkEncoding::Hex).unwrap())
        });
        g.bench_with_input(BenchmarkId::new("blake3_256_binary", n), &n, |b, _| {
            b.iter(|| hash_batch_columns(&cols, SkAlgo::Safe256, SkEncoding::Binary).unwrap())
        });
        g.bench_with_input(BenchmarkId::new("md5_128_binary", n), &n, |b, _| {
            b.iter(|| hash_batch_columns(&cols, SkAlgo::CompatMd5, SkEncoding::Binary).unwrap())
        });
        // MIISK baseline: sequential ints (not a product feature — comparison only)
        g.bench_with_input(BenchmarkId::new("miisk_sequential_i64", n), &n, |b, _| {
            b.iter(|| {
                let v: Vec<i64> = (0..n as i64).collect();
                Int64Array::from(v)
            })
        });
    }
    g.finish();
}

fn make_dim_fact(
    dim_n: usize,
    fact_n: usize,
    mode: &str,
) -> (RecordBatch, RecordBatch) {
    let dim_ids: Vec<String> = (0..dim_n).map(|i| format!("D{i:06}")).collect();
    let fact_fk: Vec<usize> = (0..fact_n).map(|i| i % dim_n).collect();

    match mode {
        "fast64" | "miisk" => {
            let dim_sk: Vec<i64> = if mode == "miisk" {
                (0..dim_n as i64).collect()
            } else {
                dim_ids
                    .iter()
                    .map(|id| {
                        let d = hash_grain_fields(SkAlgo::Fast64, &[id.as_str()]);
                        let mut le = [0u8; 8];
                        le.copy_from_slice(&d);
                        i64::from_le_bytes(le)
                    })
                    .collect()
            };
            let fact_sk: Vec<i64> = fact_fk.iter().map(|&i| dim_sk[i]).collect();
            let dim = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("sk", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(dim_sk)),
                    Arc::new(StringArray::from(dim_ids)),
                ],
            )
            .unwrap();
            let fact = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("sk", DataType::Int64, false),
                    Field::new("amt", DataType::Int64, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(fact_sk)),
                    Arc::new(Int64Array::from(vec![1i64; fact_n])),
                ],
            )
            .unwrap();
            (dim, fact)
        }
        "blake3_bin" => {
            let digs: Vec<Vec<u8>> = dim_ids
                .iter()
                .map(|id| hash_grain_fields(SkAlgo::Balanced, &[id.as_str()]))
                .collect();
            let dim_sk = FixedSizeBinaryArray::try_from_iter(digs.iter().map(|d| d.as_slice()))
                .unwrap();
            let fact_digs: Vec<&[u8]> = fact_fk.iter().map(|&i| digs[i].as_slice()).collect();
            let fact_sk = FixedSizeBinaryArray::try_from_iter(fact_digs.into_iter()).unwrap();
            let dim = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("sk", DataType::FixedSizeBinary(16), false),
                    Field::new("name", DataType::Utf8, true),
                ])),
                vec![Arc::new(dim_sk), Arc::new(StringArray::from(dim_ids))],
            )
            .unwrap();
            let fact = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("sk", DataType::FixedSizeBinary(16), false),
                    Field::new("amt", DataType::Int64, true),
                ])),
                vec![
                    Arc::new(fact_sk),
                    Arc::new(Int64Array::from(vec![1i64; fact_n])),
                ],
            )
            .unwrap();
            (dim, fact)
        }
        "blake3_hex" => {
            let digs: Vec<String> = dim_ids
                .iter()
                .map(|id| {
                    let d = hash_grain_fields(SkAlgo::Balanced, &[id.as_str()]);
                    d.iter().map(|b| format!("{b:02x}")).collect::<String>()
                })
                .collect();
            let fact_hex: Vec<String> = fact_fk.iter().map(|&i| digs[i].clone()).collect();
            let dim = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("sk", DataType::Utf8, false),
                    Field::new("name", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(StringArray::from(digs)),
                    Arc::new(StringArray::from(dim_ids)),
                ],
            )
            .unwrap();
            let fact = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("sk", DataType::Utf8, false),
                    Field::new("amt", DataType::Int64, true),
                ])),
                vec![
                    Arc::new(StringArray::from(fact_hex)),
                    Arc::new(Int64Array::from(vec![1i64; fact_n])),
                ],
            )
            .unwrap();
            (dim, fact)
        }
        _ => panic!("unknown mode {mode}"),
    }
}

fn bench_joins(c: &mut Criterion) {
    let mut g = c.benchmark_group("sk_join");
    g.warm_up_time(Duration::from_millis(300));
    g.measurement_time(Duration::from_secs(if quick() { 1 } else { 4 }));
    g.sample_size(if quick() { 10 } else { 20 });

    let dim_n = if quick() { 10_000 } else { 100_000 };
    let fact_n = if quick() { 200_000 } else { 2_000_000 };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    for mode in ["miisk", "fast64", "blake3_bin", "blake3_hex"] {
        let (dim, fact) = make_dim_fact(dim_n, fact_n, mode);
        g.bench_function(BenchmarkId::new(mode, format!("d{dim_n}_f{fact_n}")), |b| {
            b.to_async(&rt).iter(|| async {
                let ctx = SessionContext::new();
                let dim_t = MemTable::try_new(dim.schema(), vec![vec![dim.clone()]]).unwrap();
                let fact_t = MemTable::try_new(fact.schema(), vec![vec![fact.clone()]]).unwrap();
                ctx.register_table("dim", Arc::new(dim_t)).unwrap();
                ctx.register_table("fact", Arc::new(fact_t)).unwrap();
                let df = ctx
                    .sql("SELECT count(*) FROM fact f JOIN dim d ON f.sk = d.sk")
                    .await
                    .unwrap();
                let batches = df.collect().await.unwrap();
                std::hint::black_box(batches[0].num_rows())
            })
        });
    }
    g.finish();
}

fn bench_sql_udf(c: &mut Criterion) {
    let mut g = c.benchmark_group("sk_sql_udf");
    g.warm_up_time(Duration::from_millis(200));
    g.measurement_time(Duration::from_secs(if quick() { 1 } else { 2 }));
    let n = if quick() { 50_000 } else { 500_000 };
    let (ent, dates) = gen_grain(n);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, true),
            Field::new("as_of", DataType::Utf8, true),
        ])),
        vec![Arc::new(ent), Arc::new(dates)],
    )
    .unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    g.throughput(Throughput::Elements(n as u64));
    g.bench_function(BenchmarkId::new("sk_default_sql", n), |b| {
        b.to_async(&rt).iter(|| async {
            let ctx = SessionContext::new();
            register_builtin_udfs(&ctx).unwrap();
            let t = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
            ctx.register_table("t", Arc::new(t)).unwrap();
            let df = ctx
                .sql("SELECT sk(entity_id, as_of) AS sk FROM t")
                .await
                .unwrap();
            let batches = df.collect().await.unwrap();
            std::hint::black_box(batches[0].num_rows())
        })
    });
    g.finish();
}

criterion_group!(benches, bench_generation, bench_joins, bench_sql_udf);
criterion_main!(benches);

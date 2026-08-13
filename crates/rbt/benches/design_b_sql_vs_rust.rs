//! Design B: SQL DAG vs Rust DAG — heavy window transforms at scale.
//!
//! ```bash
//! cargo bench -p rbt-datalake --bench design_b_sql_vs_rust
//! ```
//!
//! Both pipelines share the same seed (`stg_events`: `symbol`, `ts`, `value`) then apply
//! the **same** feature set on `tf_features`:
//! - 200-row moving average per symbol (`ROWS BETWEEN 199 PRECEDING AND CURRENT ROW`)
//! - lag(value, 1) per symbol
//! - simple return `value / lag - 1`
//!
//! SQL does this in one windowed SELECT; Rust implements the same logic in a `RustModel`.
//! Seed is a Rust model in **both** DAGs so generation cost is matched and we isolate
//! the transform path (not seed SQL parser cost).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rbt::{
    async_trait, batches_to_stream, DagBuilder, Materialization, ModelLayer, ModelSpec,
    RbtEngineBuilder, RbtProjectConfig, RunScope, RustModel, RustModelContext, RustModelOutput,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

const WINDOW: usize = 200;
const N_SYMBOLS: usize = 20;

fn rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio")
}

fn events_schema() -> arrow::datatypes::SchemaRef {
    use arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn features_schema() -> arrow::datatypes::SchemaRef {
    use arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("ma200", DataType::Float64, true),
        Field::new("lag1", DataType::Float64, true),
        Field::new("ret1", DataType::Float64, true),
    ]))
}

/// Shared seed: n rows across [`N_SYMBOLS`] partitions (round-robin symbols).
struct SeedEvents {
    n: usize,
}

#[async_trait]
impl RustModel for SeedEvents {
    fn name(&self) -> &str {
        "stg_events"
    }
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        events_schema()
    }
    async fn execute(&self, _ctx: &RustModelContext<'_>) -> anyhow::Result<RustModelOutput> {
        use arrow::array::{Float64Array, Int64Array, StringArray};
        use arrow::record_batch::RecordBatch;

        let n = self.n;
        let mut symbols = Vec::with_capacity(n);
        let mut ts = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        for i in 0..n {
            let s = i % N_SYMBOLS;
            symbols.push(format!("S{s:02}"));
            ts.push(i as i64);
            // smooth-ish series so windows are non-trivial
            values.push((i as f64).sin() * 10.0 + (s as f64) + (i % 17) as f64 * 0.1);
        }
        let batch = RecordBatch::try_new(
            self.output_schema(),
            vec![
                Arc::new(StringArray::from(symbols)),
                Arc::new(Int64Array::from(ts)),
                Arc::new(Float64Array::from(values)),
            ],
        )?;
        // Stream path exercises B5 under load.
        Ok(RustModelOutput::Stream(batches_to_stream(
            self.output_schema(),
            vec![batch],
        )))
    }
}

/// Rust port of the SQL window feature set (ma200 / lag1 / ret1).
struct WindowFeatures;

#[async_trait]
impl RustModel for WindowFeatures {
    fn name(&self) -> &str {
        "tf_features"
    }
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        features_schema()
    }
    async fn execute(&self, ctx: &RustModelContext<'_>) -> anyhow::Result<RustModelOutput> {
        use arrow::array::{Float64Array, Float64Builder, Int64Array, StringArray};
        use arrow::datatypes::DataType;
        use arrow::record_batch::RecordBatch;
        use std::collections::HashMap;

        let df = ctx
            .session
            .sql(r#"SELECT symbol, ts, CAST(value AS DOUBLE) AS value FROM "stg_events""#)
            .await?;
        let batches = df.collect().await?;

        // Collect rows (robust to parquet Utf8 / dictionary string after ref re-read).
        let mut rows: Vec<(String, i64, f64)> = Vec::new();
        for b in &batches {
            let n = b.num_rows();
            let sym_utf8 = arrow::compute::cast(b.column(0), &DataType::Utf8)
                .map_err(|e| anyhow::anyhow!("cast symbol: {e}"))?;
            let sym = sym_utf8
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("symbol not Utf8 after cast"))?;
            let ts_i64 = arrow::compute::cast(b.column(1), &DataType::Int64)
                .map_err(|e| anyhow::anyhow!("cast ts: {e}"))?;
            let tsa = ts_i64
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("ts not Int64"))?;
            let val_f64 = arrow::compute::cast(b.column(2), &DataType::Float64)
                .map_err(|e| anyhow::anyhow!("cast value: {e}"))?;
            let val = val_f64
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("value not Float64"))?;
            for i in 0..n {
                rows.push((sym.value(i).to_string(), tsa.value(i), val.value(i)));
            }
        }
        // Group by symbol, sort by ts
        let mut by_sym: HashMap<String, Vec<(i64, f64)>> = HashMap::new();
        for (s, t, v) in rows {
            by_sym.entry(s).or_default().push((t, v));
        }
        for v in by_sym.values_mut() {
            v.sort_by_key(|(t, _)| *t);
        }

        let mut out_sym = Vec::new();
        let mut out_ts = Vec::new();
        let mut out_val = Vec::new();
        let mut out_ma = Float64Builder::new();
        let mut out_lag = Float64Builder::new();
        let mut out_ret = Float64Builder::new();

        for (sym, series) in by_sym {
            let mut window_sum = 0.0f64;
            let mut window: std::collections::VecDeque<f64> = std::collections::VecDeque::new();
            for (idx, (t, v)) in series.iter().enumerate() {
                window.push_back(*v);
                window_sum += *v;
                if window.len() > WINDOW {
                    window_sum -= window.pop_front().unwrap_or(0.0);
                }
                let ma = window_sum / window.len() as f64;
                let lag = if idx > 0 {
                    Some(series[idx - 1].1)
                } else {
                    None
                };
                let ret = lag.map(|l| if l != 0.0 { v / l - 1.0 } else { f64::NAN });

                out_sym.push(sym.clone());
                out_ts.push(*t);
                out_val.push(*v);
                out_ma.append_value(ma);
                match lag {
                    Some(l) => out_lag.append_value(l),
                    None => out_lag.append_null(),
                }
                match ret {
                    Some(r) if r.is_finite() => out_ret.append_value(r),
                    _ => out_ret.append_null(),
                }
            }
        }

        let batch = RecordBatch::try_new(
            self.output_schema(),
            vec![
                Arc::new(StringArray::from(out_sym)),
                Arc::new(Int64Array::from(out_ts)),
                Arc::new(Float64Array::from(out_val)),
                Arc::new(out_ma.finish()),
                Arc::new(out_lag.finish()),
                Arc::new(out_ret.finish()),
            ],
        )?;
        Ok(RustModelOutput::Batches(vec![batch]))
    }
}

fn sql_window_features() -> &'static str {
    r#"
SELECT
  symbol,
  ts,
  value,
  AVG(value) OVER (
    PARTITION BY symbol
    ORDER BY ts
    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
  ) AS ma200,
  LAG(value, 1) OVER (
    PARTITION BY symbol
    ORDER BY ts
  ) AS lag1,
  value / NULLIF(
    LAG(value, 1) OVER (PARTITION BY symbol ORDER BY ts),
    0
  ) - 1 AS ret1
FROM {{ ref('stg_events') }}
"#
}

fn build_sql_dag(out: &std::path::Path) -> rbt::ModelDag {
    DagBuilder::new()
        .model(
            ModelSpec::rust("stg_events")
                .layer(ModelLayer::Staging)
                .materialization(Materialization::Table)
                .output_path(out.join("sql_stg.parquet").to_string_lossy()),
        )
        .model(
            ModelSpec::sql("tf_features", sql_window_features())
                .layer(ModelLayer::Transform)
                .materialization(Materialization::Table)
                .output_path(out.join("sql_tf.parquet").to_string_lossy()),
        )
        .model(
            ModelSpec::sql(
                "obt_out",
                r#"SELECT symbol, ts, value, ma200, lag1, ret1 FROM {{ ref('tf_features') }} WHERE ma200 IS NOT NULL"#,
            )
            .layer(ModelLayer::Mart)
            .materialization(Materialization::Table)
            .output_path(out.join("sql_obt.parquet").to_string_lossy()),
        )
        .build()
        .expect("sql dag")
}

fn build_rust_dag(out: &std::path::Path) -> rbt::ModelDag {
    DagBuilder::new()
        .model(
            ModelSpec::rust("stg_events")
                .layer(ModelLayer::Staging)
                .materialization(Materialization::Table)
                .output_path(out.join("rust_stg.parquet").to_string_lossy()),
        )
        .model(
            ModelSpec::rust("tf_features")
                .refs(["stg_events"])
                .layer(ModelLayer::Transform)
                .materialization(Materialization::Table)
                .output_path(out.join("rust_tf.parquet").to_string_lossy()),
        )
        .model(
            ModelSpec::sql(
                "obt_out",
                r#"SELECT symbol, ts, value, ma200, lag1, ret1 FROM {{ ref('tf_features') }} WHERE ma200 IS NOT NULL"#,
            )
            .layer(ModelLayer::Mart)
            .materialization(Materialization::Table)
            .output_path(out.join("rust_obt.parquet").to_string_lossy()),
        )
        .build()
        .expect("rust dag")
}

fn bench_sql_vs_rust(c: &mut Criterion) {
    let runtime = rt();
    let mut g = c.benchmark_group("design_b_window_sql_vs_rust");
    g.sample_size(12);
    g.warm_up_time(Duration::from_secs(2));
    g.measurement_time(Duration::from_secs(15));

    // Larger n; 20 symbols × ~rows/symbol → window of 200 is fully warm for most rows.
    for n in [10_000usize, 50_000, 100_000] {
        g.throughput(Throughput::Elements(n as u64));

        g.bench_with_input(BenchmarkId::new("sql_window_dag", n), &n, |b, &n| {
            b.iter(|| {
                runtime.block_on(async {
                    let temp = tempfile::tempdir().expect("tmp");
                    let dag = build_sql_dag(temp.path());
                    let engine = RbtEngineBuilder::new()
                        .with_rust_model(SeedEvents { n })
                        .build()
                        .await
                        .expect("engine");
                    let cfg = RbtProjectConfig::default();
                    let scope = RunScope::default();
                    let summary = engine
                        .execute_dag_with_scope(&dag, temp.path(), temp.path(), &cfg, &scope)
                        .await
                        .expect("run");
                    black_box(summary.total_rows_produced)
                })
            });
        });

        g.bench_with_input(BenchmarkId::new("rust_window_dag", n), &n, |b, &n| {
            b.iter(|| {
                runtime.block_on(async {
                    let temp = tempfile::tempdir().expect("tmp");
                    let dag = build_rust_dag(temp.path());
                    let engine = RbtEngineBuilder::new()
                        .with_rust_model(SeedEvents { n })
                        .with_rust_model(WindowFeatures)
                        .build()
                        .await
                        .expect("engine");
                    let cfg = RbtProjectConfig::default();
                    let scope = RunScope::default();
                    let summary = engine
                        .execute_dag_with_scope(&dag, temp.path(), temp.path(), &cfg, &scope)
                        .await
                        .expect("run");
                    black_box(summary.total_rows_produced)
                })
            });
        });
    }
    g.finish();
}

criterion_group!(benches, bench_sql_vs_rust);
criterion_main!(benches);

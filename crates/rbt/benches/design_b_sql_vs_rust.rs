//! Design B: SQL DAG vs Rust DAG — same data, same transform (double id).
//!
//! ```bash
//! cargo bench -p rbt-datalake --bench design_b_sql_vs_rust
//! ```
//!
//! Both pipelines: seed rows → double `id` → filter `id > 0` mart.
//! SQL does the double in SQL; Rust does it in a `RustModel` (batches).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rbt::{
    async_trait, DagBuilder, Materialization, ModelLayer, ModelSpec, RbtEngineBuilder,
    RbtProjectConfig, RunScope, RustModel, RustModelContext, RustModelOutput,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

fn rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio")
}

/// Pure SQL: stg seed → tf double in SQL → obt filter.
fn seed_sql(n: usize) -> String {
    // Compact VALUES list (avoids stack blow-up from huge UNION ALL trees).
    let vals: String = (1..=n).map(|i| format!("({i})")).collect::<Vec<_>>().join(", ");
    format!("SELECT column1 AS id FROM (VALUES {vals}) AS t")
}

fn build_sql_dag(out: &std::path::Path, n: usize) -> rbt::ModelDag {
    DagBuilder::new()
        .model(
            ModelSpec::sql("stg_seed", seed_sql(n))
                .catalog_prefix("")
                .layer(ModelLayer::Staging)
                .materialization(Materialization::Table)
                .output_path(out.join("sql_stg.parquet").to_string_lossy()),
        )
        .model(
            ModelSpec::sql(
                "tf_double",
                r#"SELECT id * 2 AS id FROM {{ ref('stg_seed') }}"#,
            )
            .catalog_prefix("")
            .layer(ModelLayer::Transform)
            .materialization(Materialization::Table)
            .output_path(out.join("sql_tf.parquet").to_string_lossy()),
        )
        .model(
            ModelSpec::sql(
                "obt_out",
                r#"SELECT id FROM {{ ref('tf_double') }} WHERE id > 0"#,
            )
            .catalog_prefix("")
            .layer(ModelLayer::Mart)
            .materialization(Materialization::Table)
            .output_path(out.join("sql_obt.parquet").to_string_lossy()),
        )
        .build()
        .expect("sql dag")
}

struct DoubleIds;
#[async_trait]
impl RustModel for DoubleIds {
    fn name(&self) -> &str {
        "tf_double"
    }
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        use arrow::datatypes::{DataType, Field, Schema};
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }
    async fn execute(&self, ctx: &RustModelContext<'_>) -> anyhow::Result<RustModelOutput> {
        use arrow::array::Int64Array;
        use arrow::record_batch::RecordBatch;
        let df = ctx.session.sql(r#"SELECT id FROM "stg_seed""#).await?;
        let batches = df.collect().await?;
        let mut ids = Vec::new();
        for b in batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("expected Int64"))?;
            for i in 0..col.len() {
                ids.push(col.value(i) * 2);
            }
        }
        let batch = RecordBatch::try_new(
            self.output_schema(),
            vec![Arc::new(Int64Array::from(ids))],
        )?;
        Ok(RustModelOutput::Batches(vec![batch]))
    }
}

/// SQL seed → Rust double → SQL mart (same math as SQL DAG).
fn build_rust_dag(out: &std::path::Path, n: usize) -> rbt::ModelDag {
    DagBuilder::new()
        .model(
            ModelSpec::sql("stg_seed", seed_sql(n))
                .catalog_prefix("")
                .layer(ModelLayer::Staging)
                .materialization(Materialization::Table)
                .output_path(out.join("rust_stg.parquet").to_string_lossy()),
        )
        .model(
            ModelSpec::rust("tf_double")
                .refs(["stg_seed"])
                .layer(ModelLayer::Transform)
                .materialization(Materialization::Table)
                .output_path(out.join("rust_tf.parquet").to_string_lossy()),
        )
        .model(
            ModelSpec::sql(
                "obt_out",
                r#"SELECT id FROM {{ ref('tf_double') }} WHERE id > 0"#,
            )
            .catalog_prefix("")
            .layer(ModelLayer::Mart)
            .materialization(Materialization::Table)
            .output_path(out.join("rust_obt.parquet").to_string_lossy()),
        )
        .build()
        .expect("rust dag")
}

fn bench_sql_vs_rust(c: &mut Criterion) {
    let runtime = rt();
    let mut g = c.benchmark_group("design_b_sql_vs_rust");
    g.sample_size(20);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(8));

    for n in [100usize, 1_000, 2_000] {
        g.throughput(Throughput::Elements(n as u64));

        g.bench_with_input(BenchmarkId::new("sql_dag", n), &n, |b, &n| {
            b.iter(|| {
                runtime.block_on(async {
                    let temp = tempfile::tempdir().expect("tmp");
                    let dag = build_sql_dag(temp.path(), n);
                    let engine = RbtEngineBuilder::new().build().await.expect("engine");
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

        g.bench_with_input(BenchmarkId::new("rust_dag", n), &n, |b, &n| {
            b.iter(|| {
                runtime.block_on(async {
                    let temp = tempfile::tempdir().expect("tmp");
                    let dag = build_rust_dag(temp.path(), n);
                    let engine = RbtEngineBuilder::new()
                        .with_rust_model(DoubleIds)
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

# rbt Criterion benches

Package: `rbt-datalake`. Harness: [criterion](https://docs.rs/criterion) 0.5.

## Run

From the **workspace root**:

```bash
# All groups (skips e2e if bronze missing; full DAG is included when bronze exists)
cargo bench -p rbt-datalake --bench pipeline

# Filter by name substring
cargo bench -p rbt-datalake --bench pipeline -- compile
cargo bench -p rbt-datalake --bench pipeline -- run_smoke
cargo bench -p rbt-datalake --bench pipeline -- bronze_scan
cargo bench -p rbt-datalake --bench pipeline -- run_e2e_select
cargo bench -p rbt-datalake --bench pipeline -- full_e2e_dag

# Skip full 9-model DAG (still runs 1d select + bronze scan)
RBT_BENCH_FULL=0 cargo bench -p rbt-datalake --bench pipeline
```

HTML reports: `target/criterion/report/index.html` (after a run).

## Data

| Group | Dataset |
|-------|---------|
| `compile` / `run_smoke` | `examples/smoke_fixture` (in-repo) |
| `materialize_synth` | synthetic Arrow batches |
| `bronze_scan` / `run_e2e_*` | `examples/full_e2e_rbt_example/lake/bronze` (~447MB) |

E2e groups **no-op skip** if bronze is absent so `cargo bench` does not fail on a thin checkout.

## Interpreting results

- Wall times are **machine-specific**; commit messages / PRs should note CPU class.
- Full e2e uses `sample_size = 10` (Criterion minimum); each iter is ~20s on a mid laptop.
- Use these numbers as **baselines** for streaming materialize RSS/time comparisons (see `docs/STREAMING_MATERIALIZE_PLAN.md`).

## Baseline snapshot (2026-07-31)

Machine: AMD Ryzen 7 PRO 6850U (16 threads), 28 GiB RAM, NixOS, `cargo bench` release.

| Benchmark | Median (approx) | Notes |
|-----------|-----------------|-------|
| `compile/smoke_fixture` | **2.4 ms** | project load + DAG |
| `compile/full_e2e_project` | **6.5 ms** | 9 models |
| `run_smoke/full_dag_parquet` | **9.5 ms** | 3 models, tiny JSONL |
| `materialize_synth/parquet_write/10k` | **1.57 ms** | ~6.4 M rows/s |
| `materialize_synth/parquet_write/100k` | **16.1 ms** | ~6.2 M rows/s |
| `materialize_synth/parquet_write/500k` | **50.0 ms** | ~10 M rows/s |
| `bronze_scan/arrow_ipc_1d_all_files` | **32–34 ms** | 73 files, ~35k rows |
| `bronze_scan/arrow_ipc_1m_all_files` | **368 ms** | 2638 files, ~3.1M rows |
| `run_e2e_select/stg_ohlcv_1d` | **110 ms** | scan+SQL+write+tests |
| `run_e2e_select/tf_bar_metrics_1d` | **197 ms** | stg_1d + metrics |
| `run_e2e_full/full_e2e_dag_parquet` | **20.7 s** | 9 models, ~9.4M row-writes |

Re-run after streaming materialize lands and compare wall time + RSS.

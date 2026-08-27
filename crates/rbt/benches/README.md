# rbt Criterion benches

Package: `rbt-datalake`. Harness: [criterion](https://docs.rs/criterion) 0.5.

## Run

From the **workspace root**:

```bash
# Pipeline stages (compile / smoke / bronze / e2e)
cargo bench -p rbt-datalake --bench pipeline

# MemTable (2nd lifetime) vs Parquet re-read for ref()
cargo bench -p rbt-datalake --bench downstream_ref

# Filters
cargo bench -p rbt-datalake --bench pipeline -- compile
cargo bench -p rbt-datalake --bench downstream_ref -- decision
cargo bench -p rbt-datalake --bench downstream_ref -- query/count_star
RBT_BENCH_FULL=0 cargo bench -p rbt-datalake --bench pipeline   # skip 9-model e2e

# Surrogate keys (ADR-009): generation + join cost
cargo bench -p rbt-datalake --bench surrogate_key
RBT_SK_BENCH_QUICK=1 cargo bench -p rbt-datalake --bench surrogate_key
```

HTML reports: `target/criterion/report/index.html`.

## Data

| Bench | Dataset |
|-------|---------|
| `pipeline` smoke / compile | `examples/smoke_fixture` |
| `pipeline` e2e / bronze | `examples/full_e2e_rbt_example/lake/bronze` |
| `downstream_ref` synthetic | generated Arrow → temp Parquet |
| `downstream_ref` e2e_lake | first available e2e silver/gold Parquet |
| `surrogate_key` | synthetic grain rows; MemTable fact⋈dim joins |

E2e groups **skip** if bronze/outputs are absent.

## Baseline — surrogate keys (`surrogate_key`, 2026-08-27, `RBT_SK_BENCH_QUICK=1`)

Machine: same class as other benches. Quick sizes: gen 100k rows; join dim 10k × fact 200k.

### Generation (median)

| Variant | 100k rows |
|---------|----------:|
| `fast64` (xxh3 → Int64) | **~11.5 ms** |
| `blake3_128` binary | **~20.6 ms** |
| `blake3_128` hex | **~27.8 ms** |
| `blake3_256` binary | **~20.4 ms** |
| `md5_128` binary | **~27.3 ms** |
| MIISK sequential assign (bench baseline; product uses durable registry) | **~20 µs** |

### Join `fact ⋈ dim` on SK (median)

| SK type | ~median |
|---------|--------:|
| MIISK Int64 | **~2.4 ms** |
| fast64 Int64 | **~4.2 ms** |
| blake3_128 binary16 | **~5.6 ms** |
| blake3_128 hex Utf8 | **~8.3 ms** |

Takeaway: prefer **binary** over hex; `fast64` wins when N is bounded; default `balanced` binary is close on joins and far safer at scale. Full sizes: omit `RBT_SK_BENCH_QUICK`.

## Baseline — pipeline (2026-07-31)

Machine: AMD Ryzen 7 PRO 6850U (16 threads), 28 GiB RAM, NixOS.

| Benchmark | Median | Notes |
|-----------|--------|-------|
| `compile/smoke_fixture` | **2.4 ms** | project + DAG |
| `compile/full_e2e_project` | **6.5 ms** | 9 models |
| `run_smoke/full_dag_parquet` | **9.5 ms** | tiny JSONL |
| `materialize_synth` 10k / 100k / 500k | **1.6 / 16 / 50 ms** | write only |
| `bronze_scan` 1d / 1m | **~34 ms / ~368 ms** | IPC → batches |
| `run_e2e_select` stg_1d / tf_metrics_1d | **~110 / ~197 ms** | |
| `run_e2e_full` 9-model DAG | **~20.7 s** | |

## Baseline — MemTable vs Parquet `ref()` (`downstream_ref`, same machine)

### Is a size threshold “free”?

| Signal | Median |
|--------|--------|
| Known `u64` row count (from materialize) | **~0.76 ns** |
| `if rows < 100_000` | **~0.77 ns** |
| Sum `batch.num_rows()` | **~6.7 ns** |
| `fs::metadata` length | **~3 µs** |
| Parquet footer `num_rows` | **~23 µs** |
| MemTable Arc build | **~0.8 µs** |
| `register_parquet` | **~0.38 ms** |

Yes — use the row count you already counted while writing; never open the file just to decide.

### Register only

| Rows | MemTable Arc | Parquet register |
|-----:|-------------:|-----------------:|
| 1k–500k | **85–96 µs** | **0.32–0.77 ms** |
| 2M | **~118 µs** | **~0.65 ms** |

### Query (register + SQL), median

| Rows | Query | MemTable | Parquet | Δ |
|-----:|-------|---------:|--------:|--:|
| 1k | `count(*)` | 0.65 ms | 1.01 ms | +0.36 ms |
| 100k | `count(*)` | 0.66 ms | 1.13 ms | +0.47 ms |
| 2M | `count(*)` | 0.73 ms | 1.14 ms | +0.41 ms |
| 1k | filter+project | 0.55 ms | 2.45 ms | +1.9 ms |
| 100k | filter+project | 1.25 ms | 4.20 ms | +3.0 ms |
| 500k | filter+project | 1.16 ms | 7.42 ms | +6.3 ms |
| 100k | `sum(px)` | 1.14 ms | 4.08 ms | +2.9 ms |
| 500k | `sum(px)` | 1.02 ms | 6.35 ms | +5.3 ms |
| e2e stg_1d (35k) | `count(*)` | 0.65 ms | 1.49 ms | +0.84 ms |

### Policy recommendation (from these numbers)

- Absolute wall-clock gap is **milliseconds**; full e2e DAG is **~20 s**.
- **Parquet re-read for large models** (staging/facts): tiny time cost, large RSS win.
- Optional **MemTable for tiny dims** (&lt; ~10k–100k rows) if desired.
- Threshold on **already-known row count** — effectively zero wall time.

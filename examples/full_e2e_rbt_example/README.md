# full_e2e_rbt_example — medallion showcase + landing benchmarks

**Package:** `rbt-datalake` · **Host:** `full-e2e-rbt-example` (workspace member, `publish = false`)  
**Domain TA:** [`finance-solution`](https://crates.io/crates/finance-solution) (Design B/C/D)

Comparable bronze landings, silver staging, transforms, and gold OBT — with wall clocks for **Arrow IPC + spill** vs **Parquet listing** vs **Parquet + RBT-C parallel**.

---

## Quick start (fair full-DAG compare)

```bash
# From repo root — release profile required for meaningful times
cargo run -p full-e2e-rbt-example --release -- \
  -p examples/full_e2e_rbt_example compare -j 8
```

This:

1. Ensures Parquet bronze exists (`land-parquet-bronze` if missing)
2. Runs **three full DAGs** (`stg → tf_indicators_1m → obt_stocks_1m`) on the **same 82 symbols**
3. Prints a table and writes [`FINDINGS.md`](FINDINGS.md)

### Latest measured wall clocks (stg→tf→obt)

| Run | wall_secs | vs Arrow | Bronze path | TF |
|-----|----------:|---------:|-------------|-----|
| **Arrow+spill** | **14.46** | 1.00× | IPC hive, **force re-spill** | serial mega |
| **Parquet** | **9.75** | **1.48×** | parquet hive, **DF listing** | serial mega |
| **Parquet+par** | **12.10** | 1.19× | parquet hive listing | RBT-C L2 `jobs=8` |

`total_rows` ≈ 9.18M = engine sum of stg + tf + obt (~3.06M bars × 3). Same symbol scope for all three.

**Takeaway:** Landing **Parquet** (no IPC spill) is the big win. RBT-C parallel on this TA workload does not beat serial mega Parquet (WorkUnit overhead).

---

## Bronze landings

| Landing | Path | How |
|---------|------|-----|
| Arrow IPC hive | `lake/bronze/lz_stock_bars/` | Sample data (many tiny `.arrow` files) |
| Parquet hive (recommended) | `lake/bronze/lz_stock_bars_parquet/` | `land-parquet-bronze -j 8` |

```bash
cargo run -p full-e2e-rbt-example --release -- \
  -p examples/full_e2e_rbt_example land-parquet-bronze -j 8
```

---

## Host commands

| Command | Purpose |
|---------|---------|
| `compare -j N` | **Primary:** Arrow force-spill vs Parquet vs Parquet+parallel full DAG |
| `land-parquet-bronze` | Arrow → Parquet hive lander |
| `spill-bench -j N` | Spill-only mega vs partitioned IPC→Parquet |
| `diag -j N` | Segment stg / tf / obt for Design B/C/D |
| `design-b` / `design-c` / `design-d` | Individual approaches |
| `sql` / `jsonl` | Design A file project (SQL models) |

CLI Design A (SQL, Arrow stg):

```bash
rbt run -p examples/full_e2e_rbt_example --force-bronze-register --select stg_ohlcv_1m
rbt run -p examples/full_e2e_rbt_example --select stg_ohlcv_parquet_1m   # listing path
```

---

## Project layout

```text
examples/full_e2e_rbt_example/
  FINDINGS.md                 # last `compare` results (generated)
  README.md
  rbt_project.yml             # Design A defaults
  rbt_project_rust.yml
  rbt_project_parallel.yml
  src/
    main.rs
    compare_bench.rs          # fair full-DAG compare
    spill_exp.rs              # spill-bench + land-parquet-bronze
    models/
      sql_models_approach/    # Design A .sql
      silver/staging/
        stg_ohlcv_1m.rs                 # Arrow IPC + spill
        stg_ohlcv_bronzeparquet_1m.rs   # Parquet listing
      silver/transforms/      # tf_indicators + ta_kernels + per-symbol
      gold/obt_stocks_1m.rs
  lake/
    bronze/                   # IPC + parquet landings
    compare_arrow_output/     # compare run 1
    compare_parquet_output/   # compare run 2
    compare_parquet_parallel_output/
    sql_models_output/ …      # Design A runtime
    rust_models_output/ …     # Design B runtime
```

---

## Design A / B / C / D (overview)

| | A | B | C | D |
|--|---|---|---|---|
| Models | SQL under `sql_models_approach/` | Rust mega | Rust + L2 WorkUnits | N× named `tf_*` + UNION |
| Outputs | `sql_models_output/` | `rust_models_output/` | `parallel_models_output/` | `design_d_models_output/` |

See `diag` for segment breakdowns. Prefer **`compare`** for landing-format fairness.

---

## rbt product notes (this release track)

- **Recommended bronze:** Parquet hive / parts → DataFusion listing, no spill  
- **Arrow IPC:** still supported; multi-file uses spill; `scan.reuse_register` reuses spill when landings unchanged  
- **CLI:** `--force-bronze-register` forces re-spill; `--skip-if-match` skips whole DAG  

Docs: [COMPLEX_BRONZE_AND_RUN_SCOPE.md](../../docs/COMPLEX_BRONZE_AND_RUN_SCOPE.md)

---

## Related

- [FINDINGS.md](FINDINGS.md) — last compare numbers  
- [EMBEDDING.md](../../docs/EMBEDDING.md) — Design B / concurrency  
- finance-solution: [docs.rs](https://docs.rs/finance-solution)

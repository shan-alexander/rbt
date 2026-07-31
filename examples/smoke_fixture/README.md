# Smoke fixture

Tiny JSONL bronze project for CI and local smoke tests.

Compatible with **rbt-datalake 0.3.7+**. Package: **`rbt-datalake`**. Binary / lib: **`rbt`**.

Uses `roots.lake` + `$lake/...` targets (same multi-root pattern as the full e2e example, at toy scale).

```bash
# from repository root
cargo build -p rbt-datalake --release
./target/release/rbt compile -p examples/smoke_fixture --bronze-check fail
./target/release/rbt run -p examples/smoke_fixture --format parquet --select dim_ticker
./target/release/rbt test -p examples/smoke_fixture --select dim_ticker
./target/release/rbt run -p examples/smoke_fixture --format iceberg --select stg_trades
# or: bash scripts/smoke.sh
```

**DAG (3 models, 3 tiers):** `stg_trades` → `tf_ticker_stats` → `dim_ticker`

| Model | Path after run |
|-------|----------------|
| `stg_trades` | `lake/silver/stg_trades.parquet` |
| `tf_ticker_stats` | `lake/silver/tf_ticker_stats.parquet` |
| `dim_ticker` | `lake/gold/dim_ticker.parquet` |

Expected: 3 staging rows after dedupe (ids 1,2,3), 2 tickers in dim.

# Smoke fixture

Tiny JSONL bronze project for CI and local smoke tests.

```bash
cargo build -p rbt-cli --release
./target/release/rbt compile -p examples/smoke_fixture --bronze-check fail
./target/release/rbt run -p examples/smoke_fixture --format parquet --select dim_ticker
./target/release/rbt test -p examples/smoke_fixture
./target/release/rbt run -p examples/smoke_fixture --format iceberg --select stg_trades
```

Expected: 3 staging rows after dedupe (ids 1,2,3), 2 tickers in dim.

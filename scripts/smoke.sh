#!/usr/bin/env bash
# Local + CI smoke against examples/smoke_fixture (tiny JSONL bronze).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BIN="${RBT_BIN:-./target/release/rbt}"
if [[ ! -x "$BIN" ]]; then
  echo "[smoke] building rbt CLI..."
  cargo build -p rbt-datalake --release
  BIN=./target/release/rbt
fi

FIX=examples/smoke_fixture
OUT="$FIX/lake"

rm -rf "$OUT/silver" "$OUT/gold" "$FIX/target" 2>/dev/null || true

echo "[smoke] compile"
"$BIN" compile -p "$FIX" --bronze-check fail

echo "[smoke] run --select dim_ticker (ancestors included)"
"$BIN" run -p "$FIX" --format parquet --select dim_ticker --bronze-check fail

test -f "$OUT/silver/stg_trades.parquet"
test -f "$OUT/silver/tf_ticker_stats.parquet"
test -f "$OUT/gold/dim_ticker.parquet"

echo "[smoke] test (frontmatter assertions)"
"$BIN" test -p "$FIX" --select dim_ticker --bronze-check fail

echo "[smoke] iceberg format on staging only (catalog SoR commit)"
"$BIN" run -p "$FIX" --format iceberg --select stg_trades --bronze-check fail
# Catalog writes data/*.parquet + metadata/*.metadata.json (official layout; not hand-rolled v1 only)
test -d "$OUT/silver/stg_trades"
test -d "$OUT/silver/stg_trades/metadata"
# at least one metadata json and one data parquet
meta_count=$(find "$OUT/silver/stg_trades/metadata" -name '*.metadata.json' 2>/dev/null | wc -l | tr -d ' ')
data_count=$(find "$OUT/silver/stg_trades" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
test "${meta_count:-0}" -ge 1
test "${data_count:-0}" -ge 1

echo "[smoke] parquet_and_iceberg dual write"
"$BIN" run -p "$FIX" --format parquet-and-iceberg --select stg_trades --bronze-check fail
# CLI value uses underscore via ValueEnum: ParquetAndIceberg -> parquet-and-iceberg
test -f "$OUT/silver/stg_trades.parquet"
test -d "$OUT/silver/stg_trades.iceberg" || test -d "$OUT/silver/stg_trades" 

echo "[smoke] OK"

---
# =============================================================================
# Silver staging — 1-minute OHLCV from JSONL bronze (alternate lander)
# =============================================================================
# WHY a second staging model?
#   Real lakes often land the *same grain* from multiple collectors (Arrow IPC
#   bulk vs JSONL streaming). rbt treats each lander as its own scan contract;
#   you unify in silver/gold SQL or pick one source of truth.
#
# Bronze contract (external)
#   lake/bronze/lz_stock_bars_jsonl/*_minute.jsonl
#   Polygon-ish keys: t (ms), o/h/l/c, v, n, vw — NOT the Arrow schema.
#
# Demo tip: this lander is tiny (few tickers) — great for fast CLI loops.
# =============================================================================
description: 1-minute OHLCV silver from JSONL minute files (schema-mapped).
context: >
  Alternate bronze lander for 1m bars. Maps Polygon-style JSONL fields into the
  same logical grain as stg_ohlcv_1m so gold OBT can union or prefer one path.

source_format: jsonl
scan_path: "$lake/bronze/lz_stock_bars_jsonl"
# Single-file glob keeps MemTable schema stable (multi-file JSONL can widen types).
# Expand to "*_minute.jsonl" once landers share one physical schema.
path_glob: "MU_minute.jsonl"
inject_source_path: true
source_name: bronze
source_table: ohlcv_jsonl_1m

grain: [symbol, timestamp_ns]
unique_key: [symbol, timestamp_ns]
materialization: table
tags: [staging, ohlcv, equities, jsonl, timeframe_1m]

columns:
  symbol: { description: Ticker inferred from filename stem, dtype: utf8 }
  timeframe: { description: Always 1m for this lander, dtype: utf8 }
  timestamp_ns: { description: Event time ns (from t ms * 1e6), dtype: int64 }
  open: { dtype: float64 }
  high: { dtype: float64 }
  low: { dtype: float64 }
  close: { dtype: float64 }
  volume: { dtype: int64 }
  source_path: { dtype: utf8 }

tests:
  not_null: [symbol, timestamp_ns, open, high, low, close, volume]
  unique: [symbol, timestamp_ns]
  fail_on_error: true
---
-- Map Polygon-ish JSONL (t/o/h/l/c/v) → silver grain. Demo file is MU_minute.jsonl.
WITH raw AS (
    SELECT
        'MU' AS symbol,
        '1m' AS timeframe,
        CAST(t AS BIGINT) * 1000000 AS timestamp_ns,
        CAST(o AS DOUBLE) AS open,
        CAST(h AS DOUBLE) AS high,
        CAST(l AS DOUBLE) AS low,
        CAST(c AS DOUBLE) AS close,
        CAST(v AS BIGINT) AS volume,
        _source_path AS source_path
    FROM {{ source('bronze', 'ohlcv_jsonl_1m') }}
)
SELECT *
FROM raw
WHERE timestamp_ns > 0
  AND high >= low

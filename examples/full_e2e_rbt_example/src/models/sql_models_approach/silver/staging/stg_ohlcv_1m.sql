---
# =============================================================================
# Design A — silver staging from Arrow IPC bronze (hive tree)
# =============================================================================
# Materializes to: lake/sql_models_output/silver/stage/stg_ohlcv_1m.parquet
# Design B twin (pure Rust): src/models/silver/staging/stg_ohlcv_1m.rs
#   → lake/rust_models_output/silver/stage/stg_ohlcv_1m.parquet
# Grain unique_key [symbol, timestamp_ns] — latest bronze path wins (no dups).
#
# WHY this model exists
#   Bronze is *external* landings (not owned by rbt). Staging is the first
#   rbt-owned, typed, grain-honest table. Downstream transforms/marts ref() this
#   without re-scanning raw bronze.
#
# Bronze contract (external)
#   lake/bronze/lz_stock_bars/symbol=<TICKER>/timeframe=1m/*.arrow
#   Columns: symbol, timestamp (utf8), timestamp_ns (i64), open/high/low/close (f64), volume (i64)
#
# rbt practices demonstrated
#   - roots $lake expansion
#   - path_glob under mixed hive trees
#   - partition_by + require_partitions (only 1m grain)
#   - inject_source_path for latest-wins dedupe
#   - materialization: table (full refresh) — simple silver for demos;
#     production multi-entity lakes often prefer scoped_replace + --jobs (see host binary)
# =============================================================================
description: Deduped 1-minute OHLCV silver grain from Arrow IPC hive bronze.
context: >
  First rbt-owned history of equity 1m bars. External bronze lands hive-partitioned
  Arrow streams under lz_stock_bars/; this model types, filters invalid geometry,
  and keeps the latest re-ingest per (symbol, timestamp_ns).

source_format: arrow_ipc
scan_path: "$lake/bronze/lz_stock_bars"
# Strong glob: * is single path segment; ** walks hive dirs.
path_glob: "**/*.arrow"
partition_by: [symbol, timeframe]
require_partitions:
  timeframe: "1m"
inject_source_path: true
source_name: bronze
source_table: ohlcv_1m

grain: [symbol, timestamp_ns]
unique_key: [symbol, timestamp_ns]
materialization: table
tags: [staging, ohlcv, equities, timeframe_1m, arrow_ipc]

columns:
  symbol: { description: Equity ticker, dtype: utf8 }
  timeframe: { description: Bar timeframe (injected from hive), dtype: utf8 }
  bar_time: { description: Bar open as timestamp (seconds), dtype: timestamp }
  timestamp_ns: { description: Authoritative event time (ns epoch), dtype: int64 }
  open: { description: Open price, dtype: float64 }
  high: { description: High price, dtype: float64 }
  low: { description: Low price, dtype: float64 }
  close: { description: Close price, dtype: float64 }
  volume: { description: Share volume, dtype: int64 }
  bronze_dup_count: { description: Bronze rows before latest-path dedupe, dtype: int64 }
  source_path: { description: Winning bronze file path, dtype: utf8 }

tests:
  not_null: [symbol, timeframe, timestamp_ns, open, high, low, close, volume]
  unique: [symbol, timestamp_ns]
  accepted_values:
    timeframe: ["1m"]
  fail_on_error: true
---
WITH raw AS (
    SELECT
        symbol,
        timeframe,
        timestamp AS ts_raw,
        timestamp_ns,
        open,
        high,
        low,
        close,
        volume,
        _source_path
    FROM {{ source('bronze', 'ohlcv_1m') }}
),
typed AS (
    SELECT
        symbol,
        timeframe,
        ts_raw,
        CAST(timestamp_ns AS BIGINT) AS timestamp_ns,
        CAST(open AS DOUBLE) AS open,
        CAST(high AS DOUBLE) AS high,
        CAST(low AS DOUBLE) AS low,
        CAST(close AS DOUBLE) AS close,
        CAST(volume AS BIGINT) AS volume,
        _source_path
    FROM raw
    WHERE symbol IS NOT NULL
      AND TRIM(symbol) <> ''
      AND timestamp_ns IS NOT NULL
      AND timestamp_ns > 0
      AND open IS NOT NULL
      AND high IS NOT NULL
      AND low IS NOT NULL
      AND close IS NOT NULL
      AND volume IS NOT NULL
      AND volume >= 0
      AND high >= low
      AND high >= open
      AND high >= close
      AND low <= open
      AND low <= close
),
ranked AS (
    SELECT
        symbol,
        timeframe,
        ts_raw,
        timestamp_ns,
        open,
        high,
        low,
        close,
        volume,
        _source_path,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, timestamp_ns
            ORDER BY _source_path DESC
        ) AS _rn,
        COUNT(*) OVER (
            PARTITION BY symbol, timestamp_ns
        ) AS bronze_dup_count
    FROM typed
)
SELECT
    symbol,
    timeframe,
    to_timestamp_seconds(CAST(timestamp_ns / 1000000000 AS BIGINT)) AS bar_time,
    timestamp_ns,
    open,
    high,
    low,
    close,
    volume,
    bronze_dup_count,
    _source_path AS source_path
FROM ranked
WHERE _rn = 1

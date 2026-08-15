---
# =============================================================================
# Recommended bronze path — Parquet hive listing (NO spill)
# =============================================================================
# Landings (produce once):
#   cargo run -p full-e2e-rbt-example --release -- -p examples/full_e2e_rbt_example \
#     land-parquet-bronze -j 8
#   → lake/bronze/lz_stock_bars_parquet/symbol=*/timeframe=1m/data.parquet
#
# Why this is fast:
#   - source_format: parquet → DataFusion directory listing (Path A)
#   - no inject_source_path / no path_glob that force scan+spill
#   - partition keys live in the files (written at land time)
#
# Compare with stg_ohlcv_1m.sql (Arrow IPC hive → spill→Parquet → stage).
# =============================================================================
description: Deduped 1m OHLCV silver from Parquet hive bronze (recommended landing).
context: >
  Same grain as stg_ohlcv_1m but bronze is already Parquet. Registration skips
  Arrow IPC spill. Use for apples-to-apples wall-clock vs Arrow path.

source_format: parquet
scan_path: "$lake/bronze/lz_stock_bars_parquet"
source_name: bronze
source_table: ohlcv_parquet_1m
# Listing-friendly: omit path_glob / inject_source_path so DF lists the tree.
# Filter grain in SQL if landings ever mix timeframes (here lander is 1m-only).

grain: [symbol, timestamp_ns]
unique_key: [symbol, timestamp_ns]
materialization: table
tags: [staging, ohlcv, parquet_bronze, recommended]

columns:
  symbol: { description: Equity ticker, dtype: utf8 }
  timeframe: { description: Bar timeframe, dtype: utf8 }
  bar_time: { description: Bar open as timestamp (seconds), dtype: timestamp }
  timestamp_ns: { description: Event time ns, dtype: int64 }
  open: { dtype: float64 }
  high: { dtype: float64 }
  low: { dtype: float64 }
  close: { dtype: float64 }
  volume: { dtype: int64 }

tests:
  not_null: [symbol, timestamp_ns, open, high, low, close, volume]
  unique: [symbol, timestamp_ns]
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
        volume
    FROM {{ source('bronze', 'ohlcv_parquet_1m') }}
),
typed AS (
    SELECT
        symbol,
        COALESCE(timeframe, '1m') AS timeframe,
        CAST(timestamp_ns AS BIGINT) AS timestamp_ns,
        CAST(open AS DOUBLE) AS open,
        CAST(high AS DOUBLE) AS high,
        CAST(low AS DOUBLE) AS low,
        CAST(close AS DOUBLE) AS close,
        CAST(volume AS BIGINT) AS volume
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
        *,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, timestamp_ns
            ORDER BY timestamp_ns
        ) AS _rn
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
    volume
FROM ranked
WHERE _rn = 1

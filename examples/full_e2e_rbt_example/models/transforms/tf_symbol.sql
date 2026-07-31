---
description: Preparatory symbol attributes for dim_symbol.
context: >
  One row per ticker rolled up from staging 1m (and optional 1d coverage).
  Kimball dim_symbol should ref this table rather than re-aggregating staging.
  Full Parquet rewrite each run — appropriate for small/medium symbol universes.

grain: [symbol]
unique_key: [symbol]
materialization: table
tags: [transform, symbol, preparatory, full_refresh_parquet]

meta:
  feeds: dim_symbol
  rewrite: full_parquet

columns:
  symbol:
    description: Equity ticker (dimension natural key).
    context: Distinct symbols observed in stg_ohlcv_1m; left-joined to daily coverage.
    dtype: utf8
  first_bar_time_1m:
    description: Earliest 1m bar_time for the symbol.
    context: Coverage start for the 1m history spine.
    dtype: timestamp
  last_bar_time_1m:
    description: Latest 1m bar_time for the symbol.
    context: Coverage end; used for freshness checks.
    dtype: timestamp
  bar_count_1m:
    description: Count of 1m bars in staging grain.
    context: After dedupe; not bronze raw row count.
    dtype: int64
  avg_volume_1m:
    description: Mean 1m bar volume.
    context: Simple liquidity proxy for the dimension.
    dtype: float64
    unit: shares
  avg_close_1m:
    description: Mean 1m close.
    context: Rough price level marker; not VWAP.
    dtype: float64
    unit: price
  first_bar_time_1d:
    description: Earliest daily bar_time if 1d data exists.
    context: Null when symbol has 1m-only bronze coverage.
    dtype: timestamp
  last_bar_time_1d:
    description: Latest daily bar_time if present.
    context: Null when no stg_ohlcv_1d rows for symbol.
    dtype: timestamp
  bar_count_1d:
    description: Count of daily bars in staging.
    context: Zero when daily partition missing for symbol.
    dtype: int64
  has_daily_bars:
    description: Whether any 1d staging rows exist.
    context: Boolean-as-int (1/0) for simple filters in gold.
    dtype: int64

tests:
  not_null: [symbol, first_bar_time_1m, last_bar_time_1m, bar_count_1m]
  unique: [symbol]
  fail_on_error: true
---
WITH m AS (
    SELECT
        symbol,
        MIN(bar_time) AS first_bar_time_1m,
        MAX(bar_time) AS last_bar_time_1m,
        COUNT(*) AS bar_count_1m,
        AVG(volume) AS avg_volume_1m,
        AVG(close) AS avg_close_1m
    FROM {{ ref('stg_ohlcv_1m') }}
    GROUP BY symbol
),
d AS (
    SELECT
        symbol,
        MIN(bar_time) AS first_bar_time_1d,
        MAX(bar_time) AS last_bar_time_1d,
        COUNT(*) AS bar_count_1d
    FROM {{ ref('stg_ohlcv_1d') }}
    GROUP BY symbol
)
SELECT
    m.symbol,
    m.first_bar_time_1m,
    m.last_bar_time_1m,
    m.bar_count_1m,
    m.avg_volume_1m,
    m.avg_close_1m,
    d.first_bar_time_1d,
    d.last_bar_time_1d,
    COALESCE(d.bar_count_1d, 0) AS bar_count_1d,
    CASE WHEN d.symbol IS NULL THEN 0 ELSE 1 END AS has_daily_bars
FROM m
LEFT JOIN d ON m.symbol = d.symbol

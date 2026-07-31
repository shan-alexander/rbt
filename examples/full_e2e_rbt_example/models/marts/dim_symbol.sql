---
description: Kimball dimension of tradable symbols.
context: >
  Gold dimension built from tf_symbol (not raw staging). Natural key is symbol.
  Attributes include 1m/1d coverage and simple liquidity/price level fields.
  Full Parquet rewrite each run — suitable for small universes without Iceberg.

grain: [symbol]
unique_key: [symbol]
materialization: table
tags: [mart, dimension, gold, full_refresh_parquet]

meta:
  source_prep: tf_symbol
  rewrite: full_parquet

columns:
  symbol:
    description: Dimension natural key (ticker).
    context: Join target for all fact tables in this mart.
    dtype: utf8
  first_bar_time:
    description: First observed 1m bar time.
    context: Alias of first_bar_time_1m from tf_symbol.
    dtype: timestamp
  last_bar_time:
    description: Last observed 1m bar time.
    context: Freshness / coverage end for the instrument.
    dtype: timestamp
  bar_count_1m:
    description: Number of 1m bars in silver history.
    dtype: int64
    context: From tf_symbol after staging dedupe.
  bar_count_1d:
    description: Number of daily bars if present.
    dtype: int64
    context: Zero when bronze lacks timeframe=1d for the symbol.
  has_daily_bars:
    description: 1 if daily history exists else 0.
    dtype: int64
    context: Convenience flag for consumers.
  avg_volume_1m:
    description: Mean 1m volume.
    dtype: float64
    unit: shares
    context: Liquidity proxy on the dimension.
  avg_close_1m:
    description: Mean 1m close.
    dtype: float64
    unit: price
    context: Rough price band; not a valuation metric.

tests:
  not_null: [symbol, first_bar_time, last_bar_time]
  unique: [symbol]
  fail_on_error: true
---
SELECT
    symbol,
    first_bar_time_1m AS first_bar_time,
    last_bar_time_1m AS last_bar_time,
    bar_count_1m,
    bar_count_1d,
    has_daily_bars,
    avg_volume_1m,
    avg_close_1m
FROM {{ ref('tf_symbol') }}

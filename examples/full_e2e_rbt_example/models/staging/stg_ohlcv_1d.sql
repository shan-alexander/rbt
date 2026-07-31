---
description: Deduped historical daily OHLCV bars (silver grain).
context: >
  Daily companion to stg_ohlcv_1m. Same defensive typing and latest-source_path
  dedupe. Used by tf_symbol coverage stats and fact_1d_bars (daily indicators).

source_format: arrow_ipc
scan_path: "lake/bronze"
partition_by: [symbol, timeframe]
require_partitions:
  timeframe: "1d"
inject_source_path: true
source_name: bronze
source_table: ohlcv_1d

grain: [symbol, timestamp_ns]
unique_key: [symbol, timestamp_ns]
materialization: table
tags: [staging, ohlcv, equities, timeframe_1d, full_refresh_parquet]

meta:
  bronze_owner: external_ingest
  dedupe: latest_source_path
  clock: timestamp_ns
  rewrite: full_parquet

columns:
  symbol:
    description: Equity ticker symbol.
    context: Join key to dim_symbol and daily facts.
    dtype: utf8
  timeframe:
    description: Bar timeframe label.
    context: Injected from hive path; locked to 1d.
    dtype: utf8
  bar_time:
    description: Daily bar timestamp.
    context: From timestamp_ns; daily bars typically session date epoch.
    dtype: timestamp
  timestamp_ns:
    description: Authoritative event time (ns).
    context: Grain clock for daily bars.
    dtype: int64
    unit: ns_epoch
  ts_raw:
    description: Raw bronze timestamp string.
    context: Often YYYYMMDD for daily files in this lake.
    dtype: utf8
  open:
    description: Day open price.
    dtype: float64
    unit: price
    context: Session open from bronze daily OHLCV.
  high:
    description: Day high price.
    dtype: float64
    unit: price
    context: Must pass high>=low geometry filter.
  low:
    description: Day low price.
    dtype: float64
    unit: price
    context: Must pass low<=open/close geometry filter.
  close:
    description: Day close price.
    dtype: float64
    unit: price
    context: Input for daily returns and SMA/MACD on fact_1d_bars.
  volume:
    description: Day volume.
    dtype: int64
    unit: shares
    context: Feeds daily RVOL and symbol totals.
  bronze_dup_count:
    description: Bronze rows merged for this grain.
    context: Audit of re-ingest density.
    dtype: int64
  source_path:
    description: Winning bronze file path.
    context: Lineage for latest-wins dedupe.
    dtype: utf8

tests:
  not_null: [symbol, timeframe, timestamp_ns, open, high, low, close, volume]
  unique: [symbol, timestamp_ns]
  accepted_values:
    timeframe: ["1d"]
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
    FROM {{ source('bronze', 'ohlcv_1d') }}
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
    ts_raw,
    open,
    high,
    low,
    close,
    volume,
    bronze_dup_count,
    _source_path AS source_path
FROM ranked
WHERE _rn = 1

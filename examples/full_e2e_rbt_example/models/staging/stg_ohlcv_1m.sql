---
description: Deduped historical 1-minute OHLCV bars (silver grain).
context: >
  First rbt-owned table for 1m equity bars. External bronze lands hive-partitioned
  Arrow IPC streams; this model types, filters, and keeps the latest re-ingest per
  (symbol, timestamp_ns). Downstream transforms build symbol prep and bar metrics
  without re-deduping bronze.

source_format: arrow_ipc
# $lake expands from rbt_project.yml roots: (project-relative lake/ here)
scan_path: "$lake/bronze"
# Artifact isolation under a mixed hive tree (strong glob: * is single-segment; ** recursive).
# Non-empty path_glob + partition filters force the scan→MemTable path (no DF listing pushdown).
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
tags: [staging, ohlcv, equities, timeframe_1m, full_refresh_parquet]

meta:
  bronze_owner: external_ingest
  dedupe: latest_source_path
  clock: timestamp_ns
  rewrite: full_parquet

columns:
  symbol:
    description: Equity ticker symbol.
    context: Payload symbol (path partition not overwritten). Primary join key to dim_symbol.
    dtype: utf8
  timeframe:
    description: Bar timeframe label.
    context: Injected from hive path timeframe=1m; accepted_values locked to 1m.
    dtype: utf8
  bar_time:
    description: Bar open time as timestamp.
    context: Derived from timestamp_ns / 1e9 via to_timestamp_seconds. Prefer for time filters.
    dtype: timestamp
    unit: second_epoch_ts
  timestamp_ns:
    description: Authoritative event time in nanoseconds.
    context: Grain clock. Prefer over ts_raw for joins and windows.
    dtype: int64
    unit: ns_epoch
  ts_raw:
    description: Raw bronze timestamp string.
    context: 1m bars use epoch-seconds-as-string; kept for audit only.
    dtype: utf8
  open:
    description: Bar open price.
    context: Session open for the 1m interval from bronze OHLCV.
    dtype: float64
    unit: price
  high:
    description: Bar high price.
    context: Must be >= open/close/low after defensive geometry filter.
    dtype: float64
    unit: price
  low:
    description: Bar low price.
    context: Must be <= open/close/high after defensive geometry filter.
    dtype: float64
    unit: price
  close:
    description: Bar close price.
    context: Primary price series for returns, MACD/SMA inputs in tf_bar_metrics.
    dtype: float64
    unit: price
  volume:
    description: Bar share volume.
    context: Non-negative; feeds RVOL and volume SMAs.
    dtype: int64
    unit: shares
  bronze_dup_count:
    description: Count of bronze rows for this grain before dedupe.
    context: "Count greater than 1 means re-ingest or overlapping chunks; row kept is latest source_path."
    dtype: int64
  source_path:
    description: Absolute path of the winning bronze file.
    context: Lineage for latest-wins rule; useful for debugging re-landed partitions.
    dtype: utf8

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

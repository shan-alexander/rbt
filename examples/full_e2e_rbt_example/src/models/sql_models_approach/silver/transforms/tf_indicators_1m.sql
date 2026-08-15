---
# =============================================================================
# Design A — SQL window indicators (pure DataFusion — no host crate)
# =============================================================================
# Lives under src/models/sql_models_approach/ and materializes to:
#   lake/sql_models_output/silver/tf/tf_indicators_1m.parquet
#
# Design B counterpart (pure Rust):
#   src/models/silver/transforms/tf_indicators_1m.rs + ta_kernels.rs
#   → lake/rust_models_output/silver/tf/…
#
# DAG (this file)
#   stg_ohlcv_1m → tf_indicators_1m → obt_stocks_1m
#
#   rbt run -p . --select stg_ohlcv_1m,tf_indicators_1m,obt_stocks_1m
# =============================================================================
description: Design A SQL window indicators over silver 1m bars.
context: >
  Pure SQL SMA-style windows via avg() OVER. Educational / baseline; Design B
  uses finance-solution kernels instead (see src/indicators.rs).

materialization: table
tags: [transform, indicators, sql_only, timeframe_1m]
grain: [symbol, timestamp_ns]

tests:
  not_null: [symbol, timestamp_ns, close]
  fail_on_error: true
---
SELECT
    symbol,
    timeframe,
    bar_time,
    timestamp_ns,
    open,
    high,
    low,
    close,
    volume,
    avg(close) OVER (
        PARTITION BY symbol
        ORDER BY timestamp_ns
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS sma_20,
    avg(close) OVER (
        PARTITION BY symbol
        ORDER BY timestamp_ns
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) AS sma_50,
    close - lag(close, 1) OVER (
        PARTITION BY symbol
        ORDER BY timestamp_ns
    ) AS ret_1
FROM {{ ref('stg_ohlcv_1m') }}

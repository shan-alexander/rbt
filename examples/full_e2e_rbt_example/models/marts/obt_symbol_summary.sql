---
description: One-big-table symbol summary for dashboards and APIs.
context: >
  Denormalized per-symbol snapshot from dim_symbol plus daily fact aggregates
  and latest daily indicator snapshot (MACD/RVOL/RSI). Full Parquet rewrite.

grain: [symbol]
unique_key: [symbol]
materialization: table
tags: [mart, obt, gold, full_refresh_parquet]

meta:
  consumers: [dashboards, apis, agents]
  rewrite: full_parquet

columns:
  symbol:
    description: Ticker.
    dtype: utf8
    context: OBT grain key.
  first_bar_time:
    description: First 1m bar time.
    dtype: timestamp
    context: From dim_symbol.
  last_bar_time:
    description: Last 1m bar time.
    dtype: timestamp
    context: From dim_symbol.
  bar_count_1m:
    description: 1m history depth.
    dtype: int64
    context: From dim_symbol.
  bar_count_1d:
    description: Count of daily fact rows.
    dtype: int64
    context: From fact_1d_bars aggregation.
  period_low_1d:
    description: Min daily low in history.
    dtype: float64
    unit: price
    context: Period range low.
  period_high_1d:
    description: Max daily high in history.
    dtype: float64
    unit: price
    context: Period range high.
  total_volume_1d:
    description: Sum of daily volumes.
    dtype: int64
    unit: shares
    context: Long-run activity.
  last_close_1d:
    description: Most recent daily close.
    dtype: float64
    unit: price
    context: From latest daily fact by timestamp_ns.
  last_macd_hist_1d:
    description: Latest daily MACD histogram.
    dtype: float64
    unit: price
    context: Momentum snapshot for agents/UI.
  last_rvol_20_1d:
    description: Latest daily RVOL(20).
    dtype: float64
    unit: ratio
    context: Relative volume snapshot.
  last_rsi_14_1d:
    description: Latest daily RSI(14) proxy.
    dtype: float64
    unit: index_0_100
    context: RSI snapshot.

tests:
  not_null: [symbol]
  unique: [symbol]
  fail_on_error: true
---
WITH daily AS (
    SELECT
        symbol,
        COUNT(*) AS bar_count_1d,
        MIN(low) AS period_low_1d,
        MAX(high) AS period_high_1d,
        SUM(volume) AS total_volume_1d
    FROM {{ ref('fact_1d_bars') }}
    GROUP BY symbol
),
latest_daily AS (
    SELECT
        symbol,
        close AS last_close_1d,
        macd_hist AS last_macd_hist_1d,
        rvol_20 AS last_rvol_20_1d,
        rsi_14 AS last_rsi_14_1d
    FROM (
        SELECT
            symbol,
            close,
            macd_hist,
            rvol_20,
            rsi_14,
            ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY timestamp_ns DESC
            ) AS rn
        FROM {{ ref('fact_1d_bars') }}
    ) x
    WHERE rn = 1
)
SELECT
    d.symbol,
    d.first_bar_time,
    d.last_bar_time,
    d.bar_count_1m,
    COALESCE(a.bar_count_1d, 0) AS bar_count_1d,
    a.period_low_1d,
    a.period_high_1d,
    a.total_volume_1d,
    l.last_close_1d,
    l.last_macd_hist_1d,
    l.last_rvol_20_1d,
    l.last_rsi_14_1d
FROM {{ ref('dim_symbol') }} d
LEFT JOIN daily a ON d.symbol = a.symbol
LEFT JOIN latest_daily l ON d.symbol = l.symbol

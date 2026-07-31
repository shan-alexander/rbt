---
description: 1-minute fact grain with OHLCV and technical indicators.
context: >
  Grain (symbol, timestamp_ns / bar_time). Measures come from tf_bar_metrics
  (MACD-SMA proxy, RVOL, RSI proxy, SMAs, returns). Degenerate join to dim_symbol
  enforces that facts only exist for dimension members. Full Parquet rewrite.

grain: [symbol, timestamp_ns]
unique_key: [symbol, timestamp_ns]
materialization: table
tags: [mart, fact, gold, timeframe_1m, indicators, full_refresh_parquet]

meta:
  source_prep: tf_bar_metrics
  dim: dim_symbol
  rewrite: full_parquet
  macd_method: sma_proxy

columns:
  symbol:
    description: FK natural key to dim_symbol.
    dtype: utf8
    context: Same as dim natural key (no surrogate key in this example).
  bar_time:
    description: Fact event timestamp.
    dtype: timestamp
    context: 1m bar open time.
  timestamp_ns:
    description: Fact grain clock.
    dtype: int64
    unit: ns_epoch
    context: Matches staging/metrics grain.
  open:
    description: Open.
    dtype: float64
    unit: price
    context: Additive degenerate measure.
  high:
    description: High.
    dtype: float64
    unit: price
    context: Additive degenerate measure.
  low:
    description: Low.
    dtype: float64
    unit: price
    context: Additive degenerate measure.
  close:
    description: Close.
    dtype: float64
    unit: price
    context: Price fact measure.
  volume:
    description: Volume.
    dtype: int64
    unit: shares
    context: Activity measure.
  ret_1:
    description: 1-bar return.
    dtype: float64
    unit: ratio
    context: From tf_bar_metrics.
  log_ret_1:
    description: 1-bar log return.
    dtype: float64
    unit: ratio
    context: From tf_bar_metrics.
  range_pct:
    description: (H-L)/C.
    dtype: float64
    unit: ratio
    context: Intrabar volatility proxy.
  sma_20:
    description: 20-bar SMA.
    dtype: float64
    unit: price
    context: Trend measure.
  sma_50:
    description: 50-bar SMA.
    dtype: float64
    unit: price
    context: Trend measure.
  macd_line:
    description: MACD line (SMA proxy).
    dtype: float64
    unit: price
    context: See meta.macd_method.
  macd_signal:
    description: MACD signal.
    dtype: float64
    unit: price
    context: 9-bar SMA of macd_line.
  macd_hist:
    description: MACD histogram.
    dtype: float64
    unit: price
    context: line - signal.
  rvol_20:
    description: Relative volume (20).
    dtype: float64
    unit: ratio
    context: volume / prior 20-bar avg volume.
  rsi_14:
    description: RSI(14) proxy.
    dtype: float64
    unit: index_0_100
    context: SMA gain/loss RSI (not Wilder).
  volatility_20:
    description: 20-bar return stddev.
    dtype: float64
    unit: ratio
    context: Realized vol proxy.
  symbol_first_seen:
    description: Dim first_bar_time.
    dtype: timestamp
    context: Degenerate dim attribute denormalized for convenience.
  symbol_last_seen:
    description: Dim last_bar_time.
    dtype: timestamp
    context: Degenerate dim attribute denormalized for convenience.

tests:
  not_null: [symbol, timestamp_ns, close]
  unique: [symbol, timestamp_ns]
  fail_on_error: true
---
SELECT
    m.symbol,
    m.bar_time,
    m.timestamp_ns,
    m.open,
    m.high,
    m.low,
    m.close,
    m.volume,
    m.ret_1,
    m.log_ret_1,
    m.range_pct,
    m.sma_20,
    m.sma_50,
    m.macd_line,
    m.macd_signal,
    m.macd_hist,
    m.rvol_20,
    m.rsi_14,
    m.volatility_20,
    d.first_bar_time AS symbol_first_seen,
    d.last_bar_time AS symbol_last_seen
FROM {{ ref('tf_bar_metrics') }} m
INNER JOIN {{ ref('dim_symbol') }} d
    ON m.symbol = d.symbol

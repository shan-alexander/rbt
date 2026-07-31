---
description: Daily fact grain with OHLCV and daily technical indicators.
context: >
  Grain (symbol, timestamp_ns). Measures from tf_bar_metrics_1d; joined to
  dim_symbol for coverage attributes. Full Parquet rewrite.

grain: [symbol, timestamp_ns]
unique_key: [symbol, timestamp_ns]
materialization: table
tags: [mart, fact, gold, timeframe_1d, indicators, full_refresh_parquet]

meta:
  source_prep: tf_bar_metrics_1d
  dim: dim_symbol
  rewrite: full_parquet
  macd_method: sma_proxy

columns:
  symbol:
    description: FK natural key to dim_symbol.
    dtype: utf8
    context: Daily fact member.
  bar_time:
    description: Daily bar timestamp.
    dtype: timestamp
    context: Session/day timestamp from metrics prep.
  timestamp_ns:
    description: Fact grain clock.
    dtype: int64
    unit: ns_epoch
    context: Daily grain key with symbol.
  open:
    description: Day open.
    dtype: float64
    unit: price
    context: OHLCV measure.
  high:
    description: Day high.
    dtype: float64
    unit: price
    context: OHLCV measure.
  low:
    description: Day low.
    dtype: float64
    unit: price
    context: OHLCV measure.
  close:
    description: Day close.
    dtype: float64
    unit: price
    context: Price measure / indicator base.
  volume:
    description: Day volume.
    dtype: int64
    unit: shares
    context: Activity measure.
  ret_1:
    description: 1-day return.
    dtype: float64
    unit: ratio
    context: From daily metrics prep.
  sma_20:
    description: 20-day SMA.
    dtype: float64
    unit: price
    context: Trend.
  sma_50:
    description: 50-day SMA.
    dtype: float64
    unit: price
    context: Trend.
  macd_line:
    description: Daily MACD line (SMA proxy).
    dtype: float64
    unit: price
    context: See meta.macd_method.
  macd_signal:
    description: Daily MACD signal.
    dtype: float64
    unit: price
    context: 9-day SMA of line.
  macd_hist:
    description: Daily MACD histogram.
    dtype: float64
    unit: price
    context: line - signal.
  rvol_20:
    description: Daily relative volume (20).
    dtype: float64
    unit: ratio
    context: volume / 20d avg volume.
  rsi_14:
    description: Daily RSI(14) proxy.
    dtype: float64
    unit: index_0_100
    context: SMA gain/loss RSI.
  symbol_first_seen_1m:
    description: Dim first 1m bar time.
    dtype: timestamp
    context: Coverage attribute from dim.
  bar_count_1m:
    description: Dim 1m bar count.
    dtype: int64
    context: Coverage depth from dim.

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
    m.sma_20,
    m.sma_50,
    m.macd_line,
    m.macd_signal,
    m.macd_hist,
    m.rvol_20,
    m.rsi_14,
    d.first_bar_time AS symbol_first_seen_1m,
    d.bar_count_1m
FROM {{ ref('tf_bar_metrics_1d') }} m
INNER JOIN {{ ref('dim_symbol') }} d
    ON m.symbol = d.symbol

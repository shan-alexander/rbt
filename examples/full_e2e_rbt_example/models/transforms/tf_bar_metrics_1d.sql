---
description: Preparatory daily bar metrics (returns, SMA, RVOL, MACD-SMA).
context: >
  Daily analog of tf_bar_metrics over stg_ohlcv_1d. Feeds fact_1d_bars.
  Same SMA-proxy MACD and 20-day RVOL conventions as the 1m metrics model.

grain: [symbol, timestamp_ns]
unique_key: [symbol, timestamp_ns]
materialization: table
tags: [transform, metrics, ohlcv, timeframe_1d, full_refresh_parquet]

meta:
  feeds: fact_1d_bars
  rewrite: full_parquet
  macd_method: sma_proxy
  rvol_lookback_bars: "20"

columns:
  symbol:
    description: Equity ticker.
    context: Window partition key.
    dtype: utf8
  timeframe:
    description: 1d timeframe label.
    dtype: utf8
    context: From staging daily.
  bar_time:
    description: Daily bar timestamp.
    dtype: timestamp
    context: Aligns with stg_ohlcv_1d.bar_time.
  timestamp_ns:
    description: Grain clock (ns).
    dtype: int64
    unit: ns_epoch
    context: ORDER BY for daily windows.
  open:
    description: Day open.
    dtype: float64
    unit: price
    context: Passthrough.
  high:
    description: Day high.
    dtype: float64
    unit: price
    context: Passthrough.
  low:
    description: Day low.
    dtype: float64
    unit: price
    context: Passthrough.
  close:
    description: Day close.
    dtype: float64
    unit: price
    context: Indicator input series.
  volume:
    description: Day volume.
    dtype: int64
    unit: shares
    context: RVOL input.
  ret_1:
    description: 1-day simple return.
    dtype: float64
    unit: ratio
    context: close/lag(close)-1.
  sma_20:
    description: 20-day SMA close.
    dtype: float64
    unit: price
    context: Medium daily trend.
  sma_50:
    description: 50-day SMA close.
    dtype: float64
    unit: price
    context: Longer daily trend.
  macd_line:
    description: Daily MACD line (SMA12-SMA26).
    dtype: float64
    unit: price
    context: SMA proxy; see meta.macd_method.
  macd_signal:
    description: 9-day SMA of macd_line.
    dtype: float64
    unit: price
    context: Signal leg.
  macd_hist:
    description: macd_line - macd_signal.
    dtype: float64
    unit: price
    context: Histogram.
  rvol_20:
    description: Volume / 20-day avg volume.
    dtype: float64
    unit: ratio
    context: Relative volume interest.
  rsi_14:
    description: 14-day RSI proxy.
    dtype: float64
    unit: index_0_100
    context: SMA of gains/losses (not Wilder).

tests:
  not_null: [symbol, timestamp_ns, close]
  unique: [symbol, timestamp_ns]
  fail_on_error: true
---
WITH base AS (
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
        LAG(close, 1) OVER (
            PARTITION BY symbol
            ORDER BY timestamp_ns
        ) AS prev_close
    FROM {{ ref('stg_ohlcv_1d') }}
),
rets AS (
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
        CASE
            WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
            ELSE (close / prev_close) - 1.0
        END AS ret_1
    FROM base
),
windows AS (
    SELECT
        *,
        AVG(close) OVER w12 AS sma_12,
        AVG(close) OVER w26 AS sma_26,
        AVG(close) OVER w20 AS sma_20,
        AVG(close) OVER w50 AS sma_50,
        AVG(volume) OVER wvol AS vol_sma_20,
        AVG(CASE WHEN ret_1 > 0 THEN ret_1 ELSE 0.0 END) OVER w14 AS avg_gain_14,
        AVG(CASE WHEN ret_1 < 0 THEN -ret_1 ELSE 0.0 END) OVER w14 AS avg_loss_14
    FROM rets
    WINDOW
        w12 AS (PARTITION BY symbol ORDER BY timestamp_ns ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),
        w26 AS (PARTITION BY symbol ORDER BY timestamp_ns ROWS BETWEEN 25 PRECEDING AND CURRENT ROW),
        w20 AS (PARTITION BY symbol ORDER BY timestamp_ns ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
        w50 AS (PARTITION BY symbol ORDER BY timestamp_ns ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
        w14 AS (PARTITION BY symbol ORDER BY timestamp_ns ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
        wvol AS (PARTITION BY symbol ORDER BY timestamp_ns ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
),
macd_base AS (
    SELECT
        *,
        (sma_12 - sma_26) AS macd_line,
        CASE
            WHEN vol_sma_20 IS NULL OR vol_sma_20 = 0 THEN NULL
            ELSE CAST(volume AS DOUBLE) / vol_sma_20
        END AS rvol_20,
        CASE
            WHEN avg_loss_14 IS NULL OR avg_loss_14 = 0 THEN
                CASE WHEN avg_gain_14 IS NULL THEN NULL ELSE 100.0 END
            ELSE 100.0 - (100.0 / (1.0 + (avg_gain_14 / avg_loss_14)))
        END AS rsi_14
    FROM windows
)
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
    ret_1,
    sma_20,
    sma_50,
    macd_line,
    AVG(macd_line) OVER (
        PARTITION BY symbol
        ORDER BY timestamp_ns
        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    ) AS macd_signal,
    macd_line - AVG(macd_line) OVER (
        PARTITION BY symbol
        ORDER BY timestamp_ns
        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    ) AS macd_hist,
    rvol_20,
    rsi_14
FROM macd_base

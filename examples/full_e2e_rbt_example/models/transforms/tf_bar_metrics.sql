---
description: Preparatory 1m bar metrics (returns, SMA, MACD-SMA, RVOL, RSI proxy).
context: >
  Windowed indicators over stg_ohlcv_1m ordered by timestamp_ns per symbol.
  MACD here is SMA-based (sma_12 - sma_26, signal = sma_9 of that line) — a pure-SQL
  approximation without exponential smoothing UDFs. RVOL uses prior-20-bar avg volume.
  fact_1m_bars should ref this table for measures rather than recompute windows.

grain: [symbol, timestamp_ns]
unique_key: [symbol, timestamp_ns]
materialization: table
tags: [transform, metrics, ohlcv, timeframe_1m, full_refresh_parquet]

meta:
  feeds: fact_1m_bars
  rewrite: full_parquet
  macd_method: sma_proxy
  rvol_lookback_bars: "20"

columns:
  symbol:
    description: Equity ticker.
    context: Partition key for all window functions.
    dtype: utf8
  timeframe:
    description: Always 1m for this model.
    dtype: utf8
    context: Inherited from staging.
  bar_time:
    description: Bar timestamp.
    dtype: timestamp
    context: Aligns with staging bar_time.
  timestamp_ns:
    description: Grain clock (ns).
    dtype: int64
    unit: ns_epoch
    context: ORDER BY key for windows.
  open:
    description: Open price.
    dtype: float64
    unit: price
    context: Passthrough from staging.
  high:
    description: High price.
    dtype: float64
    unit: price
    context: Passthrough from staging.
  low:
    description: Low price.
    dtype: float64
    unit: price
    context: Passthrough from staging.
  close:
    description: Close price.
    dtype: float64
    unit: price
    context: Primary series for indicators.
  volume:
    description: Volume.
    dtype: int64
    unit: shares
    context: Input to RVOL and volume SMA.
  ret_1:
    description: 1-bar simple return.
    context: close / lag(close) - 1; null on first bar per symbol.
    dtype: float64
    unit: ratio
  log_ret_1:
    description: 1-bar log return.
    context: ln(close / lag(close)); null when lag missing or non-positive.
    dtype: float64
    unit: ratio
  range_pct:
    description: Intrabar range as fraction of close.
    context: (high - low) / close; null if close=0.
    dtype: float64
    unit: ratio
  typical_price:
    description: (H+L+C)/3.
    context: Common input for volume-weighted style metrics.
    dtype: float64
    unit: price
  sma_12:
    description: 12-bar simple moving average of close.
    context: MACD fast leg (SMA proxy for EMA12).
    dtype: float64
    unit: price
  sma_26:
    description: 26-bar SMA of close.
    context: MACD slow leg (SMA proxy for EMA26).
    dtype: float64
    unit: price
  sma_20:
    description: 20-bar SMA of close.
    context: Short trend reference.
    dtype: float64
    unit: price
  sma_50:
    description: 50-bar SMA of close.
    context: Medium trend reference.
    dtype: float64
    unit: price
  macd_line:
    description: MACD line (SMA12 - SMA26).
    context: Not true EMA MACD; documented as sma_proxy in meta.
    dtype: float64
    unit: price
  macd_signal:
    description: 9-bar SMA of macd_line.
    context: Signal line for MACD cross logic.
    dtype: float64
    unit: price
  macd_hist:
    description: MACD histogram (line - signal).
    context: Positive when line above signal.
    dtype: float64
    unit: price
  vol_sma_20:
    description: 20-bar SMA of volume (prior window for RVOL).
    context: AVG volume over prior 20 bars excluding current (ROWS 20 PRECEDING AND 1 PRECEDING).
    dtype: float64
    unit: shares
  rvol_20:
    description: Relative volume vs 20-bar average.
    context: volume / vol_sma_20; null when baseline missing/zero. Common short-term interest signal.
    dtype: float64
    unit: ratio
  rsi_14:
    description: 14-bar RSI proxy from average gains/losses.
    context: Classic RSI formula with SMA of gains/losses (Wilder smoothing not applied).
    dtype: float64
    unit: index_0_100
  volatility_20:
    description: 20-bar stddev of ret_1.
    context: Realized vol proxy on simple returns.
    dtype: float64
    unit: ratio

tests:
  not_null: [symbol, timestamp_ns, close, volume]
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
    FROM {{ ref('stg_ohlcv_1m') }}
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
        END AS ret_1,
        CASE
            WHEN prev_close IS NULL OR prev_close <= 0 OR close <= 0 THEN NULL
            ELSE ln(close / prev_close)
        END AS log_ret_1,
        CASE
            WHEN close = 0 THEN NULL
            ELSE (high - low) / close
        END AS range_pct,
        (high + low + close) / 3.0 AS typical_price
    FROM base
),
windows AS (
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
        log_ret_1,
        range_pct,
        typical_price,
        AVG(close) OVER w12 AS sma_12,
        AVG(close) OVER w26 AS sma_26,
        AVG(close) OVER w20 AS sma_20,
        AVG(close) OVER w50 AS sma_50,
        AVG(volume) OVER wvol AS vol_sma_20,
        STDDEV_SAMP(ret_1) OVER w20 AS volatility_20,
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
    log_ret_1,
    range_pct,
    typical_price,
    sma_12,
    sma_26,
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
    vol_sma_20,
    rvol_20,
    rsi_14,
    volatility_20
FROM macd_base

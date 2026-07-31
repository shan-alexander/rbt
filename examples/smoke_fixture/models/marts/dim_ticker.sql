---
description: Smoke dimension
grain: [ticker]
unique_key: [ticker]
tests:
  unique: [ticker]
  not_null: [ticker]
columns:
  ticker:
    description: PK
    context: Dim key
  trade_count:
    description: Trades
    context: From tf
  avg_price:
    description: Avg price
    context: From tf
---
SELECT ticker, trade_count, avg_price FROM {{ ref('tf_ticker_stats') }}

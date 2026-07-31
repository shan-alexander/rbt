---
description: Per-ticker rollup for smoke dim
grain: [ticker]
unique_key: [ticker]
tests:
  not_null: [ticker]
  unique: [ticker]
columns:
  ticker:
    description: Symbol
    context: Prep for dim
  trade_count:
    description: Count of trades
    context: From staging
  avg_price:
    description: Average price
    context: Simple metric
---
SELECT
  ticker,
  COUNT(*) AS trade_count,
  AVG(price) AS avg_price
FROM {{ ref('stg_trades') }}
GROUP BY ticker

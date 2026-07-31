---
description: Smoke staging trades with latest-wins on id
source_format: jsonl
scan_path: "lake/bronze/trades.jsonl"
source_name: bronze
source_table: trades
grain: [id]
unique_key: [id]
tests:
  not_null: [id, ticker]
  unique: [id]
  fail_on_error: true
columns:
  id:
    description: Trade id
    context: Grain key after dedupe
  ticker:
    description: Symbol
    context: Smoke ticker
  price:
    description: Price
    context: Latest price for id
  qty:
    description: Quantity
    context: Share quantity
---
WITH raw AS (
  SELECT id, ticker, price, qty FROM {{ source('bronze', 'trades') }}
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY price DESC) AS rn
  FROM raw
  WHERE id IS NOT NULL AND ticker IS NOT NULL
)
SELECT id, ticker, price, qty FROM ranked WHERE rn = 1

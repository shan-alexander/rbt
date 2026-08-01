---
description: Gold site dimension from unit inventory domains (conformed entity).
lineage_stamp: true
grain: [domain, report_date]
tests:
  not_null: [domain, report_date]
  unique: [domain, report_date]
---
SELECT DISTINCT
  domain,
  report_date,
  run_id
FROM {{ ref('stg_plan') }}

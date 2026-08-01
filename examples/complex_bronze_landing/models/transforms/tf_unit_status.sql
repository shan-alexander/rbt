---
description: Outer-style unit reconciliation — plan ⟕ scrape ⟕ failures → row_status.
stage_mode: full_refresh
grain: [url, report_date, run_id]
tests:
  not_null: [url, row_status]
  unique: [url, report_date, run_id]
  accepted_values:
    row_status: [success, failed, planned_only]
---
SELECT
  p.url,
  p.domain,
  p.report_date,
  p.run_id,
  s.title,
  s.score,
  f.error,
  CASE
    WHEN s.url IS NOT NULL THEN 'success'
    WHEN f.url IS NOT NULL THEN 'failed'
    ELSE 'planned_only'
  END AS row_status
FROM {{ ref('stg_plan') }} p
LEFT JOIN {{ ref('stg_scrape') }} s
  ON p.url = s.url
LEFT JOIN {{ ref('stg_failures') }} f
  ON p.url = f.url

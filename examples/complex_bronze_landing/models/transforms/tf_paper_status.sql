---
description: >
  Gold transform: paper-level status for this run.
  Refs silver stage endpoints only (stg_works, stg_plan, stg_failures).
  Marks works as landed; planned units without a matching success stay planned_only / failed.
stage_mode: full_refresh
grain: [unit_or_paper_id, report_date, run_id]
tests:
  not_null: [unit_or_paper_id, row_status]
  unique: [unit_or_paper_id, report_date, run_id]
  accepted_values:
    row_status: [success, failed, planned_only]
---
-- Successful landings from works
SELECT
  w.paper_id AS unit_or_paper_id,
  w.source,
  w.doi,
  w.title,
  w.abstract,
  w.authors_joined,
  w.author_count,
  w.venue,
  w.year,
  w.url,
  w.domain,
  w.report_date,
  w.run_id,
  CAST('success' AS VARCHAR) AS row_status,
  CAST(NULL AS VARCHAR) AS error
FROM {{ ref('stg_works') }} w

UNION ALL

-- Planned units that failed (rate limit, timeout, HTTP)
SELECT
  f.unit_id AS unit_or_paper_id,
  f.source,
  CAST(NULL AS VARCHAR) AS doi,
  CAST(NULL AS VARCHAR) AS title,
  CAST(NULL AS VARCHAR) AS abstract,
  CAST(NULL AS VARCHAR) AS authors_joined,
  CAST(NULL AS BIGINT) AS author_count,
  CAST(NULL AS VARCHAR) AS venue,
  CAST(NULL AS VARCHAR) AS year,
  CAST(NULL AS VARCHAR) AS url,
  f.domain,
  f.report_date,
  f.run_id,
  CAST('failed' AS VARCHAR) AS row_status,
  f.error
FROM {{ ref('stg_failures') }} f

UNION ALL

-- Planned units with no success and no failure row → planned_only (gap in lander)
SELECT
  p.unit_id AS unit_or_paper_id,
  p.source,
  CAST(NULL AS VARCHAR) AS doi,
  CAST(NULL AS VARCHAR) AS title,
  CAST(NULL AS VARCHAR) AS abstract,
  CAST(NULL AS VARCHAR) AS authors_joined,
  CAST(NULL AS BIGINT) AS author_count,
  CAST(NULL AS VARCHAR) AS venue,
  CAST(NULL AS VARCHAR) AS year,
  CAST(NULL AS VARCHAR) AS url,
  p.domain,
  p.report_date,
  p.run_id,
  CAST('planned_only' AS VARCHAR) AS row_status,
  CAST(NULL AS VARCHAR) AS error
FROM {{ ref('stg_plan') }} p
LEFT JOIN {{ ref('stg_failures') }} f
  ON p.unit_id = f.unit_id
 AND p.report_date = f.report_date
 AND p.run_id = f.run_id
WHERE f.unit_id IS NULL
  AND NOT EXISTS (
    SELECT 1
    FROM {{ ref('stg_works') }} w
    WHERE w.source = p.source
      AND w.report_date = p.report_date
      AND w.run_id = p.run_id
  )

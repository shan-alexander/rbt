---
description: >
  one row per planned url per report_date per run_id. Thin gold fact: site_sk FK +
  measures/flags from gold transform tf_unit_status (which only ref'd silver stg_*).
lineage_stamp: true
grain: [url, report_date, run_id]
tests:
  not_null: [url, site_sk, row_status]
  unique: [url, report_date, run_id]
  accepted_values:
    row_status: [success, failed, planned_only]
  relationships:
    - column: site_sk
      to_model: dim_site
      to_column: site_sk
---
SELECT
  COALESCE(d.site_sk, CAST(-1 AS BIGINT)) AS site_sk,
  u.url,
  u.report_date,
  u.run_id,
  u.score,
  u.row_status
FROM {{ ref('tf_unit_status') }} u
LEFT JOIN {{ ref('dim_site') }} d
  ON u.domain = d.domain
 AND u.report_date = d.report_date
 AND COALESCE(d.is_unknown, false) = false

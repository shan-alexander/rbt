---
description: Gold fact — successful and failed units with lineage; FK to dim_site.
lineage_stamp: true
grain: [url, report_date, run_id]
tests:
  not_null: [url, domain, row_status]
  unique: [url, report_date, run_id]
  accepted_values:
    row_status: [success, failed, planned_only]
  relationships:
    - column: domain
      to_model: dim_site
      to_column: domain
---
SELECT
  u.url,
  u.domain,
  u.report_date,
  u.run_id,
  u.title,
  u.score,
  u.error,
  u.row_status
FROM {{ ref('tf_unit_status') }} u

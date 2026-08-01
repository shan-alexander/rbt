---
description: >
  one row per domain per report_date (conformed site). Includes Unknown member site_sk=-1.
  Type-1 snapshot for this example; SCD2 is a P7 target.
lineage_stamp: true
grain: [domain, report_date]
unique_key: [site_sk]
tests:
  not_null: [site_sk, domain]
  unique: [site_sk]
  # natural key uniqueness for real members (Unknown has domain=__UNKNOWN__)
---
-- Unknown member (required): facts fall back to site_sk = -1
SELECT
  CAST(-1 AS BIGINT) AS site_sk,
  CAST('__UNKNOWN__' AS VARCHAR) AS domain,
  CAST(NULL AS VARCHAR) AS report_date,
  CAST(NULL AS VARCHAR) AS run_id,
  CAST(true AS BOOLEAN) AS is_unknown
UNION ALL
-- Real members from silver stage (not from silver/tf — dim reads stage grain)
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY domain, report_date) AS BIGINT) AS site_sk,
  domain,
  report_date,
  run_id,
  CAST(false AS BOOLEAN) AS is_unknown
FROM (
  SELECT
    domain,
    report_date,
    MAX(run_id) AS run_id
  FROM {{ ref('stg_plan') }}
  GROUP BY domain, report_date
) s

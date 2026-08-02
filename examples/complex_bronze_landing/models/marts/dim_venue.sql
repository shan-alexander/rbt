---
description: >
  one row per venue / journal name observed in silver works, plus Unknown (-1).
lineage_stamp: true
grain: [venue_name]
unique_key: [venue_sk]
tests:
  not_null: [venue_sk]
  unique: [venue_sk]
---
SELECT
  CAST(-1 AS BIGINT) AS venue_sk,
  CAST('__UNKNOWN__' AS VARCHAR) AS venue_name,
  CAST(true AS BOOLEAN) AS is_unknown
UNION ALL
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY venue_name) AS BIGINT) AS venue_sk,
  venue_name,
  CAST(false AS BOOLEAN) AS is_unknown
FROM (
  SELECT DISTINCT
    CASE
      WHEN venue IS NULL OR TRIM(venue) = '' THEN '__UNKNOWN__'
      ELSE venue
    END AS venue_name
  FROM {{ ref('stg_works') }}
  WHERE venue IS NOT NULL AND TRIM(venue) <> ''
) v

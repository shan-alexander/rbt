---
description: >
  one row per research track (semicon / agritech) plus Unknown (−1).
lineage_stamp: true
grain: [topic_code]
unique_key: [topic_sk]
tests:
  not_null: [topic_sk, topic_code]
  unique: [topic_sk]
---
SELECT
  CAST(-1 AS BIGINT) AS topic_sk,
  CAST('__UNKNOWN__' AS VARCHAR) AS topic_code,
  CAST('Unknown track' AS VARCHAR) AS topic_name,
  CAST(true AS BOOLEAN) AS is_unknown
UNION ALL
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY topic_code) AS BIGINT) AS topic_sk,
  topic_code,
  topic_name,
  CAST(false AS BOOLEAN) AS is_unknown
FROM (
  SELECT DISTINCT
    topic_track AS topic_code,
    CASE topic_track
      WHEN 'semicon' THEN 'Semiconductors / neuromorphic AI'
      WHEN 'agritech' THEN 'AI in agritech / precision agriculture'
      ELSE topic_track
    END AS topic_name
  FROM {{ ref('stg_works') }}
  WHERE topic_track IS NOT NULL AND TRIM(topic_track) <> ''
) t

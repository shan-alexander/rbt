---
description: >
  one row per paper_id (conformed paper). Type-1 snapshot; SCD2 is P7.
  Attributes from latest successful silver works landing.
lineage_stamp: true
grain: [paper_id]
unique_key: [paper_sk]
tests:
  not_null: [paper_sk, paper_id, title]
  unique: [paper_sk]
---
SELECT
  CAST(-1 AS BIGINT) AS paper_sk,
  CAST('__UNKNOWN__' AS VARCHAR) AS paper_id,
  CAST('Unknown paper' AS VARCHAR) AS title,
  CAST(NULL AS VARCHAR) AS doi,
  CAST(NULL AS VARCHAR) AS abstract,
  CAST(NULL AS VARCHAR) AS authors_joined,
  CAST(NULL AS BIGINT) AS author_count,
  CAST(NULL AS VARCHAR) AS year,
  CAST(NULL AS VARCHAR) AS url,
  CAST(true AS BOOLEAN) AS is_unknown
UNION ALL
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY paper_id) AS BIGINT) AS paper_sk,
  paper_id,
  title,
  doi,
  abstract,
  authors_joined,
  author_count,
  year,
  url,
  CAST(false AS BOOLEAN) AS is_unknown
FROM (
  SELECT
    paper_id,
    MAX(title) AS title,
    MAX(doi) AS doi,
    MAX(abstract) AS abstract,
    MAX(authors_joined) AS authors_joined,
    MAX(author_count) AS author_count,
    MAX(year) AS year,
    MAX(url) AS url
  FROM {{ ref('stg_works') }}
  GROUP BY paper_id
) p

---
description: >
  one row per bibliographic source system (pubmed, crossref, arxiv) plus Unknown (-1).
lineage_stamp: true
grain: [source_code]
unique_key: [source_sk]
tests:
  not_null: [source_sk, source_code]
  unique: [source_sk]
---
SELECT
  CAST(-1 AS BIGINT) AS source_sk,
  CAST('__UNKNOWN__' AS VARCHAR) AS source_code,
  CAST('Unknown source' AS VARCHAR) AS source_name,
  CAST(true AS BOOLEAN) AS is_unknown
UNION ALL
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY source_code) AS BIGINT) AS source_sk,
  source_code,
  source_name,
  CAST(false AS BOOLEAN) AS is_unknown
FROM (
  SELECT DISTINCT
    source AS source_code,
    CASE source
      WHEN 'pubmed' THEN 'PubMed (NCBI E-utilities)'
      WHEN 'crossref' THEN 'Crossref REST API'
      WHEN 'arxiv' THEN 'arXiv Atom API'
      ELSE source
    END AS source_name
  FROM {{ ref('stg_works') }}
) s

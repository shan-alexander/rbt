---
description: >
  Gold transform: per-source run ops stats from silver stages only.
  Joins works / failures / plan / assets for a mini landing-zone dashboard grain.
stage_mode: full_refresh
grain: [source, report_date, run_id]
tests:
  not_null: [source, report_date, run_id]
  unique: [source, report_date, run_id]
---
WITH works AS (
  SELECT
    source,
    report_date,
    run_id,
    domain,
    COUNT(*) AS works_cnt,
    SUM(CASE WHEN has_abstract THEN 1 ELSE 0 END) AS with_abstract_cnt,
    SUM(author_count) AS author_mentions,
    SUM(abstract_chars) AS abstract_chars_sum
  FROM {{ ref('stg_works') }}
  GROUP BY source, report_date, run_id, domain
),
fails AS (
  SELECT source, report_date, run_id, COUNT(*) AS fail_cnt
  FROM {{ ref('stg_failures') }}
  GROUP BY source, report_date, run_id
),
plans AS (
  SELECT source, report_date, run_id, COUNT(*) AS plan_cnt
  FROM {{ ref('stg_plan') }}
  GROUP BY source, report_date, run_id
),
asset_src AS (
  SELECT
    source,
    report_date,
    run_id,
    COUNT(*) AS asset_cnt,
    SUM(bytes) AS asset_bytes
  FROM {{ ref('stg_assets') }}
  WHERE source IN (
    'pubmed', 'crossref', 'europepmc', 'openalex', 'semanticscholar', 'arxiv',
    'robots', 'lander', 'policy'
  )
  GROUP BY source, report_date, run_id
),
keys AS (
  SELECT source, report_date, run_id FROM works
  UNION
  SELECT source, report_date, run_id FROM fails
  UNION
  SELECT source, report_date, run_id FROM plans
)
SELECT
  k.source,
  k.report_date,
  k.run_id,
  COALESCE(w.domain, CAST('' AS VARCHAR)) AS domain,
  COALESCE(p.plan_cnt, CAST(0 AS BIGINT)) AS plan_cnt,
  COALESCE(w.works_cnt, CAST(0 AS BIGINT)) AS works_cnt,
  COALESCE(f.fail_cnt, CAST(0 AS BIGINT)) AS fail_cnt,
  COALESCE(w.with_abstract_cnt, CAST(0 AS BIGINT)) AS with_abstract_cnt,
  COALESCE(w.author_mentions, CAST(0 AS BIGINT)) AS author_mentions,
  COALESCE(w.abstract_chars_sum, CAST(0 AS BIGINT)) AS abstract_chars_sum,
  COALESCE(a.asset_cnt, CAST(0 AS BIGINT)) AS asset_cnt,
  COALESCE(a.asset_bytes, CAST(0 AS BIGINT)) AS asset_bytes
FROM keys k
LEFT JOIN works w
  ON k.source = w.source AND k.report_date = w.report_date AND k.run_id = w.run_id
LEFT JOIN fails f
  ON k.source = f.source AND k.report_date = f.report_date AND k.run_id = f.run_id
LEFT JOIN plans p
  ON k.source = p.source AND k.report_date = p.report_date AND k.run_id = p.run_id
LEFT JOIN asset_src a
  ON k.source = a.source AND k.report_date = a.report_date AND k.run_id = a.run_id

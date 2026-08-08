---
description: >
  Thin ops fact: one row per source × run with landing KPIs from tf_source_run_stats.
  SK FK to dim_source; measures for works/failures/assets.
lineage_stamp: true
grain: [source, report_date, run_id]
tests:
  not_null: [source_sk, source, report_date, run_id]
  unique: [source, report_date, run_id]
  relationships:
    - column: source_sk
      to_model: dim_source
      to_column: source_sk
---
SELECT
  COALESCE(ds.source_sk, CAST(-1 AS BIGINT)) AS source_sk,
  s.source,
  s.report_date,
  s.run_id,
  s.domain,
  s.plan_cnt,
  s.works_cnt,
  s.fail_cnt,
  s.with_abstract_cnt,
  s.author_mentions,
  s.abstract_chars_sum,
  s.asset_cnt,
  s.asset_bytes
FROM {{ ref('tf_source_run_stats') }} s
LEFT JOIN {{ ref('dim_source') }} ds
  ON s.source = ds.source_code
 AND COALESCE(ds.is_unknown, false) = false

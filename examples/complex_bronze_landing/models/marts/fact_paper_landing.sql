---
description: >
  one row per successful or failed unit in a bronze run
  (grain: unit_or_paper_id × report_date × run_id).
  Thin fact: SK FKs + row_status + author_count + abstract_chars measures.
lineage_stamp: true
grain: [unit_or_paper_id, report_date, run_id]
tests:
  not_null: [unit_or_paper_id, source_sk, row_status]
  unique: [unit_or_paper_id, report_date, run_id]
  accepted_values:
    row_status: [success, failed, planned_only]
  relationships:
    - column: source_sk
      to_model: dim_source
      to_column: source_sk
    - column: venue_sk
      to_model: dim_venue
      to_column: venue_sk
    - column: paper_sk
      to_model: dim_paper
      to_column: paper_sk
    - column: topic_sk
      to_model: dim_topic
      to_column: topic_sk
---
SELECT
  COALESCE(ds.source_sk, CAST(-1 AS BIGINT)) AS source_sk,
  COALESCE(dv.venue_sk, CAST(-1 AS BIGINT)) AS venue_sk,
  COALESCE(dp.paper_sk, CAST(-1 AS BIGINT)) AS paper_sk,
  COALESCE(dt.topic_sk, CAST(-1 AS BIGINT)) AS topic_sk,
  t.unit_or_paper_id,
  t.source,
  t.doi,
  t.report_date,
  t.run_id,
  t.domain,
  t.topic_track,
  t.row_status,
  t.author_count,
  t.abstract_chars,
  t.has_abstract,
  t.error
FROM {{ ref('tf_paper_status') }} t
LEFT JOIN {{ ref('dim_source') }} ds
  ON t.source = ds.source_code
 AND COALESCE(ds.is_unknown, false) = false
LEFT JOIN {{ ref('dim_venue') }} dv
  ON t.venue = dv.venue_name
 AND COALESCE(dv.is_unknown, false) = false
LEFT JOIN {{ ref('dim_paper') }} dp
  ON t.unit_or_paper_id = dp.paper_id
 AND COALESCE(dp.is_unknown, false) = false
LEFT JOIN {{ ref('dim_topic') }} dt
  ON t.topic_track = dt.topic_code
 AND COALESCE(dt.is_unknown, false) = false

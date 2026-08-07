---
description: >
  Silver stage endpoint: planned bronze work units (one row per planned API
  call / journal slice / topic track).
source_format: jsonl
scan_path: $lake/lz/runs
path_glob: plan.jsonl
partition_by: [domain, report_date, run_id]
columns:
  unit_id: { dtype: utf8 }
  source: { dtype: utf8 }
  query: { dtype: utf8 }
  venue_filter: { dtype: utf8 }
  topic_track: { dtype: utf8 }
  planned: { dtype: bool }
  domain: { dtype: utf8 }
  report_date: { dtype: utf8 }
  run_id: { dtype: utf8 }
grain: [unit_id, report_date, run_id]
tests:
  not_null: [unit_id, source]
  unique: [unit_id, report_date, run_id]
---
SELECT
  unit_id,
  source,
  query,
  venue_filter,
  topic_track,
  planned,
  domain,
  report_date,
  run_id
FROM {{ source('bronze', 'plan') }}

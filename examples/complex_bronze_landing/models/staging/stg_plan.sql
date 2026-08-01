---
description: Planned inventory units from bronze plan artifact (required family).
source_format: jsonl
scan_path: $lake/lz/runs
path_glob: plan.jsonl
partition_by: [domain, report_date, run_id]
# Bound at runtime: --var domain=… --var report_date=… --var run_id=…
require_partitions: {}
stage_mode: full_refresh
columns:
  url:
    dtype: utf8
    description: Planned unit URL
  planned:
    dtype: bool
  domain:
    dtype: utf8
  report_date:
    dtype: utf8
  run_id:
    dtype: utf8
tests:
  not_null: [url]
---
SELECT
  url,
  planned,
  domain,
  report_date,
  run_id
FROM {{ source('bronze', 'plan') }}

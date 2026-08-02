---
description: >
  Silver stage endpoint: failed bronze work units (rate limits, timeouts, HTTP errors).
  Optional artifact — on_missing empty for partial runs.
source_format: jsonl
scan_path: $lake/lz/runs
path_glob: failures.jsonl
partition_by: [domain, report_date, run_id]
on_missing: empty
columns:
  unit_id: { dtype: utf8 }
  source: { dtype: utf8 }
  error: { dtype: utf8 }
  domain: { dtype: utf8 }
  report_date: { dtype: utf8 }
  run_id: { dtype: utf8 }
grain: [unit_id, report_date, run_id]
tests:
  not_null: [unit_id]
  unique: [unit_id, report_date, run_id]
---
SELECT
  unit_id,
  source,
  error,
  domain,
  report_date,
  run_id
FROM {{ source('bronze', 'failures') }}

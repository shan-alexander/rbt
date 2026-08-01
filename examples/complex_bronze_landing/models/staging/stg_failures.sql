---
description: Failure ledger (optional; first-class when present).
source_format: jsonl
scan_path: $lake/lz/runs
path_glob: failures.jsonl
partition_by: [domain, report_date, run_id]
on_missing: empty
stage_mode: full_refresh
columns:
  url:
    dtype: utf8
  error:
    dtype: utf8
  domain:
    dtype: utf8
  report_date:
    dtype: utf8
  run_id:
    dtype: utf8
---
SELECT
  url,
  error,
  domain,
  report_date,
  run_id
FROM {{ source('bronze', 'failures') }}

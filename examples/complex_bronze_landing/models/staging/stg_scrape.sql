---
description: Successful enrichments (optional for a partition — may be empty).
source_format: jsonl
scan_path: $lake/lz/runs
path_glob: scrape.jsonl
partition_by: [domain, report_date, run_id]
on_missing: empty
stage_mode: full_refresh
columns:
  url:
    dtype: utf8
  title:
    dtype: utf8
  score:
    dtype: int64
  domain:
    dtype: utf8
  report_date:
    dtype: utf8
  run_id:
    dtype: utf8
---
SELECT
  url,
  title,
  score,
  domain,
  report_date,
  run_id
FROM {{ source('bronze', 'scrape') }}

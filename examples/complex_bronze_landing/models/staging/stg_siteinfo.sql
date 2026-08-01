---
description: Entity-level site rows (optional; may be absent while units exist).
source_format: jsonl
scan_path: $lake/lz/runs
path_glob: siteinfo.jsonl
partition_by: [domain, report_date, run_id]
on_missing: empty
stage_mode: full_refresh
columns:
  domain:
    dtype: utf8
  site_name:
    dtype: utf8
  report_date:
    dtype: utf8
  run_id:
    dtype: utf8
---
SELECT
  domain,
  site_name,
  report_date,
  run_id
FROM {{ source('bronze', 'siteinfo') }}

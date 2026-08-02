---
description: >
  Silver stage endpoint: API/portal site inventory + robots.txt fetch status.
source_format: jsonl
scan_path: $lake/lz/runs
path_glob: siteinfo.jsonl
partition_by: [domain, report_date, run_id]
on_missing: empty
columns:
  site_id: { dtype: utf8 }
  origin: { dtype: utf8 }
  role: { dtype: utf8 }
  robots_status: { dtype: int64 }
  robots_bytes: { dtype: int64 }
  domain: { dtype: utf8 }
  report_date: { dtype: utf8 }
  run_id: { dtype: utf8 }
grain: [site_id, report_date, run_id]
tests:
  not_null: [site_id]
  unique: [site_id, report_date, run_id]
---
SELECT
  site_id,
  origin,
  role,
  robots_status,
  robots_bytes,
  domain,
  report_date,
  run_id
FROM {{ source('bronze', 'siteinfo') }}

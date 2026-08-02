---
description: >
  Silver stage: raw robots.txt line inventory (txt bronze under robots/).
  Demonstrates non-JSON bronze via path_glob + source_format txt.
source_format: txt
scan_path: $lake/lz/runs
path_glob: "**/robots/*.txt"
partition_by: [domain, report_date, run_id]
inject_source_path: true
on_missing: empty
columns:
  line_no: { dtype: int64 }
  content: { dtype: utf8 }
  domain: { dtype: utf8 }
  report_date: { dtype: utf8 }
  run_id: { dtype: utf8 }
  _source_path: { dtype: utf8 }
---
SELECT
  line_no,
  content,
  domain,
  report_date,
  run_id,
  _source_path
FROM {{ source('bronze', 'robots_txt') }}

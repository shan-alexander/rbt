---
description: >
  Silver stage endpoint: inventory of every file landed in the bronze run
  (xml, json, html cards, robots.txt, jsonl tables). Demonstrates mixed
  filetype lakehouse ops — path, kind, mime, bytes.
source_format: jsonl
scan_path: $lake/lz/runs
path_glob: assets.jsonl
partition_by: [domain, report_date, run_id]
on_missing: empty
columns:
  asset_id: { dtype: utf8 }
  rel_path: { dtype: utf8 }
  kind: { dtype: utf8 }
  source: { dtype: utf8 }
  related_id: { dtype: utf8 }
  mime_type: { dtype: utf8 }
  bytes: { dtype: int64 }
  domain: { dtype: utf8 }
  report_date: { dtype: utf8 }
  run_id: { dtype: utf8 }
grain: [asset_id, report_date, run_id]
tests:
  not_null: [asset_id, kind]
  unique: [asset_id, report_date, run_id]
---
SELECT
  asset_id,
  rel_path,
  kind,
  source,
  related_id,
  mime_type,
  bytes,
  domain,
  report_date,
  run_id
FROM {{ source('bronze', 'assets') }}

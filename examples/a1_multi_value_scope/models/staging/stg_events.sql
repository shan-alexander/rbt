---
description: >
  A1 showcase: stage events from hive bronze filtered by multi-value entity scope.
  One rbt run can bind entity IN (a,b) without forking the process per entity.
source_format: jsonl
scan_path: $lake/bronze/runs
path_glob: events.jsonl
partition_by: [entity, report_date]
columns:
  event_id: { dtype: utf8 }
  entity: { dtype: utf8 }
  payload: { dtype: utf8 }
  report_date: { dtype: utf8 }
grain: [event_id, entity, report_date]
tests:
  not_null: [event_id, entity]
  unique: [event_id, entity, report_date]
---
SELECT
  event_id,
  entity,
  payload,
  report_date
FROM {{ source('bronze', 'events') }}

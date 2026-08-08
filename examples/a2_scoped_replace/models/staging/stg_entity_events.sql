---
description: >
  A2 showcase: stage per-entity events with scoped_replace.
  Re-running the same entity/report_date replaces that scope's part only;
  peer entities keep their parts under stg_entity_events.parts/.
materialization: scoped_replace
phase: inventory
tags: [stage, a2_showcase]
source_format: jsonl
scan_path: $lake/bronze/runs
path_glob: events.jsonl
partition_by: [entity, report_date]
part_key: [entity, report_date]
columns:
  event_id: { dtype: utf8 }
  entity: { dtype: utf8 }
  payload: { dtype: utf8 }
  report_date: { dtype: utf8 }
grain: [event_id, entity, report_date]
tests:
  not_null: [event_id, entity]
---
SELECT
  event_id,
  entity,
  payload,
  report_date
FROM {{ source('bronze', 'events') }}

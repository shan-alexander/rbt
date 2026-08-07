---
description: >
  Bronze entity sightings for A7 keyed_upsert demo (full refresh stage).
materialization: table
phase: inventory
tags: [stage, a7_showcase]
source_format: jsonl
scan_path: $lake/bronze/sightings.jsonl
source_name: bronze
source_table: sightings
columns:
  entity_id: { dtype: utf8 }
  status: { dtype: utf8 }
  tier: { dtype: utf8 }
  seen_at: { dtype: utf8 }
grain: [entity_id]
tests:
  not_null: [entity_id]
  unique: [entity_id]
---
SELECT
  entity_id,
  status,
  tier,
  seen_at
FROM {{ source('bronze', 'sightings') }}

---
description: >
  Type-1 entity registry (RBT-A7 keyed_upsert). One row per entity_id.
  Re-run with same status/tier only updates last_seen_at (touch).
  Attr change replaces non-key columns.
materialization: keyed_upsert
unique_key: [entity_id]
touch_columns: [last_seen_at]
compare_columns: [status, tier]
phase: final
tags: [mart, a7_showcase, entity_registry]
columns:
  entity_id: { dtype: utf8 }
  status: { dtype: utf8 }
  tier: { dtype: utf8 }
  last_seen_at: { dtype: utf8 }
grain: [entity_id]
tests:
  not_null: [entity_id]
  unique: [entity_id]
---
SELECT
  entity_id,
  status,
  tier,
  seen_at AS last_seen_at
FROM {{ ref('stg_entity_sightings') }}

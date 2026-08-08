---
description: >
  Durable Type-1 entity dimension (one row per entity_id).
  SQL only defines the *candidate* current attributes (from tf_entity_current).
  materialization keyed_upsert merges candidates into the existing dim:
    - new entity_id → insert
    - same status/tier, newer last_seen_at → touch only
    - status/tier changed → update non-key columns
    - entities not in this run's candidates → KEPT (this is why not materialization:table)
materialization: keyed_upsert
unique_key: [entity_id]
touch_columns: [last_seen_at]
compare_columns: [status, tier]
phase: final
tags: [mart, dim, type1, a7_playbook]
grain: [entity_id]
columns:
  entity_id: { dtype: utf8 }
  status: { dtype: utf8 }
  tier: { dtype: utf8 }
  last_seen_at: { dtype: utf8 }
tests:
  not_null: [entity_id]
  unique: [entity_id]
---
-- Thin gold: candidates already entity-grained in tf_entity_current.
-- keyed_upsert (not table) so a partial candidate set never drops peer entities.
SELECT
  entity_id,
  status,
  tier,
  last_seen_at
FROM {{ ref('tf_entity_current') }}

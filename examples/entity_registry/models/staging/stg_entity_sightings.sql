---
description: >
  Event log of entity sightings from bronze landings (one row per sighting).
  Grain is NOT entity_id alone — many historical rows per entity.
  Does NOT use keyed_upsert: this is the append-style history feed.
materialization: table
phase: inventory
tags: [stage, event_log, a7_playbook]
source_format: jsonl
scan_path: $lake/bronze/sightings
path_glob: "**/*.jsonl"
source_name: bronze
source_table: sightings
partition_by: [report_date]
inject_source_path: true
columns:
  entity_id: { dtype: utf8 }
  status: { dtype: utf8 }
  tier: { dtype: utf8 }
  seen_at: { dtype: utf8 }
  report_date: { dtype: utf8 }
grain: [entity_id, seen_at]
tests:
  not_null: [entity_id, seen_at]
---
-- Full historical log visible to transforms (all landings under bronze/sightings).
SELECT
  entity_id,
  status,
  tier,
  seen_at,
  report_date,
  _source_path
FROM {{ source('bronze', 'sightings') }}

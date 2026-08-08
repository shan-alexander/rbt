---
description: >
  Candidate *current state* for entities that landed on the latest report_date
  only (not the full historical universe). keyed_upsert on dim_entity then
  merges these candidates while KEEping peers that did not land today.
  Grain: one row per entity_id among today's landings (latest seen_at if multi).
materialization: table
phase: prepare
tags: [transform, current_snapshot, a7_playbook]
grain: [entity_id]
unique_key: [entity_id]
columns:
  entity_id: { dtype: utf8 }
  status: { dtype: utf8 }
  tier: { dtype: utf8 }
  last_seen_at: { dtype: utf8 }
tests:
  not_null: [entity_id]
  unique: [entity_id]
---
-- Candidates = entities in the newest bronze day only.
-- Peers absent from this set must be retained by dim keyed_upsert (not table).
WITH bounds AS (
  SELECT max(report_date) AS max_report_date
  FROM {{ ref('stg_entity_sightings') }}
),
today AS (
  SELECT s.*
  FROM {{ ref('stg_entity_sightings') }} s
  CROSS JOIN bounds b
  WHERE s.report_date = b.max_report_date
),
ranked AS (
  SELECT
    entity_id,
    status,
    tier,
    seen_at AS last_seen_at,
    ROW_NUMBER() OVER (
      PARTITION BY entity_id
      ORDER BY seen_at DESC
    ) AS rn
  FROM today
)
SELECT
  entity_id,
  status,
  tier,
  last_seen_at
FROM ranked
WHERE rn = 1

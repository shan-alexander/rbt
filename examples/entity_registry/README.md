# Entity registry playbook (keyed_upsert)

Teaches **why** `materialization: keyed_upsert` exists for durable entity-grain tables —
not “copy 3 jsonl lines into parquet.”

## Architecture (Kimball-aligned)

```text
bronze sightings (daily landings, multi-day hive dirs)
        │
        ▼
stg_entity_sightings     materialization: table
  grain: (entity_id, seen_at)     ← EVENT LOG (many rows per entity)
        │
        ▼
tf_entity_current        materialization: table
  grain: (entity_id)              ← CANDIDATES = entities in *latest* report_date only
  SQL: filter max(report_date) + ROW_NUMBER
        │
        ▼
dim_entity               materialization: keyed_upsert
  grain: (entity_id)              ← DURABLE registry (peers not in candidates KEPT)
  unique_key / touch / compare
```

| Layer | Role | Materialization |
|-------|------|-----------------|
| `stg_*` | Historical sighting log (all days) | `table` |
| `tf_*` | Candidates from **latest landing day** only | `table` |
| `dim_*` | Durable one-row-per-entity store | **`keyed_upsert`** |

Critical: candidates are **not** the full universe every run. Day 2 candidates are
only acme+gamma; beta must survive via upsert keep, not by being re-emitted.

SQL on the dim is intentionally plain `SELECT … FROM tf_*`.  
**Merge policy lives in frontmatter** (same idea as dbt incremental config):  
candidates in, peers kept, touch vs update decided by `compare_columns`.

## Why not `materialization: table` on the dim?

If day 2 only lands acme + gamma, a full-refresh dim built only from “today’s
candidates” **drops beta**. `keyed_upsert` **keeps** beta and merges the candidate set.

That peer-retention property is the product reason for the strategy.

## Multi-day demo

```bash
# from repo root
cargo build -p rbt-datalake --release
chmod +x examples/entity_registry/scripts/demo_upsert.sh
./examples/entity_registry/scripts/demo_upsert.sh
```

| Day | Candidates (tf) | Expected dim counters | Dim state |
|-----|-----------------|----------------------|-----------|
| 1 | acme, beta | insert=2 | 2 rows |
| 2 | acme (touch), gamma (insert) | insert=1, touch=1 | 3 rows; **beta kept** |
| 3 | acme only (status change) | update=1 | acme replaced; **beta+gamma kept** |

Receipt fields: `rows_inserted`, `rows_updated`, `rows_touched`.

## Frontmatter (dim)

```yaml
materialization: keyed_upsert   # not table — merge into existing dim
unique_key: [entity_id]         # defaults to grain: when unique_key omitted
touch_columns: [last_seen_at]   # watermark-style cols when attrs unchanged
compare_columns: [status, tier] # NULL-safe; omit → all non-key non-touch
```

`keyed_upsert` is a **general** entity-key merge primitive (registries, current
snapshots, host tables). Type-1 dims are a common consumer, not the only one.

## Notes

- Incoming candidates must be **unique on `unique_key`** — duplicates fail closed
  (`E_RBT_UPSERT_KEY`), not silent last-wins.
- v1 memory bound: `RBT_UPSERT_MAX_ROWS` (default 2_000_000).
- Hygiene: `W_RBT_UPSERT_HINT` when a mart/dim looks entity-grained but uses `table`.
- Future: `{{ rbt.latest_per(...) }}` macro (roadmap RBT-A17) for the tf window pattern.

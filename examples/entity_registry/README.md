# A7 keyed upsert — entity registry

Showcase for **RBT-A7**: Type-1 entity-grain table with touch-only updates.

## Semantics

```yaml
materialization: keyed_upsert
unique_key: [entity_id]
touch_columns: [last_seen_at]
compare_columns: [status, tier]
```

| Situation | Result |
|-----------|--------|
| New `entity_id` | **insert** full row |
| Same status/tier, new `last_seen_at` | **touch** only |
| status or tier changed | **update** all non-key cols |
| Keys not in this run | **kept** |

## Run

```bash
# from repo root
cargo build -p rbt-datalake --release
EX=examples/entity_registry

# First pass: 3 inserts
./target/release/rbt run -p $EX --format parquet --json

# Same bronze again → 3 touches (attrs unchanged)
./target/release/rbt run -p $EX --format parquet --json

# Change one entity's status, re-run → 1 update + 2 touches
# (edit lake/bronze/sightings.jsonl status for acme.com, then re-run)
```

Receipt `models[]` for `dim_entity` includes:

```json
"rows_inserted": 3,
"rows_updated": 0,
"rows_touched": 0
```

## Measure

```bash
rbt measure -p examples/entity_registry --scenario entity_registry_upsert
# or synthetic N keys (no project files required for core path):
rbt measure -p examples/entity_registry --scenario entity_registry_upsert
```

Env: `RBT_MEASURE_UPSERT_KEYS` (default 5000).

## Notes

- v1 is **in-memory collect** of existing + incoming; cap via `RBT_UPSERT_MAX_ROWS` (default 2_000_000).
- Storage is a single Parquet file (full rewrite after upsert) — not parts.
- Codes: `E_RBT_UPSERT_KEY`, `E_RBT_UPSERT_TOO_LARGE`, `E_RBT_UPSERT_SCHEMA`.

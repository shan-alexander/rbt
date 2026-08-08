# A2 scoped_replace

Showcase for **RBT-A2**: re-run one partition scope and **replace** its part file
without deleting peer scopes (unlike `incremental_append`, which always adds parts).

## Layout

```text
lake/bronze/runs/
  entity=a.com/report_date=2026-08-07/events.jsonl   # 2 rows (will re-land as 3)
  entity=b.com/report_date=2026-08-07/events.jsonl   # 1 row (peer — stays)

After two scoped runs:
lake/silver/stage/stg_entity_events.parts/
  part-<scope_a>.parquet
  part-<scope_b>.parquet
  _rbt_manifest.json
```

## Demo (automated)

```bash
# from repo root
cargo build -p rbt-datalake --release
./examples/a2_scoped_replace/scripts/demo_scoped_replace.sh
```

Script: land **a** (2 rows) → land **b** (1 row) → replace **a** with 3 rows →
assert **2 parts** and `total_rows=4` (b kept). Fixtures under `fixtures/`.

### Manual steps

```bash
RBT=./target/release/rbt
EX=examples/a2_scoped_replace

$RBT run -p $EX --format parquet --var entity=a.com --var report_date=2026-08-07
$RBT run -p $EX --format parquet --var entity=b.com --var report_date=2026-08-07
# then overwrite a's bronze with 3 rows and re-run a only
cat $EX/lake/silver/stage/stg_entity_events.parts/_rbt_manifest.json
```

## Frontmatter

```yaml
materialization: scoped_replace
partition_by: [entity, report_date]
part_key: [entity, report_date]   # optional; default = partition_by ∩ run vars
```

`scope_id` = 16-hex FNV of `model + contract_version + sorted part_key vars`.

Pairs well with **A1 multi-value**: one multi run is **one** part (multi set is hashed
as a whole). Prefer per-entity `scoped_replace` when each entity should have its own part.

See [docs/COMPLEX_BRONZE_AND_RUN_SCOPE.md](../../docs/COMPLEX_BRONZE_AND_RUN_SCOPE.md) §3.

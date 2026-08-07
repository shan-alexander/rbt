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

## Demo

```bash
# from repo root
cargo build -p rbt-datalake --release
RBT=./target/release/rbt
EX=examples/a2_scoped_replace

# 1) Land entity a
$RBT run -p $EX --format parquet \
  --var entity=a.com --var report_date=2026-08-07

# 2) Land entity b (peer part)
$RBT run -p $EX --format parquet \
  --var entity=b.com --var report_date=2026-08-07

# 3) Re-land entity a with more rows (edit bronze, then re-run)
#    After rewrite, a's part is replaced; b's part unchanged.
printf '%s\n' \
  '{"event_id":"a1","entity":"a.com","payload":"alpha-v2","report_date":"2026-08-07"}' \
  '{"event_id":"a2","entity":"a.com","payload":"alpha-v2-b","report_date":"2026-08-07"}' \
  '{"event_id":"a3","entity":"a.com","payload":"alpha-v2-c","report_date":"2026-08-07"}' \
  > $EX/lake/bronze/runs/entity=a.com/report_date=2026-08-07/events.jsonl

$RBT run -p $EX --format parquet \
  --var entity=a.com --var report_date=2026-08-07

# Manifest should show 2 parts; total_rows = 3 (a) + 1 (b) = 4
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

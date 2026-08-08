# A1 multi-value partition scope

Showcase for **RBT-A1**: bind a partition key to a **set of values** in one `rbt run`.

## Layout

```text
lake/bronze/runs/
  entity=a.com/report_date=2026-08-07/events.jsonl   # 2 rows
  entity=b.com/report_date=2026-08-07/events.jsonl   # 1 row
  entity=c.com/report_date=2026-08-07/events.jsonl   # 2 rows (filtered out)
```

## Run (select a + b only)

```bash
# from repo root
cargo build -p rbt-datalake --release
RBT=./target/release/rbt
EX=examples/a1_multi_value_scope

$RBT run -p $EX --format parquet \
  --var entity=a.com --var entity=b.com \
  --var report_date=2026-08-07

# equivalent:
$RBT run -p $EX --format parquet \
  --var-file entity=$EX/entities.txt \
  --var report_date=2026-08-07

$RBT run -p $EX --format parquet \
  --var 'entity:=["a.com","b.com"]' \
  --var report_date=2026-08-07

# machine-readable summary
$RBT run -p $EX --format parquet --json \
  --var entity=a.com --var entity=b.com --var report_date=2026-08-07
```

Expected: **`stg_events` has 3 rows** (a.com×2 + b.com×1). `c.com` never enters silver.

Feature smoke (with A2 + A7): `bash scripts/smoke_feat_a1_a7.sh`

## Why this matters

Hosts often need “run these N entities / dates today” without N process forks.
Multi vars merge into hive **`require_partitions_in`** (IN filter). Path templates
still require scalar values (`{entity}` with multi → `E_RBT_VAR_MULTI`).

See [docs/COMPLEX_BRONZE_AND_RUN_SCOPE.md](../../docs/COMPLEX_BRONZE_AND_RUN_SCOPE.md).

## Benchmark (Criterion)

```bash
cargo bench -p rbt-datalake --bench pipeline -- a1_multi_value
```

Synthetic tree: **200** hive entities, multi IN of **100**. Sample machine (release):

| Bench | Time | Throughput |
|-------|------|------------|
| `list_files_200_entities_in_100` | ~9.5 ms | ~21k entities/s |
| `scan_jsonl_200_entities_in_100` | ~11.6 ms | ~17k entities/s |

Numbers vary by disk; use for relative comparison, not marketing claims.

# Complex bronze landing → silver stage

Demonstrates **P5a/P5b** primitives for multi-artifact Hive-ish bronze trees:

| Concern | How |
|---------|-----|
| Multi-artifact families | `path_glob: plan.jsonl` / `scrape.jsonl` / … |
| Hive partitions | `partition_by: [domain, report_date, run_id]` |
| Run scope | `--var domain=… --var report_date=… --var run_id=…` |
| Optional artifacts | `on_missing: empty` + `columns.*.dtype` |
| Outer reconciliation | `tf_unit_status` LEFT JOINs plan ⟕ scrape ⟕ failures |
| Receipts / skip | `.rbt/runs/*.json` + `--skip-if-match` |

## Layout

```text
lake/lz/runs/domain=…/report_date=…/run_id=…/
  plan.jsonl       # required inventory
  scrape.jsonl     # optional successes
  failures.jsonl   # optional failure ledger
  siteinfo.jsonl   # optional (absent in sample)
```

## Run

```bash
cargo build -p rbt-datalake --release
./target/release/rbt run -p examples/complex_bronze_landing --format parquet \
  --var domain=acme.com \
  --var report_date=2026-07-29 \
  --var run_id=r1 \
  --skip-if-match

# Second identical invoke should skip when bronze unchanged:
./target/release/rbt run -p examples/complex_bronze_landing --format parquet \
  --var domain=acme.com --var report_date=2026-07-29 --var run_id=r1 \
  --skip-if-match
```

Silver outputs under `lake/silver/stage/` and `lake/silver/tf/`.

## Stage modes (author intent)

Frontmatter `stage_mode` is a **documentation / future-engine hint**:

| Mode | Intent |
|------|--------|
| `full_refresh` | Silver is a consolidated rewrite of the scoped bronze slice (this example) |
| `latest_only` | Keep newest landing only (use `inject_source_path` + SQL window / QUALIFY) |
| `append` | Prefer `materialization: incremental_append` |
| `mirror_bronze` | Thin 1:1 projection of one artifact family |

MoR/CoW Iceberg write modes remain under project `materialize.iceberg` when using `--format iceberg`.

# Complex bronze landing → silver → gold star

Kimball-aligned demo for multi-artifact Hive-ish bronze and a thin gold star.

| Layer | Role | Path |
|-------|------|------|
| **Bronze** | External multi-file landing | `lake/lz/runs/domain=…/report_date=…/run_id=…/` |
| **Silver stage** (`stg_*`) | Per-artifact contracts | `lake/silver/stage/` |
| **Silver transform** (`tf_*`) | Outer recon → `row_status` | `lake/silver/tf/` |
| **Gold marts** | `dim_site` (SK + Unknown), `fact_units` (thin + SK FK) | `lake/gold/` |

Guidelines: [docs/GOLD_DEFAULT.md](../../docs/GOLD_DEFAULT.md),
[docs/concepts/star-schema-data-modeling-rules.md](../../docs/concepts/star-schema-data-modeling-rules.md).

## Bronze layout

```text
lake/lz/runs/domain=…/report_date=…/run_id=…/
  plan.jsonl       # required inventory
  scrape.jsonl     # optional successes
  failures.jsonl   # optional failure ledger
  siteinfo.jsonl   # optional (absent in sample → on_missing: empty)
```

## DAG

```text
stg_plan, stg_scrape, stg_failures, stg_siteinfo
        │
        ▼
  tf_unit_status     (silver/tf — plan ⟕ scrape ⟕ failures)
        │
        ├──────────────► dim_site   (from stg_plan; SK + Unknown −1)
        │                     │
        └─────────────────────┴──► fact_units  (SK FK + flags/measures)
```

- **Silver/tf is intentional** for reconciliation (not “wrong gold”).
- **Dim** reads **silver stage** (`stg_plan`), not `tf_*`.
- **Fact** reads silver transform for row payload + dim for **site_sk** (never NULL; Unknown −1).
- Do **not** `source()` upstream transforms; only bronze/published endpoints.

## Run

```bash
cargo build -p rbt-datalake --release
./target/release/rbt validate -p examples/complex_bronze_landing --bronze-check fail
./target/release/rbt run -p examples/complex_bronze_landing --format parquet \
  --var domain=acme.com \
  --var report_date=2026-07-29 \
  --var run_id=r1

# Gold subgraph (ancestors included)
./target/release/rbt run -p examples/complex_bronze_landing -s fact_units --format parquet \
  --var domain=acme.com --var report_date=2026-07-29 --var run_id=r1
```

## Stage modes / P5

See [docs/COMPLEX_BRONZE_AND_RUN_SCOPE.md](../../docs/COMPLEX_BRONZE_AND_RUN_SCOPE.md) for
`--var`, `on_missing: empty`, receipts, `--skip-if-match`.

# Complex bronze → silver endpoints → gold star

Correct medallion topology for rbt:

| Band | Models | Lake path | Depends on |
|------|--------|-----------|------------|
| Bronze | external files | `lake/lz/runs/...` | — |
| **Silver stage (endpoints)** | `stg_*` | `lake/silver/stage/` | bronze `source()` only (1:1 here) |
| **Gold transforms** | `tf_unit_status` | `lake/gold/tf/` | **only** `stg_*` |
| **Gold marts** | `dim_site`, `fact_units` | `lake/gold/` | gold tf + dims |

```text
bronze artifacts
  → stg_plan / stg_scrape / stg_failures / stg_siteinfo   (silver endpoints)
  → tf_unit_status                                        (gold transform)
  → dim_site (from stg_plan) + fact_units (from tf + dim)
```

**Not used:** silver/`tf` after `stg_*`. There is no `stg → silver/tf → gold` chain.

Guidelines: [docs/GOLD_DEFAULT.md](../../docs/GOLD_DEFAULT.md).

## Run

```bash
cargo build -p rbt-datalake --release
./target/release/rbt validate -p examples/complex_bronze_landing --bronze-check fail
./target/release/rbt run -p examples/complex_bronze_landing --format parquet \
  --var domain=acme.com --var report_date=2026-07-29 --var run_id=r1
```

## Dim / fact notes

- `dim_site`: SK + Unknown (−1); natural grain domain×report_date from **stg_plan**
- `fact_units`: thin; `site_sk` via dim join; payload from gold `tf_unit_status`

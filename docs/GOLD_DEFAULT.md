# Gold default surface (P6)

How rbt supports **silver → gold** star-schema work: parts-aware sources, lineage stamps,
relationship tests, and multi-env roots. Complements [COMPLEX_BRONZE_AND_RUN_SCOPE.md](COMPLEX_BRONZE_AND_RUN_SCOPE.md)
and [P4_CAPABILITIES.md](P4_CAPABILITIES.md).

## 1. External parts directories as sources (G1)

Incremental silver often lands as:

```text
$lake/silver/stg_units.parts/
  part-0000000000001.parquet
  part-0000000000002.parquet
  _rbt_manifest.json
```

Gold models can **source** that directory (not only `ref()` same-project models):

```sql
---
description: Gold fact over multi-part silver stage
source_format: parquet
scan_path: $lake/silver/stg_units.parts
parts: true                    # optional; auto-detected via manifest / *.parts name
lineage_stamp: true
grain: [unit_id, report_date]
tests:
  not_null: [unit_id]
  unique: [unit_id, report_date]
  relationships:
    - column: site_id
      to_model: dim_site
      to_column: site_id
---
SELECT
  unit_id,
  site_id,
  report_date,
  amount
FROM {{ source('silver', 'stg_units_parts') }}
WHERE row_status = 'success'   -- completeness filter (G2): project-defined status column
```

| Behavior | Detail |
|----------|--------|
| Manifest present | File list + order from `_rbt_manifest.json` |
| No manifest | All `*.parquet` under dir (skip `_` names), sorted |
| Registration | DataFusion parquet listing on the directory |

Same layout is what `materialization: incremental_append` **writes**.

## 2. Completeness filters (G2)

rbt does **not** hardcode product status enums. Filter in SQL:

```sql
WHERE product_silver_status = 'complete'
-- or
WHERE row_status IN ('success')  -- with tests.accepted_values on silver
```

Use `accepted_values` tests on the silver model that defines the status column.

## 3. Lineage stamps (G8)

```yaml
lineage_stamp: true
```

On materialize, each output row gains:

| Column | Value |
|--------|--------|
| `_rbt_run_id` | Run id |
| `_rbt_contract_version` | Project / CLI contract version |
| `_rbt_model` | Model name |
| `_rbt_bronze_fingerprint` | Run bronze fingerprint (when available) |

Idempotent if columns already exist. Works for stream and collect parquet paths.

## 4. Tests: grain, unique, relationships (G6)

```yaml
grain: [ticker, bar_ts]
unique_key: [ticker, bar_ts]   # optional; grain used if unique omitted
tests:
  not_null: [ticker, bar_ts]
  unique: [ticker, bar_ts]     # composite unique
  accepted_values:
    side: [B, S]
  relationships:
    - column: ticker
      to_model: dim_ticker
      to_column: ticker
  fail_on_error: true
```

**Relationships** run **after** the model is registered for `ref()`. Parent models must be
ancestors (materialised earlier in the DAG). Orphans fail the run when `fail_on_error` is true.

## 5. Environment roots cookbook (G10)

```yaml
# rbt_project.yml
roots:
  nonprod: /mnt/datalake/acme/nonprod/lake_us/lake
  prod: /mnt/datalake/acme/prod/lake_us/lake

layers:
  staging:
    target_path: $nonprod/silver/stage
  marts:
    target_path: $nonprod/gold
```

Promote by swapping roots or CLI-owned env:

```bash
# Point the same project at prod lake roots via absolute overrides in a prod yml,
# or keep two project dirs / two roots blocks.
rbt run -p projects/gold_nonprod --var report_date=2026-07-29
```

See [MULTI_ROOT_AND_PATH_GLOB.md](MULTI_ROOT_AND_PATH_GLOB.md).

## 6. Selective rebuild

```bash
rbt run -p my_project -s dim_site+ --var report_date=2026-07-29
rbt run -p my_project -s fact_orders   # ancestors included on execute
```

## Related

- Dual-track plan P6 · gap matrix G1–G10  
- [ICEBERG_SOR.md](ICEBERG_SOR.md) · [REF_STRATEGY.md](REF_STRATEGY.md)

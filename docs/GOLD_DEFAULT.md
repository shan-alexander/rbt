# Gold default surface (P6+)

How rbt supports **silver stage → gold star** work, aligned with Kimball layer
placement ([star-schema-data-modeling-rules](concepts/star-schema-data-modeling-rules.md)).

Modeling is guidelines that keep lakes scalable and misstatement risk low — not
religion. When a consumer needs correct data, structure still matters for ops and
review. This doc is the **contract** for gold work in rbt.

---

## 1. Two axes: medallion paths vs model layers

| Axis | What it is | rbt knobs |
|------|------------|-----------|
| **Medallion (physical)** | Where files live on the lake | `layers.*.target_path` (`silver/stage`, `silver/tf`, `gold`, …) |
| **Kimball / rbt layers (logical)** | What role a model plays | name prefix → `stg_` / `tf_` / `dim_` / `fact_` / `obt_` |

```text
Bronze (external files)
    │  source() + scan frontmatter
    ▼
stg_*     silver/stage     technical landing, contracts, light types
    │  ref()
    ▼
tf_*      silver/tf        silver transforms (recon, cleanse, status) — OK and common
    │  ref()
    ▼
dim_*/fact_*/obt_*   gold    star marts (SK/NK dims, thin facts, OBT from star)

Optional later (same project or downstream mart):
  gold tf_*   only refs/sources silver **stage** (stg_*), not silver/tf
              then feeds gold dim/fact — for multi-mart or “gold prep only from stage”
```

### Rules of thumb (rbt)

| Do | Don’t |
|----|--------|
| Put multi-source outer joins, status, controlled dedup in **`tf_*`** | Put that logic on a **fact** |
| Keep **facts thin**: dim SK FKs + measures + flags already on the transform | `DISTINCT` a fan-out away on a dim or fact |
| Dims: surrogate key, natural key, attributes, **Unknown (−1)** | Dims as `SELECT DISTINCT` from stage with no SK |
| Fact FK: join dim on NK → take SK, `COALESCE(..., -1)` | NULL surrogate keys on facts |
| `source()` external **bronze or published stage/dim/fact endpoints** | `source()` an upstream project’s **`tf_*`** (private, unstable) |
| Silver/`tf_*` under `silver/tf` when it is still silver prep | Pretend every `tf_*` is “gold” just because it is relational |
| Gold transforms (if you use them): **`ref`/`source` only silver stage (`stg_*`)** | Gold transforms depending on silver/`tf_*` |

Silver transforms (`models/transforms` → `silver/tf`) are **allowed and correct** when
they prepare stage data for marts. Gold transforms (optional second prep band) should
**not** depend on silver transforms — only on silver stage (or published dim/fact
endpoints in multi-mart setups).

---

## 2. Recommended same-project DAG

```text
bronze files
  → stg_*          (scan contracts; optional on_missing: empty)
  → tf_*           (silver transforms: grain, recon, status)
  → dim_* / fact_* (gold: SK dims, thin facts, relationships on SKs)
  → obt_*          (from dim+fact only; separate from core star — P7+)
```

Cross-project / multi-mart:

```text
upstream published dim/fact  →  source() into this project’s stg_ or gold-prep tf_
                             →  this mart’s dim/fact/obt
Never: source() upstream tf_* or stg internals as a permanent contract.
```

---

## 3. External parts directories (engine capability)

Incremental silver may land as:

```text
$lake/silver/stage/stg_units.parts/
  part-*.parquet
  _rbt_manifest.json
```

**Prefer** registering parts on a **staging** model (or a silver transform that only
reads that stage), then `ref()` into transforms and marts:

```sql
---
# models/staging/stg_units.sql  — silver stage over published parts
source_format: parquet
scan_path: $lake/silver/stage/stg_units.parts
parts: true
grain: [unit_id, report_date]
tests:
  not_null: [unit_id]
  unique: [unit_id, report_date]
---
SELECT * FROM {{ source('silver', 'stg_units') }}
```

```sql
---
# models/transforms/tf_units_ready.sql  — silver tf: completeness filter, cleanse
---
SELECT * FROM {{ ref('stg_units') }}
WHERE row_status = 'success'   -- project-defined status; not hardcoded in rbt
```

```sql
---
# models/marts/fact_units.sql  — gold fact: thin, SK FKs
lineage_stamp: true
grain: [unit_id, report_date]
tests:
  relationships:
    - column: site_sk
      to_model: dim_site
      to_column: site_sk
---
SELECT
  COALESCE(d.site_sk, CAST(-1 AS BIGINT)) AS site_sk,
  t.unit_id,
  t.report_date,
  t.amount
FROM {{ ref('tf_units_ready') }} t
LEFT JOIN {{ ref('dim_site') }} d
  ON t.site_nk = d.site_nk AND COALESCE(d.is_unknown, false) = false
```

| Engine behavior | Detail |
|-----------------|--------|
| Manifest present | File list + order from `_rbt_manifest.json` |
| No manifest | `*.parquet` under dir (skip `_` names), sorted |
| `parts: true` on **marts** | Allowed but discouraged — `rbt validate` warns |

Same layout is written by `materialization: incremental_append`.

---

## 4. Completeness filters

Do **not** hardcode product status enums in the engine. Define status on **stage or
silver transform**, test with `accepted_values`, and let **gold facts carry flags
through**, not re-derive multi-source status.

```sql
-- on silver tf or stage
WHERE row_status IN ('success')
```

---

## 5. Lineage stamps (technical, not grain)

```yaml
lineage_stamp: true
```

| Column | Value |
|--------|--------|
| `_rbt_run_id` | Run id |
| `_rbt_contract_version` | Contract version |
| `_rbt_model` | Model name |
| `_rbt_bronze_fingerprint` | Run bronze fingerprint when available |

**Do not** put `_rbt_*` columns in business `grain` / `unique` keys.

---

## 6. Tests: grain, unique, relationships

```yaml
description: "one row per ticker per bar timestamp"
grain: [ticker, bar_ts]
unique_key: [ticker, bar_ts]   # optional if tests.unique covers grain
tests:
  not_null: [ticker, bar_ts]
  unique: [ticker, bar_ts]
  accepted_values:
    side: [B, S]
  relationships:
    - column: ticker_sk          # prefer SK, not natural key alone
      to_model: dim_ticker
      to_column: ticker_sk
  fail_on_error: true
```

- **Grain** must be one sentence in `description` (human + agents).
- **Relationships** run after the child model is registered; parents must be
  ancestors (dims before facts). Prefer **SK → SK**.
- Unknown member: include `site_sk = -1` (or project convention) on every dim;
  facts use `COALESCE(fk, -1)`.

`rbt validate` emits **warnings** when grain is set without a matching unique
contract, when marts use `parts: true`, or when `source()` targets a `tf_*` /
`int_*` name (upstream transform smell).

---

## 7. Environment roots

```yaml
roots:
  nonprod: /mnt/datalake/acme/nonprod/lake_us/lake
  prod: /mnt/datalake/acme/prod/lake_us/lake
layers:
  staging:
    target_path: $nonprod/silver/stage
  transforms:
    target_path: $nonprod/silver/tf
  marts:
    target_path: $nonprod/gold
```

See [MULTI_ROOT_AND_PATH_GLOB.md](MULTI_ROOT_AND_PATH_GLOB.md).

---

## 8. Selective rebuild

```bash
rbt run -p my_project -s dim_site+ --var report_date=2026-07-29
rbt run -p my_project -s fact_orders   # ancestors included on execute
```

---

## 9. Example

[examples/complex_bronze_landing](../examples/complex_bronze_landing/): multi-artifact
bronze → silver stage → silver `tf_unit_status` → gold `dim_site` (SK + Unknown) +
`fact_units` (thin fact, SK FK, relationship on `site_sk`).

---

## 10. Roadmap touchpoints

| Item | Status |
|------|--------|
| Parts / lineage / relationships | Shipped (0.7.x) |
| SCD2 dimensions + merge | **P7** (Type-1 snapshots only until then) |
| First-class OBT layer rules | **P7** |
| REST / remote Iceberg SoR | P8 |

---

## Related

- [concepts/star-schema-data-modeling-rules.md](concepts/star-schema-data-modeling-rules.md)
- [COMPLEX_BRONZE_AND_RUN_SCOPE.md](COMPLEX_BRONZE_AND_RUN_SCOPE.md)
- [ADR_001_PROJECT_STRUCTURE.md](adr/ADR_001_PROJECT_STRUCTURE.md)
- [ICEBERG_SOR.md](ICEBERG_SOR.md)

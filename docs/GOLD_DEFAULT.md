# Medallion + gold star topology (P6+)

rbt models follow a **silver endpoint / gold construction** topology. This matches
Kimball layering ([star-schema-data-modeling-rules](concepts/star-schema-data-modeling-rules.md))
and keeps complexity scalable.

---

## 1. Canonical flow

```text
Bronze (external files)
        │  source() + scan frontmatter
        ▼
┌─────────────────────── SILVER ───────────────────────┐
│  optional silver prep transforms (tf_*)                │
│     • ref bronze (source) and/or other silver tf_*     │
│     • never ref stg_*                                  │
│                        │                               │
│                        ▼                               │
│  stg_*  ──────── SILVER ENDPOINTS ────────             │
│     • 1:1 bronze→stg when logic is simple              │
│     • or ref(tf_base_*) when prep is shared/complex    │
│     • never feed another silver tf_* after stg_*       │
└────────────────────────┬──────────────────────────────┘
                         │  ref(stg_*) only
                         ▼
┌─────────────────────── GOLD ─────────────────────────┐
│  gold transforms (tf_*)                                │
│     • ref only silver stage endpoints (stg_*)          │
│     • never ref silver tf_* or other gold bands wrongly│
│     • physical path typically $lake/gold/tf            │
│                        │                               │
│                        ▼                               │
│  dim_* / fact_* / obt_*  ── GOLD ENDPOINTS             │
│     • thin facts (SK FKs + measures + carried flags)   │
│     • dims: SK + NK + attributes + Unknown (−1)        │
│     • obt from dim+fact only (P7)                      │
└────────────────────────────────────────────────────────┘
```

### Never

| Anti-pattern | Why |
|--------------|-----|
| `stg_*` → silver `tf_*` → gold | **`stg_*` is the silver endpoint**; do not hang more silver tf after it |
| `stg_*` → silver `tf_*` → gold `tf_*` | Same; gold tf should read **stg_** only |
| `source()` an upstream project’s `tf_*` | Private, unstable; use published stg/dim/fact contracts |
| Gold `tf_*` that refs both `stg_*` and `tf_*` | Engine error `E_RBT_LAYER_TRANSFORM_BAND` |

### When to use a silver prep transform

| Situation | Prefer |
|-----------|--------|
| Bronze → stage is **1:1**, light cleanse | Logic **in `stg_*`** (no intermediate tf) |
| Prep **reused** by multiple stg tables, or heavy multi-bronze collapse | `tf_base_*` (silver) → `stg_*` |
| Multi-stg recon / status for **gold** | Gold `tf_*` that `ref`s **stg_*** only |

---

## 2. Project layout (recommended)

```yaml
layers:
  staging:
    path: models/staging
    target_path: $lake/silver/stage      # silver endpoints
    default_format: parquet
  transforms:
    path: models/transforms
    target_path: $lake/gold/tf           # gold transforms (ref stg_* only)
    default_format: parquet
  # Optional: silver prep before stage (ref bronze / other silver tf only)
  # silver_transforms:
  #   path: models/silver_transforms
  #   target_path: $lake/silver/tf
  marts:
    path: models/marts
    target_path: $lake/gold              # dim / fact / obt
    default_format: parquet
```

`models/transforms` is the **gold transform** band in the default examples. Put rare
pre-stage silver prep under a separate directory/config when you need bronze→tf→stg.

---

## 3. Engine enforcement

| Rule | Enforcement |
|------|-------------|
| `tf_*` cannot ref `dim_`/`fact_`/`obt_` | Hard error (layer boundary) |
| `stg_*` cannot ref marts or other `stg_*` | Hard error |
| `stg_*` **may** ref `tf_*` (silver prep → stage endpoint) | Allowed |
| One `tf_*` cannot ref **both** `stg_*` and `tf_*` | Hard error `E_RBT_LAYER_TRANSFORM_BAND` |
| `source(..., 'tf_*')` | Validate warning `W_RBT_SOURCE_UPSTREAM_TRANSFORM` |
| Mart with scan/parts contract | Validate warning `W_RBT_MART_SCAN_CONTRACT` |

---

## 4. Parts, lineage, tests (engine capabilities)

### External multi-part silver endpoints

Prefer scan on **staging** (silver endpoint), then gold tf / marts `ref` that stg:

```sql
---
# models/staging/stg_units.sql
source_format: parquet
scan_path: $lake/silver/stage/stg_units.parts
parts: true
---
SELECT * FROM {{ source('silver', 'stg_units') }}
```

```sql
---
# models/transforms/tf_units_ready.sql  — gold transform
---
SELECT * FROM {{ ref('stg_units') }}
WHERE row_status = 'success'
```

### Lineage stamps

`lineage_stamp: true` adds `_rbt_run_id`, `_rbt_contract_version`, `_rbt_model`,
`_rbt_bronze_fingerprint`. Keep out of business grain.

### Relationships

Prefer **SK → SK** on facts after Unknown-aware dim join (`COALESCE(fk, -1)`).

---

## 5. Examples

| Project | Topology |
|---------|----------|
| [complex_bronze_landing](../examples/complex_bronze_landing/) | Research papers mini-lake → `stg_*` → gold `tf_paper_status` / dims / facts |
| [smoke_fixture](../examples/smoke_fixture/) | bronze → `stg_trades` → gold `tf_ticker_stats` → `dim_ticker` |
| [full_e2e_rbt_example](../examples/full_e2e_rbt_example/) | bronze → `stg_ohlcv_*` → gold `tf_*` → dim/fact/obt |

---

## 6. Env roots & select

See [MULTI_ROOT_AND_PATH_GLOB.md](MULTI_ROOT_AND_PATH_GLOB.md).  
Selective rebuild: `rbt run -s fact_units` includes ancestors.

---

## 7. Roadmap

| Item | Status |
|------|--------|
| Topology + band enforcement | 0.7.2+ |
| SCD2 dims + merge | **P7** |
| OBT layer rules | **P7** |
| Remote Iceberg SoR | P8 |

## Related

- [concepts/star-schema-data-modeling-rules.md](concepts/star-schema-data-modeling-rules.md)
- [COMPLEX_BRONZE_AND_RUN_SCOPE.md](COMPLEX_BRONZE_AND_RUN_SCOPE.md)
- [adr/ADR_001_PROJECT_STRUCTURE.md](adr/ADR_001_PROJECT_STRUCTURE.md)

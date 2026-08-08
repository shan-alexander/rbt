# P4 capabilities (0.5.0)

Thesis-aligned features beyond the SQL spine: **measure packs**, **incremental append**,
**honest WAP**, and **in-process UDFs**. Multi-catalog sprawl and Rust model plugins remain deferred.

## 1. Built-in UDFs (Design A)

Registered on every engine:

| SQL name | Behavior |
|----------|----------|
| `rbt_upper(s)` | uppercase Utf8 |
| `rbt_lower(s)` | lowercase |
| `rbt_trim(s)` | trim whitespace |
| `rbt_nullif_empty(s)` | empty string → NULL |

```sql
SELECT rbt_upper(ticker) AS ticker_u FROM {{ ref('stg_trades') }}
```

Project-specific UDFs: register with `register_scalar_udf` before `execute_dag` (library hosts).

## 2. Incremental append

Frontmatter:

```yaml
materialization: incremental_append   # or append | incremental
```

| Full refresh (`table`) | Incremental append |
|------------------------|--------------------|
| Overwrites `model.parquet` | Adds `model.parts/part-*.parquet` |
| Single file for `ref()` | `ref()` lists the **parts directory** |
| Default | Opt-in |

Manifest: `model.parts/_rbt_manifest.json` (`parts`, `total_rows`, `updated_at_ms`).

**Not shipped:** `incremental_merge` (row-level MERGE) — fails with `E_RBT_INCREMENTAL`.

### Scoped replace (RBT-A2)

`materialization: scoped_replace` writes `part-{scope_id}.parquet` keyed by run vars /
`part_key`; re-runs replace that part only (peers kept). See
[COMPLEX_BRONZE_AND_RUN_SCOPE.md](COMPLEX_BRONZE_AND_RUN_SCOPE.md).

### Keyed upsert (RBT-A7)

```yaml
materialization: keyed_upsert
# unique_key defaults to grain when omitted
grain: [entity_id]
touch_columns: [last_seen_at]
compare_columns: [a, b]   # optional
```

Entity-key merge: insert / touch-only / full non-key update / **keep peers**.  
Not Type-1-only. Pattern: stage log → tf latest-per-key candidates → dim keyed_upsert.  
Duplicate candidates fail closed. See playbook `examples/entity_registry` and
[COMPLEX_BRONZE_AND_RUN_SCOPE.md](COMPLEX_BRONZE_AND_RUN_SCOPE.md).

### Consolidate policy (RBT-A5)

```yaml
materialize:
  consolidate: auto    # never | always | auto
```

- **`auto`:** table → single file; incremental/scoped_replace → parts only.
- **`never`:** table also publishes under `.parts/` only (`part-full.parquet`).
- **`always`:** after parts writes, rebuild a convenience monolith parquet.

Ops: `rbt consolidate -s <model>` merges parts → single file without deleting parts.

## 3. Write-Audit-Publish (WAP)

```yaml
materialize:
  wap: true
```

1. Write stream output to `.wap/{run_id}/{model}.parquet`
2. Assertions already applied on the stream (fail → no publish)
3. Atomic rename to production dest; write `.wap/{run_id}/{model}.audit.json`

Production dest is left unchanged on audit failure. This is **filesystem WAP**, not Iceberg branch APIs.

## 4. Measure packs

```bash
rbt measure -p examples/smoke_fixture --scenario smoke_pipeline
rbt measure -p examples/smoke_fixture --scenario validate_dx --json
rbt measure -p examples/smoke_fixture --scenario incremental_append
```

Report fields: `wall_ms`, `models_executed`, `total_rows`, `peak_rss_kb` (Linux VmRSS), `notes`, `ok`.

Public performance claims require checked-in scenarios + reports — see thesis §7.

## 5. Explicitly out of P4

| Item | Status |
|------|--------|
| Multi REST/Glue catalogs | Use `RbtEngineBuilder::with_catalog` (library); no multi-backend sprawl in core |
| First-class Rust models (Design B) | ADR-003 later |
| Dynamic `cdylib` UDF load | Later |
| Spark comparison packs | Need external Spark; not in-repo default CI |

## Related

- [ADR_003_UDF_RSMODELS.md](adr/ADR_003_UDF_RSMODELS.md)
- [ICEBERG_SOR.md](ICEBERG_SOR.md)
- [REF_STRATEGY.md](REF_STRATEGY.md)

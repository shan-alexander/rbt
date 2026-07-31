# Multi-root lakes, absolute paths, and `path_glob` (P0)

These features exist so rbt can target real messy lakes (e.g. `/mnt/datalake/kinnalake`)
without copying data into the git project tree.

## Absolute paths

`scan_path` and layer `target_path` may be **absolute**. Absolute paths are never
joined under the project directory.

```yaml
# rbt_project.yml
name: kinna_gold
version: "0.1.0"
models_dir: models
target_path: /mnt/datalake/kinnalake/nonprod/lake_us/lake/gold
layers:
  staging:
    path: models/staging
    target_path: /mnt/datalake/kinnalake/nonprod/lake_us/lake/silver/stage_rbt
    default_format: parquet
  transforms:
    path: models/transforms
    target_path: /mnt/datalake/kinnalake/nonprod/lake_us/lake/silver/tf_rbt
    default_format: parquet
  marts:
    path: models/marts
    target_path: /mnt/datalake/kinnalake/nonprod/lake_us/lake/gold
    default_format: parquet
```

## Named roots (`roots:`)

Avoid repeating long prefixes:

```yaml
roots:
  nonprod_lake: /mnt/datalake/kinnalake/nonprod/lake_us/lake
  prod_lake: /mnt/datalake/kinnalake/prod/lake_us/lake

layers:
  staging:
    path: models/staging
    target_path: $nonprod_lake/silver/stage_rbt
    default_format: parquet
  marts:
    path: models/marts
    target_path: ${nonprod_lake}/gold
    default_format: parquet
```

Templates: `$name` or `${name}`. Unknown names fail with `E_RBT_ROOT_UNKNOWN`.

Frontmatter `scan_path` expands the same way:

```sql
---
source_format: parquet
scan_path: $nonprod_lake/lz/kinnaruns
path_glob: "**/raw_snoop/crawlplan.parquet"
partition_by: [domain, report_date, run_id]
require_partitions:
  report_date: "2026-07-29"
inject_source_path: true
---
SELECT * FROM {{ source('lz', 'crawlplan') }}
```

## `path_glob` (artifact isolation)

Kinna-style bronze trees mix many filenames under the same hive path. Use
`path_glob` so each staging model reads **one artifact type**.

| Field | Semantics |
|-------|-----------|
| omitted / empty | all files matching `source_format` |
| string | single pattern |
| list | **OR** — file matches if any pattern matches |

Patterns use the [`glob`](https://docs.rs/glob) crate syntax. Matching is tried
against:

1. path relative to `scan_path` (POSIX `/` separators),
2. file basename only,
3. absolute path string.

Examples:

```yaml
path_glob: crawlplan.parquet
path_glob: "**/raw_snoop/crawlplan.parquet"
path_glob:
  - "**/enriched_scrape.parquet"
  - "**/store_summary.parquet"
```

When `path_glob`, hive partitions, or `inject_source_path` is set, bronze uses
the **scan → MemTable** path (not raw DataFusion listing), so filters apply.

## Protobuf bronze (`source_format: protobuf`)

`.pb` files are supported as **opaque** bronze: one row per file with columns

| Column | Type | Meaning |
|--------|------|---------|
| `_source_path` | Utf8 | absolute path |
| `payload` | Binary | raw file bytes |
| `payload_len` | Int64 | byte length |

Typed protobuf decode (prost + schema) is **not** in this release; land opaque
payloads into silver, decode later via Rust models / schema registry.

## Kinna `stage-append` vs rbt (design note)

From nonprod receipts, `kinnastage stage-append` roughly:

1. Discover domains for `(report_date, run_id)`.
2. Write **part files** under `silver/stage/stg_*/parts/part-{date}-{run}-*.parquet`.
3. Update domain-keyed crawlplan parts.
4. Consolidate into `stg_*.parquet` + `_manifest.json`.
5. Mode **append** / partial-run contracts.

rbt today is **full-refresh model files** (one parquet per model name) with
optional lake re-read for `ref()`. That can **own bronze→silver** for tabular
lz parquet/jsonl via:

- one staging model per artifact (`path_glob` + hive partitions),
- SQL typing/dedupe/tests,
- write to a dedicated silver root (do not overwrite Kinna’s live `stage/` until cutover).

What rbt does **not** yet mirror (P1+):

- true **append** part layout + `_manifest.json` contracts,
- directory-of-parts as first-class sources for gold,
- CAS `.zst` HTML (stay outside rbt),
- typed protobuf messages.

Recommended cutover path: rbt writes `silver/stage_rbt/` and `gold/` in parallel
to Kinna; flip readers when DAGs match.

## Related

- [REF_STRATEGY.md](REF_STRATEGY.md) — MemTable vs lake re-read
- [STREAMING_MATERIALIZE_PLAN.md](STREAMING_MATERIALIZE_PLAN.md) — large-run memory

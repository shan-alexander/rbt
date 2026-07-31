# Multi-root lakes, absolute paths, and `path_glob`

These features let rbt target multi-environment filesystem lakes without copying
data into the git project tree.

## Absolute paths

`scan_path` and layer `target_path` may be **absolute**. Absolute paths are never
joined under the project directory. Resolution failures return structured errors
(`E_RBT_LAYER_PATH`, `E_RBT_MODEL_TARGET`, `E_RBT_ROOT_UNKNOWN`, …).

```yaml
# rbt_project.yml
name: multi_root_demo
version: "0.1.0"
models_dir: models
target_path: /mnt/datalake/acme/nonprod/lake_us/lake/gold
layers:
  staging:
    path: models/staging
    target_path: /mnt/datalake/acme/nonprod/lake_us/lake/silver/stage
    default_format: parquet
  transforms:
    path: models/transforms
    target_path: /mnt/datalake/acme/nonprod/lake_us/lake/silver/tf
    default_format: parquet
  marts:
    path: models/marts
    target_path: /mnt/datalake/acme/nonprod/lake_us/lake/gold
    default_format: parquet
```

## Named roots (`roots:`)

Avoid repeating long prefixes:

```yaml
roots:
  nonprod_lake: /mnt/datalake/acme/nonprod/lake_us/lake
  prod_lake: /mnt/datalake/acme/prod/lake_us/lake

layers:
  staging:
    path: models/staging
    target_path: $nonprod_lake/silver/stage
    default_format: parquet
  marts:
    path: models/marts
    target_path: ${nonprod_lake}/gold
    default_format: parquet
```

Templates: `$name` or `${name}`. Unknown names fail with **`E_RBT_ROOT_UNKNOWN`**
and list known roots.

Frontmatter `scan_path` expands the same way (project config is loaded once per engine
run and reused for every bronze model).

```sql
---
source_format: parquet
scan_path: $nonprod_lake/lz/runs
path_glob: "**/raw_snoop/crawlplan.parquet"
partition_by: [domain, report_date, run_id]
require_partitions:
  report_date: "2026-07-29"
inject_source_path: true
---
SELECT * FROM {{ source('lz', 'crawlplan') }}
```

## `path_glob` (artifact isolation)

Hive trees often mix many filenames under the same partition path. Use `path_glob`
so each staging model reads **one artifact type**.

| Field | Semantics |
|-------|-----------|
| omitted / empty | all files matching `source_format` |
| string | single pattern |
| list | **OR** — file matches if any pattern matches |

Patterns use **globset** with **literal path separators** (strong semantics):

| Token | Meaning |
|-------|---------|
| `*` | single path segment only (does **not** cross `/`) |
| `?` | single character within a segment |
| `**` | zero or more path segments (recursive) |
| `[abc]` | character class |

This matches gitignore-style expectations: `*/crawlplan.parquet` matches one directory
level under `scan_path`, not a deep hive tree. Use `**/crawlplan.parquet` for any depth.

### Match candidates

A file matches if **any** pattern matches **any** applicable candidate:

1. **Relative path** — always tried (path of the file relative to `scan_path`, POSIX `/`).
2. **Basename** — tried when the set includes a pattern with **no** `/` (e.g.
   `crawlplan.parquet`), so bare filenames match at any depth under `scan_path`.
3. **Absolute path** — tried when a pattern starts with `/`.

Examples:

```yaml
path_glob: crawlplan.parquet                    # basename at any depth
path_glob: "**/raw_snoop/crawlplan.parquet"     # recursive under scan_path
path_glob: "*/crawlplan.parquet"                # exactly one segment above the file
path_glob:
  - "**/enriched_scrape.parquet"
  - "**/store_summary.parquet"
```

Invalid / empty patterns fail at DAG build / scan setup with **`E_RBT_PATH_GLOB_INVALID`**.

### DataFusion listing pushdown

When **any** of the following are set, bronze registration uses the **scan→MemTable**
path and **does not** use DataFusion directory listing / listing-table predicate
pushdown for that source:

- non-empty `path_glob`
- `partition_by` / `require_partitions`
- `inject_source_path: true`
- `force_scan: true`
- formats that require scan (log, txt, toml, arrow_ipc stream, protobuf)

**`path_glob` disables DF listing pushdown for that bronze source** (by design):
listing providers cannot apply rbt filename globs or hive path injection. Filters
and injection stay correct on the scan path; large directory walks are still bounded
by globs + `require_partitions`. Prefer a narrow `scan_path` + globs over pointing
`scan_path` at an entire lake root.

## Protobuf bronze (`source_format: protobuf`)

`.pb` files land as **opaque** bronze: one row per file.

| Column | Type | Meaning |
|--------|------|---------|
| `_source_path` | Utf8 | absolute path |
| `payload` | Binary | raw file bytes |
| `payload_len` | Int64 | byte length |

### Payload size limit

Default max size for a single protobuf file: **1 GiB**
(`1024 * 1024 * 1024` bytes).

### Arrow IPC bronze spill (0.3.9+)

Hive Arrow IPC trees use the scan path (globs / partitions). By default rbt
**spills file-by-file to Parquet** under `.rbt/bronze_spill/` then registers a
listing table — peak RAM is roughly one IPC file + encoder, not the full tree.

```yaml
scan:
  # optional; default is 1073741824 (1 GiB)
  protobuf_max_payload_bytes: 1073741824
  spill_arrow_ipc: true              # default
  spill_dir: .rbt/bronze_spill       # default; may use $roots
```

Set `spill_arrow_ipc: false` only for tiny trees or to force the legacy MemTable path.

Oversized files fail with **`E_RBT_PROTOBUF_TOO_LARGE`** and name the config key to raise.

Typed protobuf decode (message schemas) is a later capability (Rust models / registry).

## Append-style silver vs rbt full refresh

Some external pipelines write **part files** under `parts/` plus a consolidated table
and a `_manifest.json`. rbt today materializes **full-refresh** model files
(one parquet per model name) with lake re-read for `ref()`.

To own bronze→silver for tabular landing zones:

- one staging model per artifact (`path_glob` + hive partitions),
- SQL typing / dedupe / tests,
- write to a dedicated silver root (do not overwrite another tool’s live paths until cutover).

Not yet in rbt (later): true append part layout, directory-of-parts sources, content-addressed blob stores as tables.

## Related

- [REF_STRATEGY.md](REF_STRATEGY.md) — MemTable vs lake re-read
- [STREAMING_MATERIALIZE_PLAN.md](STREAMING_MATERIALIZE_PLAN.md) — large-run memory

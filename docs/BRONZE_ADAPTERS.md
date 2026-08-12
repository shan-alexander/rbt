---
tags: [docs, bronze, adapters, a10, guide]
node_type: concept
aliases: [bronze adapters guide, how to add bronze format]
---
# Bronze adapters guide (RBT-A10)

How rbt turns heterogeneous bronze files into Arrow for silver SQL.

Related: [[Bronze adapter matrix]] · [[Complex bronze landing zones]] · [[ADR-002 Thesis Alignment]]

## Mental model

```text
scan_path + source_format + path_glob / hive
        │
        ▼
  LakeScanner::list_files   (filters)
        │
        ▼
  BronzeAdapter::read_file  (decode one file → RecordBatch[])
        │
        ▼
  inject hive + _source_path
        │
        ▼
  MemTable | DF listing | spill parquet  →  SQL models (stg_*)
```

## Frontmatter (staging)

```yaml
---
scan_path: bronze/web
source_format: html          # or omit to infer from extension
path_glob: ["**/*.html"]
partition_by: [entity_id]
on_missing: empty
inject_source_path: true
---
SELECT _source_path, html
FROM {{ source('bronze', 'pages') }}
```

### Format cheat sheet

| `source_format` | Row shape | Use for |
|-----------------|-----------|---------|
| `jsonl` / `json` | Object fields (or jshift paths) | Events, API dumps |
| `parquet` / `csv` | Tabular | Already columnar landings |
| `arrow_ipc` / `arrow_ipc_stream` | Arrow schema | High-throughput WAL |
| `log` / `txt` | `line_no`, `content` | Line dumps |
| `html` | `_source_path`, `html`, `byte_len` | HTML landings → SQL regexp |
| `xml` | `_source_path`, `xml`, `byte_len` | Opaque XML; prefer JSONL if structured |
| `robots` | `_source_path`, `body`, `byte_len` | robots.txt whole file |
| `protobuf` | `_source_path`, `payload`, `payload_len` | Opaque PB; decode outside SQL |
| `toml` | table rows | Config-as-data |

**XML note:** rbt does not ship XPath projection. Pre-normalize complex XML to JSONL in the lander, or use whole-file `xml` + SQL/UDF.

**Protobuf note:** typed decode is host/schema-registry territory (Design A UDFs or Design B Rust models). Bronze stays opaque binary-safe.

## Silver roles (not gold)

| Prefix | Role | Layer |
|--------|------|-------|
| `stg_*` | stage | Staging |
| `ref_*` / `lkp_*` | lookup | Staging |
| `seed_*` | seed | Staging |
| `tf_*` / `int_*` | transform | Transform |
| `dim_*` / `fact_*` / `obt_*` | mart | Mart |

Do **not** force dim/fact into silver. Gold owns star contracts.

Library: `ModelRole::from_name("stg_events")`.

## Multi-file order + `_ingest_seq` (A10.13)

Directory scans **sort** files before decode (stable total order):

| `scan_order` | Order |
|--------------|--------|
| `path` (default) | Lexicographic relative path under `scan_path` |
| `mtime` | mtime ascending, then path (mtime alone is not portable) |

Optional inject columns (frontmatter):

| Flag | Column | Type |
|------|--------|------|
| `inject_source_path: true` | `_source_path` | Utf8 absolute path |
| `inject_ingest_seq: true` | `_ingest_seq` | Int64 `0..n-1` after sort |
| `inject_source_mtime: true` | `_source_mtime` | Int64 unix seconds |

**Last-wins SQL** (product-neutral):

```sql
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY grain_key
      ORDER BY _ingest_seq DESC
    ) AS rn
  FROM {{ source('bronze', 'events') }}
) t WHERE rn = 1
```

Any non-empty inject / `adapter` / path filters forces the scan→MemTable path (not DF listing).

## Host adapters (A10.12) — no fork

**Override a builtin format** (host wins for that `SourceFormat`):

```rust
use rbt::{register_host_adapter, BronzeAdapter, clear_host_adapters};
// impl BronzeAdapter for MyArrowIpc { ... format() -> ArrowIpc ... }
// register_host_adapter(Arc::new(MyArrowIpc))?;
```

**Named proprietary format** (no enum variant):

```rust
use rbt::{register_named_adapter, NamedBronzeAdapter};
// impl NamedBronzeAdapter for HostTicks { fn name(&self) -> &str { "host_ticks" } ... }
// register_named_adapter(Arc::new(HostTicks))?;
```

```yaml
# model frontmatter
scan_path: bronze/ticks
adapter: host_ticks          # must be registered before run
# source_format optional when adapter is set (still useful for docs)
inject_ingest_seq: true
```

Unregistered `adapter:` / unknown `source_format` → **`E_RBT_SOURCE_FORMAT`**.  
Duplicate host registration → **`E_RBT_ADAPTER_DUP`**.  
`clear_host_adapters()` for tests / process reconfiguration.

## Add a **builtin** format in one PR

1. Add `SourceFormat` variant + `parse` / `from_extension` / `as_str` / `ALL`.
2. Implement `BronzeAdapter` in `scan/adapter.rs` (or reuse whole-file UTF-8 helper).
3. Register in `builtin_adapters()`.
4. Unit test under `scan/adapter` or `scan` tests.
5. Row in [[Bronze adapter matrix]].

Prefer **host named adapters** for proprietary formats that should not ship in rbt.

Unknown formats must fail with `E_RBT_SOURCE_FORMAT` — never silent fallback.

## Library usage

```rust
use rbt::{LakeScanner, ScanRequest, SourceFormat, StagingFrontmatter};

// or decode one file:
use rbt::scan::adapter::read_with_adapter;
```

## Spill (A10.9)

Large Arrow IPC trees: set `scan.spill_arrow_ipc: true` in `rbt_project.yml` so bronze registration spills file-by-file to temp Parquet (bounded RAM) instead of one giant MemTable.

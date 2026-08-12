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

## Add a format in one PR

1. Add `SourceFormat` variant + `parse` / `from_extension` / `as_str` / `ALL`.
2. Implement `BronzeAdapter` in `scan/adapter.rs` (or reuse whole-file UTF-8 helper).
3. Register in `builtin_adapters()`.
4. If format needs MemTable path, add to `needs_scan_path` match in `engine/bronze.rs`.
5. Unit test under `scan/adapter` or `scan` tests.
6. Row in [[Bronze adapter matrix]].

Unknown formats must fail with `E_RBT_SOURCE_FORMAT` — never silent fallback.

## Library usage

```rust
use rbt::{LakeScanner, ScanRequest, SourceFormat, StagingFrontmatter};

// or decode one file:
use rbt::scan::adapter::read_with_adapter;
```

## Spill (A10.9)

Large Arrow IPC trees: set `scan.spill_arrow_ipc: true` in `rbt_project.yml` so bronze registration spills file-by-file to temp Parquet (bounded RAM) instead of one giant MemTable.

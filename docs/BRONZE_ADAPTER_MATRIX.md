---
tags: [docs, bronze, adapters, a10]
node_type: concept
aliases: [bronze adapter matrix, A10.1]
---
# Bronze adapter matrix (RBT-A10.1)

**Status:** truthful as of A10 implementation on `feat/l1-embeddable-library` branch work.  
**Code:** `crates/rbt/src/scan/adapter.rs` · [`SourceFormat`](../crates/rbt/src/core/frontmatter.rs)

Legend: **Done** = production path + tests · **Partial** = works with caveats · **Doc** = intentional deferral

| Format | List by ext | Scan→batches | Hive inject | `on_missing: empty` | Declared dtype empty | Spill path | Notes |
|--------|-------------|--------------|-------------|---------------------|----------------------|------------|-------|
| `parquet` | Done | Done (DF list or scan) | Done | Done | Done | n/a | Prefer DF listing |
| `csv` | Done | Done | Done | Done | Done | n/a | Prefer DF listing |
| `jsonl` / `ndjson` | Done | Done | Done | Done | Done | n/a | jshift `paths:` → MemTable |
| `json` | Done | Done | Done | Done | Done | n/a | Array → JSONL expand when jshift |
| `arrow_ipc` | Done | Done | Done | Done | Partial | Done | Spill when `scan.spill_arrow_ipc` |
| `arrow_ipc_stream` | Done | Done | Done | Done | Partial | Done | WAL-style |
| `log` | Done | Done (line_no, content) | Done | Done | Partial | n/a | Line-oriented |
| `txt` | Done | Done (line_no, content) | Done | Done | Partial | n/a | Line-oriented |
| `toml` | Done | Done | Done | Done | Partial | n/a | `toml_rows_key` |
| `protobuf` | Done | Done (path, payload, len) | Done | Done | Partial | n/a | Opaque; schema registry out of band |
| `html` | Done | Done (path, html, byte_len) | Done | Done | Partial | n/a | **Not** a browser/DOM |
| `xml` | Done | Done (path, xml, byte_len) | Done | Done | Partial | n/a | Structure: pre-normalize to JSONL |
| `robots` | Done | Done (path, body, byte_len) | Done | Done | Partial | n/a | Whole-file; use explicit `source_format: robots` for `.txt` |

## Path selection (bronze registration)

| Condition | Path |
|-----------|------|
| format prefers DF listing **and** no path_glob / partition inject / force_scan | **Path A** DataFusion listing |
| else | **Path B** scan → MemTable (or spill→Parquet for Arrow IPC) |

## Fail-closed

Unknown `source_format` string → `E_RBT_SOURCE_FORMAT`.  
Missing adapter for a format enum → same code (registry exhaustiveness tests prevent drift).

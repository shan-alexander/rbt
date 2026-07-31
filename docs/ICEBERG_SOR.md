# Iceberg system of record (P2)

## Proof gate (0.4.0)

For `--format iceberg`, rbt’s default path is:

```text
MemoryCatalog (LocalFs warehouse)
  → create_namespace / drop_table (full refresh)
  → create_table (official iceberg Schema)
  → DataFileWriter (Parquet data files)
  → Transaction::fast_append().add_data_files()
  → commit(catalog)
  → load_table + scan (row-count check)
```

This is the CONTRIBUTING proof gate: **create → write data files → commit snapshot → read back** via the official Rust `iceberg` crate.

## What is real

| Claim | Status |
|-------|--------|
| Snapshot commit through `Transaction::commit` | **Yes** |
| Durable metadata + data on local filesystem | **Yes** (LocalFsStorageFactory) |
| Manifest / snap Avro from iceberg writers | **Yes** |
| `ref()` can re-read data parquet after write | **Yes** (`.rbt_iceberg_data` hint) |
| Multi-writer REST / Glue / HMS OCC | **No** — later catalog choice |
| Time travel CLI | **No** |

## Config

```yaml
materialize:
  iceberg:
    mode: catalog     # default (SoR)
    # mode: filesystem  # hand-rolled data/ + metadata/vN.json (demos / dual-write sidecar)
    namespace: rbt
```

Dual-write `parquet-and-iceberg` still uses the **filesystem sidecar** layout next to the flat parquet file (not a second catalog commit).

## Related

- [CONTRIBUTING.md](../CONTRIBUTING.md) § Iceberg SoR decision rule  
- Hand-rolled FS layout remains available for demos and dual-write  

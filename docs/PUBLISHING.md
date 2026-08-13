# Publishing

## Unified package

```bash
cargo test -p rbt-datalake --lib
cargo build -p rbt-datalake --release
bash scripts/smoke.sh
git commit  # cargo publish refuses dirty trees by default
cargo publish -p rbt-datalake
```

Package name on crates.io: **`rbt-datalake`**. Binary: **`rbt`**. Lib import: **`rbt::`**.

### Cargo features (RBT-L1.1 / ADR-004)

| Feature | Default | Notes |
|---------|---------|--------|
| `sql` | yes | Marker (DataFusion always linked) |
| `parquet` | yes | Marker (Parquet materialize always linked) |
| `jshift` | yes | Selective JSON path extract |
| `iceberg` | yes | Iceberg **catalog** crate (+ DF provider) |
| `cli` | yes | Binary `rbt` (required-features) |

Embed without Iceberg / CLI:

```toml
rbt-datalake = { version = "0.10", default-features = false, features = ["sql", "parquet"] }
```

**Single-ABI:** in dag-enabled crates use only `rbt::arrow` / `rbt::parquet` / `rbt::datafusion`.  
See [EMBEDDING.md](EMBEDDING.md).

Link: https://crates.io/crates/rbt-datalake

## Orphan deprecation stubs (0.0.4)

`rbt-testing`, `rbt-json`, `rbt-core`, `rbt-scan`, `rbt-materializer`, `rbt-engine` — empty stubs; README + description → **https://crates.io/crates/rbt-datalake**.

See [CRATES_IO.md](CRATES_IO.md).

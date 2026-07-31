# Publishing

## Unified package

```bash
cargo test -p rbt-datalake --lib
cargo build -p rbt-datalake --release
bash scripts/smoke.sh
git commit  # cargo publish refuses dirty trees by default
cargo publish -p rbt-datalake
```

Package name on crates.io: **`rbt-datalake`** (currently **0.3.6**). Binary: **`rbt`**. Lib import: **`rbt::`**.

Link: https://crates.io/crates/rbt-datalake

## Orphan deprecation stubs (0.0.4)

`rbt-testing`, `rbt-json`, `rbt-core`, `rbt-scan`, `rbt-materializer`, `rbt-engine` — empty stubs; README + description → **https://crates.io/crates/rbt-datalake**.

See [CRATES_IO.md](CRATES_IO.md).

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

## Orphan deprecation stubs (0.0.3 — point at rbt-datalake)

`rbt-testing`, `rbt-json`, `rbt-core`, `rbt-scan`, `rbt-materializer`, `rbt-engine` — empty stubs with README → use `rbt-datalake`.

See [CRATES_IO.md](CRATES_IO.md).

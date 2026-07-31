# Publishing

## Unified package (this repo)

```bash
cargo test -p rbt --lib
cargo build -p rbt --release
bash scripts/smoke.sh
# commit first (cargo publish refuses dirty trees by default)
cargo publish -p rbt
```

If crates.io rejects the name `rbt` (already taken by an unrelated project), rename `[package].name` to an available name (e.g. `rbt-cli` or `rbt-lake`), keep `[[bin]] name = "rbt"` and `[lib] name = "rbt"`, update README install lines, then publish.

Git install always works:

```bash
cargo install --git https://github.com/shan-alexander/rbt --locked
```

## Orphan deprecation stubs (done 0.0.2)

Published outside the workspace as empty stubs with “use unified rbt” READMEs:

`rbt-testing`, `rbt-json`, `rbt-core`, `rbt-scan`, `rbt-materializer`, `rbt-engine`

See [CRATES_IO.md](CRATES_IO.md).

## Versioning

- `0.0.x` — breaking changes allowed
- Depend on / install the **unified** package only (not orphans)

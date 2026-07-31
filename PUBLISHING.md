# Publishing rbt 0.0.1

This is an **experimental** crates.io release. Prefer publishing the **workspace crates in dependency order**, or only shipping the binary from git until APIs stabilize.

## Pre-flight

```bash
cargo fmt --all -- --check
cargo clippy -p rbt-cli -p rbt-core -p rbt-engine -p rbt-materializer -p rbt-scan -p rbt-testing -- -D warnings
cargo test --workspace --lib
bash scripts/smoke.sh
```

## Suggested publish order (all at version 0.0.1)

Path dependencies must already be on crates.io:

1. `rbt-models` (optional; leaf)
2. `rbt-testing`
3. `rbt-core`
4. `rbt-json`
5. `rbt-scan`
6. `rbt-catalog` (optional stub)
7. `rbt-materializer`
8. `rbt-engine`
9. `rbt-cli` (binary `rbt`)

```bash
# Example for one crate after setting repository URL in Cargo.toml
cargo publish -p rbt-testing --dry-run
cargo publish -p rbt-testing
# …then dependents
```

Before first publish:

1. Set a real `repository` / `homepage` URL in root `Cargo.toml` `[workspace.package]`.
2. Ensure path deps use `version.workspace = true` (already) and crates.io can resolve them after publish.
3. `cargo package -p rbt-cli --list` and review included files.

## Versioning policy

- `0.0.x` — breaking changes allowed without fanfare
- Public guarantee is the **CLI smoke path**, not internal crate layouts

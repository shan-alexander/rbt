# crates.io layout

## Intended public surface

| Package | Install / use |
|---------|----------------|
| **`rbt`** (this repo) | Library + binary — see [README](../README.md) |

> **Name collision:** crates.io already has an unrelated crate named [`rbt`](https://crates.io/crates/rbt) (“Rust bot toolkit”, owner `a7g4`). Publishing **our** package may require a different crates.io name (e.g. `rbt-cli` / `rbt-lake`) while the binary stays `rbt`. Prefer git install until that is settled:
>
> ```bash
> cargo install --git https://github.com/shan-alexander/rbt --locked
> ```

## Orphan crates (0.0.1 accident → 0.0.2 deprecation stubs)

These were published as path-deps for 0.0.1. They **cannot be deleted**. Each has a **0.0.2 deprecation stub** whose README says: use the unified monorepo package.

| Crate | Status |
|-------|--------|
| [`rbt-core`](https://crates.io/crates/rbt-core) | **0.0.2 deprecated stub** |
| [`rbt-engine`](https://crates.io/crates/rbt-engine) | **0.0.2 deprecated stub** |
| [`rbt-scan`](https://crates.io/crates/rbt-scan) | **0.0.2 deprecated stub** |
| [`rbt-json`](https://crates.io/crates/rbt-json) | **0.0.2 deprecated stub** |
| [`rbt-materializer`](https://crates.io/crates/rbt-materializer) | **0.0.2 deprecated stub** |
| [`rbt-testing`](https://crates.io/crates/rbt-testing) | **0.0.2 deprecated stub** |

`rbt-cli` was never successfully published as a separate crate.

## Policy

- Workspace has **one** member: `crates/rbt` (lib + bin).
- Do **not** re-publish real code under the orphan names.
- Optionally `cargo yank --vers 0.0.1 <orphan>` after dependents have moved (0.0.2 stub remains).

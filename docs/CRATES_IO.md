# crates.io layout

## Public surface

| Package | Install / use |
|---------|----------------|
| [`rbt-datalake`](https://crates.io/crates/rbt-datalake) | `cargo install rbt-datalake` → binary **`rbt`** · dep `rbt-datalake = "0.3.6"` · `use rbt::…` |

The short name [`rbt`](https://crates.io/crates/rbt) is taken by an unrelated project (“Rust bot toolkit”, owner `a7g4`). Our package is therefore **`rbt-datalake`**, with `[[bin]] name = "rbt"` and `[lib] name = "rbt"`.

## Orphan crates (0.0.1 accident → deprecation stubs)

These were published as path-deps for 0.0.1. They **cannot be deleted**. Latest **0.3.6** is a deprecation stub whose README says: use **`rbt-datalake`**.

| Crate | Status |
|-------|--------|
| [`rbt-core`](https://crates.io/crates/rbt-core) | 0.3.6 deprecated stub |
| [`rbt-engine`](https://crates.io/crates/rbt-engine) | 0.3.6 deprecated stub |
| [`rbt-scan`](https://crates.io/crates/rbt-scan) | 0.3.6 deprecated stub |
| [`rbt-json`](https://crates.io/crates/rbt-json) | 0.3.6 deprecated stub |
| [`rbt-materializer`](https://crates.io/crates/rbt-materializer) | 0.3.6 deprecated stub |
| [`rbt-testing`](https://crates.io/crates/rbt-testing) | 0.3.6 deprecated stub |

## Policy

- Workspace has **one** member: `crates/rbt` (package name `rbt-datalake`).
- Do **not** re-publish real code under the orphan names.

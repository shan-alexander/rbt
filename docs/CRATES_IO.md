# crates.io layout

## Public surface

| Package | Install / use |
|---------|----------------|
| [`rbt-datalake`](https://crates.io/crates/rbt-datalake) | `cargo install rbt-datalake` → binary **`rbt`** · dep `rbt-datalake = "0.9.0"` · `use rbt::…` |

The short name [`rbt`](https://crates.io/crates/rbt) is taken by an unrelated project (“Rust bot toolkit”, owner `a7g4`). Our package is **`rbt-datalake`**, with `[[bin]] name = "rbt"` and `[lib] name = "rbt"`.

Latest published: **0.9.0**.

## Orphan crates (deprecation stubs)

These were published as path-deps for 0.0.1. They **cannot be deleted**. Latest **0.0.4** is a deprecation stub whose README and crate description point to:

**https://crates.io/crates/rbt-datalake**

| Crate | Latest | Description |
|-------|--------|-------------|
| [`rbt-core`](https://crates.io/crates/rbt-core) | 0.0.4 deprecated stub | → rbt-datalake |
| [`rbt-engine`](https://crates.io/crates/rbt-engine) | 0.0.4 deprecated stub | → rbt-datalake |
| [`rbt-scan`](https://crates.io/crates/rbt-scan) | 0.0.4 deprecated stub | → rbt-datalake |
| [`rbt-json`](https://crates.io/crates/rbt-json) | 0.0.4 deprecated stub | → rbt-datalake |
| [`rbt-materializer`](https://crates.io/crates/rbt-materializer) | 0.0.4 deprecated stub | → rbt-datalake |
| [`rbt-testing`](https://crates.io/crates/rbt-testing) | 0.0.4 deprecated stub | → rbt-datalake |

## Policy

- Workspace has **one** member: `crates/rbt` (package name `rbt-datalake`).
- Do **not** re-publish real code under the orphan names.

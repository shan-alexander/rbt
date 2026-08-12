---
tags: [adr, features, embedding, cargo]
node_type: adr
aliases: [ADR-004, L1.1, feature flags, slim profile]
status: accepted
---
# ADR-004: Cargo feature flags for embeddable library profiles (RBT-L1.1)

## Status

**Accepted**  
**Date:** 2026-08-08 · **Epic:** RBT-L1 embeddable library surface  
**Related:** [[library embedding and dag crate survey]] · [[ADR-002 Thesis Alignment]]

## Context

`rbt-datalake` currently pulls DataFusion, Arrow, Parquet, Iceberg, jshift, and Clap as
unconditional dependencies. That is correct for the full CLI / DE install, but forces
library embedders to compile optional stack (Iceberg catalog, jshift, CLI) even when they
only need Parquet materialize + SQL.

Patterns we follow (see rust-unofficial design patterns + peer crates):

- **Feature flags** as composition of optional subsystems (creational “thin core”).
- **Default full product** so `cargo install rbt-datalake` stays zero-config.
- Embedders use `default-features = false` + explicit features (borrowed API surface stays stable).

## Decision

1. **Cargo features** on `rbt-datalake`:

| Feature | Default | Role |
|---------|---------|------|
| `sql` | yes | Marker: DataFusion SQL engine (always compiled; marker for future splits) |
| `parquet` | yes | Marker: Parquet lake materialize (always compiled) |
| `jshift` | yes | Selective JSON path extract (`jshift` crate) |
| `iceberg` | yes | Iceberg catalog + FS dual-write paths |
| `cli` | yes | Binary `rbt` + clap |

2. **Default = all of the above** so existing users and CI are unchanged.

3. **Embed profile example:**

```toml
rbt-datalake = { version = "0.9", default-features = false, features = ["sql", "parquet"] }
```

4. **Disabled features fail closed** at runtime with `E_RBT_FEATURE: … not enabled` when
   code paths are exercised (e.g. `--format iceberg` without `iceberg` feature).

5. Binary requires `cli` (`required-features = ["cli"]`).

## Consequences

- Faster/leaner deps for embedders who disable `iceberg` / `jshift` / `cli`.
- More `#[cfg(feature = …)]` in materializer/engine/json (maintainability cost).
- Docs and PUBLISHING must list features.
- Does **not** remove DataFusion from the core path (SQL is the product spine).

## Alternatives rejected

| Alt | Why not |
|-----|---------|
| Default without Iceberg | Breaks existing CLI installs and examples using iceberg format |
| Split crates again | Contradicts monocrate policy; feature flags suffice |
| Always-on Iceberg forever | Blocks pulse-adjacent binaries that only need Parquet |

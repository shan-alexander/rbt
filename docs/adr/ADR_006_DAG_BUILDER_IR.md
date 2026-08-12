---
tags: [adr, library, dag, builder, ir]
node_type: adr
aliases: [ADR-006, L1.3, DagBuilder, ModelSpec]
status: accepted
---
# ADR-006: Programmatic DAG IR — DagBuilder / ModelSpec (RBT-L1.3)

## Status

**Accepted**  
**Date:** 2026-08-08 · **Epic:** RBT-L1  
**Related:** [[ADR-001 Project Layout]] · [[ADR-004 Feature flags]]

## Context

Today the only supported product path is **on-disk** `models/**/*.sql` + frontmatter
via `RbtProjectConfig::build_dag`. Library hosts that already own orchestration need to
build the same `ModelDag` from Rust structs without a models directory.

Rust patterns: **Builder** (creational), **newtype / IR** as the stable middle end,
frontends (files, programmatic) compile into it.

## Decision

1. Introduce **`ModelSpec`** — owned description of one model (name, SQL, materialization,
   format, path, frontmatter, description).
2. Introduce **`DagBuilder`** — fluent API:

```rust
let dag = DagBuilder::new()
    .model(ModelSpec::sql("stg_x", "SELECT 1 AS id")
        .layer(ModelLayer::Staging)
        .materialization(Materialization::Table)
        .output_path(path)
        .frontmatter(fm))
    .build()?;
```

3. **File projects remain a frontend:** `RbtProjectConfig::build_dag` continues to load
   files and produce `ModelDag` (same IR). Future: optional refactor to funnel through
   `DagBuilder` without behavior change.
4. `build()` validates graph (cycles, layer bands) via existing `ModelDag::build_graph`.
5. Bronze registration still uses frontmatter on each node when `execute_dag*` runs
   (same as file-built DAGs).

## Consequences

- Embedders can unit-test materialize without writing temp SQL files.
- File CLI and library share `ModelDag` / engine path (one execution SoT).
- Does not replace file projects for DE humans.

## Alternatives rejected

| Alt | Why not |
|-----|---------|
| Only document `add_model_with_format` | Incomplete product surface; bronze/frontmatter awkward |
| Separate “library DAG” type | Two engines; drift |
| Macro-only DSL | Harder to compose; Builder is enough |

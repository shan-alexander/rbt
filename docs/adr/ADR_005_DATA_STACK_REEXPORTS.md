---
tags: [adr, arrow, datafusion, embedding, versions]
node_type: adr
aliases: [ADR-005, L1.2, arrow re-export]
status: accepted
---
# ADR-005: Re-export Arrow / Parquet / DataFusion (RBT-L1.2)

## Status

**Accepted**  
**Date:** 2026-08-08 · **Epic:** RBT-L1  
**Related:** [[ADR-004 Feature flags]]

## Context

Embedders that also depend on `arrow` / `datafusion` often pull a **different major**
than rbt (e.g. app on Arrow 54, rbt on 58). Dual-linking Arrow majors breaks type
identity (`RecordBatch` from crate A is not crate B’s `RecordBatch`).

## Decision

1. **Public re-exports** from `rbt`:

```rust
pub use arrow;
pub use parquet;
pub use datafusion;
#[cfg(feature = "iceberg")]
pub use iceberg;
```

2. **Host guidance:** depend on types via `rbt::arrow` / `rbt::datafusion`, or pin the
   same majors as `rbt-datalake`’s Cargo.toml.

3. rbt continues to use workspace pins as the single SoT for versions per release.

## Consequences

- Embedders can write `fn f(b: rbt::arrow::record_batch::RecordBatch)` without dual link.
- Semver: bumping Arrow major in rbt is a **breaking** change for re-export consumers
  (acceptable; document in CHANGELOG).
- Does not force hosts to *only* use re-exports—optional discipline.

## Alternatives rejected

| Alt | Why not |
|-----|---------|
| “Just document the pin” | Too easy to dual-link in workspaces |
| Newtype wrappers for every Arrow type | Unusable ergonomics |

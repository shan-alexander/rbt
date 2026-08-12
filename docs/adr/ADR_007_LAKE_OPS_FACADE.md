---
tags: [adr, library, ops, facade, skip, upsert]
node_type: adr
aliases: [ADR-007, L1.4, lake ops]
status: accepted
---
# ADR-007: Lake ops façade for embedders (RBT-L1.4)

## Status

**Accepted**  
**Date:** 2026-08-08 · **Epic:** RBT-L1  
**Related:** [[ADR-006 DagBuilder IR]] · [[ADR-004 Feature flags]] ·
[[library embedding and dag crate survey]]

## Context

Embedders need the **80% silver path** without shelling out to the CLI or re-implementing
skip / stage / upsert wiring. The primitives already exist (`bronze_fingerprint`,
`materialize_keyed_upsert`, frontmatter, receipts) but are scattered across modules with
CLI-shaped call sites.

Rust patterns:

- **Façade** (structural): a thin, intentional module that composes existing subsystems.
- **Not** a second execution engine — same materializer and receipt SoT.
- Borrowed args (`&Path`, `&str`, slices) for library-friendly call sites.

## Decision

1. Introduce public module **`rbt::ops`** with:
   - **`SkipPlan` / `plan_skip`** — compute current bronze fingerprint + compare to
     latest successful receipt for scope (library equivalent of `--skip-if-match`).
   - **`stage_model_spec`** — build a `ModelSpec` for a staging SQL + bronze scan contract
     without hand-rolling frontmatter field-by-field for the common case.
   - **`upsert_registry`** — write path wrapper around `materialize_keyed_upsert` with
     explicit `UpsertConfig` (host-owned candidates already as `RecordBatch`es).
2. No new materialization semantics; ops only package existing behaviour.
3. Hosts that need full DAG execute still use `TransformationEngine::execute_dag*`.

## Consequences

- Lake daemons can skip dirty work and upsert dims without inventing receipt paths.
- Docs and examples can point at `ops` as the “library silver API”.
- Risk of façade drift is mitigated by thin wrappers (no duplicated logic bodies).

## Alternatives rejected

| Alt | Why not |
|-----|---------|
| Only document low-level APIs | High friction; hosts re-copy engine skip logic |
| Separate “daemon SDK” crate | Premature split; same version pin as rbt |
| Macro DSL for stage | Builder + helper functions are enough |

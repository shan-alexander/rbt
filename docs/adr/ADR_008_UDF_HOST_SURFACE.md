---
tags: [adr, library, udf, design-a, embedding]
node_type: adr
aliases: [ADR-008, L1.5, UDF host surface, with_udfs]
status: accepted
---
# ADR-008: UDF host surface — Design A pack hooks (RBT-L1.5)

## Status

**Accepted**  
**Date:** 2026-08-08 · **Epic:** RBT-L1  
**Related:** [[ADR-003 UDF and Rust models]] · [[ADR-005 Data stack re-exports]] ·
[[library embedding and dag crate survey]]

## Context

ADR-003 Design A ships built-in `rbt_*` scalars and `register_scalar_udf`. Embedders need a
**product-grade registration seam** so host kernels (feature packs, domain math) live
**outside** rbt while SQL models call them. Without a documented hook, hosts either fork
the engine constructor or re-register after every `TransformationEngine::new()`.

Rust patterns:

- **Builder** optional steps (`RbtEngineBuilder::with_udfs`).
- **Strategy / plugin**: host supplies registration behaviour; engine owns session lifecycle.
- **Trait object or FnOnce** for pack registration without subclassing.

## Decision

1. **`RbtEngineBuilder::with_udfs(F)`** — `F: FnOnce(&SessionContext) -> Result<()> + Send + 'static`.  
   Hooks run **after** built-in `rbt_*` UDFs on `build()`. Multiple hooks allowed (order preserved).
2. **`TransformationEngine::register_udfs(F)`** — same shape for late registration on a live engine.
3. **`UdfPack` trait** — `fn register(&self, ctx: &SessionContext) -> Result<()>` for named packs;
   `with_udf_pack` / `register_udf_pack` move or borrow packs into the same hooks.
4. **Non-goals:** reimplement host math inside rbt; Design B Rust model nodes (still ADR-003);
   untrusted dynamic loading.
5. **NULL policy (document):** host scalar UDFs should treat Arrow nulls as SQL NULL and return
   null for undefined results (Option-style). Empty string ≠ NULL unless the UDF defines it
   (`rbt_nullif_empty` is the built-in escape hatch).
6. **Ordering guidance:** window / ordered kernels use SQL
   `PARTITION BY … ORDER BY …` around the UDF; rbt does not invent a second ordering model.

## Consequences

- Hybrid hosts: SQL orchestrates medallion; kernels stay in host crates registered once per engine.
- Re-exports (`rbt::datafusion`) ensure UDF types match the session ABI.
- Does not replace Design B; keeps Design A as the embed path of least resistance.

## Alternatives rejected

| Alt | Why not |
|-----|---------|
| Only document `ctx.register_udf` | No lifecycle guarantee vs builtins |
| Subclass / wrap engine | Un-Rusty; fights composition |
| Proc-macro UDF registry | Overkill for v1 |

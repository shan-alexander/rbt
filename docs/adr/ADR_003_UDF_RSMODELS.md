---
tags: [adr, polyglot, udf, rust-models, datafusion]
node_type: adr
aliases: [ADR-003, ADR-003 Polyglot DAG, Design A, Design B, polyglot DAG, Rust models]
status: accepted
---
# ADR-003: Polyglot DAG — SQL Models, Rust Models, and In-Process UDFs

## Status

**Accepted / Planned** (Design A builtins shipped **0.5.0**; Design B planned)  
**Date:** 2026-07-28 · **Deciders:** Project maintainers  
**Extends:** [[ADR-001 Project Layout]], [[ADR-002 Thesis Alignment]]  
**Related code/docs:** [thesis.md](../../thesis.md), [CONTRIBUTING.md](../../CONTRIBUTING.md), [docs/P4_CAPABILITIES.md](../P4_CAPABILITIES.md)

> **Monocrate note (2026-07):** Workspace is a single package `rbt` (lib + bin). Historical multi-crate names map to modules under `crates/rbt/src/`.

## Summary

`rbt` will support a **polyglot, monomorphic-data DAG**:

| Design | Name | Description |
|--------|------|-------------|
| **A** | **SQL + Rust UDFs** | Default models remain SQL; domain logic registers as DataFusion scalar / aggregate / window UDFs callable from SQL. |
| **B** | **Rust models** | First-class DAG nodes: `fn(Context) → Arrow stream/batches`, same layers, materialize, tests, selectors. |

**Arrow `RecordBatch` is the ABI** between SQL and Rust. No blended `.rsx` language on the near roadmap.

## Context

1. SQL is the right default for medallion / Kimball work ([[Star schema data modeling rules]]).
2. Some transforms fit poorly in SQL (sessionization, as-of joins, library kernels).
3. “Rewrite in Polars because SQL is slow” is often false when SQL already runs on DataFusion — measure first ([[Measured claims before marketing]]).
4. dbt’s escape hatch is Python models; rbt is in-process and can offer a **native Rust** hatch with zero-copy Arrow.
5. Harden the SQL spine first ([[Primary path spine]]); design seams before ad-hoc UDF hacks.

### Explicit non-goals

| Out of scope | Why |
|--------------|-----|
| `.rsx` / mixed SQL+Rust grammar | Language trap |
| “Rust always faster than SQL” | Measure first |
| Ballista multi-node models | Single-node thesis path |
| Untrusted `libloading` without policy | Security surface |
| Polars as default SQL engine | DataFusion stays default |
| Full dbt Python model parity | Rust is the native extension |

## Goals

1. One project graph for SQL and Rust models; medallion layer rules; topo tiers.
2. Design A: UDF packages register into `SessionContext` before compile/execute.
3. Design B: Rust model = normal DAG node with shared materializer and tests.
4. Shared materialization (Parquet / Iceberg) and testing for both kinds.
5. Validate / explain / preview apply to SQL always; Rust gets deps + schema contract semantics.
6. Analytics authors stay in SQL; platform owns UDFs and Rust models.

## Decision

### Decision 4.1 — Polyglot DAG, monomorphic data

**Accepted.** Model kind may be `sql` | `rust`. Data boundary is always **Arrow**. UDFs are not model kinds — they are registered functions.

### Decision 4.2 — Design A: UDFs callable from SQL

**Accepted.** Scalar / aggregate / window logic inside a SQL model’s plan.

### Decision 4.3 — Design B: First-class Rust models

**Accepted.** Whole-node transforms where the author owns the operator graph.

### Decision 4.4 — No `.rsx` language

**Accepted.** No blended file format in v0–v0.x.

### Decision 4.5 — DataFusion as SQL runtime

**Accepted.** Rust models may use DF and/or other Arrow libraries; outputs must register as DataFusion-visible tables for downstream `ref()`.

### Decision 4.6 — Discovery and packaging

**Accepted (direction).** SQL via `*.sql` discovery; Rust models and UDFs via explicit project registry crate (not “compile every `.rs` under models/” in v1). Dynamic `cdylib` load is later.

### Decision 4.7 — Dependency declaration

SQL: `{{ ref() }}` / `{{ source() }}`. Rust models: explicit `refs` / `sources` lists. UDFs: not DAG nodes.

## Architecture (sketch)

```text
                    ┌─────────────────────────────────────┐
                    │  core: Project + ModelDag             │
                    │  nodes: SqlModel | RustModel          │
                    └─────────────────┬───────────────────┘
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                       ▼
     UDF Registry              engine SessionContext     Rust model runner
     (Design A)  ────────────▶ SQL plan/exec             Context → batches
                                    │                      │
                                    └──────────┬───────────┘
                                               ▼
                                    Arrow RecordBatch stream
                          materializer · testing · ref registration
```

**When to choose B over A**

| Prefer **UDF (A)** | Prefer **Rust model (B)** |
|--------------------|---------------------------|
| Column function inside SQL | Logic *is* the whole transform |
| Planner should see surrounding filters | Multi-step / non-relational graph |
| Analytics author stays in `.sql` | Platform-owned performance node |

## Implementation phases

| Phase | Deliverable | Status |
|-------|-------------|--------|
| Document ADR | Seams designed | Done |
| Design A MVP | Builtin + register path | **Shipped 0.5.0** — `rbt_upper` / `lower` / `trim` / `nullif_empty` |
| Design B MVP | `RustModel` + registry; materialize + ref | Planned |
| Project extension crate pattern | Sample + `output_schema` | Planned |
| Stream outputs; optional Polars | Large-batch safe | Planned |
| Optional `cdylib`; table UDFs | Plugin policy | Deferred |

**Do not block** Iceberg SoR, bronze hardening, or memory-honest materialize on Design B.

## Alternatives considered

| Alternative | Outcome |
|-------------|---------|
| Only SQL forever | Rejected — no native escape hatch |
| Only Polars API, no SQL | Rejected — kills dbt-shaped adoption |
| `.rsx` blended language | Rejected for now |
| dbt-style Python models as primary | Rejected |
| WASM UDF plugins | Deferred |
| External process per Rust model | Rejected for default path |

## Consequences

### Positive

- Clear story: SQL by default, Rust when it matters; one DAG and lake.
- Arrow-native hybrid beats warehouse Python models on locality.
- Future sugar can desugar to Design B without redoing the DAG.

### Costs

- Teams needing B need a small Rust workspace.
- Optimization fence at Rust model boundaries.
- Registry/linking must stay simple.

### Success metrics

1. SQL model calls a documented UDF in tests.
2. Rust model can `ref` SQL staging, materialize, and be `ref`’d by SQL.
3. Layer violations involving Rust fail at graph build.
4. Docs never claim Rust is universally faster than DataFusion SQL.
5. CONTRIBUTING still ranks SQL spine and Iceberg SoR above expanding UDF surface.

## Decision record (one paragraph)

We implement **Design A (in-process DataFusion UDFs)** and **Design B (first-class Rust models)** as the only supported polyglot extensions. Both share the Model DAG, medallion rules, materializer, and Arrow data path. We will **not** implement a blended `.rsx` language in the foreseeable roadmap. SQL remains the default authoring model; Rust is the host escape hatch for functions (A) and whole nodes (B).

## Related

- Goal: [[Polyglot UDFs and Rust models]]
- Goals: [[Primary path spine]], [[Product north star]], [[Measured claims before marketing]], [[Honest product surface]]
- Prior: [[ADR-001 Project Layout]], [[ADR-002 Thesis Alignment]]
- Concept: [[Star schema data modeling rules]]

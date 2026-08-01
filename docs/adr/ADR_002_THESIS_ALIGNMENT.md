---
tags: [adr, thesis, bronze, jshift, dx, iceberg, datafusion]
node_type: adr
aliases: [ADR-002, ADR-002 Thesis Alignment, thesis alignment, bronze edge, developer loop]
status: accepted
---
# ADR-002: Thesis Alignment, Bronze Edge Ingestion, and Star Schema Pipeline Design

## Status

**Approved / Active**  
**Date:** July 2026 · **Deciders:** Project maintainers  
**Scope:** `thesis.md` alignment, jshift bronze edge, prost diagnostics (direction), Iceberg & DataFusion star-schema pipeline  

> **Monocrate note (2026-07):** Workspace is a single package `rbt` (lib + bin). Historical multi-crate names below map to modules under `crates/rbt/src/`.

## Context

A review of [thesis.md](../../thesis.md) against early architecture and [[ADR-001 Project Layout]] found five understated design aspects:

1. **Raw bronze edge (`jshift`)** — Enterprise pipelines start at raw JSONL/CSV/Parquet on object storage; selective path extract beats full `serde_json::Value` DOM parse.
2. **Machine-readable diagnostics** — Agent repair loops need structured errors (`E_RBT_*`) and run reports (JSON first; prost direction).
3. **Instant feedback loop** — `validate → explain → preview → run → test` before expensive scans.
4. **Star-schema metadata** — Explicit dimension/fact grain and relationships, not only generic SQL nodes.
5. **Crate/module topology** — Align modules to bronze → SQL → materialize → test flow (now monocrate modules).

## Decision

### Decision 2.1: Bronze ingestion with jshift

For raw JSONL, use **jshift** to extract target paths and stamp metadata during byte iteration, yielding Arrow batches without full DOM allocation. Module path: `rbt::json` / bronze scan path.

### Decision 2.2: Machine-readable diagnostics

Execution diagnostics and reports are agent-oriented: structured error codes, model name, suggestions. **JSON** is the shipped surface; **prost** remains a direction for binary packing once shapes stabilize (do not block spine on prost).

### Decision 2.3: Developer feedback loop

Five CLI/library verbs ([[Instant-feedback DX loop]]):

```text
1. rbt validate ──> syntax, refs, layer rules, contracts (minimal/no full materialize)
2. rbt explain  ──> compiled SQL / plan / deps / bronze contract
3. rbt preview  ──> LIMIT N sample rows
4. rbt run      ──> full materialization (+ Iceberg commit when format is iceberg)
5. rbt test     ──> not_null, unique, relationship-style assertions
```

### Decision 2.4: Iceberg + DataFusion as stack defaults

- **DataFusion** — in-process SQL / vectorized execution.
- **Iceberg** — long-term table truth; prove via catalog snapshot commit ([[Iceberg system of record]]).
- **Star schema** — first-class modeling intent; see [[Star schema data modeling rules]].

### Decision 2.5: Module topology (historical crate map → monocrate)

| Historical name | Responsibility (now under `crates/rbt`) |
|-----------------|----------------------------------------|
| `rbt` facade/CLI | `main.rs` + lib root |
| `rbt-core` | project, DAG, paths, frontmatter |
| `rbt-bronze` / scan | bronze registration, formats |
| `rbt-json` | jshift JSONL |
| `rbt-engine` | DataFusion session, execute, UDFs |
| `rbt-materializer` | Parquet / Iceberg / stream / WAP / incremental |
| `rbt-testing` | frontmatter assertions on batches |
| measure | thesis scenario packs |

Do not re-publish orphan crates.

## Operational comparison (intent)

| Stage | Legacy Spark + dbt | rbt intent |
|-------|--------------------|------------|
| Bronze | Full JSON parse | Selective extract (jshift) + projection |
| Model graph | Jinja + warehouse | In-process DAG + `ref` / `source` |
| Engine | Spark / warehouse | DataFusion |
| Table truth | Warehouse / Delta | Iceberg (proof gate) + FS lake |
| Tests | Post-hoc SQL | In-flight Arrow assertions |
| Diagnostics | Text logs | Structured `E_RBT_*` (+ JSON reports) |

## Consequences

- Aligns product with [[Product north star]] and [[Team-scale lake positioning]].
- DX verbs and bronze edge are first-class, not afterthoughts.
- Multi-crate publish topology from early thesis is **superseded** by monocrate honesty ([[Honest product surface]]).
- prost is directional; do not claim shipped binary diagnostics until implemented.
- Measure packs gate public efficiency claims ([[Measured claims before marketing]]).

## Related

- Goals: [[Product north star]], [[Primary path spine]], [[Bronze contracts multi-root and path_glob]], [[Instant-feedback DX loop]], [[Iceberg system of record]], [[Measured claims before marketing]]
- Prior: [[ADR-001 Project Layout]]
- Next: [[ADR-003 Polyglot DAG]]
- Concept: [[Star schema data modeling rules]]

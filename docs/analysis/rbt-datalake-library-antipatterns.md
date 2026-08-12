---
tags: [analysis, library, antipatterns, rust-patterns, embedding]
node_type: analysis
aliases: [rbt library antipatterns, L1 debt]
status: open
---
# rbt-datalake library surface — known antipatterns / debt (L1 era)

**When:** 2026-08-08  
**Related:** [[library embedding and dag crate survey]] · [[ADR-004 Feature flags]] ·
[[ADR-006 DagBuilder IR]] · rust-unofficial design patterns catalogue

This note tracks **product/API antipatterns** we accept or are actively fixing in L1.
Not a full architecture review—signal for embedders and implementers.

## Fixed or mitigated by L1

| Issue | Pattern violated | Mitigation |
|-------|------------------|------------|
| Always-on Iceberg / jshift / clap | Feature composition | **L1.1** optional features; default remains full CLI product |
| Dual-linking Arrow 54 vs 58 | Monomorphization / ABI | **L1.2** re-export `arrow`/`parquet`/`datafusion` |
| File project as only frontend | IR-first dual frontend | **L1.3** `DagBuilder` / `ModelSpec` |
| Skip/upsert only via CLI paths | Façade missing | **L1.4** `ops` module |
| Host UDF pack undocumented | Plugin / Strategy surface | **L1.5** `with_udfs` / `register_udfs` |

## Still open (do not paper over)

| Issue | Why it hurts | Direction (not L1) |
|-------|--------------|--------------------|
| **`execute_dag*` is a god method** | Hard to unit-test materialize alone; long arg lists | Split plan / register / materialize stages later |
| **Filesystem-only lake** | Paths are the control plane; no trait for object store | A8 / storage backends |
| **Package `rbt-datalake` / import `rbt`** | Discoverability tax | Document; optional thin `rbt` re-export crate later |
| **`sql` / `parquet` features are markers** | Do not yet strip DF/Parquet from compile | Real split only if compile times force it |
| **Clone-heavy DAG nodes** | `ModelNode` cloned into graph + map | Accept for IR size today; pool later if hot |
| **`anyhow` at public boundaries** | Embedders cannot match on typed errors easily | Gradual `thiserror` public enum for stable codes |
| **Global `SessionContext` ownership** | Hosts that already own DF context must re-register | Future: `from_session(ctx)` constructor |

## Patterns we deliberately use (not antipatterns)

- **Builder** for multi-config construction (`DagBuilder`, `RbtEngineBuilder`, `ModelSpec` chain).
- **Fail-closed** optional features (`E_RBT_FEATURE` when jshift path extract used without feature).
- **Newtype-ish IR** (`ModelDag`) shared by file and programmatic frontends.
- **Borrowed args** on hot helpers (`&str`, `&Path`, `&[RecordBatch]`).
- **Prefix conventions** `stg_` / `tf_` / `dim_` as DE layer names (star-schema transform, not timeframe).

## Consumer guidance

Prefer:

```rust
// Slim host
rbt-datalake = { version = "0.8", default-features = false, features = ["sql", "parquet"] }

use rbt::{arrow, datafusion, DagBuilder, ModelSpec, RbtEngineBuilder, ops};
```

Avoid:

- Re-depending on a second `arrow` major.
- Shelling out to `rbt run` from a long-lived daemon (use library + receipts).
- Putting host math SoT SQL-only inside rbt; register UDFs instead.

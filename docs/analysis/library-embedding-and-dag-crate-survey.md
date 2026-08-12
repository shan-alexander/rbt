---
tags: [analysis, library, embedding, dag, udf, feature-flags, quant]
node_type: analysis
aliases:
  - qsys embedding friction
  - DagBuilder API
  - rbt as library
---
# Library embedding friction + DAG crate survey (post-0.8.0)

**When:** 2026-08-08  
**Inputs:** Host feedback from embedding `rbt-datalake` in a quant lake stack (qsys-lake);
crates.io “DAG” peers: `pldag`, `oxigeo-workflow`, `rskit-dag`, `luciole`, `ascii-dag`,
`qudag`, `awint_dag`.  
**Status:** recommendations only (no code in this note).

Related: [[ADR-002 Thesis Alignment]] · [[ADR-003 UDF and Rust models]] ·
[[rbt-datalake feature roadmap]] · [[A10 bronze-to-silver approach]] ·
[[Product north star]]

---

## 1. Verdict on the host feedback

The friction report is **accurate and high signal**. rbt 0.8.0 is a strong **CLI +
on-disk medallion project** product; the library path works but is a second-class
front-end of that product:

```text
project dir → models/*.sql + frontmatter → ModelDag → TransformationEngine
```

That is correct for DE hosts. It is awkward for **embedders** who already own
orchestration, rings/bronze I/O, and a math SoT in Rust.

### Friction → product interpretation

| Friction | Interpretation | rbt response |
|----------|----------------|--------------|
| Heavy default (DF + Arrow + Iceberg) | “Pro tool compile path” | **Feature flags / profiles** — highest ROI for embedders |
| Project filesystem as only API | File project is one *frontend* | **Programmatic DAG / IR** as peer frontend |
| `tf_` collides with timeframe | Layer prefix collision | **Docs + optional aliases** (`int_`, `xform_`); never use `tf` for bar periods |
| Two SoTs (SQL gold vs Rust math) | Not an rbt bug if boundaries are clear | **UDF registration as product surface**; SQL orchestrates, kernels live outside |
| Package `rbt-datalake` / import `rbt` | Discoverability tax | Keep (crates.io name conflict); document louder; optional `rbt` re-export crate later |
| Don’t link into IBKR pulse binary | Correct architecture | Slim feature; document “batch lake only” |

### Hybrid host architecture (agreed)

```text
qsys-engine     → rings + bronze (never rbt)
qsys-lake       → bronze I/O + silver/gold orchestration
                  ├─ default: Rust path (dedupe, packs, OBT)
                  └─ feature "dag": rbt TransformationEngine
lake_dags/      → optional human/CLI view of the same conceptual graph
qsys-features   → finance-solution (math SoT)
```

**Do not** delete `lake_dags/` as the contract graph.  
**Do not** force every run through SQL files.  
**Use rbt** for DE materializations, scopes, receipts, tests.  
**Use Rust** for math-critical gold (optionally *called from* rbt UDFs).

UDFs wrapping finance-solution for gold SQL is the **right** dual path—not reimplementing
EMA/RSI in pure SQL, and not calling UDFs from the pulse path.

---

## 2. Peer crate survey (what “DAG on crates.io” actually means)

Most crates tagged `dag` are **not** lake/SQL engines. Steal patterns carefully.

| Crate | What it is | Useful to rbt? |
|-------|------------|----------------|
| **[rskit-dag](https://crates.io/crates/rskit-dag)** | Tiny async **task** DAG: `DagNode` trait, `add_node`/`add_edge`, parallel execute, failure policies, cancel token | **Yes — API shape.** Programmatic graph, failure modes (`FailFast` / `Continue` / `SkipDependents`), bounded parallelism. ~800 LOC of pure orchestration |
| **[oxigeo-workflow](https://crates.io/crates/oxigeo-workflow)** | Domain workflow engine: `WorkflowDag`, `TaskNode`, scheduler, retries, templates, optional HTTP server | **Yes — builder + ops façade.** `WorkflowDefinition { dag }` as IR; feature flags (`server`, `http-client`); execution plan levels; monitoring hooks. Domain is geospatial not lake SQL—but **library-first** |
| **[luciole](https://crates.io/crates/luciole)** | Actor runtime + WaitGraph / streaming pipelines (WASM-safe) | **Partial.** Streaming/cancel semantics inspiration; not medallion |
| **[ascii-dag](https://crates.io/crates/ascii-dag)** | Terminal DAG **renderer** (no_std) | **Nice-to-have** for `rbt explain --graph` / debug, not core |
| **[pldag](https://crates.io/crates/pldag)** | Combinatorial / constraint / ILP logic DAG | **No product overlap** — different meaning of “DAG” |
| **[awint_dag](https://crates.io/crates/awint_dag)** | Big-integer bitwidth DAG for `awint` | **No product overlap** |
| **[qudag](https://crates.io/crates/qudag)** | Quantum-resistant agent darknet / distributed DAG | **No product overlap** |

### Patterns worth copying

1. **IR first, frontends second** (oxigeo, rskit): graph is structs; YAML/SQL is a loader.  
2. **Feature flags for optional weight** (oxigeo: `server` off by default of full stack).  
3. **Trait-shaped nodes** (rskit `DagNode`) vs rbt’s SQL-only `ModelNode` — for rbt, nodes stay data-plane SQL/materialize, but **building** the graph should not require disk.  
4. **Failure policies & cancel tokens** for embedders (daemon cancel, partial multi-entity).  
5. **ASCII graph** for DX (`explain`), not for execution.

### Patterns *not* to copy

- Becoming a Temporal/Airflow clone (scheduler + HTTP server as core).  
- JSON-blob task configs as the primary model language.  
- Generic actor frameworks as the silver path.

rbt’s moat remains: **medallion materializations on lake files** (scan, grain, scoped_replace, keyed_upsert, receipts, tests)—not generic DAG scheduling.

---

## 3. Recommended product response (priority order)

### P0 — Slim profiles / feature flags (**do next after real-lake notes, or in parallel**)

```toml
# aspirational
rbt-datalake = { version = "0.9", default-features = false, features = ["parquet", "sql"] }
# optional: "iceberg", "jshift", "cli", "measure"
```

| Feature | Pulls |
|---------|--------|
| `sql` (default) | DataFusion + Arrow + Parquet |
| `iceberg` | iceberg / iceberg-datafusion |
| `jshift` | selective JSON extract |
| `cli` | clap + bin `rbt` |
| `measure` | measure packs |

**Goal:** a lake daemon can depend on rbt for silver materialize **without** Iceberg in every binary.  
**Arrow policy:** pin one Arrow major per release; **re-export** `arrow` / `parquet` / `datafusion` from `rbt` so hosts don’t dual-link 54 vs 58.

### P1 — Programmatic DAG IR (`DagBuilder`)

Keep file projects as a **frontend** that compiles to the same IR:

```rust
// aspirational public API
let dag = DagBuilder::new()
    .model(ModelSpec {
        name: "stg_bars_1m",
        layer: ModelLayer::Staging,
        materialization: Materialization::ScopedReplace,
        sql: "...",  // or sql_from_file
        frontmatter: StagingFrontmatter { scan_path: Some(...), grain: Some(...), .. },
        ..
    })
    .model(/* tf / mart */)
    .build()?;

engine
    .execute_dag_with_scope(&dag, project_root, output, &config, &scope)
    .await?;
```

Minimum viable:

1. `ModelSpec` / `DagBuilder` in `core` (or `core::programmatic`).  
2. `RbtProjectConfig::build_dag` becomes `file_frontend → DagBuilder`.  
3. One library example under `examples/programmatic_dag/` (no models dir required).  
4. Document: **file project and DagBuilder are equal citizens**.

Already partially possible today via `ModelDag::add_model_with_format` + hand-set frontmatter—but it is undocumented, incomplete for bronze scan, and not a product surface.

### P2 — Lake ops façade (80% silver)

High-level helpers that hide frontmatter for common cases:

| Helper | Maps to |
|--------|---------|
| `stage_from_bronze(scan, grain, …)` | stg model + table/append |
| `scoped_replace_entity(model, scope, …)` | A2 materialize path |
| `dirty_keys_from_receipts(…)` / `should_skip(scope, fingerprint)` | A4 library without CLI |
| `upsert_registry(cfg, candidates, dest)` | A7 pure + write |

These should call the same materializer code as the engine.

### P3 — UDF registration as product surface

Today: `register_scalar_udf`, builtins `rbt_*` (ADR-003 Design A).  
Needed for quant hosts:

1. Documented **session hook**: `engine.with_udfs(|ctx| register_feature_udfs(ctx, &reg))?`  
2. **Window / ordered** UDF guidance: `PARTITION BY symbol ORDER BY timestamp_ns`  
3. NULL ↔ `Option` policy docs  
4. Example: “thin SQL + external kernel crate” (no finance formulas in rbt)  
5. Optional: table-valued “pack” UDFs later (harder; not v1)

**Non-goals:** reimplement indicators in SQL; run UDFs on pulse/ring path.

### P4 — Docs vocabulary (cheap, ship soon)

| Term | Meaning in rbt |
|------|----------------|
| `tf_*` | **Transform layer** (prep SQL), not bar period |
| `timeframe=` / `bar_period` | Hive partition / column for OHLCV grain |
| `stg_*` | Silver stage endpoint |
| `dim_*` / `fact_*` / `obt_*` | Gold mart |

Add a short **“Quant / OHLCV hosts”** section to README or COMPLEX_BRONZE.

### P5 — Receipts / skip as library types (mostly done; package it)

Export a small module doc + helpers:

- `bronze_fingerprint` + `fingerprints_match_for_skip` (exist)  
- `RunReceipt::load` / `latest_for_scope`  
- `DirtySet` / `plan_skip(scope, project)` convenience  

So a lake daemon never shells out to `rbt run --skip-if-match`.

---

## 4. Suggested roadmap epic: **RBT-L1 — Embeddable library surface**

Independent of A10 bronze adapters; can ship in slices:

| ID | Slice | Acceptance |
|----|-------|------------|
| **L1.1** | Feature flags: `iceberg`, `cli` optional; default parquet+sql | qsys-like dep compiles without iceberg |
| **L1.2** | Re-export `arrow` / `datafusion` (or document single major) | No dual Arrow majors in tree |
| **L1.3** | `DagBuilder` / `ModelSpec` + file frontend adapter | Programmatic DAG example green |
| **L1.4** | Lake ops helpers (stage, skip, upsert) | 3 helpers + unit tests |
| **L1.5** | UDF product docs + `with_udfs` builder method | External crate registers UDFs; SQL uses them |
| **L1.6** | Quant vocabulary docs | `tf_` vs timeframe spelled out |

A10 (bronze adapters) remains valuable for DE hosts; **L1 is what unblocks quant embedders**.

---

## 5. What *not* to do

| Anti-pattern | Why |
|--------------|-----|
| Make rbt a generic scheduler (oxigeo clone) | Dilutes lake moat; hosts already have orchestrators |
| Force all quant gold through SQL | Two SoTs unless UDFs wrap math |
| Auto-rename `tf_` → something else without aliases | Breaks medallion ecosystem; docs + aliases first |
| Pull finance formulas into rbt | Host math SoT stays outside |
| Link heavy rbt into pulse binary | Architecture already correct—feature flags reinforce it |

---

## 6. Summary recommendation

1. **Agree** with hybrid: `lake_dags/` as contract + optional CLI; qsys-lake default Rust; rbt behind `dag` feature.  
2. **Prioritize L1** (features + DagBuilder + UDF surface + skip helpers) for library quality.  
3. **Steal API taste** from rskit-dag / oxigeo-workflow (programmatic graph, flags), not domain.  
4. **Keep A10** for bronze→silver DE quality on real lakes—orthogonal to L1.  
5. **Ship small docs now** (`tf_` vs timeframe) without waiting for code.

Next implementation PR candidates (pick one):

- **L1.1 feature flags** (highest embedder ROI, mechanical), or  
- **L1.3 DagBuilder MVP** (highest API clarity), or  
- **L1.6 + L1.5 docs/hooks** (fastest to land).

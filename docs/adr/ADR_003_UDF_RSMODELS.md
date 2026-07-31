# ADR-003: Polyglot DAG — SQL Models, Rust Models, and In-Process UDFs

- **Status**: Accepted / Planned  
- **Date**: 2026-07-28  
- **Deciders**: Project maintainers  
- **Supersedes**: Nothing (extends ADR-001 layout, ADR-002 thesis alignment)  
- **Related**: [thesis.md](../thesis.md), [CONTRIBUTING.md](../CONTRIBUTING.md), sample [rbt_project.yml](../examples/smoke_fixture/rbt_project.yml)

---

> **Monocrate note (2026-07):** Workspace is a single package `rbt` (lib + bin). Historical multi-crate names below map to modules under `crates/rbt/src/`.


## 1. Summary

`rbt` will support a **polyglot, monomorphic-data DAG**:

| Design | Name | Description |
|--------|------|-------------|
| **A** | **SQL + Rust UDFs** | Default models remain SQL; performance- and domain-specific logic registers as DataFusion scalar / aggregate / window UDFs (and later custom physical plans) callable from SQL. |
| **B** | **Rust models** | First-class DAG nodes implemented as Rust functions (`fn(Context) → Arrow stream / batches`) that `ref` / `source` upstream tables like SQL models, materialize with the same writers, and participate in the same layers, tests, and selectors. |

**Arrow `RecordBatch` (and DataFusion table registration) is the ABI** between SQL and Rust. We deliberately **do not** introduce a blended `.rsx` / JSX-like language in this ADR or in the near roadmap.

This is the rbt equivalent of React’s insight (composition in one runtime with host escape hatches)—**without** inventing a new surface syntax for analytics authors.

---

## 2. Context & Motivation

### 2.1 Problem

1. **SQL is the right default** for medallion / Kimball work (filters, joins, aggs, simple windows). Most adopters coming from dbt should stay in SQL.
2. **Some transforms are a poor fit for SQL**: sessionization, as-of joins, custom calendars, tick reconstruction, specialized sketches, multi-step graphs that force excessive intermediate materialization, or reuse of existing Rust libraries.
3. **“Rewrite it in Polars because SQL is slow” is often false** when SQL already runs on DataFusion (Rust + Arrow). Real gains come from **better algorithms**, **fewer stages**, or **custom kernels**—not from a file extension.
4. dbt’s escape hatch is **Python models** (separate runtime, warehouse friction). rbt is **in-process**; we can offer a **native Rust escape hatch** with zero-copy Arrow handoff—a genuine differentiator.
5. Contributors previously brainstormed `.rsx` (SQL interleaved with Rust). We reject that as a flagship: language-design tax, IDE cold-start, audience split. **Designs A and B** capture the value without a new dialect.

### 2.2 Decision drivers

- Preserve **dbt-shaped UX** for the common path (`ref`, `source`, layers, materialize, test).
- Keep **one DAG, one catalog/lake identity model**, one materializer path.
- Use **Arrow as the only data boundary** (no serde_json DOM, no CSV round-trips between model kinds).
- Prefer **boring, implementable APIs** over syntactic fashion.
- Harden the **SQL spine first** (CONTRIBUTING P0); polyglot is a **planned capability**, not a blocker for Parquet/Iceberg SoR work—but the **seams** must be designed before ad-hoc UDF hacks land.

### 2.3 Explicit non-goals (this ADR)

| Out of scope | Why |
|--------------|-----|
| `.rsx` / mixed SQL+Rust grammar | Language trap; revisit only as thin sugar over B later |
| Guaranteeing “Rust faster than SQL” for ordinary windows | Misleading; measure first |
| Ballista / multi-node Rust model distribution | Single-node thesis path first |
| Arbitrary `libloading` of untrusted `.so` in prod without policy | Security surface; design registry carefully |
| Replacing DataFusion SQL with Polars as the default engine | SQL path stays DataFusion; Polars may be an *optional* backend inside a Rust model later |
| Full dbt Python model parity | Rust is the native extension language for rbt |

---

## 3. Goals

1. **One project graph** discovers SQL models and Rust models; resolves dependencies; enforces medallion layer rules; executes in topological tiers.
2. **Design A**: Project- or crate-level UDF packages register into the DataFusion `SessionContext` before SQL compile/execute; SQL can call them by stable names.
3. **Design B**: A Rust model is a normal DAG node with name, layer, materialization, output path/format, deps, and optional tests—authoring surface is idiomatic Rust + a small `rbt` context API.
4. **Shared materialization**: Both kinds write through `rbt-materializer` (Parquet now; Iceberg when SoR proof lands).
5. **Shared testing**: `rbt-testing` assertions run on Arrow batches from either kind.
6. **Validate / explain / preview** (when implemented) apply to SQL always; for Rust models, validate = deps + type/schema contract; explain = documented as “opaque physical stage” unless the model provides a plan hint; preview = run with limit if the API supports it.
7. **Clear ownership**: Analytics engineers own SQL; platform engineers own UDFs and Rust models; SQL may call UDFs without opening a Rust crate every time.

---

## 4. Decisions

### Decision 4.1 — Polyglot DAG, monomorphic data

**Accepted.** Model *kind* may be `sql` | `rust` (and later `udf` is not a model kind—UDFs are registered functions). Data crossing the boundary is always **Arrow**.

### Decision 4.2 — Design A: UDFs callable from SQL

**Accepted.** Extension mechanism for *scalar / row-group-local / aggregate / window* logic that remains inside a SQL model’s logical plan.

### Decision 4.3 — Design B: First-class Rust models

**Accepted.** Extension mechanism for *whole-node* transforms where the author owns the full operator graph (Polars lazy, custom loop, multi-step Arrow pipeline).

### Decision 4.4 — No `.rsx` language

**Accepted.** No blended file format in v0–v0.x. Optional future sugar (`sql!` macros inside Rust models) may compile to Design B APIs; that is not a separate language product.

### Decision 4.5 — DataFusion as SQL runtime; Rust models may use DF and/or other Arrow libraries

**Accepted.** SQL models execute via `rbt-engine` / DataFusion. Rust models receive a context that can:

- resolve upstream tables as DataFusion `DataFrame` / `RecordBatch` streams, and/or  
- expose Polars (optional feature) for ergonomic frame APIs,

…but **registration of outputs** back into the session for downstream SQL `ref()` must use a DataFusion-visible table (MemTable / listing / Iceberg provider).

### Decision 4.6 — Discovery and packaging

**Accepted (direction).**

| Kind | Discovery (target) |
|------|--------------------|
| SQL | `*.sql` under configured `models_path` / packages (existing + medallion layout) |
| Rust models | Explicit registration module in the project or workspace crate, e.g. `models/rust/mod.rs` or `rbt_models` crate with `inventory` / `linkme` / build-script registry—not “compile every `.rs` under models/” in v1 |
| UDFs | Same registry pattern: `rbt_udfs` module or project feature crate linked into the binary / `cdylib` loaded by policy |

Rationale: Rust needs a compilation unit. Unlike SQL files, free-floating `.rs` under `models/` are not loadable without embedding a compiler or requiring a project crate. **v1 assumes a small project-side Rust crate or workspace member** that depends on `rbt-*` and exports models/UDFs. Dynamic plugin loading is a later hardening topic.

### Decision 4.7 — Dependency declaration

| Kind | How deps are known |
|------|--------------------|
| SQL | Existing `{{ ref() }}` / `{{ source() }}` extraction |
| Rust models | Explicit list on the model attribute / builder: `refs = ["stg_stock_trades"]`, `sources = [("bronze", "raw_stock_trades")]` — required for DAG; no silent whole-catalog access in validate mode |
| UDFs | Not DAG nodes; no deps. They may only see columns/args passed from SQL |

---

## 5. Architecture

### 5.1 Logical diagram

```text
                    ┌─────────────────────────────────────┐
                    │  rbt-core: Project + ModelDag         │
                    │  nodes: SqlModel | RustModel          │
                    │  edges: ref / source                   │
                    └─────────────────┬───────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                       ▼
     ┌─────────────────┐   ┌─────────────────┐   ┌────────────────────┐
     │ UDF Registry    │   │ rbt-engine      │   │ Rust model runner  │
     │ (Design A)      │──▶│ SessionContext  │   │ (Design B)         │
     │ scalar/agg/win  │   │ SQL plan/exec   │   │ Context → batches  │
     └─────────────────┘   └────────┬────────┘   └─────────┬──────────┘
                                    │                      │
                                    └──────────┬───────────┘
                                               ▼
                                    Arrow RecordBatch stream
                                               │
                          ┌────────────────────┼────────────────────┐
                          ▼                    ▼                    ▼
                   MemTable register    rbt-materializer      rbt-testing
                   (downstream ref)     (Parquet/Iceberg)     (assertions)
```

### 5.2 Node model (conceptual)

```rust
// Conceptual — implementation may live in rbt-core / rbt-engine
pub enum ModelKind {
    Sql,
    Rust,
}

pub struct ModelNode {
    pub name: String,
    pub kind: ModelKind,
    pub layer: ModelLayer,           // Staging / Transform / Mart (or bronze/silver/gold)
    pub materialization: Materialization,
    pub output_format: OutputFormat,
    pub output_path: Option<String>,
    pub dependencies: Vec<DependencyRef>,
    pub frontmatter: Option<StagingFrontmatter>, // primarily SQL bronze extract
    // Sql: compiled_sql; Rust: handler id / function pointer / trait object key
    pub body: ModelBody,
}

pub enum ModelBody {
    Sql { raw: String, compiled: String },
    Rust { model_id: RustModelId },
}
```

### 5.3 Design A — UDF architecture

**Registration lifecycle (each `run` / `preview` / `validate` that needs execution):**

1. Build `SessionContext`.
2. Register Iceberg/filesystem catalogs as available.
3. **Register all project UDFs** (and built-in rbt UDFs) into the context.
4. Register bronze sources + execute tiers.
5. SQL models plan with UDF names bound; unknown UDF → fail at validate/plan time with `E_RBT_UDF_NOT_FOUND`.

**UDF categories (DataFusion-aligned):**

| Category | Use when | Example |
|----------|----------|---------|
| Scalar UDF | Row-local pure transform | Ticker normalize, hash, parse venue code |
| Aggregate UDF | Custom reduction | Session VWAP with business calendar |
| Window UDF | Partition/order dependent | Specialized rolling that DF SQL expresses poorly |
| Table UDF / generator (later) | Expand rows | Optional phase 2+ |

**Authoring sketch (project crate):**

```rust
use rbt_engine::udf::{ScalarUdfs, register_project_udfs};
use datafusion::logical_expr::create_udf;
// …

pub fn register(ctx: &SessionContext) -> Result<()> {
    register_project_udfs(ctx, &[
        // stable SQL name: rbt_normalize_ticker
        normalize_ticker_udf(),
        vwap_partial_agg(),
    ])
}
```

**SQL usage:**

```sql
select
  rbt_normalize_ticker(ticker) as ticker,
  trade_time,
  price,
  volume
from {{ source('bronze', 'raw_stock_trades') }}
```

**Stability rules:**

- UDF SQL names are **project-stable contracts** (breaking rename = major project version).
- Prefer prefix `rbt_` for builtins; project UDFs use team prefix (`sm_`, `acme_`) to avoid collisions.
- Determinism: document whether a UDF is deterministic (cache/plan implications).
- Nullability and return types must be declared; validate against usage where possible.

### 5.4 Design B — Rust model architecture

**Trait / handler sketch:**

```rust
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

#[async_trait]
pub trait RustModel: Send + Sync {
    fn name(&self) -> &str;
    fn refs(&self) -> &[&str];
    fn sources(&self) -> &[(&str, &str)]; // (schema, table)
    // optional: layer, materialization overrides

    async fn run(&self, ctx: &RbtContext) -> rbt_engine::Result<RustModelOutput>;
}

pub struct RustModelOutput {
    pub batches: Vec<RecordBatch>, // v1; stream type later
    // pub schema: SchemaRef, implied by batches
}

pub struct RbtContext {
    // resolves ref/source to batches or DF DataFrame
    // exposes session, env, namespace, run_id, config
}

impl RbtContext {
    pub async fn ref_batches(&self, model: &str) -> Result<Vec<RecordBatch>>;
    pub async fn source_batches(&self, schema: &str, table: &str) -> Result<Vec<RecordBatch>>;
    pub async fn ref_df(&self, model: &str) -> Result<datafusion::dataframe::DataFrame>;
    pub fn env(&self) -> &str;
    pub fn namespace(&self) -> &str;
    // pub fn polars(&self) -> …  // optional feature
}
```

**Attribute / inventory sketch (ergonomics, not required for MVP):**

```rust
#[rbt::model(name = "tf_session_bars", layer = "transform", refs = ["stg_stock_trades"])]
async fn tf_session_bars(ctx: &RbtContext) -> Result<Vec<RecordBatch>> {
    let trades = ctx.ref_batches("stg_stock_trades").await?;
    // custom sessionization kernel…
    Ok(out_batches)
}
```

**Execution rules:**

1. Rust model runs only when all dependency nodes in prior tiers completed and registered.
2. Output batches are passed to materializer (same as SQL).
3. Non-empty outputs register as MemTable (or lake table) under `model.name` for downstream `ref`.
4. Empty output policy: **fail or explicit allow_empty** (avoid silent downstream breaks—align with SQL empty-batch policy when fixed).
5. Layer boundaries apply identically to SQL models.

**When to choose B over A:**

| Prefer **UDF (A)** | Prefer **Rust model (B)** |
|--------------------|---------------------------|
| Logic is a function of columns inside a larger SQL statement | Logic *is* the whole transform |
| Planner should see surrounding filters/joins | Multi-step procedural / non-relational graph |
| Analytics author stays in one `.sql` file | Platform-owned performance node |
| Single expression / agg / window | Replaces a chain of intermediate models |

---

## 6. Project layout (target)

```text
my_rbt_project/
├── rbt_project.yml
├── models/
│   ├── bronze/   # or staging/
│   │   └── stg_trades.sql
│   ├── silver/
│   │   ├── tf_1m_bars.sql
│   │   └── …                 # SQL majority
│   └── gold/
│       └── fact_1m_bars.sql
├── rust/                     # optional workspace member or subcrate
│   ├── Cargo.toml            # depends on rbt-engine, arrow, …
│   └── src/
│       ├── lib.rs            # register_models(); register_udfs();
│       ├── models/
│       │   └── tf_session_bars.rs
│       └── udfs/
│           └── normalize_ticker.rs
└── lake/ …
```

**CLI note:** Host binary (`rbt`) either:

- is built **with** the project crate linked (monorepo / custom runner), or  
- loads a **cdylib** specified in `rbt_project.yml` (`rust_extension: ./target/release/libmy_rbt_ext.so`) under an explicit allow policy.

v1 documentation should recommend the **workspace member + custom binary or `rbt` feature path** for safety and simplicity; dynamic load is phase 2.

---

## 7. DAG, select, and CLI semantics

| Concern | Behavior |
|---------|----------|
| `compile` | Discover SQL + registered Rust models; extract/declare deps; build graph; layer checks; UDF names not fully typechecked until plan |
| `validate` | SQL: bind/plan against schemas + UDF signatures. Rust: deps exist; optional dry-run schema contract if model advertises `output_schema` |
| `explain` | SQL: DataFusion plan (+ UDF black boxes). Rust: print model id + deps + “physical: rust_model” |
| `preview` | SQL: `LIMIT N`. Rust: `run` then truncate batches to N (or context `preview_limit`) |
| `run` | Tier execution; both kinds materialize |
| `test` | Assertions on materialization output for either kind |
| `--select` | By model name; works for both kinds |

Rust models **must** appear in selection and documentation like SQL models (manifest entry: `kind: rust`).

---

## 8. Nuances & design tensions

### 8.1 Optimization boundary

A Rust model is an **optimization fence**. Predicates and projections from downstream SQL do not automatically push into the Rust function unless the model implements an explicit interface (future: `supports_pushdown`). UDFs *do* participate in DF planning for their call site but custom kernels may inhibit some rewrites—document cost.

### 8.2 Schema contracts

SQL schemas come from plan output. Rust models should eventually declare:

```yaml
# optional sidecar or attribute
columns:
  - name: ticker
    type: utf8
  - name: session_vwap
    type: f64
```

v1 may infer schema from first batch (fragile). Prefer **declared schema** for validate and consumer contracts.

### 8.3 Streaming vs collect

Today’s engine often `collect()`s. Design B should target `SendableRecordBatchStream` early so large silver nodes do not OOM. UDFs already run inside DF’s execution model.

### 8.4 Determinism & testing

- Unit-test UDFs and Rust models with golden Arrow batches (crate-level `#[test]`).
- Integration-test via sample project select on one Rust node.
- Seed / time: inject clocks via `RbtContext` (no bare `SystemTime` in pure transforms when avoidable).

### 8.5 Security

- Dynamic libraries: version pin, hash allowlist, no download-execute.
- UDFs run in-process: treat as trusted code equal to the binary.
- Multi-tenant servers (if ever): not in scope; assume single-tenant CLI/job.

### 8.6 Error codes (illustrative)

| Code | Meaning |
|------|---------|
| `E_RBT_UDF_NOT_FOUND` | SQL references unregistered function |
| `E_RBT_UDF_TYPE_MISMATCH` | Arg/return types incompatible |
| `E_RBT_RUST_MODEL_NOT_FOUND` | Registry missing model id |
| `E_RBT_RUST_MODEL_DEP` | Declared ref/source missing |
| `E_RBT_RUST_MODEL_SCHEMA` | Output schema contract violated |
| `E_RBT_RUST_MODEL_EMPTY` | Empty output without allow_empty |

### 8.7 Interaction with medallion / publisher-consumer

- Rust models respect the same **layer** and **namespace write** rules as SQL.
- A Rust model may **not** write outside project `publishes` when enforcement exists.
- Consumed shared dims remain `source()` from both SQL and `RbtContext::source_batches`.

### 8.8 Performance guidance (contributor rule)

Do **not** rewrite a simple `group by ticker, bar_1m` SQL model into Rust “for speed” without a profile. Prefer Rust when:

1. Explain plan shows structural pathology, or  
2. Algorithm is non-relational / library-backed, or  
3. One Rust node replaces a fragile multi-model SQL chain with clear maintainability win.

---

## 9. Implementation phases

Aligned with CONTRIBUTING: **P0 SQL spine first**; polyglot lands when seams do not destabilize `compile`/`run`.

| Phase | Deliverable | Exit criteria |
|-------|-------------|---------------|
| **P0** | Document ADR; stub module paths `rbt-engine::udf`, `rbt-engine::rust_model` | ADR accepted; no behavior change required |
| **P1 — Design A MVP** | Trait + register builtins; 1 sample UDF; SQL call works in sample or unit test | **Shipped 0.5.0** — `rbt_upper`/`lower`/`trim`/`nullif_empty` auto-registered |
| **P2 — Design B MVP** | `RustModel` trait + manual registry; 1 sample Rust model in DAG; materialize + ref from downstream SQL | End-to-end: SQL → Rust → SQL or SQL → Rust → gold |
| **P3** | Project extension crate pattern in sample; `output_schema`; better errors | Stockmarket or sibling example documents both A and B |
| **P4** | Stream outputs; optional Polars feature; select/preview parity | Large-batch safe path |
| **P5** | Optional `cdylib` load; inventory macros; table UDFs | Documented plugin policy |

**Do not block** Iceberg SoR, bronze frontmatter hardening, or env path isolation on P2–P5.

---

## 10. Crate responsibilities

| Crate | Responsibility |
|-------|----------------|
| `rbt-core` | `ModelKind`, deps for Rust nodes, discovery metadata, DAG tiers |
| `rbt-engine` | UDF registration helpers, `RbtContext`, Rust model executor, DF integration |
| `rbt-materializer` | Unchanged contract: batches + format + path |
| `rbt-testing` | Unchanged: assertions on batches |
| `rbt-cli` | Ensure registry loaded before run; surface errors |
| Project `rust/` crate | User UDFs + Rust models (not published as part of rbt core) |

Optional later: `rbt-udf` for shared builtin UDF pack (ticker, time-bucket helpers).

---

## 11. Alternatives considered

| Alternative | Outcome |
|-------------|---------|
| **Only SQL forever** | Rejected — leaves no native escape hatch; loses differentiation vs dbt |
| **Only Polars API, no SQL** | Rejected — kills dbt-shaped adoption and analyst UX |
| **`.rsx` blended language** | Rejected for now — see §2.1 / Decision 4.4 |
| **dbt-style Python models** | Rejected as *primary* extension — wrong runtime for a Rust lake engine; Python may appear at edges later but is not Design A/B |
| **WASM plugins for UDFs** | Deferred — strong sandbox story, extra complexity; revisit for untrusted UDF markets |
| **External process per Rust model** | Rejected for default path — breaks zero-copy and ops simplicity |

---

## 12. Consequences

### Positive

- Clear story: “SQL by default, Rust when it matters,” one DAG and lake.
- Arrow-native hybrid beats warehouse dbt Python on locality and copies.
- Encourages measured use of custom kernels without forcing all authors into Rust.
- Future `.rsx` / `sql!` sugar can desugar to Design B without redoing the DAG.

### Negative / costs

- Project tooling becomes a **small Rust workspace** for teams that need B/A beyond builtins.
- Two skill sets in one repo (mitigate with ownership conventions).
- Optimization fences at Rust model boundaries.
- Registry / linking story must stay simple or adoption will stall.

### Neutral

- Sample stockmarket remains SQL-only until P2/P3 add an optional Rust node for illustration (e.g. session bars).

---

## 13. Success metrics

1. A SQL model can call a documented project UDF and pass unit + integration tests.  
2. A Rust model can `ref` a SQL staging table, write gold/silver output, and be `ref`’d by a downstream SQL fact.  
3. Layer boundary violations involving Rust models fail at compile/graph build.  
4. No user-facing docs claim Rust is universally faster than DataFusion SQL.  
5. CONTRIBUTING “should I work on X?” still ranks SQL spine and Iceberg SoR above expanding UDF surface.

---

## 14. References

- Internal: polyglot brainstorm (Design A/B vs `.rsx`), CONTRIBUTING positioning table, thesis MVP order.  
- Apache DataFusion: User-defined functions, aggregates, window functions.  
- Apache Arrow: `RecordBatch` as interchange.  
- dbt: Python models (prior art for multi-language DAG—not the implementation model).  

---

## 15. Decision record (one paragraph)

We will implement **Design A (in-process DataFusion UDFs)** and **Design B (first-class Rust models)** as the only supported polyglot extensions for rbt. Both share the Model DAG, medallion rules, materializer, and Arrow data path. We will **not** implement a blended `.rsx` language in the foreseeable roadmap. SQL remains the default authoring model; Rust is the host escape hatch for functions (A) and whole nodes (B). Implementation is phased after the SQL `compile`/`run` spine is reliable, with UDF registration and a minimal `RustModel` + `RbtContext` API as the first concrete code milestones.

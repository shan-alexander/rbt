---
tags: [plan, design-b, rust-models, polyglot, adr-003]
node_type: plan
aliases: [Design B plan, Rust model nodes]
status: draft
---
# Design B — First-class Rust models (implementation plan)

**Status:** **B1–B5 shipped** (registry, table/upsert/parts, receipt kind, stream output).  
**Parent ADR:** [ADR-003](../adr/ADR_003_UDF_RSMODELS.md)  
**Depends on:** L1 embed surface (shipped), Design A UDFs (shipped), shared materializer  
**Non-goals:** `.rsx` language, untrusted `cdylib` v1, finance kernels in rbt core

---

## 1. Problem

SQL models + scalar UDFs (Design A) cover most medallion transforms. Some nodes are
**whole-operator graphs** that do not fit a single SQL statement well:

- multi-step Arrow pipelines with intermediate schemas  
- non-relational algorithms (sessionization, complex as-of, graph walks)  
- host-owned kernels that should share the **same DAG**, materialize, `ref()`, layers, and receipts  

Design B makes those nodes **first-class**: same layers, topo tiers, materialization, tests,
and lake outputs as SQL models — boundary is always **Arrow `RecordBatch`**.

---

## 2. Product shape (what “highly useful” means)

| Requirement | Acceptance |
|-------------|------------|
| One DAG | Rust and SQL models intermix; topo + layer rules apply |
| Same materialize | `table` / `scoped_replace` / `keyed_upsert` / parts work for Rust outputs |
| Same `ref()` | Downstream SQL can `{{ ref('rust_node') }}` |
| Same bronze | Rust nodes can declare `sources` / use registered bronze tables |
| Host-owned code | Logic lives in the **host crate**, not inside rbt (no domain formulas in core) |
| Fail closed | Missing registry entry, schema contract miss, panic policy → stable `E_RBT_*` |
| Library-first | Works with `DagBuilder` / `ModelSpec` without a `models/` tree |
| Measurable | Smoke example + unit tests; no “Rust is always faster” claims |

---

## 3. Core API sketch

### 3.1 Model kind on the IR

```text
ModelNode {
  kind: Sql | Rust,
  // SQL: compiled_sql, frontmatter…
  // Rust: rust_key: String,  // registry lookup
  //       output_schema: Option<SchemaRef>,  // contract
  //       refs: Vec<String>, sources: Vec<(schema, table)>,
}
```

File frontend (later phase): optional `models/rust.toml` or project registry — **v1 prefers
programmatic registration** (library hosts).

### 3.2 Host registry (mirror UdfPack)

```rust
/// Host implements one or more named Rust models.
pub trait RustModel: Send + Sync {
    fn name(&self) -> &str;  // DAG node name OR key mapped by ModelSpec

    /// Declared output schema (required for empty/zero-row and validate).
    fn output_schema(&self) -> SchemaRef;

    /// Run the transform. Inputs are already registered on `ctx` as tables
    /// (`ref` / `source` names the model declared).
    fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput>;
}

pub struct RustModelContext<'a> {
    pub session: &'a SessionContext,
    pub project_dir: &'a Path,
    pub scope: &'a RunScope,
    pub model_name: &'a str,
    // optional: run_id, contract_version, bronze_fingerprint for lineage
}

pub enum RustModelOutput {
    /// In-memory batches (v1; size-bounded).
    Batches(Vec<RecordBatch>),
    /// Stream for large outputs (v1.1).
    // Stream(SendableRecordBatchStream),
}
```

```rust
// Registration
RbtEngineBuilder::new()
    .with_rust_model(MyModel)
    .with_rust_models(pack)
    .build()
    .await?;

// Or process/engine:
engine.register_rust_model(Arc::new(MyModel))?;
```

**Fail closed:** DAG node with `kind=Rust` and unknown key → `E_RBT_RUST_MODEL`.

### 3.3 Programmatic IR (`DagBuilder` / `ModelSpec`)

```rust
ModelSpec::rust("tf_sessionize")
    .output_schema(schema)
    .refs(["stg_events"])
    .materialization(Materialization::Table)
    .output_path("…/tf_sessionize.parquet")
    .layer(ModelLayer::Transform)
```

`build()` inserts a `ModelNode` with empty `compiled_sql` and `kind=Rust`.

### 3.4 Execution path

```text
execute tier model:
  if SQL → ctx.sql(compiled) → stream → materialize_*
  if Rust →
      resolve RustModel from registry
      (optional) assert deps registered
      out = model.execute(RustModelContext { … })
      validate out schema vs output_schema (column names + types; nullability policy TBD)
      feed batches into existing materialize_stream / upsert / scoped_replace
```

**Critical:** reuse materializer entry points; do not invent a second publish path.

### 3.5 Dependencies

- SQL: `{{ ref() }}` / `{{ source() }}` in text  
- Rust: explicit `refs` + `sources` on the node (engine registers bronze for sources like staging)  
- Graph edges built from those lists at `DagBuilder::build` / project load  

Layer band rules (stg / tf / dim) apply unchanged.

### 3.6 Testing

Frontmatter-style tests on Rust nodes (unique, not_null, accepted_values) run on output
batches the same way stream materialize does today.

Optional: `RustModel::self_test()` hook for host unit tests — not required for v1.

---

## 4. Phased delivery

| Phase | Deliverable | Exit criteria |
|-------|-------------|----------------|
| **B0** | ADR-003 refresh + this plan | Done (plan + ADR note) |
| **B1** | IR: `ModelKind`, `ModelSpec::rust`, registry on engine | **Done** |
| **B2** | Execute Rust → batches → table parquet + keyed_upsert | **Done** — `design_b_sql_rust_sql_ref_chain` |
| **B3** | Parts strategies for Rust (`scoped_replace`, `incremental_append`, table+parts) | **Done** |
| **B4** | Receipt `kind` + `materialization` on `ModelRunResult` | **Done** |
| **B5** | `RustModelOutput::Stream` + `batches_to_stream` | **Done** |
| **B6** | File/project discovery (optional) | Planned — library first |
| **Later** | Optional `cdylib` load policy | Deferred |

**Shipped surface:** `RustModel` + `#[async_trait]`, `RbtEngineBuilder::with_rust_model`, `ModelSpec::rust().refs()`, `E_RBT_RUST_*` codes.

---

## 5. Error codes (proposed)

| Code | When |
|------|------|
| `E_RBT_RUST_MODEL` | Unknown registry key / missing model |
| `E_RBT_RUST_SCHEMA` | Output schema mismatch vs declared |
| `E_RBT_RUST_PANIC` | Optional catch_unwind boundary (policy: default **no** catch — panics fail the run) |
| `E_RBT_RUST_DEPS` | Declared ref/source not registered |

---

## 6. Design choices to confirm (please tweak)

1. **Identity:** Is registry key **equal** to DAG model name, or separate (`rust_key` + `name`)?  
   - *Recommendation:* equal by default; optional alias only if needed.  
2. **Panic policy:** abort run vs catch and `E_RBT_RUST_PANIC`?  
   - *Recommendation:* no catch in v1 (simpler, honest).  
3. **Empty output:** require `output_schema` always, or infer from first batch?  
   - *Recommendation:* **always declare** `output_schema` (aligns with A6 declared schema).  
4. **Async:** `execute` sync vs async?  
   - *Recommendation:* `async` in the trait if we need DF collect inside; else sync + `spawn_blocking` for heavy CPU. Prefer **async fn in trait** (RPITIT) or `execute` returns `BoxFuture` for object safety.  
5. **Object safety:** trait object `Arc<dyn RustModel>` vs generics only?  
   - *Recommendation:* object-safe with boxed future **or** sync execute for v1.  
6. **File frontend:** skip until library path is solid?  
   - *Recommendation:* **yes** — library first (matches L1 embedders).  
7. **Polars inside Rust models:** allowed if host converts to Arrow batches?  
   - *Recommendation:* allowed in host code; rbt only sees Arrow.  

---

## 7. Explicit non-goals

- Shipping domain feature math inside rbt  
- Replacing SQL as the default authoring model  
- Ballista / multi-node Rust models  
- Automatic discovery of arbitrary `.rs` files under `models/` in v1  
- Claiming Rust models are universally faster than DataFusion SQL  

---

## 8. Docs & examples (when implementing)

- ADR-003 status → partially implemented  
- `docs/EMBEDDING.md` section: “Design B Rust models”  
- Minimal **product-agnostic** example (e.g. synthetic events → Rust window sessionize → SQL mart) under `examples/` — **not** desk-specific  
- CHANGELOG entry under next minor  

---

## 9. Success metrics (from ADR-003, refined)

1. SQL model can `ref` a Rust model’s Parquet output.  
2. Rust model can `ref` SQL staging and read bronze via registered sources.  
3. Layer violations involving Rust fail at graph build.  
4. Host registers models without patching rbt.  
5. Docs never claim Rust ≫ SQL without a measure scenario.  

---

## 10. Open follow-ups after B2

- Typed public errors (`thiserror`) for registry/schema  
- `from_session(ctx)` so hosts own the SessionContext  
- Align with partition dirty-plan (L2) when only some scopes need Rust re-run  

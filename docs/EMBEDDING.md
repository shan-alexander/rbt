---
tags: [docs, library, embed, l1]
node_type: concept
aliases: [embedding rbt, single ABI, host migration]
---
# Embedding rbt-datalake (library hosts)

How to consume **`rbt-datalake`** as a library without dual-linking Arrow/DataFusion or
forking the CLI path.

Related: [ADR-004](adr/ADR_004_FEATURE_FLAGS.md) · [ADR-005](adr/ADR_005_DATA_STACK_REEXPORTS.md) ·
[PUBLISHING.md](PUBLISHING.md) · [library antipatterns](analysis/rbt-datalake-library-antipatterns.md)

## Single-ABI rule (P0)

**In any crate that enables rbt’s DAG/SQL path, depend on Arrow / Parquet / DataFusion only via rbt re-exports.**

```rust
use rbt::arrow;
use rbt::parquet;
use rbt::datafusion;
// optional:
// use rbt::iceberg;  // when feature "iceberg" is on
```

Do **not** also add `arrow = "54"` (or any other major) next to `rbt-datalake` in that crate.
Dual majors compile twice, break monomorphic `RecordBatch` boundaries, and waste disk/RAM.

### Workspace recipe (recommended)

Pin the **same** majors rbt uses in the workspace root, and use rbt re-exports in code:

```toml
# Cargo.toml (workspace root)
[workspace.dependencies]
# Keep these aligned with rbt-datalake's Cargo.toml [workspace.dependencies]
arrow = { version = "58.0" }
parquet = { version = "58.0", features = ["arrow"] }
datafusion = { version = "53.1" }

[workspace.dependencies.rbt-datalake]
version = "0.9"
default-features = false
features = ["sql", "parquet"]
# add "jshift", "iceberg", "cli" only where needed
```

```toml
# host crate that runs DAGs
[dependencies]
rbt-datalake = { workspace = true }
# Prefer NOT listing arrow/datafusion here — use rbt::arrow / rbt::datafusion in code.
# If you must list them (e.g. for a non-rbt leaf crate), use workspace = true only:
# arrow = { workspace = true }
```

### Split binaries

| Binary | Link rbt? | Notes |
|--------|-----------|--------|
| Lake worker / batch DAG | Yes | Full or slim features |
| Hot live path (market data, UI) | Prefer **no** | Keep heavy DF out of the hot process |

rbt does not ship a “DF-free” mode; DataFusion is the SQL engine. Slim means drop `iceberg` /
`jshift` / `cli`, not drop SQL.

### CI check (optional)

```bash
# Fail if the dag-enabled package pulls two arrow majors
cargo tree -p your-dag-crate -i arrow | head
```

## Feature profiles

```toml
# Full product (CLI + Iceberg + jshift) — default
rbt-datalake = "0.9"

# Embed / lake worker
rbt-datalake = { version = "0.9", default-features = false, features = ["sql", "parquet"] }
```

| Feature | Default | Role |
|---------|---------|------|
| `sql` | yes | Marker (DataFusion always linked today) |
| `parquet` | yes | Marker (Parquet materialize always linked) |
| `jshift` | yes | Selective JSON path extract |
| `iceberg` | yes | Iceberg catalog deps |
| `cli` | yes | Binary `rbt` |

## Host extension points

| Need | API |
|------|-----|
| Programmatic DAG | `DagBuilder`, `ModelSpec` |
| Skip decision | `ops::plan_skip` / `stage_plan_skip` |
| Stage re-entry | `stage_register_bronze`, `stage_execute_tiers`, `stage_write_receipt` |
| Bronze formats | `register_host_adapter`, `register_named_adapter` |
| UDFs | `UdfPack`, `RbtEngineBuilder::with_udf_pack` |
| Design B Rust models | `RustModel`, `ModelSpec::rust`, `with_rust_model` |

### Design B sketch

```rust
use rbt::{async_trait, ModelSpec, RbtEngineBuilder, RustModel, RustModelContext, RustModelOutput};
use rbt::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

struct MyNode;
#[async_trait]
impl RustModel for MyNode {
    fn name(&self) -> &str { "tf_my_node" }
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }
    async fn execute(&self, ctx: &RustModelContext<'_>) -> anyhow::Result<RustModelOutput> {
        let df = ctx.session.sql(r#"SELECT id FROM "stg_upstream""#).await?;
        Ok(RustModelOutput::Batches(df.collect().await?))
    }
}
// ModelSpec::rust("tf_my_node").refs(["stg_upstream"])
// RbtEngineBuilder::new().with_rust_model(MyNode).build().await?;
```

## `catalog_prefix` for library DAGs (L1.10)

`ModelSpec` defaults to **empty** `catalog_prefix`, so `{{ ref('stg_x') }}` → bare `stg_x`, matching
engine `ref()` registration. Prefer the default for library embeds.

```rust
// Default is correct for most embeds:
ModelSpec::sql("stg_x", "SELECT 1 AS id").output_path(...)

// Only if you intentionally use a catalog schema:
ModelSpec::sql("stg_x", "...").catalog_prefix("rbt")  // needs dual-register or DF catalog setup
```

File-based projects still use their project compiler / template engine prefix independently.

## Anti-patterns

- Second Arrow major in the same package graph as rbt DAG code  
- Shelling out to `rbt run` from a long-lived daemon (use library + receipts)  
- Domain math formulas inside rbt core (register UDFs or Design B Rust models)  
- Product-specific examples in rbt (keep host SoT examples in the host repo)  
- Setting `.catalog_prefix("rbt")` without dual-registering tables (planning fails on `rbt.*`)

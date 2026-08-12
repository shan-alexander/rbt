---
tags: [concept, architecture, stages, sqlite, object-store, systems]
node_type: concept
aliases: [pipeline stages, execute_dag stages, sqlite in rbt, storage backends]
---
# DAG pipeline stages & future storage (concept)

**Not an ADR** — product intuition for implementers. Code today: `engine/stages.rs`, `ops::plan_skip`.

## Single surface, named stages

Users and agents should still learn **one** verb:

```text
execute_dag_with_scope(dag, project, output, config, scope) → Summary
```

That façade must not be a *god object* that re-implements every concern inline. Stages:

| Stage | Responsibility | Today |
|-------|----------------|-------|
| **1. PlanSkip** | Fingerprint bronze + compare receipt | `ops::plan_skip` / `stages::stage_plan_skip` |
| **2. RegisterBronze** | Adapter decode → DF tables | `register_bronze_sources_for_dag_scoped` |
| **3. ExecuteTiers** | Topo SQL + materialize dispatch | loop in `execute_dag_with_scope` |
| **4. WriteReceipt** | Persist run identity | `RunReceipt::write` |

Further splits (materialize_one, WAP) can be extracted without changing the public entry.

**Principle:** one façade for DX; pure stages for systems engineering and library reuse.

## Object store (later)

Plugs under **list + read + atomic publish** inside RegisterBronze / materialize — not a second DAG product. Same Arrow/Parquet model; different keyspace (S3 vs FS).

## SQLite / Postgres — when helpful, when not

### Not as the lake of record

rbt’s SoT for silver/gold is **lake files** (Parquet / optional Iceberg). Putting pipeline truth in SQLite would fight:

- multi-reader lakes  
- Arrow monomorphism  
- DF/Iceberg ecosystem  

A8 “sqlite storage backend” as **host serving** for small dims is different from engine internals.

### Possible *internal* uses (only if measured)

| Use | Value | Cost |
|-----|-------|------|
| Run receipt / skip index | Faster “latest per scope” than many JSON files | New dep + migration story |
| Compile cache (SQL hash → plan) | Cold-start | Complexity; DF already caches sessions |
| Model catalog / lineage graph | Nice UX | Overlap with `.rbt/runs` + future catalog |
| Gold dim “hot” mirror for embeds | Host concern more than core | Scope creep |

**Recommendation for now:** **do not** add `rusqlite` to rbt core. Receipts + fingerprints on FS are honest and enough. Revisit SQLite only if:

1. receipt directory scale becomes a real ops pain, or  
2. we ship an **optional** `feature = "control_plane_sqlite"` for hosts that want it — still not the lake SoT.

Postgres is a **serving / warehouse** concern for hosts; rbt may one day *write* to it as a materialization backend (A8 family), not use it as the engine’s control plane by default.

## Bottom line

- Stages reduce god-surface **thickness** while keeping one execute API.  
- Object store = deployment backend under the same lake model.  
- SQLite is optional control-plane spice, not a core dependency until a concrete stage needs it.

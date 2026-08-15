---
tags: [analysis, concurrency, partition, scheduler, design-b, materialization, user-feedback]
node_type: analysis
aliases:
  - partition concurrent feedback
  - work unit scheduler analysis
  - medallion partition parallelism
status: open
---
# Partition-aware concurrent execution — user feedback vs rbt 0.10.x

**When:** 2026-08-13  
**Baseline:** crates.io **rbt-datalake 0.10.1**  
**Audience:** maintainers + implementers planning the next maturity arc  
**Related goals:** [[Memory-honest materialization]] · [[Honest incremental materialization]] · [[Team-scale lake positioning]] · [[Product north star]] · [[Measured claims before marketing]] · [[Polyglot UDFs and Rust models]]  
**Related plans:** [[Dual-track maturity roadmap]] · [[Design B — First-class Rust models]] · [[rbt-datalake feature roadmap]]  
**Follow-on plan:** [[Partition work units and concurrent scheduler]]

---

## 1. Executive verdict

The feedback is **correct, high-leverage, and product-aligned**. It does **not** ask for a cost-based SQL optimizer or a Spark clone. It asks rbt to finish the contract it already half-built:

> **Partition layout is an execution unit.** Schedule work over that layout with optional concurrency. Keep global plans available when grain is not partition-local.

**0.10.x already has the IR for this** (tiers, multi-value scope, `partition_by` / `part_key`, `scoped_replace` parts, stream writers, Design B Stream output). **The runtime still serializes the easy wins** and treats multi-value scope as one filtered mega-plan. Wall-clock for partition-local medallion lakes (OHLCV + TA per symbol, multi-TF staging) is dominated by **serial I/O + full-table collect/rewrite**, not by Parquet encoding.

This is the natural **P5c → scale honesty** and **T2 fan-out** bridge that dual-track already implies, elevated from “host must shell N processes” to **first-class optional scheduler + layout contract**.

---

## 2. What the user claims (compressed)

| Claim | Implication |
|-------|-------------|
| Parquet is not the bottleneck | Layout + scheduler are |
| Tiers log “parallel” but run serial | Honesty bug or missing L1 concurrency |
| Multi-value scope = IN filter, not fan-out | Correctness path exists; scale path does not |
| `scoped_replace` = peer-safe replace, not concurrent writers | A2 non-goals already said this |
| Design B often `collect()`s full upstream | Need `execute_partition` + part-only input |
| Identity marts rewrite multi-GB | Productize alias / zero-copy |
| Shared `SessionContext` + concurrent register = footgun | Private sessions or process workers |
| Prefer parts/hive + rich manifest over N generated models | One physical contract, many work units |
| Publish both in-process scheduler **and** external work-unit protocol | Matches T1 library + T2 host orchestration |

---

## 3. Codebase ground truth (0.10.1)

### 3.1 Already shipped (foundation — do not reinvent)

| Capability | Location / note | Feedback mapping |
|------------|-----------------|------------------|
| Topo **tiers** | `ModelDag::execution_tiers` | L1 IR ready |
| Multi-value scope | A1: `ScopeValue::Multi`, IN filters | Filter path; not WorkUnit fan-out |
| `scoped_replace` + `part_key` | A2 materializer | Unit of isolation for **correctness** |
| Parts dir + `_rbt_manifest.json` | `scan/parts.rs`, incremental writers | Manifest v1 thin: names + total_rows |
| Stream materialize | P1 / `materialize_stream` | Memory-honest when used |
| Fingerprint skip | A4 bronze + scope identity | **Scope-level only**, not per-part |
| Design B | `RustModel` + `Batches` / `Stream` | Whole-table execute; no partition hook |
| Consolidate | A5 | “One file for humans” already exists |
| Stage re-entry | L1.9 `stage_execute_tiers` | Host can re-enter; still serial inside |
| WAP + `wap_root` | FS publish | Concurrent publish needs lock/WAP discipline |
| Dual-track T2 | host owns orchestration | External workers already envisioned (P8) |

### 3.2 Confirmed gaps (user is right)

| Gap | Evidence |
|-----|----------|
| **Serial tier loop** | `engine/mod.rs`: log `"… N parallel models"` then `for model in tier` |
| **Multi-value = filter** | A1 design + A2.10: one part for whole multi-set / IN filter; fan-out not shipped |
| **Zero-copy reserved** | `Materialization::ZeroCopyClone` + `OutputFormat::ZeroCopyClone`; writer comment: *“currently materializes Parquet (clone semantics later)”* |
| **Thin manifest** | `PartsManifest { strategy, parts: Vec<String>, total_rows, updated_at_ms }` — no keys, bytes, content_fp, sort contract, `parallel_safe` |
| **No WorkUnit IR** | No expand(scope × partition_by) → units; no `--jobs`; no concurrent config |
| **Shared session** | `TransformationEngine` owns one `SessionContext`; Design B gets `&self.ctx` |
| **No ParallelContract** | Design B trait is `execute` only |
| **No per-part dirty skip** | Skip is bronze fingerprint ± full scope |
| **Measure pack missing** | No `concurrent_tier_vs_serial` / `multi_value_in_vs_fanout` |

### 3.3 Historical intent (archive / goals)

- Archive `ADR_001_IMMEDIATE_NEXT_STEPS` already sketched Tokio `JoinSet` + semaphore per tier — **never productized**.
- Grok audit (2026-07-25) listed “true tier parallelism” as open.
- [[Team-scale lake positioning]] and dual-track **P8** document **partitioned workers + Iceberg multi-writer**, not single-process petabyte shuffles.
- A2 roadmap **explicit non-goal:** “Automatic multi-process concurrent writers without locking” — concurrency must be **opt-in with a publish protocol**, not silent.

So the feedback is not a pivot; it is **finishing unfinished architecture** with better layout honesty.

---

## 4. Alignment with product north star

| Goal / thesis | Feedback fit |
|---------------|--------------|
| Medallion DAG, in-process Rust + Arrow + DF | Keep default serial; concurrency opt-in |
| Memory-honest stream materialize | Alias + part streams **reduce** rewrites and collect |
| Honest incremental / parts | Manifest v2 makes parts a real index |
| Measured claims | Phase 0 measure packs are mandatory |
| Team-scale / not Spark | L1+L2 on one node; L-process / external protocol for host fleet |
| Dual-track T2 | External work-unit list + merge = host orchestrator contract |
| Design B | `execute_partition` is the missing scale API for partition-local kernels |
| Do not become generic scheduler | Lake **work units over lake parts**, not Temporal |

**Non-goals to protect:** full cost-based SQL optimizer, Ballista as product identity, inventing 107 YAML models per symbol, requiring a globally sorted monolith.

---

## 5. Three concurrency levels (map to rbt)

```text
L1  Model-tier concurrency     IR exists (execution_tiers); exec serial
L2  Partition-value concurrency  Biggest lake win; multi-value × partition_by
L3  Stream pipeline within model Stream write exists; Design B often collect()s
```

**L2 is highest leverage** for the reporter’s medallion pattern. L1 is cheaper and unblocks multi-TF independent staging. L3 is documentation + API pressure on Design B (prefer Stream; later map_partitions).

---

## 6. Layout contract (agree fully)

Two legal **physical** modes for parallel silver/gold:

| Mode | Shape | Status today |
|------|--------|--------------|
| **A — Parts** | `model.parts/part-{id}.parquet` + manifest | Shipped (hash scope_id; not always key-named) |
| **B — Hive dirs** | `model/symbol=AAPL/data.parquet` | Bronze scan understands hive; **write layout not first-class product** |
| **C — Sorted monolith** | One file, row-group stats | Default `table`; hard concurrent write; keep as **consolidate** product |

**Do not** generate N models. **Do** make the manifest the optimizer index (dirty parts, fan-out, skip, prune, concurrent write safety).

---

## 7. Session / isolation (critical)

User’s blunt warning is correct for in-process concurrency:

> Concurrent models on one shared DataFusion `SessionContext` will bit-rot.

Recommended ownership model (v1 product design):

| Role | Owns |
|------|------|
| Coordinator | DAG plan, semaphore, atomic manifest merge, receipt |
| Worker *i* | Private session **or** read-only catalog snapshot + private temp views |
| Spill | `{project}/.rbt/spill/{run_id}/worker_{i}/` |
| Part writes | WAP then publish into shared parts dir under lock |

**Simpler ship-first alternative (v0.5 of concurrency):** process-level workers (`rbt run --worker-id` / host spawns N single-symbol scopes) and **merge receipts + manifests only**. This matches dual-track T2 and proves the layout contract before Tokio session isolation is perfect.

**Publish both long-term:**

1. In-process concurrent scheduler (feature `concurrent` or config-gated)
2. External work-stealing protocol (JSON WorkUnit list + merge)

---

## 8. Pass-through / zero-copy (easy wall-clock)

Identity marts (`SELECT * FROM {{ ref('upstream') }}`) should not rewrite multi-GB files.

| Today | Target |
|-------|--------|
| `ZeroCopyClone` reserved; writes Parquet | `materialization: alias` / `zero_copy_ref` |
| Consumer path re-encodes | hardlink / symlink / catalog pointer to upstream path |
| Hosts pay 2× rewrite | Engine detects pure identity SQL **or** explicit alias |

This is **Phase 0** leverage: small code, large wall-clock, aligns with memory-honest goal.

---

## 9. Design B gap (partition API)

Shipped:

```text
async fn execute(&self, ctx: &RustModelContext<'_>) -> RustModelOutput
// Stream path exists but hosts often collect full upstream table
```

Missing (user Design B extension — call it **B6**):

```text
fn parallel_contract(&self) -> ParallelContract
async fn execute_partition(&self, ctx, part, input) -> RustModelOutput
```

Engine then: open **one part’s** batches → transform → write **one gold part** → never 1.5 GB mega-batch. Default fallback: whole-table `execute` (compat).

---

## 10. Risks and correctness rules (must not ship without)

1. Concurrent writers only for **disjoint** `part_id` / scope keys.  
2. Manifest update is **atomic merge** (file lock / WAP of `_rbt_manifest.json`).  
3. Models that are not partition-local (global window, cross-symbol join) → **serial mega plan** (`ParallelContract::Global` / `Unknown`).  
4. Fingerprint hierarchy: bronze fp + **per-part content_fp** → dirty-part skip.  
5. Default remains **serial** for 0.10.x compatibility.  
6. Measure before marketing speedups ([[Measured claims before marketing]]).  
7. Do not claim “parallel models” in logs until true or rename.

---

## 11. Priority vs existing roadmap

| Existing epic | Relationship |
|---------------|--------------|
| A1/A2/A4/A5 | **Prerequisites shipped** — extend, don’t redo |
| A9 per-entity report | Natural consumer of WorkUnit failures |
| A11–A13 Iceberg/lint | Orthogonal; can interleave after Phase 0–1 |
| A8 SQLite storage | Do **not** block partition concurrency; lake-first still correct |
| Design B B1–B5 | Shipped; **B6** = partition API |
| Dual-track P8 fan-out | **Absorb** external protocol into this epic |
| P7 merge/SCD | Different axis (row-level SoR); do not conflate |

**Recommendation:** treat **RBT-C** (concurrent partition execution) as the **next principal product arc** after 0.10.1 diagnostics polish — ahead of A8 SQLite and full P7 merge, because it multiplies value of every existing A1–A5 and Design B path for real lakes.

---

## 12. Acceptance sketch (how we know we won)

- Multi-TF independent staging: wall clock with `max_in_flight_models > 1` < serial (measure pack).  
- Multi-value `symbol` × `partition_by: [symbol]` with concurrency: N part files, peer-safe, atomic manifest, no shared-session corruption.  
- Re-run with one dirty symbol: only that part rewrites (per-part fp).  
- Identity mart with alias: ~0 rewrite bytes for gold pass-through.  
- Design B PartitionLocal model never builds full-table batch for multi-symbol scope.  
- `rbt explain --plan` shows WorkUnits; `rbt run --jobs 8` opt-in.  
- Docs never claim parallel when serial.

---

## 13. Conclusion

rbt 0.10 already has **partition-aware materialization**. It lacks a **partition-aware scheduler**, **honest concurrency logs**, **alias materialization**, **hierarchical fingerprints**, and a **Design B partition contract**. The user’s RFC Phases 0–3 are the right systems shape. Implement as principal engineering — layout contract first, concurrency second, optimizer heuristics third — not as a one-off Tokio `spawn` on the shared session.

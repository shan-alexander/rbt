---
tags: [plan, concurrency, partition, work-unit, scheduler, design-b, materialization]
node_type: plan
status: backlog
aliases:
  - Partition work units and concurrent scheduler
  - RBT-C concurrent execution
  - concurrent partition scheduler plan
---
# Partition work units & concurrent scheduler (RBT-C)

**Status:** in_progress (Phases 0–3 implemented Unreleased → 0.11)  
**Baseline:** rbt-datalake **0.10.1**  
**Analysis:** [[Partition-aware concurrent execution — user feedback vs rbt 0.10.x]]  
**Depends on:** A1 multi-value scope · A2 scoped_replace · A4 fingerprints · A5 parts/consolidate · Design B B1–B5 · stream materialize · L1 stages  
**Unlocks goals:** [[Memory-honest materialization]] · [[Team-scale lake positioning]] · [[Measured claims before marketing]] · dual-track **T2** fan-out  
**Non-goals:** cost-based SQL optimizer · Ballista product identity · N generated models per partition · silent concurrent writers on one shared DF session

---

## Intent

Make **partition layout a first-class execution contract**, then schedule **WorkUnits** over that layout with **optional** concurrency:

1. **Honesty** — logs, docs, and measure packs match runtime.  
2. **Alias / zero-copy** — identity marts stop rewriting multi-GB files.  
3. **L1** — concurrent independent models within a tier (bounded).  
4. **L2** — multi-value scope × `partition_by` → partition WorkUnits (highest leverage).  
5. **Design B B6** — `ParallelContract` + `execute_partition` + part-only input.  
6. **Manifest v2** — stats, content fingerprints, sort contract, `parallel_safe`.  
7. **Dual publish** — in-process scheduler **and** external work-unit protocol for hosts.

Default remains **serial** (0.10.x compat). All concurrency is **opt-in**.

---

## Architecture principles (principal systems design)

### P1 — Layout before threads

Concurrency without a part/hive contract is a race. Ship / harden layout + manifest merge before max_workers > 1.

### P2 — Isolation unit = part

| Layer | Isolation |
|-------|-----------|
| Correctness (A2 today) | `part-{scope_id}` replace, peers kept |
| Concurrency (this epic) | Disjoint `part_id` only; atomic manifest merge |
| Fingerprint | Per-part `content_fp` + table_fp over sorted part fps |

### P3 — Session isolation

```text
Coordinator  → DAG, WorkUnit plan, semaphore, manifest CAS, receipt
Worker i     → private SessionContext (or RO catalog snapshot + private temps)
Spill        → {project}/.rbt/spill/{run_id}/worker_{i}/
Publish      → WAP part → lock → merge manifest → publish
```

**Do not** register concurrent tables on one shared `SessionContext` in production paths.

### P4 — Two delivery vehicles

| Vehicle | Use when | Ship order |
|---------|----------|------------|
| **External workers** | Host already fans out; prove layout + merge | Phase 1a (fast proof) |
| **In-process** | Single CLI / library run, `execution.concurrency` | Phase 1b (feature `concurrent` optional) |

### P5 — Classify before fan-out

```text
ParallelContract / SQL heuristics
  PartitionLocal | MapOnly  → eligible for L2 fan-out
  Global | Unknown          → serial mega plan (IN filter path today)
```

### P6 — Measure, then market

Every phase ships a measure scenario or criterion bench. No “N× faster” in README without packs.

---

## Target IR (shared by CLI, library, external protocol)

```text
WorkUnit {
  id: String,
  model: String,
  partition_bindings: BTreeMap<String, String>,  // optional
  input_parts: Vec<PartRef>,                     // from upstream manifests
  output_part: Option<PartRef>,
  deps: Vec<WorkUnitId>,
  estimated_bytes: Option<u64>,
  estimated_rows: Option<u64>,
  parallel_contract: ParallelContract,
}

ExecutionPlan {
  strategy: serial | model_tier | partition | auto,
  max_workers: usize,
  units: Vec<WorkUnit>,
  barriers: …  // global models wait for upstream partition writers
}
```

CLI:

```bash
rbt run -p proj --jobs 8
rbt run -p proj --execution-strategy partition
rbt explain -p proj --plan          # prints WorkUnits + strategy
rbt work-units -p proj --json       # external protocol export
rbt merge-manifests …               # or merge via run --merge-only
```

Config (`rbt_project.yml`):

```yaml
execution:
  concurrency:
    enabled: false              # default: serial (compat)
    max_workers: 1
    strategy: serial            # serial | model_tier | partition | auto
    multi_value_fanout_threshold: 4
    max_inflight_bytes: null    # Phase 3
    partition_keys: null        # inherit model.partition_by / part_key
```

---

## Phase map (RFC checklist → engineering)

### Phase 0 — Honesty / quick wins (0.10.x or 0.11.0)

| ID | Task | Acceptance |
|----|------|------------|
| **C0.1** | Rename tier log to `"tier with N independent models (serial exec)"` **or** implement L1 concurrent in same release | Log never says parallel unless concurrent |
| **C0.2** | Document: multi-value scope = **filter**, not fan-out (`COMPLEX_BRONZE`, README, EMBEDDING) | Doc test / link from explain |
| **C0.3** | **Alias / zero-copy materialization** | See § Alias below |
| **C0.4** | Measure pack skeletons: `concurrent_tier_vs_serial`, `multi_value_in_vs_fanout` (serial baseline first) | `rbt measure` scenarios exist |
| **C0.5** | Doc: Design B prefer `RustModelOutput::Stream`; collect only small dims | EMBEDDING + design-b plan |
| **C0.6** | Optional: auto-detect pure `SELECT * FROM ref(X)` → suggest alias in `rbt doctor` / explain | Non-blocking warning |

**Exit criteria:** users understand true behavior; identity marts can avoid rewrite; baselines measured.

#### Alias / zero-copy (C0.3 detail)

Productize reserved surface:

```yaml
# mart is identity of upstream
materialization: alias   # aliases: zero_copy_ref, zero_copy_clone
ref: tf_indicators_1m    # or infer single ref() dependency
```

Implementation options (prefer in order):

1. **Catalog alias** — register consumer name → same lake path as upstream (no new file).  
2. **Hardlink** (same volume) / **symlink** (fallback) of parquet or parts dir.  
3. **Sidecar pointer** `_rbt_alias.json` → upstream path; `ref()` resolver follows.

Fail closed on: multi-ref without explicit `ref:`, format mismatch, parts→monolith mismatch.

Also wire `Materialization::ZeroCopyClone` / `OutputFormat::ZeroCopyClone` to **not** re-encode Parquet.

---

### Phase 1 — Partition work units (highest leverage) → 0.11 / 0.12

| ID | Task | Detail |
|----|------|--------|
| **C1.1** | `execution.concurrency` config | Parse + defaults serial; CLI `--jobs` overrides `max_workers` |
| **C1.2** | WorkUnit planner | Expand multi-value when `partition_by ⊆ multi vars` and size ≥ threshold and model eligible |
| **C1.3** | Eligibility heuristic v1 | `scoped_replace` + `part_key`/`partition_by` present → PartitionLocal candidate; SQL with window over all symbols without PARTITION BY → Global (conservative: Unknown → serial) |
| **C1.4** | **External protocol v1** | `rbt work-units --json` export; host runs N× `rbt run --select M --var symbol=X`; `rbt merge-receipts` / manifest merge helper |
| **C1.5** | Concurrent **scoped_replace** writes | Per-unit private stream write to `part-{id}`; coordinator merges manifest under lock |
| **C1.6** | Atomic manifest merge | CAS/WAP `_rbt_manifest.json`; retries on conflict |
| **C1.7** | Per-part fingerprint | `content_fp` on write; dirty-part skip when bronze+scope subset clean |
| **C1.8** | L1 model-tier concurrent (in-process) | Semaphore + private sessions per model in tier; feature `concurrent` |
| **C1.9** | L2 partition concurrent (in-process) | Same isolation; fan-out WorkUnits |
| **C1.10** | CLI `--jobs` / `explain --plan` | Plan JSON includes units, strategy, estimated counts |
| **C1.11** | Spill dirs | `.rbt/spill/{run_id}/worker_{i}/` |
| **C1.12** | Tests | Disjoint parts; race on manifest; serial fallback Global; multi-value threshold |
| **C1.13** | Measure | Fan-out vs IN-filter wall_ms + peak RSS on synthetic multi-symbol |

**Recommended ship order inside Phase 1:**

```text
C1.1 config → C1.2 planner (serial exec of units still OK) → C1.4 external protocol
  → C1.5–C1.7 manifest/fp → C1.8 L1 concurrent → C1.9 L2 concurrent → CLI polish
```

External-first proves layout without DF session isolation complexity.

**Correctness rules (document in COMPLEX_BRONZE):**

1. Isolation unit = part file.  
2. Concurrent workers only write disjoint part keys.  
3. Manifest merge atomic.  
4. Non-partition-local SQL opt-out → serial mega plan.  
5. Receipts may be scope-level or list per-unit results (prefer per-unit for A9 later).

---

### Phase 2 — Design B partition API (B6) → 0.12 / 0.13

| ID | Task | Detail |
|----|------|--------|
| **C2.1** | `ParallelContract` enum | `Unknown`, `Global`, `PartitionLocal { keys }`, `MapOnly` |
| **C2.2** | `RustModel::parallel_contract()` | Default `Unknown` |
| **C2.3** | `execute_partition(...)` | Default returns “not implemented” → engine falls back to `execute` |
| **C2.4** | `PartitionInput` | Stream/batches for **one** part only (engine opens upstream part paths from manifest) |
| **C2.5** | Engine path | If contract PartitionLocal and units fan-out → call `execute_partition` per unit |
| **C2.6** | Document Stream default | Recommend Stream; collect only dims; update ADR-003 / design-b plan |
| **C2.7** | Optional `map_partitions` later | Host closure over batches — not blocking |
| **C2.8** | Bench | Design B partition-local vs full collect multi-symbol |

```rust
#[async_trait]
pub trait RustModel: Send + Sync {
    fn name(&self) -> &str;
    fn output_schema(&self) -> SchemaRef;

    async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput>;

    fn parallel_contract(&self) -> ParallelContract {
        ParallelContract::Unknown
    }

    async fn execute_partition(
        &self,
        ctx: &RustModelContext<'_>,
        part: &PartitionKey,
        input: PartitionInput,
    ) -> Result<RustModelOutput> {
        let _ = (ctx, part, input);
        bail!("E_RBT_RUST_PARTITION: execute_partition not implemented")
    }
}
```

---

### Phase 3 — Smarter layout & heuristics → 0.13+

| ID | Task | Detail |
|----|------|--------|
| **C3.1** | Manifest schema v2 | grain, partition_by, sort_within_part, parts[{keys, path, rows, bytes, content_fp, stats, min/max}], table_fp, parallel_safe |
| **C3.2** | Backward compat | v1 manifests still load; writers emit v2 |
| **C3.3** | Optional hive target layout | `layout: parts | hive` on model / project |
| **C3.4** | consolidate remains human path | No change to product role |
| **C3.5** | Cost heuristics | bytes-aware scheduling, max_inflight_bytes, large parts first |
| **C3.6** | Row-group stats optional | min/max for prune on read |
| **C3.7** | Doctor / explain | Surface parallel_safe, dirty parts, strategy choice |

---

## Module / file plan (implementation sketch)

| Area | Path | Change |
|------|------|--------|
| Config | `core/project.rs` | `ExecutionConfig` / concurrency block |
| Plan IR | `core/work_unit.rs` **new** | WorkUnit, ExecutionPlan, ParallelContract |
| Planner | `engine/plan.rs` **new** | expand multi-value, classify, barriers |
| Scheduler | `engine/scheduler.rs` **new** | serial / concurrent; semaphore |
| Session factory | `engine/session.rs` **new** | private SessionContext builders |
| Manifest | `scan/parts.rs` + materializer | v2 schema, merge, content_fp |
| Alias | `materializer/alias.rs` **new** + engine dispatch | zero-copy publish |
| Design B | `engine/rust_model.rs` | B6 APIs |
| CLI | `main.rs` | `--jobs`, `explain --plan`, `work-units` |
| Measure | `measure/` | concurrent packs |
| Feature | `Cargo.toml` | optional `concurrent` (tokio multi-thread already present) |

Reuse: `materialize_scoped_replace_stream`, WAP, `scope_part_id`, receipts, stages.

---

## Versioning sketch

| Release | Content |
|---------|---------|
| **0.10.2 / 0.11.0** | Phase 0 (honesty, alias, measure baselines, docs) |
| **0.11.x** | Phase 1a–1b (planner serial units, external protocol, then concurrent) |
| **0.12.x** | Phase 2 Design B B6 + harden concurrent |
| **0.13.x** | Phase 3 manifest v2 / hive / heuristics |

Exact numbers flexible; **do not** ship concurrent writers without C1.5–C1.6.

---

## Error codes (new / extended)

| Code | When |
|------|------|
| `E_RBT_CONCURRENT` | Concurrency enabled but unsafe (shared session, overlapping parts) |
| `E_RBT_WORK_UNIT` | Plan expansion failure |
| `E_RBT_MANIFEST_MERGE` | CAS/conflict exhausted |
| `E_RBT_PART_FP` | Fingerprint mismatch / skip logic |
| `E_RBT_ALIAS` | Alias materialization invalid |
| `E_RBT_RUST_PARTITION` | Partition API misuse / missing impl when required |
| `E_RBT_PARALLEL_CONTRACT` | Global model forced into partition strategy |

---

## Testing strategy

| Layer | Coverage |
|-------|----------|
| Unit | planner expansion, eligibility, manifest merge CAS, part_fp stability |
| Integration | `examples/a1` / `a2` extended: multi-value fan-out parts count |
| Concurrency stress | 8 workers, 32 symbols, scramble order; peers intact |
| Isolation | concurrent register footgun regression (must use private sessions) |
| Alias | identity mart bytes_written ≈ 0 |
| Design B | execute_partition only opens one part path (mock FS counters) |
| Measure | checked-in sample JSON shapes for packs |

---

## Relationship to dual-track / T2

```text
T1 open-core: in-process optional scheduler + parts/hive contract + alias
T2 host:      work-units JSON + N scoped rbt processes + merge manifests/receipts
```

Hosts that already orchestrate should adopt **external protocol** first; pure CLI lakes adopt **in-process** when stable.

Petabyte lakes remain **many workers × scoped slices**, not one SQL shuffle — consistent with [[Team-scale lake positioning]] and dual-track P8.

---

## Explicit non-goals (this plan)

- [~] Replacing DataFusion’s SQL optimizer  
- [~] Embedding Temporal/Airflow  
- [~] Generating one YAML model per partition value  
- [~] Requiring sorted multi-symbol monolith for correctness  
- [~] Shared-session concurrent `register_table`  
- [~] SQLite storage (A8) as prerequisite  
- [~] Full Iceberg multi-writer commit protocol (later; parts FS first)

---

## Open decisions (resolve in Phase 0 ADR or first PR)

1. **Default fan-out threshold** (proposal: 4 multi values).  
2. **Alias mechanism** (catalog pointer vs hardlink vs symlink) — prefer catalog + hardlink.  
3. **Feature flag** `concurrent` vs always-compiled opt-in config — prefer config always, heavy isolation code behind feature if compile cost bites.  
4. **SQL auto ParallelContract** — v1 conservative (Unknown unless frontmatter `parallel_safe: true` or Design B contract).  
5. **Receipt shape** — single receipt with nested unit results vs N receipt files.

---

## Backlog checkboxes

### Phase 0

- [x] C0.1 Honest tier log  
- [x] C0.2 Document multi-value = filter  
- [x] C0.3 Alias / zero-copy materialization  
- [x] C0.4 Measure pack baselines  
- [x] C0.5 Design B Stream docs  
- [x] C0.6 Doctor/explain identity-mart hint  

### Phase 1

- [x] C1.1 execution.concurrency config  
- [x] C1.2 WorkUnit planner  
- [x] C1.3 Eligibility heuristic v1  
- [x] C1.4 External work-unit protocol  
- [x] C1.5 Concurrent scoped_replace  
- [x] C1.6 Atomic manifest merge  
- [x] C1.7 Per-part fingerprint + dirty skip  
- [x] C1.8 L1 model-tier concurrent  
- [x] C1.9 L2 partition concurrent  
- [x] C1.10 CLI --jobs / explain --plan  
- [x] C1.11 Spill dirs  
- [x] C1.12 Tests  
- [x] C1.13 Measure fan-out  

### Phase 2

- [x] C2.1–C2.8 Design B ParallelContract + execute_partition (engine PartitionInput, fallback to execute)

### Phase 3

- [x] C3.1–C3.7 Manifest v2, hive layout, cost heuristics, doctor/explain

---

## Success metrics

| Metric | Target (directional) |
|--------|----------------------|
| Identity mart rewrite bytes | → ~0 with alias |
| Multi-TF independent staging wall | ~min(serial, models/workers) with L1 |
| Multi-symbol PartitionLocal wall | near-linear to ~num_cpus for I/O bound |
| Dirty single symbol re-run | O(1 part) rewrite |
| Shared-session corruption incidents | 0 (isolation tests) |

---

## Related

- Analysis: [[Partition-aware concurrent execution — user feedback vs rbt 0.10.x]]  
- Goals: [[Memory-honest materialization]] · [[Team-scale lake positioning]] · [[Measured claims before marketing]]  
- Plans: [[Dual-track maturity roadmap]] · [[Design B — First-class Rust models]] · [[rbt-datalake feature roadmap]]  
- Concepts: [[COMPLEX_BRONZE_AND_RUN_SCOPE]] · [[STREAMING_MATERIALIZE_PLAN]] · [[REF_STRATEGY]]

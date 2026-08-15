---
tags: [plan, roadmap, maturity, bronze, gold, dual-track]
node_type: plan
status: in_progress
aliases:
  - dual-track roadmap
  - maturity roadmap
  - P5+ roadmap
  - Dual-track maturity roadmap
---
# Dual-track maturity roadmap

## Status

in_progress

## Intent

Evolve **rbt** into a crate real lake engineers can trust for:

1. **Complex bronze landing zones → silver tables** (multi-artifact Hive-ish trees, optional sources, scoped runs, honest publish/idempotency primitives).
2. **Silver → gold** star-schema marts with parts-aware sources, stronger tests, merge/SCD when SoR allows, remote catalog later.

Run **two tracks** without collapsing them:

| Track | Audience | Success |
|-------|----------|---------|
| **T1 — Open-core product** | Any medallion lake team | General primitives in `rbt-datalake`; docs honest; measure-backed |
| **T2 — Host-integrated confidence** | Teams with durable orchestrators + domain fan-out | Host owns receipts/layout contracts; rbt is scoped **subprocess** DAG executor |

Do **not** invent a second state/idempotency contract for a host fleet — emit/consume the approved one, or provide **compatible fields** hosts validate.

Unlocks goals: [[Complex bronze landing zones]], [[Primary path spine]], [[Iceberg system of record]], [[Measured claims before marketing]], [[Honest product surface]].

Analysis backbone: [[Bronze-to-silver maturity gap matrix]].

## Priority / order (execution sequence)

1. **P5a — Scoped lakes & optional sources** (T1; unblocks complex bronze)
2. **P5b — Job contract** (RunReceipt, fingerprint, skip-if-match)
3. **P5c — Prove scale** (measure packs at whale-ish partition sizes)
4. **P6 — Gold default surface** (external parts, lineage, tests, env docs)
5. **P7 — Merge / SCD** (when Iceberg/SoR supports honest upsert)
6. **P8 — Remote SoR** (object_store + REST catalog)
7. **P9 — Polyglot depth** (project UDF packs, Design B)
8. **Parallel — Simple multi-artifact bronze example** (no outer spine)
9. **T2 host** — vendor/simple silver or gold wrap first; outer-spine product stage only after High confidence

## Backlog

### P5a — Scoped lakes & optional sources

- [x] CLI/env **run vars** + partition binds for whole DAG (`report_date`, `run_id`, arbitrary keys) — generalizes B3
- [x] **Optional sources**: missing artifact → empty **typed** frame (B2/B5 foundation)
- [x] Document Hive-ish multi-artifact bronze pattern under [[Complex bronze landing zones]]
- [x] Example `examples/complex_bronze_landing` + [COMPLEX_BRONZE_AND_RUN_SCOPE.md](../COMPLEX_BRONZE_AND_RUN_SCOPE.md)

### P5b — Job contract (idempotency primitives)

- [x] **RunReceipt** JSON (rows, status, fingerprint, vars, contract_version) — B21
- [x] Content **fingerprint** over input bronze set + **contract_version** — B13
- [x] Skip materialize when fingerprint + contract match (identical re-drive) — B17
- [x] Wire receipt after publish (FS atomic publish already Done) — B14/B15

### P5c — Prove scale

- [x] Measure scenarios: `stream_vs_collect`, `whale_synthetic`, `complex_bronze` — B19
- [x] ModeCompare JSON (stream vs collect wall + RSS) — directional VmRSS
- [ ] Deterministic ordering enforcement for fingerprint stability (ORDER BY product flag) — B20 optional
- [x] No public “beats X engine” claims without packs — [[Measured claims before marketing]]

### P6 — Gold default

- [x] External **parts-dir sources** (+ optional manifest awareness) — G1
- [x] Lineage stamp helpers (`lineage_stamp: true`) — G8
- [x] Stronger grain / FK-ish / not_null test tiers (`tests.relationships`) — G6
- [x] Completeness filter patterns documented (G2) in GOLD_DEFAULT.md
- [x] Env roots cookbook (G10) in GOLD_DEFAULT.md

### P7 — Merge / SCD / OBT

- [ ] Honest `incremental_merge` or Iceberg MERGE path — G4
- [ ] **SCD2 dimensions** (valid_from/to, is_active, open-end 2099-12-31, Unknown −1) — first-class
- [ ] As-of / snapshot filters by business date (+ optional run id) — G5
- [ ] First-class **OBT** layer rules (from dim+fact only; not core star bypass)
- [ ] Align with [[Star schema data modeling rules]]

### P8 — Remote SoR & petabyte *workers*

- [ ] `object_store` bronze + lake write when committed product path
- [ ] REST / Lakekeeper (or one real remote catalog) after local proof stays green — G7
- [ ] Document **partitioned fan-out**: many rbt workers, Iceberg multi-writer, host orchestrator — petabyte *lake size*, single-node job slices
- [ ] **RBT-C external work-unit protocol** (JSON units + merge) — first-class host fan-out; see [[Partition work units and concurrent scheduler]]

### P8b — Partition work units & concurrent scheduler (RBT-C) — *next principal arc*

> Promoted from user feedback (2026-08) + analysis [[Partition-aware concurrent execution — user feedback vs rbt 0.10.x]].  
> **Does not** require remote object_store first: FS parts + optional in-process workers.

- [x] Phase 0: honest tier logs, multi-value=filter docs, **alias/zero-copy**, measure baselines (Unreleased / toward 0.11)
- [x] Phase 1: WorkUnit planner, external protocol, concurrent scoped_replace, per-part fp, L1/L2 concurrency (Unreleased)
- [x] Phase 2: Design B `ParallelContract` + `execute_partition` (B6) (Unreleased)
- [x] Phase 3: manifest v2, optional hive write layout, bytes-aware heuristics (Unreleased)

### P9 — Polyglot

- [x] Design B Rust models MVP (B1–B5 in **0.10.0**) — [[Polyglot UDFs and Rust models]] / [[ADR-003 Polyglot DAG]]
- [ ] Project UDF packs (Design A beyond builtins)
- [ ] Design B B6 partition API — under RBT-C Phase 2

### Parallel easy win

- [ ] Example project: multi-artifact bronze (plan + optional success + failures) → silver tables with outer-join SQL **after** P5a; or simpler vendor-style single-family bronze first
- [ ] CI smoke remains fast; large packs local/nightly

### Track 2 — Host integration (not in-core product theater)

- [ ] Subprocess pattern: orchestrator owns CLI contract + receipts; `rbt run -p …` with lake root + partition vars
- [ ] First wrap: **simple silver** or **gold** (no outer multi-source spine)
- [ ] Feature-flag host backend only when High on B6–B18 class gates
- [ ] Never double-write same silver table from two engines for one generation
- [ ] Prefer subprocess until Arrow/DF versions align with host workspace

## In Progress

- [/] P7 merge / SCD (Iceberg-backed upsert)

## QA

- [?] Gap matrix reviewed against 0.5.0 code surfaces (multi-root, stream, WAP, incremental_append, Iceberg proof)

## Done

- [x] P0 spine (compile/run/test)
- [x] P1 stream + spill
- [x] P2 local Iceberg catalog commit proof
- [x] P3 validate/explain/preview
- [x] P4 slice: measure, incremental_append, FS WAP, builtin UDFs
- [x] Product goals + ADRs + star-schema concept in rustbrain
- [x] Gap matrix analysis + dual-track plan + [[Complex bronze landing zones]] goal
- [x] P5a + P5b shipped in **0.6.0** (run scope, empty sources, receipts, skip-if-match)
- [x] P5c measure packs (`stream_vs_collect`, `whale_synthetic`, `complex_bronze`)
- [x] P6 gold default (**0.7.0**): parts sources, lineage stamps, relationships

## Cancelled

- [~] Treating multi-catalog sprawl as near-term core work
- [~] In-tree product-specific status enums as engine API
- [~] Claiming StageAppend-class SoT replacement without High confidence gates

## Blocked

- [!] Full keyed merge/SCD until Iceberg SoR depth + design ADR
- [!] Public Spark comparison packs until external baseline env exists

## Out of scope (this plan)

- Building ingest/scrape/CAS writers inside rbt
- Embedding a durable workflow engine inside rbt
- Petabyte **single-process** shuffles as a v0 architecture (petabyte **lakes via fan-out** is in scope for docs/design under P8)
- Warehouse-dbt Cloud parity checklists

## Petabyte note (positioning, not a milestone)

rbt is not required to be a global shuffle fabric. **Petabyte-scale work with rbt** means:

- Host fans out partition keys (domain, date, warehouse shard, …)
- Each rbt process owns a **slice** (tens of GB → low TB relevant data)
- Streaming materialize bounds RSS; Iceberg/catalog owns multi-writer table truth
- Elephant cross-partition joins may still use Spark/Trino — rbt owns declared medallion DAGs on pruned slices

## Related

- Analysis: [[Bronze-to-silver maturity gap matrix]]
- Goal: [[Complex bronze landing zones]]
- Goals: [[Product north star]], [[Primary path spine]], [[Bronze contracts multi-root and path_glob]], [[Memory-honest materialization]], [[Iceberg system of record]], [[Honest incremental materialization]], [[Filesystem write-audit-publish]], [[Measured claims before marketing]], [[Honest product surface]]
- ADRs: [[ADR-001 Project Layout]], [[ADR-002 Thesis Alignment]], [[ADR-003 Polyglot DAG]]
- Concept: [[Star schema data modeling rules]]

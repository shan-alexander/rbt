RBT Code Review: Intent, State, and Recommendations

What this crate is trying to be

Core thesis (thesis.md (thesis.md)): a Rust-native, dbt-shaped transform layer for data lakes — not warehouse SQL pushdown — with:

┌─────────────┬────────────────────────────────────────┐
│ Pillar      │ Intent                                 │
├─────────────┼────────────────────────────────────────┤
│ Positioning │ Replace Spark+dbt for team-scale lakes │
│             │ (tens of GB → low TBs), not petabyte   │
│             │ shuffles                               │
├─────────────┼────────────────────────────────────────┤
│ Bronze edge │ Selective JSONL/CSV/Parquet ingest (   │
│             │ jshift, projection) → Arrow            │
├─────────────┼────────────────────────────────────────┤
│ Table truth │ Apache Iceberg via official Rust       │
│             │ crates + iceberg-datafusion            │
├─────────────┼────────────────────────────────────────┤
│ Compute     │ In-process DataFusion (docs also       │
│             │ mention Polars; code is DataFusion     │
│             │ -only)                                 │
├─────────────┼────────────────────────────────────────┤
│ Modeling    │ Medallion (bronze/silver/gold) +       │
│             │ Kimball (dims/facts/grains/            │
│             │ relationships)                         │
├─────────────┼────────────────────────────────────────┤
│ DX          │ validate → explain → preview → run →   │
│             │ test, agent-repairable diagnostics     │
├─────────────┼────────────────────────────────────────┤
│ Ops         │ Lightweight DAG engine so transforms   │
│             │ are declared models, not ad-hoc        │
│             │ scripts                                │
└─────────────┴────────────────────────────────────────┘

The opinionated project shape from the newer ADR (docs/adr_001_project_structure_and_zero_copy_cloning.md) is real product design, not fluff:

• Layers: staging/ (stg_) → transforms/ (tf_) → marts/ (dim_/fact_/obt_)
• Layer boundary enforcement on the DAG
• Frontmatter on staging SQL for lake scan config
• Layer-aware output paths (lake/silver, lake/gold)

The sample stock-market project is the clearest expression of that intent and actually runs end-to-end on the current debug CLI (4 tiers, 48 rows, ~250ms).

───

Reality check: docs vs code

You have three overlapping document eras that disagree:

┌────────────────┬───────────────────┬─────────────────┐
│ Artifact       │ Era / tone        │ Alignment with  │
│                │                   │ code            │
├────────────────┼───────────────────┼─────────────────┤
│ thesis.md      │ Best product      │ North star —    │
│                │ spec: honest      │ still the right │
│                │ scope, MVP order, │ guide           │
│                │ measure packs     │                 │
├────────────────┼───────────────────┼─────────────────┤
│ ARCHITECTURE   │ Enterprise        │ Mostly          │
│ .md + paradigm │ marketing (WAP,   │ aspirational    │
│ paper          │ OpenLineage,      │                 │
│                │ multi-catalog)    │                 │
├────────────────┼───────────────────┼─────────────────┤
│ docs/ADR_001_  │ Post-P0 execution │ Partially       │
│ IMMEDIATE      │ plan              │ started;        │
│ _NEXT_STEPS.md │                   │ Iceberg/WAP     │
│                │                   │ still stubs     │
├────────────────┼───────────────────┼─────────────────┤
│ docs/adr_001_  │ Project layout +  │ Most            │
│ …zero_copy…    │ layer rules       │ implemented of  │
│                │                   │ the ADRs        │
├────────────────┼───────────────────┼─────────────────┤
│ docs/LOW_      │ Learning notes (  │ Not product     │
│ LEVEL_RUST…    │ SIMD, io_uring,   │ requirements    │
│                │ jemalloc…)        │ for v0          │
└────────────────┴───────────────────┴─────────────────┘

What actually works today

┌──────────────────────────────┬───────────────────────┐
│ Component                    │ Status                │
├──────────────────────────────┼───────────────────────┤
│ rbt-core DAG, refs/sources,  │ Solid foundation      │
│ frontmatter parse, layer     │                       │
│ rules, project loader        │                       │
├──────────────────────────────┼───────────────────────┤
│ rbt-engine DataFusion SQL +  │ Working demo path     │
│ tiered DAG + MemTable        │                       │
│ handoff                      │                       │
├──────────────────────────────┼───────────────────────┤
│ rbt-materializer Parquet     │ Working; Iceberg      │
│ /JSONL/CSV write             │ empty                 │
├──────────────────────────────┼───────────────────────┤
│ rbt-json jshift → Arrow      │ Working unit-level    │
├──────────────────────────────┼───────────────────────┤
│ rbt-scan multi-format scan   │ Working unit-level,   │
│                              │ not wired into CLI/   │
│                              │ engine                │
├──────────────────────────────┼───────────────────────┤
│ rbt-catalog                  │ Thin wrapper; no      │
│                              │ REST/Glue factories   │
├──────────────────────────────┼───────────────────────┤
│ rbt-testing                  │ Basic not_null /      │
│                              │ unique / accepted     │
│                              │ _values               │
├──────────────────────────────┼───────────────────────┤
│ rbt-models                   │ Structs only, unused  │
├──────────────────────────────┼───────────────────────┤
│ CLI compile / run            │ Works on sample;      │
│                              │ select ignored;       │
│                              │ bronze path hardcoded │
├──────────────────────────────┼───────────────────────┤
│ CLI test / validate /        │ Stub or missing       │
│ explain / preview            │                       │
├──────────────────────────────┼───────────────────────┤
│ Iceberg commits / WAP /      │ Not implemented       │
│ prost diagnostics            │                       │
└──────────────────────────────┴───────────────────────┘

───

Code review findings

Strengths

1. Right crate boundaries for the thesis — core / scan / json / engine / materializer / catalog / testing / models / cli maps cleanly to bronze → silver → gold.
2. rbt-core is the most mature piece — petgraph DAG, cycle detection, execution tiers, layer boundary errors, YAML frontmatter, minijinja path, unit tests that match the product story.
3. Demo pipeline is real — sample project shows medallion + bar aggregation, not just SELECT 1.
4. Dependency choices are sensible — arrow/parquet/datafusion/iceberg/iceberg-datafusion/jshift/minijinja are the correct stack for the thesis.
5. jshift integration is more than a stub — path extract + typed Arrow builders + tests.

Critical gaps / risks

1. Iceberg is declared but not the system of record
OutputFormat::Iceberg and ParquetAndIceberg write local Parquet (or no-op). WapMaterializer only logs. Catalog has no new_rest_catalog. Downstream “tables” are MemTables in process — fine for MVP, but README/docs claim Iceberg-native.

2. Bronze path is a special case, not a product feature
CLI hardcodes:

            let bronze_jsonl = project_dir.join("lake/bronze/raw_stock_trades.jsonl");
            if bronze_jsonl.exists() {
                // CREATE EXTERNAL TABLE bronze.raw_stock_trades ...

Staging frontmatter (scan_path, source_format) is parsed and stored, then never used. rbt-scan / rbt-json never enter the run path. That is the biggest product/implementation disconnect.

3. “Parallel tiers” are sequential
execution_tiers() exists; execute_dag loops models one-by-one with full collect(). No semaphore, no stream sink, no memory bound.

4. CLI surface vs thesis DX
Missing: validate, explain, preview. select is unused. test validates a hardcoded toy batch, not project models. Release binary appears stale vs debug (old compile message).

5. Dead / half-wired surface area
• sqlparser in rbt-core — unused
• RbtTemplateEngine — not used by DAG compile (regex path only)
• rbt-models — not consumed
• prost in workspace — unused
• Materialization enum mostly ignored (Table always)
• CLI depends on catalog/materializer but doesn’t use them meaningfully

6. Correctness hazards
• assert_unique is per-batch, not global; multi-batch unique can false-pass.
• Staging layer rule forbids any model dep (including other stg_) — may be intended, but sources aren’t modeled as nodes so missing bronze registration fails at runtime, not compile.
• fact_1m_stock_bars uses windowed transform + GROUP BY all columns — works as dedupe hack, not as grain-aware fact materialization.
• Silent empty batches skip MemTable registration → confusing downstream failures.
• Regex Jinja extract won’t handle ref(var), nested macros, whitespace edge cases minijinja would.

7. Docs debt
Two different “ADR-001” files. README still describes a smaller crate set and absolute file:// links. Marketing claims (10x Spark, WAP, multi-catalog) outrun the binary. That will hurt open-source credibility more than slow code.

8. Build environment
cargo test failed here on Unrecognized option: 'check-cfg' with current toolchain/crate mix — worth pinning a known-good Rust/toolchain before CI claims green.

───

Recommendations (prioritized)

P0 — Make the product honest and the demo general

1. Freeze v0 scope to what thesis Milestone A–D says, and demote everything else (Ballista, io_uring, Puffin, OpenLineage, MOR) to “later / research.”
2. Rewrite README from working sample: compile + run on stockmarket; list Iceberg/WAP as “in progress,” not shipped features.
3. Wire frontmatter → rbt-scan / rbt-json → DataFusion registration
   Staging models should register sources from scan_path + source_format, not a hardcoded filename. This is the bronze edge the whole thesis hangs on.
4. Delete or quarantine dead paths
   Either use RbtTemplateEngine for compile or drop it from the hot path; remove unused sqlparser until needed; don’t advertise formats that no-op.
5. One ADR series
   Rename/consolidate ADR-001s; archive paradigm/low-level docs under docs/archive/ so contributors know the live contract is thesis + layout ADR + current milestones.

P1 — Close the “dbt-shaped DAG engine” gap

6. Implement --select (and optionally +/@ later) — even name-only selection is enough for iteration.
7. Ship validate before more Iceberg work
   • Missing refs/sources at compile time
   • Optional: DataFusion logical plan bind after sources registered
   Structured errors (E_RBT_*) as JSON first; prost later if needed.
8. preview --limit N — cheap win for DX; same execution path as run with LIMIT.
9. Connect tests to models
   YAML or frontmatter assertions; run against materialization output or stream; fix unique to be global (or stream with a proper distinct sketch).
10. Stop always materializing as Table
    Honor layer defaults + config/frontmatter for materialization and format; don’t let CLI --format blindly override every model unless documented as override.

P2 — Iceberg as table truth (real differentiator)

11. Filesystem or REST catalog only for v0 — one path that creates table, appends Parquet, commits snapshot, registers via iceberg-datafusion.
12. Materializer = thin Iceberg writer, not a second file format zoo. Local Parquet dual-write is fine as debug mode; production path is Iceberg table identity.
13. WAP only after basic commits work — branching APIs in Rust Iceberg may lag; feature-detect rather than invent a fake WAP that only logs.
14. Use rbt-models for grain/relationship tests once Iceberg or MemTable outputs exist — dims/facts should drive unique(grain) and relationship checks automatically.

P3 — Engine quality (without premature micro-opts)

15. True tier parallelism with a concurrency limit + bounded memory (Semaphore is enough; skip custom allocators).
16. Stream write path (execute_stream → writer) instead of full collect() so larger silver/gold jobs don’t OOM.
17. Register intermediate results by catalog/schema identity, not bare model name only — matches compiled SQL and Iceberg namespaces.
18. Add rbt-measure / experiments/ as thesis says — one JSONL→Parquet baseline vs serde and optionally Spark local. Claims need numbers.

P4 — Project hygiene

19. Integration test: cargo test / CI runs sample project compile + run and asserts row counts / parquet existence.
20. Pin toolchain (rust-toolchain.toml) so check-cfg/edition surprises don’t block contributors.
21. Crate rename consistency: docs say rbt-bronze; code has rbt-scan — pick one.
22. Binary name rbt via [[bin]] name = "rbt" so UX matches docs.
23. Config schema validation for rbt_project.yml with clear errors.
24. Sources as first-class config (sources.yml or project section) so bronze isn’t only external-table luck.

───

Suggested near-term build order (aligned with thesis, grounded in code)

1. Frontmatter-driven bronze registration (scan + jshift)  ← product core
2. validate + select + preview CLI verbs                   ← DX core
3. Model-linked tests (not_null / unique global / accepted) ← quality core
4. One real Iceberg write/read round-trip                  ← table truth
5. Measure pack on JSONL → silver                           ← thesis proof
6. Star-schema metadata + relationship tests                ← Kimball layer

Do not invest next in: custom SIMD kernels, io_uring, Ballista, multi-catalog factories, prost, or more architecture essays. The sample pipeline already proves the DAG story; the missing product is general bronze wiring + honest Iceberg + developer loop.

───

Bottom line

Intent is clear and differentiated: lightweight Rust DAG for medallion+Kimball lakes on Arrow/DataFusion/Iceberg, with a byte-efficient bronze edge and dbt-like models — filling the “we don’t want Spark or warehouse-dbt for this” gap.

Implementation is an early vertical slice: strong rbt-core + working DataFusion DAG demo + real jshift/scan crates, but Iceberg, WAP, diagnostics, selection, and the bronze product path are still scaffold or hardcoded. Marketing docs are ahead of the code; the stockmarket example and thesis MVP list should be the governing sources of truth.

Highest-leverage next move: make staging frontmatter drive rbt-scan/rbt-json into the engine, then add validate/preview/select, then one Iceberg commit path. That sequence turns R&D scaffolding into a crate someone can honestly try instead of dbt+Spark for small/medium lake transforms.

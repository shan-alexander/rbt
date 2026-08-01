---
tags: [concept, kimball, dimensional-modeling, star-schema, grain, scd, layers]
node_type: concept
aliases:
  - star schema
  - Kimball rules
  - dimensional modeling
  - grain discipline
  - data modeling rules
description: Kimball dimensional modeling rules — grain discipline, layer placement (transform vs dimension vs fact), dependency direction, fact/SCD type selection, and anti-patterns. Use when designing or reviewing a model, deciding which layer logic belongs in, debating grain or dependency direction, or choosing whether to extend an existing dim vs. create a new one.
---
# Star schema data modeling rules

**Last Updated**: 2026-07-06  
**Owner**: Shan Newton  
**Scope**: Applies to all `stg_*`, `tf_*`, `dim_*`, and `fact_*` models in layered lakehouse / mart pipelines (Iceberg + medallion). Use during design and agent-assisted modeling. Originally authored for Kinna-platform operations; the rules apply to rbt project layouts ([[ADR-001 Project Layout]]) unless an rbt-specific ADR overrides them.

Concise dimensional-modeling guidance (Kimball, *The Data Warehouse Toolkit*, 3rd Ed). These are rules, not theory. **Must / Never** rules are near-absolute; **Rules of thumb** are defaults you may override when context clearly justifies it — state the justification when you do.

Related goals/ADRs: [[Primary path spine]] · [[Product north star]] · [[Iceberg system of record]] · [[Honest incremental materialization]] · [[ADR-001 Project Layout]] · [[ADR-002 Thesis Alignment]]

---

## 1. Grain

**Must**
- State the grain in one sentence before writing any SQL: "one row per ___ [per ___]". Every later decision flows from it.
- Make the surrogate key inputs exactly the grain-defining natural keys — no more, no less. If a column is in the key but not in the grain statement (or vice versa), one of them is wrong.
- Keep grain uniform: every row in a table is at the same grain.
- Prefer the most atomic grain the source supports. You can always aggregate up; you cannot disaggregate.
- Declare the grain statement in the model YAML description (or structured comment) so it is machine-readable.
- The grain must be testable: the natural key combination should support a uniqueness test (or `COUNT(*) = COUNT(DISTINCT <grain keys>)`).

**Never**
- Mix grains in one table (e.g., some rows daily, some monthly). Split into separate tables.
- Use `DISTINCT` (or `GROUP BY` with no aggregation, or a silent `ROW_NUMBER` filter) to make a fan-out "go away." Fix the join or the upstream grain instead.
- Add a column that does not belong at the stated grain. It belongs in another table at its own grain.

**Rules of thumb**
- If you cannot state the grain crisply, the model is not ready to build.
- A surprise change in row count after adding a join is a grain bug until proven otherwise.

---

## 2. Layer placement — where logic belongs

Layers: **sources → stage (`stg_*`) → transforms (`tf_*`) → dimensions (`dim_*`) / facts (`fact_*`)**.

**Must**
- Do identity resolution, cleansing, normalization, type casting, and controlled deduplication (explicit `ROW_NUMBER`, qualify, etc.) in **transforms**.
- Sources / raw ingestion (bronze) layers perform minimal technical landing (schema enforcement, basic type casting, partitioning, watermarking). Business cleansing, identity resolution, controlled deduplication, and normalization belong in tf_* transforms.
- Keep **facts thin**: assemble dimension foreign keys + measures, apply fact-grain measures, and carry through flags already computed upstream.
- Keep **dimensions descriptive**: surrogate key, natural key, and attributes only.
- Resolve a fact's FK to a dimension by joining the dimension on its natural key and selecting the dimension's surrogate key (with an Unknown-member fallback). Surrogate-key assembly is legitimately the fact's job.
- Every dimension must contain an Unknown member (typically surrogate key -1). Fact FK resolution must never emit NULL surrogate keys — always fall back to the Unknown member.

**Never**
- Put a deduplication window, multi-source identity collapse, or heavy cleansing on a fact. That is transform work.
- Put measures or aggregations on a dimension.
- Put event-grain or transaction-grain rows in a dimension.

**Rules of thumb**
- If you are about to wrap a fact in a CTE to run a window function, stop — the logic almost certainly belongs in the feeding transform, exposed as a column the fact reads.
- Before scoping work to "edit the fact," ask whether the right layer for the change is actually the transform. Scope guesses that name only the fact often steer logic into the wrong layer.
- If two facts both need the same derived attribute, derive it once in a shared transform, not in each fact.

---

## 3. Dependency direction

**Must**
- Flow data downstream only: raw → stage → transform → (dimension | fact).
- Reference dimensions from facts via FK only.

**Never**
- Reference a fact from another fact (fact → fact). Grains differ; results double-count and the DAG risks cycles.
- Reference a fact from a dimension or a transform. Facts are terminal — nothing downstream consumes them within the model layer.
- Create circular dependencies of any length.

**Rules of thumb**
- Need data from two facts together? Build a separate aggregate/reporting model at a common grain, or combine in the BI layer — never by joining the facts directly.
- Dimension-to-dimension references are allowed only as self-references (hierarchies) or sparing outriggers; prefer denormalizing a small attribute into the parent dimension over adding an outrigger.

### Cross-repo dependency (downstream marts)

Some marts are **downstream of another mart** — The single-mart layering still holds inside each mart; only the source boundary shifts. The legal flow is: **upstream dim/fact endpoint → downstream transform (`tf_*`) → downstream dim/fact/obt**.

**Must**
- In a downstream mart, source ONLY from the upstream mart's published **dim/fact (mart) endpoints** — the terminal, contracted models — brought in via `source()`-style references.
- Build the downstream mart's own layers on top of those endpoints: upstream endpoint → downstream transform → downstream dimension/fact/obt.
- Treat upstream endpoints as read-only contracts: consume their grain and keys as-is.

**Never**
- Source from an upstream mart's **transforms** (`tf_*`) or other intermediate/staging models. They are private to the owning mart and unstable; depending on them couples repos to internal logic and can break the DAG.
- Reach back into an upstream repo's raw sources to recompute something the upstream mart already publishes — consume the published endpoint instead.
- Redefine upstream business logic locally, or create a cross-repo cycle (a downstream model an upstream model then consumes).

**Rule of thumb**
- The clean cross-repo seam is "upstream fact/dim out → downstream transform in." If you find yourself wanting an upstream `tf_*`, the attribute you need probably belongs on the upstream endpoint — request it upstream rather than reaching past the contract.

---

## 4. Conformed dimensions & reuse

**Must**
- Reuse an existing conformed dimension when one already covers the entity at the right grain. Extend it rather than forking.
- Keep a conformed dimension's key, attributes, and definition identical everywhere it is used.

**Never**
- Create a second dimension for an entity that already has one at the same grain.
- Silently redefine an upstream/shared business definition in a downstream model. Escalate the change to the owning model instead.

**Rule of thumb**
- "New entity or new grain → maybe a new dim. Same entity, missing attribute → extend the existing dim."

---

## 5. Fact type selection

Pick the fact type from the question being answered:

| Question | Fact type | Grain | Additivity note |
|---|---|---|---|
| Did a discrete event occur, recorded once? | Transaction | one row per event | usually fully additive |
| What is the state at regular intervals? | Periodic snapshot | one row per entity per period | typically semi-additive — do not sum a balance/ARR across periods |
| How does a process move through milestones? | Accumulating snapshot | one row per process instance, updated over time | milestone dates + lag measures |
| Did something happen, with no measure? | Factless | one row per event/coverage combination | count rows, no measures |

**Must**
- Treat periodic-snapshot balances (ARR, MRR, headcount, inventory) as semi-additive: filter to a point in time (or average) rather than summing across the time axis.

**Rule of thumb**
- A degenerate dimension (operational id like an invoice/order number stored on the fact, no dim table) is fine when the id has no useful attributes of its own.
- Prefer storing the most atomic additive base measures. Calculate ratios, percentages, and non-additive metrics in the semantic/BI layer or presentation views when possible.
- Pre-compute derived measures only when query performance or usage frequency clearly justifies it — and document the trade-off.

---

## 6. Slowly Changing Dimensions

| Type | Behavior | Use when |
|---|---|---|
| 0 | Never changes | truly fixed attributes |
| 1 | Overwrite, no history | corrections, history doesn't matter |
| 2 | New row per change, full history | history matters for analysis (**project default**) |
| 3 | Previous-value column | only current + prior needed |
| 4 | Mini-dimension | rapidly changing attributes split off |
| 6 | Hybrid 1+2+3 | current + full history together |

**Must (Type 2 — the default here)**
- New surrogate key per version; natural key constant across versions.
- Carry `RECORD_VALID_FROM` / `RECORD_VALID_TO` and an `IS_ACTIVE_RECORD` flag; use the project's open-end convention (`2099-12-31`) for current rows.
- Query current state via the active-record flag; query history via a point-in-time date-range join.
- For soft deletes, either set RECORD_VALID_TO to the deletion timestamp or use a consistent IS_DELETED flag alongside the validity dates. Do not mix approaches across dimensions.

**Rule of thumb**
- Default to Type 2 unless there is a reason not to keep history; choose Type 1 only when history genuinely has no analytic value.

---

## 7. Special dimension patterns (use sparingly)

- **Role-playing**: one dimension joined multiple times under different FKs (e.g., several date roles). Same dimension, distinct FK columns.
- **Junk**: combine several low-cardinality flags into one small dimension to narrow a wide fact.
- **Bridge**: resolve a many-to-many between a fact and a dimension; the bridge must carry an allocation/weighting factor so aggregations stay correct.
- **Outrigger**: a dimension referenced by another dimension; prefer denormalizing instead unless the attribute set is shared and sizable.

---

## 8. Star schema vs. One Big Table (OBT)

**Must**
- Treat the star schema (dims + facts) as the source of truth.
- Build any OBT table or view **from** star-schema models, never bypassing dimensions or reading raw transforms.
- Keep OBTs (tables or views) in a separate layer, not in the core mart.

**Rule of thumb**
- Reach for an OBT only for fixed, repetitive reports or self-service consumers; keep ad-hoc/analyst-facing work on the star schema for flexibility.

---

## 9. Anti-patterns (never ship these)

- Fact-to-fact joins.
- Mixed grain in a single table.
- `DISTINCT` / silent dedup to hide a fan-out or grain defect.
- Circular dependencies.
- Measures or aggregations on a dimension.
- A transform (or dimension) that reads from a fact.
- Heavy transformation or a dedup window living on a fact instead of its transform.
- Transforming ID fields (`UPPER`/`LOWER`/`TRIM`) in join keys.

---

## 10. Pre-build checklist

Any model:
1. Grain stated in one sentence.
2. Correct layer chosen (transform vs dimension vs fact) for each piece of logic.
3. Dependency direction legal (no fact→fact, dim→fact, or transform→fact).
4. Existing conformed dims reused, not duplicated.
5. If downstream of another repo: sources are upstream dim/fact endpoints only (never upstream transforms), no cross-repo cycle.

Dimension:
1. Entity has no existing dim at this grain (else reuse/extend).
2. Grain is one row per entity (or per version for SCD2).
3. Surrogate key from grain-defining natural key; Unknown member present and used as FK fallback.
4. Descriptive attributes only — no measures.

Fact:
1. Grain stated and atomic; surrogate key matches it.
2. All referenced dimensions exist; FKs fall back to Unknown.
3. No dependency on another fact.
4. No dedup window / identity resolution that belongs in the feeding transform.

---

## 11. Lakehouse Layering (Bronze / Silver / Gold + Serving Layer)

**Platform mapping for GAI / Kinna-style stacks**:
- **Bronze**: Raw partitioned Parquet files on minIO (filesystem landing zone). Append-only and immutable. Contains source data plus minimal technical metadata (source system, ingestion timestamp, batch/load id).
- **Silver**: Iceberg tables ("stage tables"). Progressive cleansing, deduplication, normalization, type standardization, and basic quality enforcement. Still relatively source-aligned. Early `tf_*` transforms commonly land here or feed Silver creation.
- **Gold**: Business star schema. `dim*` (conformed dimensions, typically SCD2) + `fact*` tables at the declared business grain. This is the analytical source of truth and the primary contract for downstream consumers.
- **Serving / Semantic Layer**: Spice.ai (customer-facing). Used for curated views, OBTs, metric definitions, and acceleration (materializing hot subsets into DuckDB or Cayenne). Iceberg Gold remains the source of truth.

**rbt mapping note:** See [GOLD_DEFAULT.md](../GOLD_DEFAULT.md) for the full topology.

- **`stg_*`** = **silver endpoints** (`silver/stage`). Prefer 1:1 bronze→stg when simple.
- **Silver prep `tf_*`** (optional): ref bronze / other silver `tf_*` only, then land **`stg_*`**. Never hang silver `tf_*` *after* `stg_*`.
- **Gold `tf_*`** (`gold/tf`): ref **only** `stg_*`, then feed dim/fact.
- **`dim_*` / `fact_*` / `obt_*`**: gold endpoints. Never `source()` upstream private `tf_*`.
**Must**
- Keep the conformed star schema (`dim*` + `fact*`) in the Gold layer on Iceberg. Do not define core business grain, conformed dimensions, or fact logic in Spice.
- Use Spice as a **semantic + acceleration layer** on top of Gold, not as the storage layer for the dimensional model.
- Enforce explicit contracts between layers: Bronze → Silver focuses on technical quality and usability; Silver → Gold focuses on business conformance, grain alignment, and reusability.

**Never**
- Perform heavy business logic, grain decisions, or conformed dimension building in Silver Iceberg tables (push this work to Gold transforms).
- Build OBTs, business metrics, or core analytical logic directly in Spice by bypassing Gold.

**Rules of thumb**
- If a cleansing, deduplication, or normalization rule is shared across multiple Gold models, implement it once in Silver Iceberg tables.
- For very high-volume or frequently reprocessed sources, retain longer history in Bronze/Silver and apply stricter retention + compaction policies in Gold.
- Leverage Spice acceleration (DuckDB/Cayenne materialization) for hot dashboards, self-service, and agentic workloads. Route cold/ad-hoc analytical queries to the Iceberg Gold layer.
- Treat Gold dim/fact models as the read-only contract for Spice views and OBTs.

---

## 12. Iceberg Table Design & Lifecycle

**Must**
- Design Iceberg partitioning and clustering based on both common query filter patterns and the table’s grain. High-value patterns usually combine a date column with one or more high-cardinality dimensions or surrogate keys.
- Use Iceberg **hidden partitioning** where possible. Document and control partition spec evolution (treat breaking changes as model changes).
- Choose the Iceberg write mode deliberately:
  - Prefer **Merge-on-Read (MoR)** with delete files for high-ingestion or CDC-style workloads (faster writes, lower write amplification).
  - Use **Copy-on-Write (CoW)** when read performance is paramount and updates/deletes are infrequent.
- Implement **scheduled compaction** for all Silver and Gold Iceberg tables. Compaction is mandatory when using Merge-on-Read to remove delete files and control file count/size. Target file sizes in the 128MB–1GB range for analytics workloads.
- Define and enforce retention policies by layer (Bronze longest, Gold shortest) and use Iceberg time travel + metadata tables for reprocessing and debugging instead of retaining everything in Gold.

**Never**
- Create large or high-query-volume Iceberg tables with no partitioning or clustering strategy.
- Use Merge-on-Read without a reliable scheduled compaction process — read performance will degrade over time as delete files accumulate.
- Allow unchecked small-file growth in Silver or Gold (be disciplined about compaction & garbage collection).

**Rules of thumb**
- Run compaction as part of Silver and Gold pipelines (or as scheduled maintenance) rather than as an after-the-fact operation.
- Prefer Iceberg row-level `MERGE` / `DELETE` for incremental updates in Gold when full rewrites become expensive.
- Monitor Iceberg metadata tables (`$snapshots`, `$manifests`, `$partitions`, `$files`) as part of data observability and pipeline health.
- Use different compaction and retention strategies per layer — aggressive compaction and shorter retention in Gold, lighter touch in Bronze/Silver.

---

## 13. Incremental Processing, CDC & Reprocessing Safety

**Must**
- Design Gold transforms (`tf_*` feeding `dim*` and `fact*`) to be **idempotent** or safely re-runnable. Prefer watermark-based incremental patterns + `MERGE`/`UPSERT` into Iceberg over full refreshes when the logic permits.
- For SCD2 dimensions in Gold, implement `MERGE` logic that correctly maintains `RECORD_VALID_FROM`, `RECORD_VALID_TO`, and the open-end convention (`2099-12-31`).
- Support reprocessing from Bronze using Iceberg time travel or controlled re-ingestion when upstream data corrections occur.

**Never**
- Build Gold models that can only be executed as full refreshes on large tables without a documented and tested reprocessing path.
- Mix incremental and full-refresh logic in the same model without clear guards, watermarks, or isolation.

**Rules of thumb**
- Start with incremental + `MERGE` patterns for high-volume facts. Fall back to full refresh only when business logic complexity makes safe incrementality impractical.
- Maintain a “last successful watermark” or batch identifier in Silver/Gold metadata so pipelines and agents can determine freshness and safe reprocessing windows.
- Use Iceberg snapshots (and branching when available via the catalog) for safe experimentation and rollback during major Gold changes.
- Keep reprocessing logic simple and deterministic — the goal is to be able to replay a Gold model from a known Bronze/Silver state without side effects.

**rbt note:** Today rbt ships honest `incremental_append` (part files + manifest), not full Iceberg `MERGE`/SCD2 engines. Prefer declaring intent in models; implement merge/SCD2 when the SoR path supports it ([[Honest incremental materialization]], [[Iceberg system of record]]).

### Reliable Ingestion & Maintenance Orchestration

**Must**
- Use a durable execution engine (such as Restate) for all ingestion and maintenance workflows that write to Bronze and Silver Iceberg tables. This provides exactly-once semantics, resumability, and safe handling of partial failures during long-running jobs.
- Orchestrate scheduled compaction jobs through the same durable execution layer (Restate) so compaction runs reliably and can be retried or rolled back cleanly.

**Rules of thumb**
- Leverage Restate’s durability and state management for watermark tracking, backfill/reprocessing workflows, and triggering compaction across layers. This reduces the need for fragile external schedulers or manual intervention.
- Keep ingestion jobs (Bronze → Silver) and compaction jobs as separate, independently observable and restartable workflows.

---
tags: [analysis, roadmap, bronze, silver, maturity, gap-matrix]
node_type: analysis
aliases:
  - gap matrix
  - B-items gap matrix
  - bronze silver confidence
  - stage-append class gap analysis
---
# Bronze-to-silver maturity gap matrix

## Question / scope

What capabilities does **rbt 0.5.0** already have vs what real multi-artifact bronze landing zones and partial-run silver need?  
What belongs in **open-core rbt** vs a **host orchestrator** (durable workflow, domain fan-out, product-specific receipts)?

This analysis generalizes a StageAppend-class production pattern: multi-file bronze under Hive-ish partitions, optional artifact families, outer-join reconciliation, fingerprint skip, generation receipts, and peer-safe re-drives. Product-specific column names stay out of the engine.

**When:** 2026-07-31 (post 0.5.0; after dual-track roadmap discussion).

## Framing

| Axis | Declarative lake DAG (rbt today) | StageAppend-class job (host pattern) |
|------|----------------------------------|--------------------------------------|
| Job | SQL model graph over lake files | Domain/run reconciliation + contracts |
| Input grain | Sources + models by path/glob | Multi-file bronze keyed by partition dimensions |
| Semantics | Full refresh / part append + tests | Outer multi-source spine, status, quarantine |
| Publish | Stream Parquet / FS Iceberg / FS WAP | Parts + manifests + generation receipt + fingerprint skip |
| Engine | DataFusion | Often Polars (or rbt) in-process under a host CLI |

**Efficiency honesty:** rbt can beat ad-hoc full rewrites on pure SQL transforms. Wall-clock on happy-path selects is not the hard problem for StageAppend-class silver — **correct multi-source outer joins, stable schemas, skip/receipt, and partial-run honesty** are. A confident “replace host transform” rating requires implementing or faithfully hosting that **contract**, not only scanning Parquet faster.

## Reference bronze pattern (generic)

Real landing zones are rarely one flat `bronze/` directory. A **complex bronze landing zone** typically looks like:

```text
$lake/lz/runs/
  domain=<d>/report_date=<date>/run_id=<id>/
    plan.parquet              # inventory / intended work
    scrape.parquet            # successful enrichments (optional)
    failures.parquet          # failed URLs (optional; first-class)
    siteinfo.parquet          # site-level (optional; may exist without product rows)
    residual_mime.parquet     # late fields (optional; no-clobber coalesce)
    …
```

Requirements that pattern implies for any serious bronze→silver tool:

1. Multi-root / absolute lake roots and Hive-ish partition filters.
2. **Per-artifact-family** sources (path_glob / registration), with **missing → empty typed frame**.
3. Run-scoped and entity-scoped execution (CLI/env partition binds, not only model `--select`).
4. Outer reconciliation SQL (planned ⟕ success ⟕ failures), not inner-only happy paths.
5. Stable physical schemas (typed NULL columns always present when artifacts absent).
6. Idempotent publish: fingerprint over bronze set + contract version; generation receipt as consumer authority; atomic table publish; identical re-drive writes nothing new.
7. Host-level peer isolation (one bad domain must not poison peers) — often **one scoped rbt invoke per domain**, not multi-domain quarantine inside the engine.

See goal: [[Complex bronze landing zones]].

## Gap matrix — bronze → silver

Legend: **Done** · **Partial** · **Missing** · **Host** (orchestrator / project, not core rbt)

### A1. Lake / path contracts

| # | Capability | Status | Evidence / notes |
|---|------------|--------|------------------|
| **B1** | Multi-root / absolute roots + Hive-ish partitions | **Partial → mostly Done** | [[Bronze contracts multi-root and path_glob]]: `roots`, absolute targets, `partition_by`, `require_partitions`, `path_glob`. Missing: **global run-var injection** for the whole DAG from CLI. |
| **B2** | Source per artifact family; missing = empty typed frame | **Partial** | path_glob isolates families. Optional empty frame is **Missing** (absent files often fail bronze checks). |
| **B3** | Run-scoped + entity-scoped select (`--report-date`, `--run-id`, `--domains` class) | **Partial** | Model `--select` is graph selection, not partition scope. Closest: static `require_partitions` in frontmatter. |
| **B4** | Canonical grain helpers / project UDF packs | **Host + hooks** | Design A registry + builtins; domain policy UDFs stay **project-side** ([[Polyglot UDFs and Rust models]]). |
| **B5** | Stable physical schemas (typed NULL columns always present) | **Missing** | Schema follows files/SQL; no “contract columns always emitted” product surface. |

### A2. Partial-run / reconciliation semantics

| # | Capability | Status | Notes |
|---|------------|--------|-------|
| **B6** | Outer multi-source spine (plan ⟕ success ⟕ failures) | **Missing as product** | Expressible in SQL **if** empty sources + stable schemas exist. Not a built-in “spine” mode. |
| **B7** | First-class row/site completeness status columns | **Host / models** | Conventions live in project SQL + tests; engine must not hardcode product vocabulary. |
| **B8** | 0 vs NULL policy (measured zero ≠ unavailable) | **Missing as policy** | SQL can encode; want tests / validate rules so silent metric coalesce fails closed. |
| **B9** | Inventory silver without full product completion | **Model pattern** | Depends on B2/B6/B10. |
| **B10** | Failure ledger as first-class source | **Project sources** | Same registration surface as B2. |
| **B11** | Field-level no-clobber coalesce (late residual artifacts) | **SQL / optional UDF** | Document pattern; optional future helper UDF. |
| **B12** | Entity quarantine: one bad peer fails closed; others publish | **Host preferred** | Prefer Restate-class fan-out: one domain (or partition key) per `rbt run`. In-process multi-entity quarantine is optional later. |

### A3. Idempotency & publication barrier

| # | Capability | Status | Notes |
|---|------------|--------|-------|
| **B13** | Content fingerprint over bronze set + contract version | **Missing** | |
| **B14** | Generation / result receipt (JSON) as consumer authority | **Missing** | Measure JSON is scenario-oriented, not a run receipt. |
| **B15** | Atomic publish per table (WAP or rename) | **Done (FS)** | Stream partial rename + FS WAP ([[Filesystem write-audit-publish]]). Iceberg commit path separate. |
| **B16** | Per-entity or per-table parts + manifest | **Partial** | `incremental_append` parts + `_rbt_manifest.json` ([[Honest incremental materialization]]). Not host layout schema; not per-domain by default. |
| **B17** | Identical re-drive = zero new parts; changed bronze invalidates skip | **Missing** | Re-run can append again today. |
| **B18** | Affected-entity re-drive preserves unaffected peers | **Host + B3** | Natural with per-entity invokes. |

### A4. Engine / ops

| # | Capability | Status | Notes |
|---|------------|--------|-------|
| **B19** | Streaming / low-RSS at whale partition sizes | **Partial** | Stream + bronze spill shipped ([[Memory-honest materialization]]); **unproven** at large multi-partition packs without measure scenarios. |
| **B20** | Deterministic ordering for snapshot diffs / fingerprints | **Partial** | SQL `ORDER BY` possible; no product fingerprint stability guarantee. |
| **B21** | Exit codes + machine-readable run result (rows, status, fingerprint, quarantined) | **Partial** | Structured errors; no standard **RunReceipt**. |
| **B22** | Lake-only I/O; offline nonprod root; no secret surprises | **Mostly Done** | FS-first. Remote `object_store` still open. |

### A5. Explicit non-goals for rbt bronze→silver

- Ingest / scrape / CAS write paths
- Durable workflow engine (host owns Restate-class orchestration)
- Replacing host storage layouts wholesale

## Gap matrix — silver → gold

Gold is closer to rbt’s natural product shape.

| # | Capability | Status | Notes |
|---|------------|--------|-------|
| **G1** | Sources = multi-part stage tables (parts dirs + manifests) | **Partial** | Own `ref()` to parts works; external stage trees need path_glob + optional manifest awareness. |
| **G2** | Filter on publication/completeness columns | **Model SQL** | Requires upstream status columns (B7 class). |
| **G3** | Star-schema grains / SCD-lite | **Partial** | Frontmatter grain/tests + [[Star schema data modeling rules]]; full SCD2 engine not shipped. |
| **G4** | `incremental_merge` / keyed upsert | **Missing** | Honest error today; needs Iceberg-backed or explicit design. |
| **G5** | Snapshot / as-of by business date + optional run id | **Partial** | Partition filters; not a snapshot-as-of API. |
| **G6** | Model tests as CI (unique, FK-ish, not_null) | **Partial → strong** | `rbt test` + frontmatter; relationship depth thinner than dbt. |
| **G7** | REST/Lakekeeper Iceberg commit | **Missing** | Local catalog proof gate done ([[Iceberg system of record]]). |
| **G8** | Lineage stamps (generation / fingerprint / model version) | **Missing** | High-value, small surface. |
| **G9** | Selective rebuild (`--select dim_x+`) | **Done** | |
| **G10** | Environments (nonprod/prod lake roots) first-class | **Done** | Multi-root / absolute targets. |

## Confidence scorecard (honest)

### Bronze → silver (StageAppend-class)

| Confidence | Requirements |
|------------|--------------|
| **Low (today)** | SQL over Parquet; full refresh; FS WAP; multi-root/globs — **without** empty sources, run vars, fingerprint/receipt, outer spine reliability |
| **Medium** | B1–B5, B13–B15, B19–B21 + proven outer-join silver models on a real nonprod lake |
| **High** | Medium + B6–B12, B16–B18 + host re-drive gates green |
| **Production replace host SoT** | High + identical path/receipt contracts the fleet already reads + whale load tests + no dual-contract drift |

Until **High**, treat rbt as research / side transforms / vendor-simple silver, **not** product StageAppend-class SoT.

### Silver → gold

| Confidence | Requirements |
|------------|--------------|
| **Low** | Manual SQL over single Parquet dumps |
| **Medium** | G1–G3, G6, G8–G10; nightly CLI on nonprod |
| **High** | Medium + G4–G5 + scheduled host + safe filters on partial silver |
| **DaaS-backed** | High + G7 aligned with REST catalog / serving layer |

**Today:** Low for product StageAppend-class silver SoT. **Medium-adjacent for gold only after a real nonprod project**, not from 0.5.0 docs alone.

## Findings (summary)

1. Path/glob/multi-root work is **ahead** of partial-run contract work — good foundation for [[Complex bronze landing zones]].
2. Atomic FS publish is **real**; fingerprint + generation receipt + skip-on-identical-redrive are the main **idempotency gap**.
3. Many “reconciliation” items are **model/SQL + optional sources**, not engine features — invest in **empty typed frames, run vars, schema contracts, RunReceipt**.
4. Entity quarantine and peer preserve are **better as host fan-out** than as multi-domain engine magic.
5. Gold adoption probability > product silver replacement **if** G1, G4, G6, G8 land.
6. Unmeasured “beats Polars/Spark” claims remain invalid ([[Measured claims before marketing]]).

## Recommendations (not a decision)

1. Capture dual-track execution plan: [[Dual-track maturity roadmap]].
2. Productize **complex bronze landing zone** primitives under [[Complex bronze landing zones]] before any host “replace StageAppend” narrative.
3. Prefer subprocess host wrap (orchestrator owns contract; rbt executes scoped DAG) over early library link into host crates.
4. First real projects: **simple multi-artifact bronze→silver** (no outer spine), then gold marts; outer spine last.
5. Keep product-specific status vocabularies in project models; keep rbt open-core honest ([[Honest product surface]]).

## Open questions / edge cases

- Should optional empty sources be frontmatter (`missing: empty`) or project-level policy?
- Fingerprint algorithm: file set hash + sizes/mtimes vs content hash vs Iceberg snapshot id?
- RunReceipt schema versioning: JSON first; prost only after shape stabilizes.
- When does in-process multi-entity quarantine beat “one rbt per entity”?
- External parts manifests: consume host manifests vs only `_rbt_manifest.json`?

## Related

- Goal: [[Complex bronze landing zones]]
- Plan: [[Dual-track maturity roadmap]]
- Goals: [[Primary path spine]], [[Bronze contracts multi-root and path_glob]], [[Memory-honest materialization]], [[Honest incremental materialization]], [[Filesystem write-audit-publish]], [[Iceberg system of record]], [[Measured claims before marketing]], [[Honest product surface]], [[Polyglot UDFs and Rust models]]
- ADRs: [[ADR-001 Project Layout]], [[ADR-002 Thesis Alignment]]
- Concept: [[Star schema data modeling rules]]
- Docs: [MULTI_ROOT_AND_PATH_GLOB.md](../MULTI_ROOT_AND_PATH_GLOB.md), [P4_CAPABILITIES.md](../P4_CAPABILITIES.md), [ICEBERG_SOR.md](../ICEBERG_SOR.md)

---
tags: [product, goal, bronze, landing-zone, silver, multi-artifact]
node_type: goal
aliases:
  - complex bronze
  - multi-artifact bronze
  - bronze landing zones
  - Hive bronze pattern
---
# Complex bronze landing zones

**One-line:** rbt must turn **real multi-artifact, multi-partition bronze landing zones** into reliable silver tables — not only a single flat bronze directory with happy-path files.

## Goals

- Support **Hive-ish bronze trees**: partition dimensions such as `entity=` / `report_date=` / `run_id=` under multi-root or absolute lake roots.
- Register **one staging source per artifact family** (plan inventory, success payloads, failure ledgers, site/entity metadata, late residual files, …) via `scan_path` + `path_glob` (and successors), not one mega-glob of mixed filenames.
- Allow **optional artifacts**: when a family is absent for a partition, produce an **empty typed frame** (stable columns, zero rows) so outer-join silver SQL remains valid — partial runs must not “lose columns.”
- Scope a run by **partition binds / run vars** (CLI or env), not only by model-graph `--select`, so orchestrators can drive one entity-date-run slice at a time.
- Enable silver models that implement **outer multi-source reconciliation** (inventory ⟕ success ⟕ failures) entirely in SQL once sources are honest — without engine hardcoding product status enums.
- Preserve **0 vs NULL** discipline at the contract level (measured zero ≠ unavailable) via model rules and tests, not silent coalesce.
- Provide **idempotent publish primitives**: atomic table publish (have), plus content fingerprint + generation receipt + skip-on-identical-redrive (planned).
- Prefer **host fan-out** for peer isolation (one bad entity fails closed; peers still publish) via scoped rbt invokes; keep core free of proprietary workflow engines.

## Reference pattern (generic example)

A production-style landing zone often looks like:

```text
$lake/lz/runs/
  domain=<entity>/report_date=<date>/run_id=<id>/
    plan.parquet           # what was supposed to happen (inventory)
    scrape.parquet         # successful enrichments (may be missing)
    failures.parquet       # failed units (first-class; may be missing)
    siteinfo.parquet       # entity-level rows independent of product completion
    residual_*.parquet     # late fields; coalesce without clobbering priors
```

**Silver targets** might be:

| Silver table | Grain (example) | Built from |
|--------------|-----------------|------------|
| `stg_entity` | one row per entity per report_date | siteinfo ⟕ inventory completeness |
| `stg_unit` | one row per planned unit | plan ⟕ success ⟕ failures → row_status |
| `stg_unit_success` | one row per successful unit | success only, thin |

Row status and completeness column **names** live in the **project**, not in rbt core. rbt’s job is to make that SQL **safe under partial bronze**.

## Non-goals

- Owning ingest, crawl, scrape, or content-addressed write paths.
- Embedding Restate/Airflow-class orchestration inside rbt.
- Hardcoding a single vendor’s column vocabulary or receipt schema into the engine.
- Claiming StageAppend-class SoT replacement before gap gates reach High — see [[Bronze-to-silver maturity gap matrix]].

## Status

- **Foundation + P5a/P5b (0.6.0):** multi-root/globs, **run vars**, **`on_missing: empty`**, empty schema from `columns.*.dtype`, **fingerprint + RunReceipt + `--skip-if-match`**, example [complex_bronze_landing](../../examples/complex_bronze_landing/).
- Docs: [COMPLEX_BRONZE_AND_RUN_SCOPE.md](../COMPLEX_BRONZE_AND_RUN_SCOPE.md).
- **Still open:** whale measure packs (P5c), merge/SCD, remote SoR, host quarantine patterns.
- Execution plan: [[Dual-track maturity roadmap]].

## Success criteria

1. A sample project can model the reference multi-artifact tree with **optional** failure/success files and still compile + run outer-join silver SQL.
2. Orchestrator can pass `report_date` / `run_id` / entity list (or equivalent) without editing frontmatter for every run.
3. Re-drive with identical bronze + contract version produces **no new parts** once fingerprint skip lands.
4. Docs describe the pattern generically; no proprietary product names required to understand the feature.

## Related

- Analysis: [[Bronze-to-silver maturity gap matrix]]
- Plan: [[Dual-track maturity roadmap]]
- Goals: [[Bronze contracts multi-root and path_glob]], [[Primary path spine]], [[Memory-honest materialization]], [[Honest incremental materialization]], [[Filesystem write-audit-publish]], [[Honest product surface]]
- ADR: [[ADR-001 Project Layout]], [[ADR-002 Thesis Alignment]]
- Concept: [[Star schema data modeling rules]]

---
tags: [product, goal, thesis]
node_type: goal
aliases: [north-star, rbt thesis, product vision]
---
# Product north star

**One-line:** dbt-shaped medallion transforms on filesystem / object-storage lakes, in-process Rust + Arrow + DataFusion, with Iceberg as the long-term table truth.

## Goals

- Ship a **project + model DAG** UX: models, `{{ ref() }}` / `{{ source() }}`, layers (staging → transforms → marts), frontmatter tests, CLI select.
- Execute **in-process** (no JVM, no Python runtime, no warehouse pushdown as the default path).
- Land bronze files (JSONL/CSV/Parquet/Arrow/protobuf) → silver/gold materialization with a **byte-efficient bronze edge** where it matters (jshift-class path work, column projection).
- Keep the product **niche-true**: medallion lakes and declared DAGs, not warehouse-dbt parity theater.
- Package honestly as **`rbt-datalake`** on crates.io; binary and lib import path remain **`rbt`**.

## Non-goals

- Full dbt Cloud / multi-cluster Spark *as the product identity*.
- Proprietary lake or customer names in-repo.
- Claiming “most efficient in the world” without [[Measured claims before marketing]].

Petabyte-scale **lakes** are allowed via host fan-out + scoped rbt workers + Iceberg table truth — see [[Dual-track maturity roadmap]] and [[Team-scale lake positioning]]. Single-process petabyte shuffles are still not the architecture.

## Status

- Thesis: [thesis.md](../../thesis.md). Contributor contract: [CONTRIBUTING.md](../../CONTRIBUTING.md).
- Spine and many milestones shipped through **0.5.0**; remaining work is complex bronze, SoR depth, remote lakes, Design B, and measured public comparisons.
- Maturity path: [[Dual-track maturity roadmap]]; bronze depth: [[Complex bronze landing zones]].

## Related

- [[Primary path spine]]
- [[Team-scale lake positioning]]
- [[Iceberg system of record]]
- [[Honest product surface]]
- [[Complex bronze landing zones]]
- ADRs: [[ADR-002 Thesis Alignment]], [[ADR-001 Project Layout]]
- Concept: [[Star schema data modeling rules]]
- Analysis: [[Bronze-to-silver maturity gap matrix]]
- Plan: [[Dual-track maturity roadmap]]

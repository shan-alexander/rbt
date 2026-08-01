---
tags: [product, goal, P4, UDF, polyglot, ADR-003]
node_type: goal
aliases: [Design A, Design B, Rust models, rbt UDFs]
---
# Polyglot UDFs and Rust models

**One-line:** One DAG, monomorphic Arrow data — Design A (SQL + in-process UDFs) first; Design B (first-class Rust models) when the SQL spine is trusted.

## Goals

- **Design A:** Register scalar/aggregate/window UDFs into DataFusion; SQL models call stable names (`rbt_upper`, …; project UDFs via library host).
- **Design B (planned):** Rust model nodes as normal DAG members with shared materialize, tests, layers, and selectors.
- Arrow `RecordBatch` as the only ABI between SQL and Rust.
- Preserve dbt-shaped UX for the common path; Rust is the native escape hatch (not Python models).

## Non-goals

- `.rsx` / mixed SQL+Rust surface language as flagship.
- Untrusted `cdylib` loading without policy.
- Guaranteeing “Rust faster than SQL” for ordinary windows without measure.
- Ballista multi-node model distribution on the v0 path.

## Status

- **Design A (builtin UDFs):** shipped **0.5.0** (`rbt_upper` / `lower` / `trim` / `nullif_empty`).
- **Design B:** planned in [docs/adr/ADR_003_UDF_RSMODELS.md](../adr/ADR_003_UDF_RSMODELS.md); multi-catalog / dynamic plugins deferred with P4 remainder.

## Related

- [[Primary path spine]]
- [[Product north star]]
- [[Measured claims before marketing]]
- ADR: [[ADR-003 Polyglot DAG]]

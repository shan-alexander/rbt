---
tags: [product, goal, P4, incremental]
node_type: goal
aliases: [incremental_append, incremental strategies]
---
# Honest incremental materialization

**One-line:** Incremental means real append of part files (and a manifest), not a fake merge sold as MERGE.

## Goals

- Opt-in `materialization: incremental_append` (aliases append / incremental) writes `model.parts/part-*.parquet` + `_rbt_manifest.json`.
- `ref()` lists the parts directory so downstream SQL sees cumulative data.
- Full refresh remains the default table overwrite path.
- Fail clearly (`E_RBT_INCREMENTAL`) for unshipped strategies such as row-level `incremental_merge`.

## Non-goals

- Claiming SCD2, merge-on-read, or dbt `merge` parity without implementation.
- Silent fallback from “merge” config to append.

## Status

- **Shipped** incremental_append in **0.5.0**. Merge strategy deferred.
- Docs: [docs/P4_CAPABILITIES.md](../P4_CAPABILITIES.md). Measure scenario: `incremental_append`.

## Related

- [[Memory-honest materialization]]
- [[Honest product surface]]
- [[Measured claims before marketing]]

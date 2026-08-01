---
tags: [product, goal, P4, WAP]
node_type: goal
aliases: [WAP, write audit publish, FS WAP]
---
# Filesystem write-audit-publish

**One-line:** Stage → audit → atomic publish on the filesystem when `materialize.wap: true`; do not claim Iceberg branch WAP until that path exists.

## Goals

- Write stream output under `.wap/{run_id}/`, run stream assertions, then rename to production dest with audit JSON.
- Leave production unchanged on audit failure.
- Make the mechanism discoverable in config and docs without overselling.

## Non-goals

- Iceberg branch/clone environments presented as this feature.
- Multi-writer coordinated WAP across engines.

## Status

- **Shipped** FS WAP in **0.5.0**. Iceberg-native WAP remains future/out of scope for this goal.
- Docs: [docs/P4_CAPABILITIES.md](../P4_CAPABILITIES.md).

## Related

- [[Iceberg system of record]]
- [[Honest product surface]]
- [[Memory-honest materialization]]

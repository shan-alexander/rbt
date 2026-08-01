---
tags: [product, goal, honesty, docs]
node_type: goal
aliases: [no theater, product honesty, docs match code]
---
# Honest product surface

**One-line:** Document only what the code does; prefer a smaller true surface over a larger fictional one.

## Goals

- README, CHANGELOG, and CLI help match **shipped** behavior for the release they describe.
- Prefer explicit errors (`E_RBT_*`) and deferred features over silent stubs that look complete.
- Call out interim designs honestly (e.g. FS Iceberg layout vs catalog snapshot commit; FS WAP vs Iceberg branches).
- Never invent ADR history; promote decisions only when they are real.
- One monocrate workspace member (`crates/rbt`); orphan crates.io stubs stay deprecation-only.

## Non-goals

- Marketing language that outruns measure packs or proof gates.
- Multi-catalog / multi-writer “product” narrative before one path is solid.
- WAP, merge, or Iceberg branch claims that are only filesystem renames without saying so.

## Status

- Active discipline for all releases. CONTRIBUTING §3: do not document unfinished features as shipped.
- P4 docs deliberately name **honest** incremental and WAP limits ([docs/P4_CAPABILITIES.md](../P4_CAPABILITIES.md)).

## Related

- [[Product north star]]
- [[Measured claims before marketing]]
- [[Iceberg system of record]]
- [[Filesystem write-audit-publish]]

---
tags: [product, goal, positioning, niche]
node_type: goal
aliases: [team-scale, not Spark, niche positioning]
---
# Team-scale lake positioning

**One-line:** Optimize for tens of GB → low TBs of relevant partitions on a serious single-node Rust process — not petabyte ad-hoc shuffles or warehouse-dbt parity.

## Goals

- Win where Spark’s fixed costs (JVM, planning, shuffle ops, cluster ops) dominate: bronze cleanup, silver typing, dimensional models, partition-incremental refreshes.
- Document the split: common path in rbt; “call Spark/Trino for elephant jobs” when needed.
- Keep bronze edge selective and ops surface small (no YARN/K8s requirement for every transform).
- Defend the claim with [[Measured claims before marketing]], not slogans.

## Non-goals

- Becoming a multi-cluster compute fabric *inside* the rbt binary.
- Warehouse-dbt checklist parity that fights the lake niche.
- Single-process petabyte shuffles as the default architecture (see petabyte *via fan-out* under Related plan).

## Status

- Positioning fixed in [thesis.md](../../thesis.md) and CONTRIBUTING §1.
- Empirical Spark packs still open; logical argument is already the product filter for scope.
- Petabyte **lakes** are in scope as **partitioned workers + Iceberg multi-writer**, not as “one SQL plan shuffles 1 PB.” See [[Dual-track maturity roadmap]] P8.

## Related

- [[Product north star]]
- [[Measured claims before marketing]]
- [[Honest product surface]]

---
tags: [product, goal, P2, iceberg, SoR]
node_type: goal
aliases: [P2, table truth, Iceberg SoR, snapshot commit]
---
# Iceberg system of record

**One-line:** Iceberg remains the thesis target for silver/gold table truth; prove it via official create → write → commit snapshot → read, without multi-catalog theater.

## Goals

- **Proof gate:** create table → write data files → **commit snapshot** via official Rust `iceberg` → read back (DataFusion / catalog scan).
- Stable table identity for `ref()` when Iceberg is the materialize format.
- Atomic publish semantics readers can trust (no half-written runs).
- One working catalog path first (local FS warehouse + MemoryCatalog + durable storage factory is the current proof).
- Document FS layout mode separately from catalog SoR mode (`materialize.iceberg.mode`).

## Non-goals (until proven)

- REST/Glue multi-writer OCC production claims.
- Multi-catalog sprawl (Polaris + Glue + Nessi in core) before one path is solid.
- Iceberg branch/WAP APIs presented as shipped when only filesystem stage dirs exist.
- Reimplementing the table format outside the official crate.

## Status

- **0.4.0:** catalog create→write→commit→scan proof gate on local FS warehouse.
- REST/Glue multi-writer still **open**. FS layout remains available.
- Docs: [docs/ICEBERG_SOR.md](../ICEBERG_SOR.md). Decision rule: CONTRIBUTING §4.

## Related

- [[Product north star]]
- [[Honest product surface]]
- [[Memory-honest materialization]]
- [[Filesystem write-audit-publish]]

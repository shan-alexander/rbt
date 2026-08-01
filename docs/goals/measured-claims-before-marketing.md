---
tags: [product, goal, P4, measure, proof]
node_type: goal
aliases: [measure packs, rbt measure, thesis proof, no unmeasured claims]
---
# Measured claims before marketing

**One-line:** Public “replaces Spark for X” or efficiency claims require checked-in scenario packs and reports — not branding.

## Goals

- Ship **`rbt measure`** scenarios with stable fields: wall time, rows, peak RSS (where available), notes, ok.
- Grow packs toward thesis §7 (JSONL extract, incremental partition, star build, validate latency).
- Use Criterion benches under `crates/rbt/benches/` for engineering measurement; keep CI light.
- Promotion rule: README/marketing claims only after packs + reproducible notes (machine class, commit SHA, dataset seed).

## Non-goals

- Spark comparison in default PR CI without an external Spark environment.
- “Most efficient in the world” as day-zero copy.
- Micro-optimizations (SIMD, custom allocators) before packs show need.

## Status

- **0.5.0:** scenarios `smoke_pipeline`, `validate_dx`, `incremental_append`.
- Spark/serde public comparison packs still open.
- Thesis: [thesis.md](../../thesis.md) §7. Docs: [docs/P4_CAPABILITIES.md](../P4_CAPABILITIES.md).

## Related

- [[Product north star]]
- [[Honest product surface]]
- [[Memory-honest materialization]]
- [[Team-scale lake positioning]]

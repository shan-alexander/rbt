---
tags: [product, goal, P1, streaming, memory]
node_type: goal
aliases: [P1, streaming materialize, bronze spill, lake as truth]
---
# Memory-honest materialization

**One-line:** Pull Arrow batches, assert and encode immediately, drop them, then point `ref()` at the lake file — never hold a second full in-memory copy of a model result by default.

## Goals

- Default **stream** materialize (`execute_stream` → Parquet write → atomic `.rbt-partial` publish).
- Default **`ref_strategy: parquet`** (lake re-read) for downstream models; optional memtable with row cutoff for tiny dims.
- Streaming assertions (not_null / accepted_values / unique via trackers) without collecting full frames.
- Bronze multi-file Arrow IPC **spill** to Parquet listing tables when needed (`scan.spill_arrow_ipc`).
- Keep **collect** only as emergency/debug (`RBT_MATERIALIZE_MODE=collect` / config).

## Non-goals

- Custom allocators / io_uring / SIMD as core architecture before measure proves need.
- Remote object_store writers until remote lake write is a committed roadmap item (see streaming plan §13).

## Status

- **Shipped** stream + spill path (**0.3.8 / 0.3.9**). Further harden optional.
- Plan: [docs/STREAMING_MATERIALIZE_PLAN.md](../STREAMING_MATERIALIZE_PLAN.md). Ref tradeoffs: [docs/REF_STRATEGY.md](../REF_STRATEGY.md).

## Related

- [[Primary path spine]]
- [[Measured claims before marketing]]
- [[Iceberg system of record]]

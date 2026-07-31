# `ref()` registration: lake Parquet vs MemTable

> **0.3.8+:** Model SQL is written with **stream materialize** by default
> (`materialize.mode: stream`). This page covers only the **downstream `ref()`**
> backend (MemTable vs lake re-read). See [STREAMING_MATERIALIZE_PLAN.md](STREAMING_MATERIALIZE_PLAN.md)
> for write-path streaming.

After each model materializes, rbt must expose it so later models can
`{{ ref('model_name') }}`. Two backends are supported; the default is
**lake-as-truth Parquet re-read**.

## Defaults (no yaml required)

If `materialize:` is omitted from `rbt_project.yml`:

| Setting | Default |
|---------|---------|
| `ref_strategy` | **`parquet`** — always re-read the written lake file |
| `memtable_max_rows` | **`50000`** — only applies when strategy is `memtable` |

You do **not** need to add any config for the recommended path.

## Optional config

```yaml
# rbt_project.yml — all optional
materialize:
  # parquet (default) | memtable
  # aliases: parquet_reread, lake, file  |  mem_table, memory, arc
  ref_strategy: parquet

  # Only used when ref_strategy: memtable.
  # Keep MemTable if row_count < this; otherwise re-read lake file.
  # Default: 50000
  memtable_max_rows: 50000
```

### Opt into MemTable for small models

```yaml
materialize:
  ref_strategy: memtable
  # memtable_max_rows: 50000   # optional; defaults to 50_000
```

With this set:

- `row_count < memtable_max_rows` → register DataFusion **MemTable** (Arrow Arc retain)
- `row_count >= memtable_max_rows` → **Parquet re-read** (same as default)

## Why the default is Parquet re-read

Criterion suite: `cargo bench -p rbt-datalake --bench downstream_ref`  
(Machine: AMD Ryzen 7 PRO 6850U, 2026-07-31 — see [crates/rbt/benches/README.md](../crates/rbt/benches/README.md))

### Decision cost (size threshold)

| Signal | Median |
|--------|--------|
| Known row count from materialize | **~0.8 ns** |
| `if rows < 100_000` | **~0.8 ns** |
| Parquet footer `num_rows` | **~23 µs** |
| `register_parquet` | **~0.4 ms** |

A cutoff check is free relative to either backend.

### Query wall clock (register + SQL)

| Rows | Query | MemTable | Parquet re-read | Δ |
|-----:|-------|---------:|----------------:|--:|
| 1k–2M | `count(*)` | ~0.65–0.73 ms | ~1.0–1.15 ms | **+~0.4 ms** |
| 100k | filter + project | ~1.25 ms | ~4.2 ms | **+3 ms** |
| 500k | `sum(px)` | ~1.0 ms | ~6.4 ms | **+5 ms** |
| e2e stg 1d (35k) | `count(*)` | 0.65 ms | 1.49 ms | **+0.8 ms** |

Full e2e 9-model DAG is **~20 s**. Absolute MemTable gains are **milliseconds** per `ref`.

### Tradeoffs

| | Parquet re-read (default) | MemTable (opt-in, small rows) |
|--|---------------------------|-------------------------------|
| Peak RSS | **Low** — drop batches after write | High if large tables retained |
| Wall time per `ref` | Slightly higher (ms) | Slightly lower |
| Lake as truth | **Yes** | RAM can diverge until re-run |
| Streaming materialize | Natural fit | Fights stream-and-drop design |

**Recommendation:** keep the default. Enable `memtable` only if you have many tiny dims and measured a need.

## Related

- Streaming plan: [STREAMING_MATERIALIZE_PLAN.md](STREAMING_MATERIALIZE_PLAN.md)
- Benches: [../crates/rbt/benches/README.md](../crates/rbt/benches/README.md)

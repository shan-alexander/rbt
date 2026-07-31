# Implementation Plan: Streaming Materialize

- **Status**: **Phases 1–4 landed through 0.3.9** (stream write, assertions, lake `ref()`, **Arrow IPC bronze spill→Parquet**). Phase 5+ (object store) still planned.  
- **Date**: 2026-07-31  
- **Package**: `rbt-datalake` (lib import `rbt::`)  
- **Related**: [CONTRIBUTING.md](../CONTRIBUTING.md) (stream write priority), [thesis.md](../thesis.md), [adr/ADR_001_PROJECT_STRUCTURE.md](adr/ADR_001_PROJECT_STRUCTURE.md)  
- **Goal**: End-to-end model execution that **never holds a full model result in RAM as `Vec<RecordBatch>`**, while preserving correctness of frontmatter tests, full-refresh Parquet (and FS Iceberg layout), and downstream `ref()` resolution.

### Shipped (0.3.8)

| Item | Location |
|------|----------|
| `materialize_stream` / `write_parquet_stream` | [`crates/rbt/src/materializer/stream.rs`](../crates/rbt/src/materializer/stream.rs) |
| Atomic `.rbt-partial` → rename | `atomic_publish` |
| `StreamingAssertionRunner` | [`crates/rbt/src/testing/mod.rs`](../crates/rbt/src/testing/mod.rs) |
| Engine `MaterializeMode::Stream` default | [`crates/rbt/src/engine/mod.rs`](../crates/rbt/src/engine/mod.rs) |
| Config `materialize.mode` / row-group knobs | [`MaterializeConfig`](../crates/rbt/src/core/project.rs) |
| `collect` fallback | `materialize.mode: collect` or env `RBT_MATERIALIZE_MODE=collect` |

---

## 1. Why this is huge

Today’s hot path in [`TransformationEngine::execute_dag`](../crates/rbt/src/engine/mod.rs) is:

```text
ctx.sql(compiled) → DataFrame::collect() → Vec<RecordBatch>
                  → MultiFormatWriter::write_batches(&[…])
                  → MemTable::try_new(schema, vec![batches.clone()])  // 2× RAM
                  → register_table for downstream ref()
```

On the full e2e market project this already materializes **~3.1M rows** for `stg_ohlcv_1m` / `tf_bar_metrics` / `fact_1m_bars`. Peak RSS is roughly:

| Contributor | Rough order |
|-------------|-------------|
| Bronze `ScanMemTable` for Arrow IPC hive | large (all selected partitions) |
| `collect()` of model output | **O(result size)** |
| Parquet encode buffer (row group) | O(row group) |
| `batches.clone()` into `MemTable` | **another O(result size)** |

Streaming materialize attacks the **two O(result)** copies on the write path and re-registration path. Bronze ingest streaming is a **second phase** (see §8); this plan prioritizes **SQL model → lake file** first, because that is the wall every large transform hits after bronze is registered.

---

## 2. Definitions (be precise)

| Term | Meaning in rbt |
|------|----------------|
| **Streaming materialize** | Consume a DataFusion `SendableRecordBatchStream` batch-by-batch; encode/write each batch (or small window) to disk; drop batch before pulling the next. Peak memory ≈ **peak in-flight batches + Parquet row-group encoder state**, not full result. |
| **Zero-copy (Arrow sense)** | Share `Buffer` / `ArrayData` via `Arc` without cloning payload bytes. `RecordBatch::clone` is shallow (refcount); **`Vec<RecordBatch>` still pins all batches alive**. Zero-copy does **not** mean “Parquet without buffers.” |
| **Byte-level / selective edge** | Bronze JSONL via jshift path extract; Parquet/CSV projection pushdown. Orthogonal to SQL materialize streaming but part of the same memory story. |
| **Bounded encode buffer** | Parquet **must** buffer at least one row group before flush. That is a format constraint, not a failure of streaming. Target: **configurable max row-group bytes/rows**, early `flush()`. |

**Non-goals for v1 of this plan**

- Distributed / multi-node shuffle (Ballista, etc.).
- True Iceberg OCC multi-writer commits (separate SoR proof).
- Replacing DataFusion with a custom vectorized engine.
- Inventing a new file format.
- Guaranteeing “zero allocations” during SQL (DF allocates; we bound **retained** memory).

---

## 3. Target architecture

```text
                    ┌─────────────────────────────────────┐
                    │  DataFusion SessionContext          │
                    │  (ListingTable / MemTable / future  │
                    │   lake providers for upstream ref)  │
                    └─────────────────┬───────────────────┘
                                      │ execute_stream()
                                      ▼
                    ┌─────────────────────────────────────┐
                    │  StreamMaterialize pipeline         │
                    │  for each RecordBatch:              │
                    │    1. streaming assertions (local)  │
                    │    2. global unique state (bounded) │
                    │    3. ArrowWriter::write(batch)     │
                    │    4. optional spill index / stats  │
                    │    5. drop batch (Arc refcount → 0) │
                    │  close writer → atomic rename      │
                    └─────────────────┬───────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │  Downstream ref() registration      │
                    │  Prefer ListingTable on written path│
                    │  (no MemTable of full result)       │
                    └─────────────────────────────────────┘
```

### 3.1 API shape (proposed)

```rust
// conceptual — materializer module
pub struct StreamWriteStats {
    pub rows: usize,
    pub batches: usize,
    pub bytes_written: u64,
    pub peak_writer_memory: usize,
    pub path: PathBuf,
}

pub async fn materialize_stream(
    stream: SendableRecordBatchStream,
    format: &OutputFormat,
    dest: &Path,
    opts: &MaterializeOptions,
    assertions: &[Assertion],
) -> Result<StreamWriteStats>;
```

Engine change (core of the win):

```rust
// BEFORE
let batches = df.collect().await?;
MultiFormatWriter::write_batches(&batches, &format, &dest)?;
let mem = MemTable::try_new(schema, vec![batches.clone()])?;

// AFTER
let stream = df.execute_stream().await?;
let stats = materialize_stream(stream, &format, &dest, &opts, &assertions).await?;
register_downstream_from_path(&self.ctx, &model.name, &dest, &format).await?;
```

`execute_sql` already returns `SendableRecordBatchStream`; the DAG path simply never called it for materialize.

---

## 4. Zero-copy and “byte-shifting” — what is realistic

### 4.1 Where zero-copy is real today

| Layer | Mechanism | Already / plan |
|-------|-----------|----------------|
| Arrow batches | `Arc<Buffer>`, `Array::slice` O(1) | Use; never deep-copy batches in the stream loop |
| DF stream | Pull-based `RecordBatchStream` | Wire `execute_stream` instead of `collect` |
| Parquet read (upstream) | Row-group / page pruning, projection | Prefer `ListingTable` for `ref()` so DF does not reload full tables into MemTables |
| JSONL bronze | jshift path extract without full DOM | Keep; later stream file-by-file into writer |

### 4.2 Where “byte-shift” is **not** free (honesty)

| Operation | Reality |
|-----------|---------|
| **Parquet encode** | Must re-layout columnar pages, compress (Snappy/Zstd), buffer a **row group**. Payload is **not** a memcpy of Arrow buffers into the file. |
| **CSV / JSONL write** | Row-wise serialization; streaming is still valuable (bounded), but not zero-copy. |
| **Unique / grain tests across full table** | Need a global key set or external spill; cannot be pure per-batch without state. |
| **Joins / sorts / window in SQL** | DataFusion may still materialize intermediates inside the physical plan. Streaming **output** does not make every operator O(1) memory. |

### 4.3 Design principle

> **Drop ownership of each `RecordBatch` as soon as it has been (a) assertion-updated and (b) accepted by `ArrowWriter::write`.**  
> Do not retain `Vec<RecordBatch>` for re-registration. Re-read from the lake path (Parquet listing) or register a **lazy** table provider.

That is the high-leverage definition of “streaming materialize” for rbt.

---

## 5. Memory budget model

Configure via `rbt_project.yml` / env (names TBD):

| Knob | Default (proposal) | Role |
|------|--------------------|------|
| `materialize.batch_size` | DF default (~8k rows) | Soft; DF produces what it produces |
| `materialize.max_row_group_rows` | 1_000_000 | Parquet `WriterProperties` |
| `materialize.max_row_group_bytes` | 128 MiB | Trigger `ArrowWriter::flush` when `in_progress_size` exceeds |
| `materialize.unique_mode` | `hash_in_memory` \| `spill` \| `skip` | Global uniqueness strategy |
| `materialize.downstream` | `listing` \| `mem_small` | How `ref()` sees the model |

**Peak RSS target (v1 acceptance):**

```text
peak ≈ bronze_resident
     + max(DF_operator_state)      // we don't fully control; measure
     + max_row_group_encoder
     + unique_state                 // if enabled
     + O(1) stream batches in flight
```

**Regression guard:** full e2e 1m path must complete with **RSS well below 2× result Parquet size** (today often ≫ because of collect + MemTable clone). Exact number locked by a measure pack (§10).

---

## 6. Streaming tests (hardest correctness piece)

Frontmatter tests today run on `&[RecordBatch]` after collect ([`RecordBatchValidator`](../crates/rbt/src/testing/mod.rs)).

| Assertion | Streaming strategy |
|-----------|--------------------|
| `not_null` | Per-batch; fail-fast |
| `accepted_values` | Per-batch; fail-fast |
| `unique` / `unique_key` / `grain` | **Global** — need cross-batch state |

### 6.1 Global unique (v1)

**In-memory hash set of keys** (current semantics, streaming ingestion):

- Key encode: already in `array_value_to_key` (improve later with hash of raw bytes / XxHash of concatenated fixed widths).
- Insert each row’s key; duplicate → fail (or warn per `on_error`).
- Memory: O(cardinality × key size). For `fact_1m_bars` grain `(symbol, timestamp_ns)` ≈ 3.1M keys → tens of MB, acceptable; for pathological high-cardinality string keys, need spill.

### 6.2 Global unique (v2 spill)

Only if measured pressure:

1. Spill sorted key runs to temp Parquet/Arrow IPC (or `tempfile` + sort).
2. External merge to detect duplicates.
3. Prefer **battle-tested** building blocks only (see §7); do **not** invent a new LSM.

### 6.3 Optional: two-pass mode

For users who refuse large unique state:

1. Pass A: stream write Parquet **without** unique (or with sampling).
2. Pass B: DF `SELECT key, count(*) … HAVING count(*) > 1` against **written** file via ListingTable.

Slower but **bounded RAM**. Expose as `tests.unique_strategy: reaggregate`.

---

## 7. Crate research (battle-tested only)

Policy: **prefer stack we already depend on**; any **new** crate must be widely used, maintained, and preferably Apache/Arrow ecosystem or industry standard.

### 7.1 Already in the tree (primary tools — use these)

| Crate | Version (workspace) | Role in streaming materialize | Quality |
|-------|---------------------|--------------------------------|---------|
| **`datafusion`** | 53.x | `DataFrame::execute_stream`, physical plan, `ListingTable` | Apache; production (Influx, etc.) |
| **`arrow`** | 58.x | `RecordBatch`, buffers, IPC/CSV/JSON writers | Apache |
| **`parquet`** | 58.x | `ArrowWriter`, `WriterProperties`, `AsyncArrowWriter`, flush/memory_size | Apache; **do not replace** |
| **`tokio`** | 1.x | Async stream consumption | de facto standard |
| **`iceberg` / `iceberg-datafusion`** | 0.10 | Later: stream data files then commit snapshot | Official Rust Iceberg |
| **`jshift`** | 0.6 | Bronze JSONL selective extract (edge streaming later) | Niche but already chosen |

**Critical APIs (no new dep):**

- [`DataFrame::execute_stream`](https://docs.rs/datafusion) → `SendableRecordBatchStream`
- [`parquet::arrow::ArrowWriter`](https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/struct.ArrowWriter.html): `write`, `flush`, `in_progress_size`, `memory_size`, `close`
- `WriterProperties::builder().set_max_row_group_size(…)` / byte limits where available on our pin
- `datafusion::datasource::listing::ListingTable` (+ `ListingTableConfig`) for downstream `ref()` without MemTable

### 7.2 Strong candidates if we **must** add a dependency

| Crate | Use | Battle-tested? | Verdict |
|-------|-----|----------------|---------|
| **`object_store`** | Unified local/S3/GCS async IO; multipart upload; works with Arrow/Parquet async paths | **Yes** — Apache Arrow project (Influx donation) | **Approve** when we leave local FS-only; pin version compatible with our `arrow`/`parquet` |
| **`bytes`** | Shared byte buffers; often already transitive | Yes | Prefer via parquet/arrow; don’t add unless direct API needs it |
| **`futures`** | `StreamExt`, `try_next` on DF streams | Yes; usually already transitive via DF | Use; explicit dep OK if cleaner |
| **`tempfile`** | Atomic write: write `*.tmp` then rename | Yes; already `dev-dependency` | Promote to normal dep for materialize staging files |
| **`tikv-jemallocator`** or **`mimalloc`** | Global allocator; can cut fragmentation under Arrow churn | Both production-grade (TiKV / Microsoft) | **Optional feature flag only**; measure before defaulting. Not required for correctness of streaming. |
| **`ahash` / `rustc-hash`** | Faster hash for unique keys | Widely used | Optional if `HashSet` shows up in profiles; start with std |

### 7.3 Rejected / defer (not for v1)

| Crate / idea | Why not |
|--------------|---------|
| Custom SIMD Parquet writer | Reimplements `parquet`; maintenance death |
| Polars as default materialize engine | Second engine; DF already streams; dual runtime cost |
| RocksDB / redb for unique spill | Heavy; only if hash spill is proven necessary |
| `io_uring` crates for v1 | Portability + complexity; tokio + buffered `File` first |
| Unmaintained “zero copy parquet” experiments | Stick to apache `parquet` |
| prost for stream control plane | Unrelated to materialize bytes |

### 7.4 Compression codecs

Already available via `parquet` features we can enable deliberately:

| Codec | When |
|-------|------|
| Snappy | Default-ish; fast encode (CPU vs size) |
| Zstd | Better ratio; more CPU — config knob |
| Uncompressed | Debug only |

Do **not** pull separate compression crates; use parquet’s feature flags.

---

## 8. Phased implementation plan

### Phase 0 — Instrumentation (1–2 days)

1. Log per model: `collect` wall time, `num_batches`, `num_rows`, process RSS (optional `memory-stats` **or** `/proc/self/status` behind cfg — prefer **no new dep**: parse VmRSS on Linux only for debug builds).
2. Add a microbench / script: smoke fixture + subset e2e under `RBT_MATERIALIZE=collect|stream` once stream exists.
3. Document baseline numbers in CHANGELOG when stream lands.

**Exit:** We can prove streaming reduced peak RSS on the same workload.

### Phase 1 — Stream write Parquet (core win) (3–5 days)

1. Implement `write_parquet_stream(stream, path, WriterProperties)` in `materializer`:
   - Open `File` (or `BufWriter` with large buffer, e.g. 1–8 MiB).
   - First batch establishes schema → `ArrowWriter::try_new`.
   - Loop: `stream.next().await` → `writer.write(&batch)` → if `in_progress_size() > threshold` then `writer.flush()`.
   - `writer.close()`; on error delete partial file.
2. **Atomic publish:** write to `dest.with_extension("parquet.partial")` (or `.#rbt-tmp-*`) then `std::fs::rename` to final path (POSIX atomic on same filesystem).
3. Wire engine path behind flag:
   - env `RBT_STREAM_MATERIALIZE=1` or project `materialize.mode: stream|collect`
   - default: **`stream` for Parquet** once tests pass; keep `collect` for emergency.
4. Extend multi-format:
   - JSONL/CSV: stream via existing `LineDelimitedWriter` / `csv::Writer` (same loop).
   - Iceberg FS layout: stream into `data/part-00000.parquet` then write metadata JSON (same as today, but data file from stream).
5. Unit tests: synthetic DF stream with many small batches; file readable; row count matches.

**Exit:** Full e2e `fact_1m_bars` succeeds in stream mode; peak RSS drop measured.

### Phase 2 — Downstream `ref()` without full MemTable (3–5 days)

This is the **second half** of the win (`batches.clone()` into MemTable).

1. After successful write, register model name as:
   - **Parquet:** `ListingTable` over the single file (or directory if we switch to dir-per-model later).
   - **Iceberg FS:** provider that reads the layout, or still ListingTable on `data/*.parquet` until real catalog.
2. Ensure `ref('model')` SQL resolves to that table name (already does via `register_table`).
3. **Small-result fast path:** if `rows < N` (e.g. 100k) and `materialize.downstream = auto`, keep MemTable for fewer file opens in tiny dims (`dim_symbol` 83 rows).
4. Cache schema from stream’s first batch / writer to avoid opening file twice when possible.

**Exit:** DAG of 9 models runs with **no** full-result MemTable for multi-million-row models; dims still fast.

### Phase 3 — Streaming assertions (2–4 days)

1. Split `RecordBatchValidator`:
   - `validate_batch_local` — not_null, accepted_values
   - `UniqueKeyTracker` — streaming insert API
2. Integrate into stream loop; fail-fast option aborts writer and removes partial file.
3. Document memory of unique tracker; add `unique_strategy: reaggregate` (two-pass) as escape hatch.

**Exit:** Smoke + e2e frontmatter tests pass under stream mode with same semantics as collect.

### Phase 4 — Bronze path streaming (larger; after Phase 1–3) (1–2 weeks)

Today `ScanMemTable` loads all Arrow IPC partitions into RAM before SQL ([`engine/bronze.rs`](../crates/rbt/src/engine/bronze.rs)).

| Format | Plan |
|--------|------|
| Parquet / CSV bronze | Prefer DF **ListingTable** (already Path A for some formats) with projection + hive partition pruning |
| Arrow IPC stream files | Option A: convert/landing as Parquet offline; Option B: multi-file IPC reader as custom `TableProvider` that streams file-by-file (more work) |
| JSONL + jshift | Stream file line windows → batch builder → either temp Parquet partition or DF `MemTable` **per file** then union — avoid one giant MemTable |

**Exit:** e2e bronze 1m does not require full IPC set resident if user configures listing/pruned path.

### Phase 5 — Object storage & async write (later)

1. Add **`object_store`** (Apache) when `s3://` / `gs://` targets are product-required.
2. Use `parquet::arrow::async_writer::AsyncArrowWriter` + object_store multipart upload.
3. Keep local `File` path as default (simpler, often faster for single-node lakes).

### Phase 6 — Optional performance polish (measure-driven)

1. Feature `jemalloc` / `mimalloc` global allocator — A/B on e2e RSS and time.
2. Larger `BufWriter` / `WriterProperties` dictionary limits.
3. Parallel column writers (`ArrowColumnWriter` / row-group factory) **only if** single-thread encode is the bottleneck after streaming lands.
4. Content-defined chunking (parquet CDC) — only if page skip benefits show up in queries.

---

## 9. Atomicity and failure semantics

| Event | Behavior |
|-------|----------|
| Stream error mid-write | Delete `.partial`; leave previous successful artifact if any (full refresh: document “last good file remains”) |
| Assertion failure | Same as stream error |
| Rename success | Readers see complete Parquet (footer written on `close` before rename) |
| Crash during rename | Rare; same-FS rename is atomic; document recovery (`*.partial` garbage collect) |

Full refresh today overwrites destination; streaming keeps that model: **new file fully written, then replace**.

---

## 10. Acceptance criteria & measurement

### Functional

- [ ] `RBT_STREAM_MATERIALIZE=1` (or default stream) passes `scripts/smoke.sh`
- [ ] Full e2e 9-model DAG completes with identical row counts (±0) vs collect mode
- [ ] Frontmatter unique/not_null behavior matches collect mode on smoke + e2e
- [ ] Downstream models that `ref()` multi-million-row tables still correct
- [ ] CI remains green under clippy `-D warnings`

### Performance (document numbers, don’t invent)

On the stock full e2e bronze snapshot (or a fixed subset fixture checked into CI if full bronze is too large for GH Actions):

| Metric | Collect (baseline) | Stream (target) |
|--------|--------------------|-----------------|
| Peak RSS during `fact_1m_bars` | measure | **≪** collect (goal: no second full copy) |
| Wall time full DAG | ~25s class (workstation) | not more than ~1.2–1.5× unless RSS win is large |
| Output Parquet row count | 3,110,044 (1m facts) | equal |

Add `examples/smoke_fixture` memory test optional; full e2e remains local/nightly if CI disk/RAM limited.

---

## 11. Suggested code touch map

| Module | Change |
|--------|--------|
| `engine/mod.rs` | `execute_stream` path; register listing; feature flag |
| `materializer/mod.rs` | `materialize_stream`, atomic rename, WriterProperties |
| `testing/mod.rs` | Streaming local asserts + `UniqueKeyTracker` |
| `core/project.rs` / `rbt_project.yml` | Optional materialize config block |
| `main.rs` | Flag / log stream vs collect |
| `engine/bronze.rs` | Phase 4 only |
| `examples/*/README.md` | Document flag and memory expectations |
| `scripts/smoke.sh` | Run once with stream mode |

No new workspace crates required for Phases 1–3.

---

## 12. Risk register

| Risk | Mitigation |
|------|------------|
| DF operators still OOM (sort/hash join) | Streaming output ≠ streaming all ops; partition prune; incremental models later; measure |
| Unique set OOM | `reaggregate` two-pass; spill phase |
| ListingTable slower than MemTable for tiny dims | Auto threshold → MemTable for small results |
| Schema evolution mid-stream | Reject schema change after first batch (hard error) |
| Async vs sync writer complexity | Phase 1 sync `File` + `spawn_blocking` if needed for pure sync ArrowWriter under tokio |
| Double-open file for register | Pass schema + path; ListingTable metadata cache |

---

## 13. Recommended default product policy

1. **Ship Phase 1+2+3** as the default materialize path for Parquet full refresh.  
2. Keep **`collect` mode** behind config for debugging and bisect.  
3. **Do not** add exotic dependencies until Phase 1 is green on full e2e.  
4. Add **`object_store` only** when remote lake write is a committed roadmap item.  
5. Treat allocator crates as **optional features**, not core architecture.

---

## 14. One-sentence thesis for contributors

**Pull Arrow batches from DataFusion, assert and Parquet-encode them immediately, drop them, then point `ref()` at the file — never build a second full in-memory copy of a model result.**

---

## 15. Next concrete PR sequence

| PR | Title | Depends |
|----|-------|---------|
| 1 | `materializer`: stream Parquet writer + atomic rename + unit tests | — |
| 2 | `engine`: `execute_stream` path + env/project flag; keep collect fallback | 1 |
| 3 | `engine`: register `ListingTable` for large outputs; small MemTable fast path | 2 |
| 4 | `testing`: streaming not_null/accepted + UniqueKeyTracker | 2 |
| 5 | smoke + e2e verification + CHANGELOG RSS notes | 3, 4 |
| 6 | (optional) bronze listing-first / IPC streaming | 5 |
| 7 | (optional) `object_store` + AsyncArrowWriter | 5 |

This plan is ready to execute starting at PR 1 without further research blockers.

---

## 16. After streaming materialize is implemented — other roadmap items

Streaming materialize is a **memory/throughput foundation**. Once Phases 1–3 land and measure packs show RSS wins, prioritize the following product tracks (order is guidance, not a hard Gantt chart). Criterion benches under `crates/rbt/benches/` are the measurement harness for claims below.

### 16.1 Bench tests & performance program

| Item | Why |
|------|-----|
| Grow Criterion suite (smoke / e2e-1d / e2e-full / stream vs collect) | Defend “team-scale lake” claims with numbers |
| RSS / peak memory capture alongside wall time | Streaming SoR for success criteria (§10) |
| Fixed dataset seeds + machine class notes | Comparable runs across machines |
| Optional `rbt-measure` scenario packs (thesis) | Public Spark/serde comparisons only after packs exist |
| CI: lightweight benches only; full e2e nightly/local | Keep PR CI fast |

### 16.2 Iceberg system-of-record proof

| Item | Why |
|------|-----|
| Official Rust `iceberg` create → write data files → **commit snapshot** → read back via DF | CONTRIBUTE / thesis table-truth gate |
| One catalog path first (filesystem or REST), not multi-catalog sprawl | Avoid WAP theater |
| Stream materialize → Iceberg data files, then commit | Compose with this plan’s Phase 1–5 |
| Time travel / snapshot id in run report | Ops and debugging |

### 16.3 `preview` / `validate` (DX loop)

| Item | Why |
|------|-----|
| `rbt validate` — syntax, refs, layer rules, optional schema bind | Fail before heavy IO |
| `rbt preview --limit N` — `execute_stream` + take N rows (shares stream path) | Instant feedback; natural fit after streaming |
| `rbt explain` — DataFusion logical/physical plan dump | Trust and tuning |
| Structured errors (`E_RBT_*`, suggestions) | Agent-repairable DX (JSON first; prost later) |

### 16.4 Ergonomic enhancements

| Item | Why |
|------|-----|
| Clearer CLI progress (model, rows/s, phase: scan/sql/write) | Long e2e runs need signal |
| Project config defaults + comments polish | Onboarding |
| Better `--select` UX / error messages | Already partial |
| Frontmatter schema docs / examples | Column `description`/`context` already started |
| Small-result MemTable vs ListingTable auto threshold | Phase 2 of this plan |

### 16.5 Autodetect bronze structural layouts

| Item | Why |
|------|-----|
| Infer hive partitions from path (`key=value`) without full frontmatter | Faster onboarding |
| Detect Arrow IPC file vs stream (partially done) | Fewer footguns |
| Infer format from extension / magic bytes | `source_format` optional |
| Sample N files → draft staging frontmatter | `rbt init` / `rbt discover` style |
| Partition value cardinality / schema drift warnings | Lake hygiene |

### 16.6 CI

| Item | Why |
|------|-----|
| Keep `fmt` + `clippy -D warnings` + unit + smoke | Gate |
| Optional bench job (smoke-scale only) | Catch regressions without 447MB bronze on every PR |
| Full e2e + full Criterion as scheduled/manual workflow | When runner disk/RAM allows |
| Cache cargo + bronze artifact strategy if e2e moves to CI | Cost control |

### 16.7 Cloud integration

| Item | Why |
|------|-----|
| `object_store` (Apache) for `s3://` / `gs://` / `az://` | Same code path local + cloud |
| `AsyncArrowWriter` + multipart upload | Streaming write to object storage |
| Credentials via env/standard AWS/GCP chains | Ops reality |
| Remote bronze ListingTable scan | Don’t download whole lake to disk first |
| Document cost/latency expectations | No magic cloud claims |

### 16.8 Suggested sequencing after stream land

```text
stream materialize (this plan Phases 1–3)
    → expand benches + RSS (16.1)
    → preview + validate (16.3)          # cheap DX wins on stream path
    → Iceberg SoR proof (16.2)           # table truth
    → bronze autodetect (16.5)           # adoption
    → cloud object_store (16.7)          # when users leave local FS
    → CI expansion (16.6) interleaved
```

Ergonomics (16.4) land continuously as small PRs; never block the spine.

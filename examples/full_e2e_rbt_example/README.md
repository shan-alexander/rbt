# Full E2E example: external bronze → silver → gold with `rbt`

Compatible with **rbt-datalake 0.3.7+** (package `rbt-datalake`, binary / lib import `rbt`).

This example shows how **`rbt` sits between a data lake landing zone and analytics-ready tables**.

Another system already wrote real equity OHLCV bars into a hive-partitioned bronze zone (Arrow IPC streams). You do **not** re-ingest with rbt. You declare models, compile a DAG, and materialize **silver** and **gold** Parquet tables under the same lake root.

### 0.3.7 practices used here

| Practice | Where |
|----------|--------|
| Named **`roots:`** + `$lake/...` templates | [`rbt_project.yml`](rbt_project.yml) |
| Lake-as-truth **`ref()`** (default parquet re-read) | omit `materialize:` (default) |
| **`path_glob: "**/*.arrow"`** on staging | isolate artifacts under hive trees |
| **`partition_by` + `require_partitions`** | grain filter (`1m` / `1d`) without scanning other timeframes |
| **`inject_source_path`** | latest-path dedupe lineage |
| Scan→MemTable bronze path | forced by globs + partitions (DF listing pushdown off by design) |

See [docs/MULTI_ROOT_AND_PATH_GLOB.md](../../docs/MULTI_ROOT_AND_PATH_GLOB.md) and [docs/REF_STRATEGY.md](../../docs/REF_STRATEGY.md).

| Layer | Owner | Role in this example |
|-------|--------|----------------------|
| **Bronze** | External ingest (not rbt) | `lake/bronze/symbol=*/timeframe=*/*.arrow` |
| **Silver** | staging + transforms | Deduped OHLCV grain; `tf_symbol` + `tf_bar_metrics*` prep |
| **Gold** | marts | `dim_symbol`, `fact_*` (MACD/RVOL/RSI/…), `obt_symbol_summary` |

**Materialization policy:** **full Parquet rewrite** on every successful model run (no Iceberg). That is intentional: small projects often prefer a pure DAG + clean tables over table-format ops.

**DAG shape:**

```text
stg_ohlcv_1m ─┬─► tf_bar_metrics ──► fact_1m_bars ─┐
              │                                    │
              └─► tf_symbol ──► dim_symbol ────────┼─► obt_symbol_summary
stg_ohlcv_1d ─┬─► tf_bar_metrics_1d ► fact_1d_bars ┘
              └─► (into tf_symbol coverage)
```

---

## Prerequisites

1. **Rust toolchain** (stable, matching `rustc`/`cargo` from the same install — e.g. rustup).
2. Build the CLI from the repository root:

```bash
cd /path/to/rbt
cargo build -p rbt-datalake --release
# binary: ./target/release/rbt
# or: cargo install rbt-datalake
```

3. **Bronze data present** under:

```text
examples/full_e2e_rbt_example/lake/bronze/
  symbol=NVDA/timeframe=1m/*.arrow
  symbol=AMD/timeframe=1d/*.arrow
  …
```

If you only have this README and empty bronze, restore bronze from your lake backup; rbt will not fabricate market data.

4. Optional: [DuckDB](https://duckdb.org/) CLI to spot-check Parquet outputs (examples below).

---

## What’s in the bronze (external contract)

- **Format:** Arrow IPC **stream** (many files use a `.arrow` extension; rbt auto-detects file vs stream).
- **Layout:** Hive partitions  
  `symbol=<TICKER>/timeframe=<1m|1d|5s>/bulk_chunk_*.arrow`
- **Payload schema (all timeframes):**

| Column | Type | Notes |
|--------|------|--------|
| `symbol` | Utf8 | Also present in path |
| `timestamp` | Utf8 | 1m: epoch seconds as string; 1d: `YYYYMMDD` |
| `timestamp_ns` | Int64 | Authoritative event time (ns) |
| `open`, `high`, `low`, `close` | Float64 | |
| `volume` | Int64 | |

`timeframe` is **only** in the path. Staging models use frontmatter `partition_by` + `require_partitions` so rbt injects `timeframe` and scans one grain at a time.

---

## Staging design (this example)

Staging is the **first rbt-owned, analytics-ready history** of a ticker—not a raw dump of bronze.

### Frontmatter (contract)

| Field | Role |
|-------|------|
| `description` | Human / agent docs |
| `scan_path` / `source_format` | External bronze root |
| `partition_by` / `require_partitions` | Hive path → columns + grain filter (`1m` / `1d`) |
| `inject_source_path: true` | Adds `_source_path` so SQL can pick **latest chunk** on dups |
| `grain` / `unique_key` | `(symbol, timestamp_ns)` |
| `tests.not_null` / `tests.unique` / `accepted_values` | Run after materialize; fail the model on error |
| `meta.dedupe` | Documents strategy (`latest_source_path`) |

### SQL shape (CTEs)

```text
raw     → read bronze source (+ path columns)
typed   → casts, null/OHLC geometry filters
ranked  → ROW_NUMBER() PARTITION BY grain ORDER BY _source_path DESC
final   → _rn = 1  (+ bar_time, bronze_dup_count, source_path)
```

**Dedupe rule:** for the same `(symbol, timestamp_ns)`, keep the row whose bronze file path sorts last. Chunk names look like `bulk_chunk_YYYYMMDD_HHMMSS_…arrow`, so lexicographic order approximates **most recent ingest**.

Transforms **do not** re-dedupe bronze again:
- `tf_symbol` — symbol rollups → `dim_symbol`
- `tf_bar_metrics` / `tf_bar_metrics_1d` — window indicators → facts

### Frontmatter on every model

Each `.sql` file has YAML frontmatter with at least:

| Field | Purpose |
|-------|---------|
| `description` | Short human summary |
| `context` | Longer agent-oriented rationale |
| `columns.<name>.description` | Short column label |
| `columns.<name>.context` | Verbose column intel for agents |
| `columns.<name>.dtype` / `unit` | Optional type/unit hints |
| `grain` / `unique_key` / `tests` | Contract + post-run assertions |
| `tags` / `meta` | Selection and tool metadata |

### Indicators (facts)

Computed in `tf_bar_metrics*` (prep), selected into facts:

| Metric | Definition in this example |
|--------|----------------------------|
| `ret_1` / `log_ret_1` | Simple / log return vs prior bar |
| `sma_12/20/26/50` | Simple moving averages of close |
| `macd_line/signal/hist` | **SMA-proxy MACD** (not EMA); documented in `meta.macd_method` |
| `rvol_20` | `volume / avg(volume)` over prior 20 bars |
| `rsi_14` | RSI from average gains/losses (SMA, not Wilder) |
| `volatility_20` | Stddev of `ret_1` (1m) |

True EMA-MACD can move to a Rust UDF later (ADR-003) without changing the DAG shape.

---

## Indexes, optimization, and Iceberg (FAQ)

### Would a table “index” make sense?

On a **warehouse** (Snowflake/Postgres), yes—B-tree indexes help point lookups. On a **file lake + columnar engine**, the analogous tools are:

| Mechanism | Helps with | In this example |
|-----------|------------|-----------------|
| **Hive / Iceberg partitions** | Prune whole directories/files (`symbol=`, `timeframe=`, later `dt=`) | Bronze already hive-partitioned; staging could write `symbol=…` dirs later |
| **Parquet row-group stats + sorting** | Min/max skip inside files | Sort by `(symbol, timestamp_ns)` on write (future materializer option) |
| **Z-order / clustering** (Iceberg/Delta) | Multi-column prune | After Iceberg SoR |
| **In-process hash indexes** | Rare for batch rbt runs | Not needed for full medallion materialize |

For **silver staging models** specifically, the highest ROI optimizations are usually:

1. **Don’t re-scan all bronze every time** — incremental by partition / watermark (future).  
2. **Push filters to scan** — `require_partitions`, path pruning (done for timeframe).  
3. **Sort + compact Parquet** on write by grain keys so gold joins/filters skip row groups.  
4. **Avoid double materialize** — staging owns grain; transforms stay thin (done).  
5. **Stream write** instead of full `collect()` for larger lakes (engine roadmap).  
6. **Column projection** at bronze — only columns staging needs (already narrow OHLCV).

Classical B-tree indexes on a single Parquet file are **low value** for DataFusion full scans of multi-GB tables; **partitioning + sorted Parquet + stats** are the lake-native “indexes.”

### Full Parquet rewrite vs Iceberg

| Option | When to prefer |
|--------|----------------|
| **Full Parquet rewrite (this example)** | Small/medium lakes, single writer, simple ops, “just give me clean tables + a DAG” |
| **Iceberg (product roadmap)** | Multi-writer, time travel, branch/clone envs, multi-engine consumers |

| | Guidance |
|--|----------|
| **Today** | Example is **full Parquet rewrite** end-to-end. Iceberg write/commit is not production-complete in rbt. |
| **Not a compromise** | Full rewrite is a **supported project style**, not a fallback shame mode. |
| **When to add Iceberg** | After create → append → commit → read works; then opt gold (or silver) into Iceberg without changing model SQL. |

---

## Project layout (recreate from bronze only)

Starting from **only** `lake/bronze/**`, the rbt project looks like this:

```text
full_e2e_rbt_example/
├── README.md                 ← this runbook
├── rbt_project.yml           ← project + layer paths
├── models/
│   ├── staging/              ← deduped history (frontmatter + columns.* + tests)
│   │   ├── stg_ohlcv_1m.sql
│   │   └── stg_ohlcv_1d.sql
│   ├── transforms/           ← preparatory tables for dim/fact
│   │   ├── tf_symbol.sql
│   │   ├── tf_bar_metrics.sql      # 1m MACD/RVOL/RSI/…
│   │   └── tf_bar_metrics_1d.sql
│   └── marts/
│       ├── dim_symbol.sql
│       ├── fact_1m_bars.sql
│       ├── fact_1d_bars.sql
│       └── obt_symbol_summary.sql
└── lake/
    ├── bronze/               ← INPUT (external; do not overwrite with rbt)
    ├── silver/               ← OUTPUT (created by rbt run)
    └── gold/                 ← OUTPUT (created by rbt run)
```

If you are scaffolding a **new** project against an existing lake:

```bash
mkdir -p my_project/models/{staging,transforms,marts}
# point lake_root / scan_path at your bronze
# copy model patterns from this example
```

---

## DAG (what will run)

**9 models / 5 tiers** (matches `rbt compile` output):

```text
Tier 0  stg_ohlcv_1m ──┐
        stg_ohlcv_1d ──┼── (bronze sources registered once each)
                       │
Tier 1  tf_bar_metrics    ← stg_ohlcv_1m
        tf_bar_metrics_1d ← stg_ohlcv_1d
        tf_symbol         ← stg_ohlcv_1m (+ optional 1d coverage)
                       │
Tier 2  dim_symbol ← tf_symbol
                       │
Tier 3  fact_1m_bars ← tf_bar_metrics    ⋈ dim_symbol
        fact_1d_bars ← tf_bar_metrics_1d ⋈ dim_symbol
                       │
Tier 4  obt_symbol_summary ← dim_symbol ⟕ fact_1d_bars (+ latest daily metrics)
```

Layer rules: staging only reads `source()`; transforms do not depend on marts; gold can join silver + gold.

---

## Runbook

All commands from the **repository root** (`rbt/`), unless noted.

### 1. Compile (DAG + bronze path checks)

```bash
./target/release/rbt compile \
  -p examples/full_e2e_rbt_example \
  --bronze-check fail
```

Expect:

- 5 execution tiers, **9 models**  
- `compile succeeded (bronze sources ok)`  
- Failures if `lake/bronze` is missing or frontmatter paths are wrong  

### 2. Run (materialize silver + gold)

```bash
./target/release/rbt run \
  -p examples/full_e2e_rbt_example \
  --format parquet \
  --bronze-check fail
```

Expect logs similar to:

```text
Registered bronze source bronze.ohlcv_1d …
Registered bronze source bronze.ohlcv_1m …
Executing DAG Tier 0 … Tier 4 …
Completed 9 models (… rows, 2 bronze sources) in ~tens of seconds (machine-dependent)
```

Outputs (paths relative to the example project):

| Model | Path |
|-------|------|
| `stg_ohlcv_1m` | `lake/silver/stg_ohlcv_1m.parquet` |
| `stg_ohlcv_1d` | `lake/silver/stg_ohlcv_1d.parquet` |
| `tf_bar_metrics` | `lake/silver/tf_bar_metrics.parquet` |
| `tf_bar_metrics_1d` | `lake/silver/tf_bar_metrics_1d.parquet` |
| `tf_symbol` | `lake/silver/tf_symbol.parquet` |
| `dim_symbol` | `lake/gold/dim_symbol.parquet` |
| `fact_1m_bars` | `lake/gold/fact_1m_bars.parquet` |
| `fact_1d_bars` | `lake/gold/fact_1d_bars.parquet` |
| `obt_symbol_summary` | `lake/gold/obt_symbol_summary.parquet` |

### 3. Spot-check with DuckDB (optional)

```bash
cd examples/full_e2e_rbt_example

duckdb -c "
SELECT 'stg_1m' t, count(*) c FROM read_parquet('lake/silver/stg_ohlcv_1m.parquet')
UNION ALL SELECT 'tf_metrics_1m', count(*) FROM read_parquet('lake/silver/tf_bar_metrics.parquet')
UNION ALL SELECT 'tf_symbol', count(*) FROM read_parquet('lake/silver/tf_symbol.parquet')
UNION ALL SELECT 'dim', count(*) FROM read_parquet('lake/gold/dim_symbol.parquet')
UNION ALL SELECT 'fact_1m', count(*) FROM read_parquet('lake/gold/fact_1m_bars.parquet')
UNION ALL SELECT 'obt', count(*) FROM read_parquet('lake/gold/obt_symbol_summary.parquet');
"

duckdb -c "
SELECT * FROM read_parquet('lake/gold/obt_symbol_summary.parquet')
ORDER BY bar_count_1m DESC
LIMIT 10;
"

duckdb -c "
SELECT symbol, bar_time, open, high, low, close, volume
FROM read_parquet('lake/gold/fact_1m_bars.parquet')
WHERE symbol = 'NVDA'
ORDER BY bar_time
LIMIT 5;
"
```

**Illustrative counts** (this bronze snapshot; your lake may differ):

| Table | Rows (this bronze snapshot) |
|-------|------:|
| `stg_ohlcv_1m` / `tf_bar_metrics` / `fact_1m_bars` | 3,110,044 |
| `stg_ohlcv_1d` / `tf_bar_metrics_1d` / `fact_1d_bars` | 35,093 |
| `tf_symbol` / `dim_symbol` / `obt_symbol_summary` | 83 |

Wall time for full DAG on a typical workstation: ~25s (collect + Parquet rewrite; not a benchmark claim).

Note: some symbols (e.g. NVDA in this dump) may have **1m only** and no `timeframe=1d` files; `obt_symbol_summary` uses a left join so they still appear with null daily metrics.

### 4. Clean outputs and re-run

```bash
rm -rf examples/full_e2e_rbt_example/lake/silver \
       examples/full_e2e_rbt_example/lake/gold
./target/release/rbt run -p examples/full_e2e_rbt_example --format parquet
```

Bronze is never deleted by rbt.

---

## How staging frontmatter maps to the lake

```yaml
# models/staging/stg_ohlcv_1m.sql (header) + roots from rbt_project.yml
# roots:
#   lake: lake
source_format: arrow_ipc
scan_path: "$lake/bronze"
path_glob: "**/*.arrow"          # strong glob: * = one segment, ** = recursive
partition_by: [symbol, timeframe]
require_partitions:
  timeframe: "1m"
inject_source_path: true
source_name: bronze
source_table: ohlcv_1m
```

| Key | Behavior |
|-----|----------|
| `scan_path` | Root of the external bronze tree (`$lake` → `roots.lake`) |
| `path_glob` | Keep only matching files (OR list allowed); disables DF listing pushdown for this source |
| `source_format` | `arrow_ipc` (auto file/stream) |
| `partition_by` | Inject Utf8 columns from `key=value` path segments when missing in the file |
| `require_partitions` | Only read files under matching hive segments (here: 1m bars only) |
| `inject_source_path` | Adds `_source_path` for latest-wins dedupe |
| `source()` | Registers `bronze.ohlcv_1m` for SQL |

The 1d model is the same pattern with `timeframe: "1d"` and `ohlcv_1d`.

---

## Model intent (Kimball / medallion)

| Model | Layer | Intent |
|-------|-------|--------|
| `stg_ohlcv_1m` / `stg_ohlcv_1d` | Bronze edge → silver | Absorb external Arrow IPC; type/filter; latest-path dedupe |
| `tf_bar_metrics` / `tf_bar_metrics_1d` | Silver transforms | Windowed returns, SMAs, MACD-SMA proxy, RVOL, RSI proxy |
| `tf_symbol` | Silver transforms | Per-symbol rollups for the dimension |
| `dim_symbol` | Gold dim | Instrument dimension from `tf_symbol` |
| `fact_1m_bars` / `fact_1d_bars` | Gold facts | Grain `(symbol, bar_time)` + indicators + dim attrs |
| `obt_symbol_summary` | Gold OBT | One row per symbol for dashboards / APIs |

No UDFs or Rust models are required for this path (see `docs/adr/ADR_003_UDF_RSMODELS.md` for future escape hatches).

---

## Recreating the example from scratch (checklist)

1. Obtain hive-partitioned bronze OHLCV Arrow streams (or copy `lake/bronze` from this tree).  
2. Create `rbt_project.yml` with `models_dir`, `layers.staging|transforms|marts` target paths under `lake/silver` and `lake/gold`.  
3. Add staging SQL with frontmatter `scan_path` + `require_partitions` for each grain you own.  
4. Add silver transforms (`ref` staging only).  
5. Add gold dims/facts (`ref` silver; facts may `ref` dims).  
6. `rbt compile --bronze-check fail` until the DAG and paths are clean.  
7. `rbt run --format parquet` and validate row counts / sample symbols.  

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `E_RBT_BRONZE_SCAN_PATH_NOT_FOUND` | Wrong cwd or missing bronze | Run from repo root; check `lake/bronze` |
| `No arrow_ipc files found (after partition filters)` | No `timeframe=1m` (or 1d) dirs | Inspect hive layout; adjust `require_partitions` |
| Arrow footer / parse errors | Stream IPC labeled `.arrow` | Use current rbt (auto file→stream); upgrade CLI |
| OOM on huge lakes | Full collect + bronze MemTable path | Narrow `require_partitions` / `path_glob`; see streaming materialize plan |
| Empty gold join rows | Dim built from 1m but fact from empty 1d | Check bronze coverage per symbol |
| `E_RBT_ROOT_UNKNOWN` | `$name` not in `roots:` | Define the root in `rbt_project.yml` |
| `E_RBT_PATH_GLOB_INVALID` | Bad glob syntax | Use globset syntax; `*` is single-segment, `**` recursive |
| `cargo` / `check-cfg` build errors | Mismatched cargo vs rustc | Use rustup’s matching `cargo`+`rustc` pair |

---

## Engine capabilities exercised here

- Project discovery + multi-tier DAG  
- Layer boundary conventions (`stg_` / `tf_` / `dim_` / `fact_` / `obt_`)  
- Frontmatter bronze contracts on **external** lake data  
- **`roots:`** templates for scan + layer targets  
- Nested hive directory scan with **`path_glob`** + partition filters  
- Arrow IPC stream + file auto-detect  
- Hive `partition_by` injection + `require_partitions` filters  
- Dual bronze sources in one project (`ohlcv_1m`, `ohlcv_1d`)  
- Default lake-as-truth **`ref()`** (Parquet re-read after materialize)  
- SQL `ref` / `source` compilation  
- Parquet materialization to layer target paths  
- Frontmatter tests on materialize  
- Dim ∩ fact joins and OBT summary  

**Not yet claimed:** real Iceberg catalog snapshot commits, `validate`/`preview`/`explain`, WAP, Rust models/UDFs, streaming materialize. (`--select` and frontmatter `test` work.)

---

## Related docs

- [thesis.md](../../thesis.md) — product positioning  
- [CONTRIBUTING.md](../../CONTRIBUTING.md) — priorities and lake vs code  
- [docs/MULTI_ROOT_AND_PATH_GLOB.md](../../docs/MULTI_ROOT_AND_PATH_GLOB.md) — roots, globs, protobuf, listing pushdown  
- [docs/REF_STRATEGY.md](../../docs/REF_STRATEGY.md) — MemTable vs lake re-read  
- [docs/adr/ADR_003_UDF_RSMODELS.md](../../docs/adr/ADR_003_UDF_RSMODELS.md) — polyglot extensions (later)  
- Smaller smoke example: [../smoke_fixture](../smoke_fixture)

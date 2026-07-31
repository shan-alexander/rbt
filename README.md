# rbt — Rust lake build tool

**Medallion SQL DAGs** for filesystem / object-storage lakes: bronze files → silver → gold, with dbt-shaped models, frontmatter contracts, and in-process DataFusion execution.

> **Status:** **`0.3.8`.** One package: **library + CLI binary `rbt`** (`rbt-datalake` on crates.io). Spine works (`compile` / `run` / `test` / `--select`). Default materialization is **streaming full-refresh Parquet** (no full-result RAM retention); default `ref()` is **lake Parquet re-read**. Multi-root paths, `path_glob`, opaque protobuf bronze. **Filesystem Iceberg-style tables** via `--format iceberg` (data + metadata layout; not multi-catalog OCC).

## Why rbt

| | |
|--|--|
| **Niche** | Bronze → silver → gold on a lake, not warehouse-dbt |
| **Stack** | Rust + Arrow + DataFusion + optional Iceberg-style FS tables + jshift |
| **UX** | Models, `ref` / `source`, frontmatter tests, CLI select |
| **Claim** | Replace ad-hoc scripts / Spark for team-scale medallion jobs |

## Install

```bash
cargo install rbt-datalake
rbt --help
```

**Library** (crate name differs from import path):

```toml
[dependencies]
rbt-datalake = "0.3.8"
```

```rust
use rbt::RbtProjectConfig; // lib name is still `rbt`
```

**Git / from source:**

```bash
cargo install --git https://github.com/shan-alexander/rbt --locked
```

**From this repo:**

```bash
git clone https://github.com/shan-alexander/rbt && cd rbt
cargo build -p rbt-datalake --release
# binary: ./target/release/rbt
```

Requires a matching `rustc`/`cargo` pair (see `rust-toolchain.toml`).

> **crates.io notes**
>
> - **Package name:** [`rbt-datalake`](https://crates.io/crates/rbt-datalake) (the short name [`rbt`](https://crates.io/crates/rbt) is an unrelated “Rust bot toolkit”). Binary and lib import path remain **`rbt`**.
> - Early `0.0.1` internals (`rbt-core`, `rbt-engine`, …) are **orphaned**. Each has a **0.0.4 deprecation stub** README pointing here. **Do not depend on them.**

## Quick start (smoke fixture)

Tiny JSONL bronze → staging → dim:

```bash
cargo build -p rbt-datalake --release
./target/release/rbt compile -p examples/smoke_fixture --bronze-check fail
./target/release/rbt run -p examples/smoke_fixture --select dim_ticker --format parquet
./target/release/rbt test -p examples/smoke_fixture --select dim_ticker
```

Full market example (large Arrow IPC bronze):

```bash
./target/release/rbt compile -p examples/full_e2e_rbt_example --bronze-check fail
./target/release/rbt run -p examples/full_e2e_rbt_example --format parquet
```

See [examples/smoke_fixture/README.md](examples/smoke_fixture/README.md) and
[examples/full_e2e_rbt_example/README.md](examples/full_e2e_rbt_example/README.md).

## CLI

| Command | Purpose |
|---------|---------|
| `rbt compile -p <proj> [--select …]` | DAG + bronze path checks |
| `rbt run -p <proj> [--select …] [--format parquet\|iceberg\|…]` | Execute subgraph (ancestors always included) |
| `rbt test -p <proj> [--select …]` | Run subgraph + frontmatter tests |
| `rbt bench` | In-memory throughput microbench |

### `--select` (dbt-like)

| Spec | Meaning |
|------|---------|
| `dim_ticker` | That model **+ all ancestors** on `run`/`test` |
| `stg_trades+` | Model + descendants (+ ancestors on execute) |
| `+fact_x` | Model + ancestors |
| `a,b` | Union of selectors |

### `--format`

| Value | Output |
|-------|--------|
| `parquet` | Single `.parquet` file per model (default) |
| `iceberg` | Table dir: `data/part-*.parquet` + `metadata/v1.metadata.json` |
| `parquet-and-iceberg` | Flat parquet **and** sibling `.iceberg/` dir |
| `jsonl` / `csv` | Line/CSV files |

Iceberg here is a **filesystem table layout** written by rbt (full refresh), not Glue/REST multi-writer production yet.

## Project shape

```text
my_project/
  rbt_project.yml
  models/staging|transforms|marts/*.sql   # YAML frontmatter + SQL
  lake/bronze/…                           # external landing
  lake/silver/…                           # rbt outputs
  lake/gold/…
```

Staging frontmatter: `scan_path`, `source_format`, `path_glob`, `partition_by`, `grain`, `tests`, `columns.*.description` / `context`.  
Multi-root lakes, absolute targets, glob semantics, protobuf bronze, and when `path_glob` disables DataFusion listing pushdown: [docs/MULTI_ROOT_AND_PATH_GLOB.md](docs/MULTI_ROOT_AND_PATH_GLOB.md).

### Materialize + `ref()` (optional)

By default rbt **streams** model results to disk (`execute_stream` → atomic Parquet publish) and
**re-reads the lake file** for downstream `{{ ref() }}` (lake-as-truth; lower peak RSS).

```yaml
materialize:
  mode: stream                 # default; use collect only for emergency/debug
  # max_row_group_rows: 1000000
  # max_row_group_bytes: 134217728   # 128 MiB flush hint
  ref_strategy: parquet        # default lake re-read for ref()
  # ref_strategy: memtable
  # memtable_max_rows: 50000
```

Env: `RBT_MATERIALIZE_MODE=collect` forces legacy collect path.  
Tradeoffs: [docs/REF_STRATEGY.md](docs/REF_STRATEGY.md), [docs/STREAMING_MATERIALIZE_PLAN.md](docs/STREAMING_MATERIALIZE_PLAN.md).

## Package layout

Single workspace member: [`crates/rbt`](crates/rbt) (crates.io: **`rbt-datalake`**) — lib modules (`core`, `engine`, `scan`, `json`, `materializer`, `testing`) + bin/lib name **`rbt`**.

## Docs

- [CONTRIBUTING.md](CONTRIBUTING.md) — priorities and positioning  
- [thesis.md](thesis.md) — product thesis  
- [docs/README.md](docs/README.md) — ADR index, archive, publishing  
- [docs/adr/ADR_003_UDF_RSMODELS.md](docs/adr/ADR_003_UDF_RSMODELS.md) — polyglot roadmap  

## Benchmarks

```bash
cargo bench -p rbt-datalake --bench pipeline
# see crates/rbt/benches/README.md — uses smoke + full_e2e bronze when present
```

## CI

```bash
bash scripts/smoke.sh
```

GitHub Actions: [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

## Version / publish

See [CHANGELOG.md](CHANGELOG.md) and [docs/PUBLISHING.md](docs/PUBLISHING.md).

## License

Apache-2.0 — see [LICENSE](LICENSE).

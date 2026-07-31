# rbt — Rust lake build tool

**Medallion SQL DAGs** for filesystem / object-storage lakes: bronze files → silver → gold, with dbt-shaped models, frontmatter contracts, and in-process DataFusion execution.

> **Status:** **`0.0.2` experimental.** One package: **library + CLI binary `rbt`**. Spine works (`compile` / `run` / `test` / `--select`). Default materialization is **full Parquet rewrite**. **Filesystem Iceberg-style tables** via `--format iceberg` (data + metadata layout; not multi-catalog OCC). APIs may break between `0.0.x` releases.

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
rbt-datalake = "0.0.3"
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
> - Early `0.0.1` internals (`rbt-core`, `rbt-engine`, …) are **orphaned**. Each has a **0.0.3 deprecation stub** README pointing here. **Do not depend on them.**

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

Staging frontmatter: `scan_path`, `source_format`, `grain`, `tests`, `columns.*.description` / `context`.

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

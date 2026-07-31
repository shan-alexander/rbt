# rbt — Rust lake build tool

**Medallion SQL DAGs** for filesystem / object-storage lakes: bronze files → silver → gold, with dbt-shaped models, frontmatter contracts, and in-process DataFusion execution.

> **Status:** **`0.0.1` experimental.** The spine works (`compile` / `run` / `test` / `--select`). Default materialization is **full Parquet rewrite**. **Filesystem Iceberg-style tables** are supported via `--format iceberg` (data + metadata layout; not a full multi-catalog OCC product yet). APIs may break between `0.0.x` releases.

## Why rbt

| | |
|--|--|
| **Niche** | Bronze → silver → gold on a lake, not warehouse-dbt |
| **Stack** | Rust + Arrow + DataFusion + optional Iceberg-style FS tables + jshift |
| **UX** | Models, `ref` / `source`, frontmatter tests, CLI select |
| **Claim** | Replace ad-hoc scripts / Spark for team-scale medallion jobs |

## Install / build

```bash
git clone <repo> && cd rbt
cargo build -p rbt-cli --release
# binary: ./target/release/rbt
```

Requires a matching `rustc`/`cargo` pair (see `rust-toolchain.toml`).

## Quick start (smoke fixture)

Tiny JSONL bronze → staging → dim:

```bash
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

Staging frontmatter example: `scan_path`, `source_format`, `grain`, `tests`, `columns.*.description/context`.

## Workspace crates

| Crate | Role |
|-------|------|
| `rbt-cli` | Binary `rbt` |
| `rbt-core` | Project, frontmatter, DAG, select |
| `rbt-engine` | DataFusion + bronze registration + run |
| `rbt-scan` / `rbt-json` | Bronze multi-format / jshift |
| `rbt-materializer` | Parquet / Iceberg-FS / JSONL / CSV writers |
| `rbt-testing` | Arrow assertions |
| `rbt-catalog` / `rbt-models` | Thin / evolving |

## Docs

- [CONTRIBUTING.md](CONTRIBUTING.md) — priorities and positioning  
- [thesis.md](thesis.md) — product thesis  
- [docs/ADR_003_UDF_RSMODELS.md](docs/ADR_003_UDF_RSMODELS.md) — polyglot roadmap  

## CI

```bash
bash scripts/smoke.sh
```

GitHub Actions: [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

## Version / publish

See [CHANGELOG.md](CHANGELOG.md) and [PUBLISHING.md](PUBLISHING.md).  
crates.io: set a real `repository` URL in root `Cargo.toml`, then publish in dependency order at `0.0.1`.

## License

Apache-2.0 — see [LICENSE](LICENSE).

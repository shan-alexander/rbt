# Changelog

All notable changes to this project are documented in this file.

## [0.3.7] — 2026-07-31

### Added
- **`path_glob`** on staging frontmatter (string or list, OR match) for multi-artifact hive bronze
- **`roots:`** map in `rbt_project.yml` with `$name` / `${name}` path templates
- Explicit absolute + multi-root path resolution for layer `target_path` and `scan_path`
- **`source_format: protobuf`** opaque bronze (one row/file: `_source_path`, `payload`, `payload_len`)
- Docs: [docs/MULTI_ROOT_AND_PATH_GLOB.md](docs/MULTI_ROOT_AND_PATH_GLOB.md)

## [0.3.6] — 2026-07-31

### Added
- Configurable `ref()` registration via optional `materialize:` in `rbt_project.yml`
  - **Default:** lake-as-truth Parquet/file re-read (no config required)
  - **Opt-in:** `ref_strategy: memtable` with `memtable_max_rows` (default **50_000**)
- Docs: [docs/REF_STRATEGY.md](docs/REF_STRATEGY.md) with Criterion tradeoffs
- `TransformationEngine::execute_dag_with_materialize` for library/tests

### Changed
- Downstream `{{ ref() }}` no longer always retains a full-result MemTable

### Fixed
- Unique/grain key encoding for Parquet-sourced `Utf8View` / large strings (false duplicate failures after lake re-read)

## [0.0.3] — 2026-07-31

### Fixed
- Clippy clean under `-D warnings` (CI)
- Full e2e example README aligned to real 9-model DAG (`tf_bar_metrics*`, `tf_symbol`)
- Smoke fixture README: package name, select flags, output table

### Verified
- `cargo test -p rbt-datalake --lib` (41 tests)
- `bash scripts/smoke.sh`
- Full e2e: 9 models, ~3.1M 1m bars, 83 symbols, ~25s on workstation bronze snapshot

## [0.0.2] — 2026-07-31

### Changed
- **Single package `rbt-datalake`** (binary + lib import `rbt`): library + CLI binary in one crate (`crates/rbt` only)
- Workspace members reduced to `crates/rbt`; legacy `crates/rbt-*` trees removed
- Docs reorganized: active ADRs under `docs/adr/`, essays under `docs/archive/`
- Orphan crates on crates.io (`rbt-core`, `rbt-engine`, `rbt-scan`, `rbt-json`, `rbt-materializer`, `rbt-testing`) republished as **0.0.3 deprecation stubs** (README → use unified monorepo)

### Note
- crates.io package is **`rbt-datalake`** (`rbt` short name taken by unrelated crate). See [docs/CRATES_IO.md](docs/CRATES_IO.md).

## [0.0.1] — 2026-07-31

### Added
- CLI binary `rbt`: `compile`, `run`, `test`, `bench`
- `--select` with dbt-like `name` / `+name` / `name+` / `+name+` (execute mode always includes ancestors)
- Frontmatter-driven model tests on materialize; `rbt test` runs the selected subgraph
- Filesystem Iceberg-style table layout (`--format iceberg`, dual-write `parquet-and-iceberg`)
- Bronze scan: hive partitions, `require_partitions`, Arrow IPC stream auto-detect, `_source_path`
- Smoke fixture + `scripts/smoke.sh` + GitHub Actions CI workflow
- Column-level frontmatter (`description` / `context` / `dtype` / `unit`)

### Known limitations
- Iceberg path is full-refresh filesystem layout, not multi-catalog OCC / REST commit
- No `validate` / `explain` / `preview` yet
- Full in-memory collect for model execution
- API is `0.0.x` — may break

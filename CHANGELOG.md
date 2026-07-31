# Changelog

All notable changes to this project are documented in this file.

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
- Library API is multi-crate and still evolving (semver: 0.0.x may break)

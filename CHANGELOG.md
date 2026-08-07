# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added (RBT-A4 — content-addressed bronze fingerprints)

- `fingerprint:` in `rbt_project.yml`: `mode` (`path_stat` \| `content_hash`), `algo`
  (`blake3` \| `sha256`), `max_bytes_per_file`
- Fingerprint prefixes: `path_stat:fnv1a64:…`, `content:blake3:…`, `content:sha256:…`
- Legacy bare `fnv1a64:…` still matches path_stat on skip
- Mode mismatch forces re-execute (no false skip)
- CLI `--fingerprint-mode`; env `RBT_FINGERPRINT_MODE` / `_ALGO` / `_MAX_BYTES`

### Added (RBT-A3 — phased publish metadata on receipts)

- Frontmatter `phase:` (optional free-form) + existing `tags:` flow into receipts
- Receipt `models[]` entries: `name`, `status`, `row_count`, `phase`, `tags`,
  `elapsed_ms`, `output_path` (JSON key renamed from `model_results`; still reads legacy)
- `schema_version: 2`; skip receipts keep empty `models`
- CLI **`rbt run --json`**: compact run summary to stdout (`models[]`, `wall_ms`, fingerprint)
- CLI `--receipt-json`: full on-disk receipt body (debug); prefer `--json` for orchestrators

### Added (RBT-A2 — scoped_replace materialization)

- `materialization: scoped_replace` — deterministic `part-{scope_id}.parquet` under
  `{model}.parts/`; re-run of the same scope **replaces** that part only
- `part_key:` frontmatter (default: `partition_by` ∩ run vars); `scope_part_id` FNV hex
- Manifest `part_rows` map for accurate totals after replace
- Peer scopes untouched; `ref()` still lists the parts directory

### Added (RBT-A1 — multi-value partition scope)

- **`ScopeValue`**: run vars are scalar or multi (`Single` / `Multi`)
- **Repeated `--var key=v`**, **`--var key:=["a","b"]`**, **`--var-file key=path`**
- Multi partition vars → `require_partitions_in` (hive path **IN** filter); scalar still equality
- Path templates refuse multi-value keys (`E_RBT_VAR_MULTI`); use partition filters instead
- Receipts serialize multi vars as JSON arrays; `scope_key` is multi-aware
- Limit: `DEFAULT_MULTI_VAR_LIMIT` (100_000) → `E_RBT_VAR_LIMIT`

### Added (P0 contracts + contract-diff)

- **`contracts.enums`** in `rbt_project.yml`: named value registries with `values`, `on_new`
  (`fail`|`warn`|`allow`), optional `labels`, optional `probe: { model, column }`
- Frontmatter `tests.accepted_values` may reference a contract by name
  (`source: works.source`) or keep an inline list
- **`rbt validate --contract-diff`**: sample bronze (jsonl/json) for registered enums and
  report `new_in_bronze` vs registry; optional `--var` partition filters
- Codes: `E_RBT_CONTRACT_NEW_VALUE`, `W_RBT_CONTRACT_NEW_VALUE`, `E_RBT_CONTRACT_UNKNOWN`, …

### Examples

- **`complex_bronze_landing`** research mini-lake v2: dual tracks (**AI × semiconductors** +
  **AI agritech**), polite APIs (PubMed XML, Crossref JSON, Europe PMC JSON, **OpenAlex**,
  **Semantic Scholar** + seed, arXiv Atom + seed), `assets.jsonl` inventory, richer works
  schema (`authors_json`, abstracts, keywords, `topic_track`), gold `tf_source_run_stats` +
  `dim_topic` + `fact_source_run`. Policy skips: Google Scholar + Examine.com (not free papers).
- Example uses `contracts.enums` for `works.source` / `works.topic_track`.
- Analysis: [docs/analysis/bronze-source-onboarding-friction.md](docs/analysis/bronze-source-onboarding-friction.md)
  — frictions + rbt product enhancements for bronze source growth.

## [0.7.3] — 2026-08-02

### Examples

- **`complex_bronze_landing`** rebuilt as a **research-papers mini lakehouse**
  (semiconductors + ML): polite bronze lander (`scripts/fetch_bronze.py`) for
  PubMed E-utilities (XML), Crossref JSON, arXiv Atom (when not rate-limited),
  `robots.txt`, and HTML abstract cards
- Mixed bronze filetypes under hive partitions; silver `stg_*` endpoints; gold
  `tf_paper_status` + dims/fact with SK + Unknown + relationships
- Measure scenario `complex_bronze` reads `lake/lz/LATEST_RUN.json`

## [0.7.2] — 2026-08-01

### Fixed (medallion topology)

- **Silver endpoints are `stg_*` only.** Never `stg_*` → silver/`tf_*` → gold.
- **Gold transforms** (`models/transforms` → `gold/tf`) may **only `ref` `stg_*`**.
- **Silver prep transforms** may only ref bronze/`tf_*`, then land `stg_*` (optional).
- Engine: `E_RBT_LAYER_TRANSFORM_BAND` if a `tf_*` refs both `stg_*` and `tf_*`.
- Staging may `ref` silver prep transforms; cannot ref other `stg_*` or marts.
- Examples (smoke, full_e2e, complex_bronze) use `silver/stage` + `gold/tf` + `gold`.
- Docs: [GOLD_DEFAULT.md](docs/GOLD_DEFAULT.md), ADR-001 layer decision updated.

## [0.7.1] — 2026-08-01

### Fixed / docs (Kimball gold hygiene)

- Rewrite [docs/GOLD_DEFAULT.md](docs/GOLD_DEFAULT.md): silver/`tf` allowed; gold transforms only touch silver **stage**; no `source()` of upstream `tf_*`
- Example `complex_bronze_landing`: `dim_site` SK + Unknown (−1); thin `fact_units` with `site_sk` FK + relationship on SK
- `rbt validate` modeling warnings: grain/unique, mart scan contracts, `source(tf_*)`, fact without relationships

## [0.7.0] — 2026-08-01

### Added (P6 — gold default surface)

- **Parts sources (G1):** `parts: true` / auto-detect `.parts` dirs and `_rbt_manifest.json`; multi-file parquet registration
- **Lineage stamps (G8):** frontmatter `lineage_stamp: true` → `_rbt_run_id`, `_rbt_contract_version`, `_rbt_model`, `_rbt_bronze_fingerprint`
- **Relationship tests (G6):** `tests.relationships` FK-ish checks vs already-materialised models
- Docs: [docs/GOLD_DEFAULT.md](docs/GOLD_DEFAULT.md) (parts, completeness filters, lineage, tests, env roots)
- Example gold models on `complex_bronze_landing`: `dim_site`, `fact_units`

## [0.6.0] — 2026-08-01

### Added (P5a — scoped lakes & optional bronze)

- **Run scope:** CLI `--var key=value` (repeatable), env `RBT_VAR_*` / `RBT_VARS`, library [`RunScope`](crates/rbt/src/core/run_scope.rs)
- Template expansion `{key}` / `${key}` in `scan_path`, `path_glob`, `require_partitions`
- Partition binds: vars merge into effective `require_partitions` for `partition_by` keys
- **`on_missing: empty|error`** on bronze frontmatter — optional artifact families register typed empty frames
- **`columns.*.dtype`** drives empty-frame Arrow schema (`utf8`, `int64`, `float64`, `bool`, …)
- Frontmatter **`stage_mode`** hint (`full_refresh` | `latest_only` | `append` | `mirror_bronze`)
- Project **`contract_version`** for fingerprint identity

### Added (P5b — job contract)

- **Bronze fingerprint** (fnv1a64 over filtered file set + contract version)
- **`RunReceipt`** JSON under `{project}/.rbt/runs/` (+ `latest_{scope_key}.json`)
- CLI `--skip-if-match`, `--run-id`, `--contract-version`, `--write-receipt`, `--receipt-json`
- `TransformationEngine::execute_dag_with_scope`
- Example: [examples/complex_bronze_landing](examples/complex_bronze_landing/) — multi-artifact outer reconciliation
- Docs: [docs/COMPLEX_BRONZE_AND_RUN_SCOPE.md](docs/COMPLEX_BRONZE_AND_RUN_SCOPE.md)

### Added (P5c — prove scale)

- Measure scenarios: `stream_vs_collect`, `whale_synthetic`, `complex_bronze`
- [`ModeCompare`](crates/rbt/src/measure/mod.rs) in JSON reports (stream vs collect wall + RSS)
- Synthetic whale generator: `RBT_MEASURE_ROWS` (default 100k), `RBT_MEASURE_PARTS` (default 20)
- Unit tests for small whale + stream_vs_collect on smoke fixture

### Notes

- RSS is Linux VmRSS after each pass (directional, not allocator peak)
- Host domain quarantine / Restate stays out of core (one scoped invoke per entity)
- P6+ gold/merge/remote still on dual-track roadmap

## [0.5.0] — 2026-07-31

### Added (P4 — measure, incremental, WAP, UDFs)

#### Built-in scalar UDFs (ADR-003 Design A)
- Auto-registered on every `TransformationEngine`: `rbt_upper`, `rbt_lower`, `rbt_trim`, `rbt_nullif_empty`
- API: `register_builtin_udfs`, `register_scalar_udf`, `BUILTIN_UDF_NAMES`

#### Incremental append (honest, part files)
- Frontmatter `materialization: incremental_append` (also `append` / `incremental`)
- Writes `model.parts/part-*.parquet` + `_rbt_manifest.json` (no full rewrite)
- `ref()` registers the **parts directory**
- **Not** row-level MERGE (`incremental_merge` errors with clear code)

#### Write-Audit-Publish (honest FS protocol)
- `materialize.wap: true` → stage under `.wap/{run_id}/`, audit, atomic publish
- Failed audit leaves production dest untouched; audit JSON retained
- **Not** Iceberg branch WAP theater

#### Measure packs
- `rbt measure --scenario smoke_pipeline|validate_dx|incremental_append`
- JSON report: wall_ms, rows, models, optional Linux VmRSS
- Default path: `{project}/.rbt/measure/{scenario}.json`

### Docs
- [docs/P4_CAPABILITIES.md](docs/P4_CAPABILITIES.md)

## [0.4.0] — 2026-07-31

### Added (P2 full — Iceberg catalog SoR proof gate)
- **Official Iceberg catalog materialize** for `--format iceberg` (default)
  - `MemoryCatalog` + **LocalFs** warehouse → `create_table` → `DataFileWriter` → `Transaction::fast_append` → **`commit`**
  - Post-commit **scan** asserts row count (same process)
  - Durable files: `data/rbt-*.parquet`, `metadata/*.metadata.json`, manifests/snap avro
  - `ref()` via `.rbt_iceberg_data` hint or first parquet under table root
- Config:
  ```yaml
  materialize:
    iceberg:
      mode: catalog      # default; use filesystem for hand-rolled vN layout
      namespace: rbt
  ```
- APIs: `write_iceberg_catalog_batches`, `write_iceberg_catalog_stream`, `verify_iceberg_catalog_table`

### Fixed (audit)
- MemoryCatalog default storage was **in-memory only** — forced `LocalFsStorageFactory` so SoR files persist
- Iceberg `ref()` registration for catalog UUID data files
- Smoke checks updated for official metadata layout

### Honesty
- Still **not** multi-writer REST/Glue OCC; in-process MemoryCatalog with FS warehouse is the proof gate
- Dual-write `parquet-and-iceberg` still uses filesystem sidecar layout

## [0.3.9] — 2026-07-31

### Added (P1 remainder + P2 lite + P3 DX)
- **Bronze Arrow IPC spill→Parquet** (`scan.spill_arrow_ipc`, default **true**)
  - Multi-file / hive Arrow IPC streams file-by-file into `.rbt/bronze_spill/{schema}__{table}.parquet`
  - Registers via DataFusion listing (`BronzeRegistrationMode::ScanSpillParquet`) — peak RAM ≈ one file + encoder
  - Config: `scan.spill_dir` (default `.rbt/bronze_spill`); set `spill_arrow_ipc: false` for legacy MemTable path
- **Iceberg FS multi-version metadata** (P2 lite, honest)
  - Full-refresh data file retained; `v{n}.metadata.json` history + `version-hint.text` increments
  - **Not** REST/Glue OCC — local snapshot log only
- **DX verbs (P3):**
  - `rbt validate` — DAG + bronze + ref hygiene; `--json` for CI
  - `rbt explain -s <model>` — compiled SQL, deps, bronze contract; `--json`
  - `rbt preview -s <model> [--limit N]` — ancestors materialize, target `LIMIT` (no target write)

### Tests
- Arrow IPC spill registration + spill file existence
- Iceberg stream metadata v1→v2 retention

## [0.3.8] — 2026-07-31

### Added (P1 — streaming materialize)
- **Default write path is stream**: `DataFrame::execute_stream` → batch write → drop batch → atomic publish
  - No full-result `Vec<RecordBatch>` retained for Parquet / JSONL / CSV / Iceberg FS
  - Atomic publish via `.name.ext.rbt-partial` → `rename` (same filesystem)
  - Partial artifacts deleted on stream/assert failure; last good dest retained until successful replace
- **`materialize.mode`**: `stream` (default) | `collect` (legacy emergency)
  - Env override: `RBT_MATERIALIZE_MODE=stream|collect` or `RBT_STREAM_MATERIALIZE=0|1`
- **`materialize.max_row_group_rows`** (default 1_000_000) and **`max_row_group_bytes`** (default 128 MiB) for Parquet flush
- **`StreamingAssertionRunner` / `UniqueKeyTracker`**: not_null, accepted_values, unique/grain across batches without holding all batches
- **`ref()` registration without in-memory result**: lake file path only; MemTable opt-in re-reads small files from lake
- Structured errors: `E_RBT_MATERIALIZE_*`, `E_RBT_REF_*`, `E_RBT_SQL`
- Docs: streaming plan status updated; [docs/STREAMING_MATERIALIZE_PLAN.md](docs/STREAMING_MATERIALIZE_PLAN.md)

### Tests
- Multi-batch stream Parquet write + unique fail cleans partial
- Streaming unique tracker cross-batch
- `materialize.mode` YAML parse

## [0.3.7] — 2026-07-31

### Added
- **`path_glob`** on staging frontmatter (string or list, OR match) via **globset**
  - **Strong semantics:** `literal_separator` — `*` / `?` stay in one path segment; use `**` for recursion
  - Basename-only patterns (no `/`) match any depth; path-shaped patterns match relative (and absolute when pattern starts with `/`)
- **`roots:`** map in `rbt_project.yml` with `$name` / `${name}` path templates
- Fallible absolute + multi-root path resolution with structured **`E_RBT_*`** errors
  (`E_RBT_ROOT_UNKNOWN`, `E_RBT_LAYER_PATH`, `E_RBT_MODEL_TARGET`, `E_RBT_PATH_GLOB_INVALID`, …)
- Engine **caches** project config (roots, materialize, scan limits) per `project_dir` for large DAGs
- **`source_format: protobuf`** opaque bronze (`_source_path`, `payload`, `payload_len`)
- **`scan.protobuf_max_payload_bytes`** — default **1 GiB** (`1024³`); optional override under `scan:` in `rbt_project.yml`
- Docs: [docs/MULTI_ROOT_AND_PATH_GLOB.md](docs/MULTI_ROOT_AND_PATH_GLOB.md) — multi-root, globs, protobuf cap, **DF listing pushdown disabled when `path_glob` is set**

### Changed
- Examples (`smoke_fixture`, `full_e2e_rbt_example`) use **`roots:` + `$lake/...`**, document `path_glob` / partition best practices for 0.3.7
- `RbtProjectConfig::load` errors use **`E_RBT_PROJECT_LOAD`** with parse hints

### Tests
- Workspace example project load + DAG build unit test
- Glob literal-separator / protobuf payload cap coverage

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

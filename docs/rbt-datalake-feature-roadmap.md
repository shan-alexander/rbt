# rbt-datalake — Product-Neutral Feature Roadmap (Implementer Brief)

**Audience:** An AI agent or engineer implementing open-core **rbt-datalake** with **no other product context**.  
**Scope:** Abstract lakehouse DAG features. **Do not** hardcode host product names, column vocabularies, or workflow engines into the core.  
**Package:** crates.io `rbt-datalake` · lib import path `rbt` · binary `rbt`  
**Canonical source tree (example local checkout):** `/home/farmer/dev-other/rbt`  
**Baseline version reviewed:** **0.7.3**  
**Doc owner note:** Hosts (e-commerce collectors, research landers, market data, etc.) only *consume* these APIs; policies stay in the host.

---

## 0. Agent operating rules

1. **Read first:** `README.md`, `docs/COMPLEX_BRONZE_AND_RUN_SCOPE.md`, `docs/P4_CAPABILITIES.md`, `docs/MULTI_ROOT_AND_PATH_GLOB.md`, `crates/rbt/src/lib.rs` public API surface.
2. **Change only** `crates/rbt/**` + docs/examples/tests unless a workspace root file must change (`Cargo.toml`, `CHANGELOG.md`).
3. **Error codes:** Prefer stable `E_RBT_*` strings (existing style: `E_RBT_INCREMENTAL`, `E_RBT_ON_MISSING`, `E_RBT_FINGERPRINT`, …).
4. **Fail closed** on unshipped strategies (never silently fall back from merge → append).
5. **Tests:** unit tests colocated under `#[cfg(test)]`; integration via `examples/smoke_fixture` and a new minimal example when needed.
6. **Measure:** Any performance claim needs a `rbt measure` scenario or criterion bench with checked-in sample output format.
7. **Do not** depend on Restate, Kinna, Spark, or host-specific path conventions inside core.
8. **Partition / entity key names are free-form strings** (`entity_id`, `ticker`, `domain`, `account_id` are all valid project choices).

---

## 1. Current architecture (0.7.3) — what you extend

| Module path | Responsibility |
|-------------|----------------|
| `crates/rbt/src/core/project.rs` | `RbtProjectConfig`, `MaterializeConfig`, layers, roots |
| `crates/rbt/src/core/frontmatter.rs` | Staging frontmatter: `scan_path`, `path_glob`, `partition_by`, `columns`, `on_missing`, … |
| `crates/rbt/src/core/run_scope.rs` | `RunScope { vars: BTreeMap<String,String>, skip_if_fingerprint_match, … }` — **single string value per var today** |
| `crates/rbt/src/core/receipt.rs` | `bronze_fingerprint` (path + size + mtime + contract_version, FNV), `RunReceipt` |
| `crates/rbt/src/core/dag.rs` | `Materialization` enum: `View`, `Table`, `IncrementalAppend`, `IncrementalMerge` (merge **fails** at execute), `ZeroCopyClone` |
| `crates/rbt/src/engine/mod.rs` | `TransformationEngine::execute_dag_with_scope`, bronze register, materialize dispatch |
| `crates/rbt/src/engine/bronze.rs` | Bronze listing/registration, empty frames for `on_missing: empty` |
| `crates/rbt/src/materializer/stream.rs` | Stream Parquet write + atomic publish |
| `crates/rbt/src/materializer/incremental.rs` | `incremental_append` → `model.parts/part-*.parquet` + `_rbt_manifest.json` |
| `crates/rbt/src/materializer/wap.rs` | Filesystem WAP |
| `crates/rbt/src/materializer/iceberg_catalog.rs` | FS Iceberg layout helpers |
| `crates/rbt/src/scan/` | Lake scanners, path globs, parts listing |
| `crates/rbt/src/measure/` | Measure packs |
| `crates/rbt/src/main.rs` | CLI: compile, validate, run, test, measure, bench, explain, preview |

**Existing materializations:**

- `table` / full refresh → single file overwrite (stream write).
- `incremental_append` → append-only parts (no peer replace by scope key).
- `incremental_merge` → **parse OK, execute fails** with `E_RBT_INCREMENTAL`.

---

## 2. Epic dependency graph (suggested ship order)

```text
RBT-A4 (fingerprint modes)
RBT-A1 (multi-value vars) ──┬──► RBT-A2 (scoped part replace)
                            ├──► RBT-A9 (per-entity failure map)
RBT-A6 (schema emit) ───────┘
RBT-A3 (receipt phase tags) ──► works anytime after A4 polish

RBT-A5 (parts-only / consolidate policy)

RBT-A7 (keyed_upsert) ──► RBT-A8 (storage backends: sqlite)
                      └──► measure packs R1–R5

RBT-A10 (bronze adapters) — parallel track
RBT-A11 / A12 / A13 — after A2/A5 or parallel if staffed

RBT-L1 (embeddable library) — parallel: features, re-exports, DagBuilder, ops, UDFs
```

**MVP slice for hosts that do partial multi-entity runs:** A1 → A2 → A4 → A3.  
**MVP slice for entity registry tables:** A7 → A8 → benches.  
**MVP slice for multi-format bronze:** A10 (incremental adapters).  
**MVP slice for library embedders:** L1.1 → L1.5 (see below).

---

## 2b. Epic RBT-L1 — Embeddable library surface

> **Status:** **L1.1–L1.5 implemented** on `feat/l1-embeddable-library` (not published).  
> **Canonical detail:** also mirrored in [`docs/plans/rbt-datalake-feature-roadmap.md`](plans/rbt-datalake-feature-roadmap.md).  
> **Survey:** [`docs/analysis/library-embedding-and-dag-crate-survey.md`](analysis/library-embedding-and-dag-crate-survey.md).  
> **Antipatterns:** [`docs/analysis/rbt-datalake-library-antipatterns.md`](analysis/rbt-datalake-library-antipatterns.md).  
> **ADRs:** [ADR-004](adr/ADR_004_FEATURE_FLAGS.md) · [ADR-005](adr/ADR_005_DATA_STACK_REEXPORTS.md) ·
> [ADR-006](adr/ADR_006_DAG_BUILDER_IR.md) · [ADR-007](adr/ADR_007_LAKE_OPS_FACADE.md) ·
> [ADR-008](adr/ADR_008_UDF_HOST_SURFACE.md)

| ID | Task | ADR | Outcome |
|----|------|-----|---------|
| **L1.1** | Cargo features: default full; optional `iceberg`, `jshift`, `cli` | ADR-004 | Slim embed: `default-features=false, features=["sql","parquet"]` |
| **L1.2** | Re-export `arrow` / `parquet` / `datafusion` (+ `iceberg` when enabled) | ADR-005 | One Arrow major per release; monomorphic batches |
| **L1.3** | `DagBuilder` / `ModelSpec` programmatic IR; file project as peer frontend | ADR-006 | No `models/` dir required for hosts |
| **L1.4** | Lake ops façade (skip, stage helpers, upsert write) | ADR-007 | 80% silver without inventing paths in host |
| **L1.5** | `with_udfs` / `with_udf_pack` / `UdfPack` host pack hook | ADR-008 | Design A: SQL orchestrates, kernels outside rbt |

**Non-goals for L1:** generic Temporal/Airflow scheduler; host math kernels inside rbt;
re-documenting star-schema `tf_` = transform (DE convention, not timeframe).

**Rust patterns enabled for consumers:** Builder (`DagBuilder`, `RbtEngineBuilder`),
feature composition, façade (`ops`), Strategy/plugin hooks (`with_udfs`), re-export
facade for stack crates (avoid dual-link).

---

## 3. Feature RBT-A1 — Multi-value partition scope

### Goal

Allow a run scope to bind a partition **key** to a **set of values** (or a file of values), so one `rbt run` can materialize bronze filtered to *any of* those values without N process forks.

### Non-goals

- SQL `IN` pushdown to every format (best-effort).  
- Distributed multi-writer.  
- Host-specific key names.

### Current gap

`RunScope.vars: BTreeMap<String, String>` is scalar-only. `require_partitions` after scope apply is single-value equality.

### Target public API

**CLI**

```bash
# Repeated vars (last-wins today) → become multi-value union
rbt run -p proj --var entity=a.com --var entity=b.com --select stg_x

# File of values (one per line; # comments; trim)
rbt run -p proj --var-file entity=entities.txt --select stg_x

# Explicit multi via JSON-ish (optional)
rbt run -p proj --var entity:='["a.com","b.com"]'
```

**Library**

```rust
scope.with_var_multi("entity", ["a.com", "b.com"]);
scope.with_var_file("entity", Path::new("entities.txt"))?;
```

**Frontmatter interaction**

If `partition_by` includes `entity` and scope provides multi `entity`, effective filter is:

`partition_value(entity) IN scope_values(entity)`.

If a var is multi but **not** in `partition_by`, still expand templates only if single; multi non-partition vars → `E_RBT_VAR_MULTI` (cannot expand `{entity}` in path when multi).

### Microtasks

| ID | Task | Detail |
|----|------|--------|
| **RBT-A1.1** | Design `ScopeValue` enum | `Single(String)` \| `Multi(BTreeSet<String>)` in `run_scope.rs`. Serialize as string or list in JSON receipts. |
| **RBT-A1.2** | Migrate `RunScope.vars` | Change to `BTreeMap<String, ScopeValue>` **or** add parallel `multi_vars` map; prefer single map with enum. Update `with_var` to insert Single. |
| **RBT-A1.3** | `with_var_multi` / `extend_multi` | Dedup, reject empty strings, max count config default 100_000 with `E_RBT_VAR_LIMIT`. |
| **RBT-A1.4** | `with_var_file(key, path)` | Read UTF-8 lines; skip empty/`#`; normalize optional trim. Errors: `E_RBT_VAR_FILE`. |
| **RBT-A1.5** | CLI `--var-file key=path` | Parse in `main.rs`; document in README CLI table. |
| **RBT-A1.6** | Repeated `--var key=v` | If key already Single and new differs → promote to Multi; if Multi → insert. |
| **RBT-A1.7** | Template expansion rules | `expand_braced_vars`: Multi → error if used in path template. Document. |
| **RBT-A1.8** | Partition filter apply | In `apply_scope_to_frontmatter` / scan filter: for multi, set internal `require_partitions_in: BTreeMap<String, BTreeSet<String>>`. |
| **RBT-A1.9** | LakeScanner list_files | Filter hive segments: accept file if partition key value ∈ set. Support both `key=value` dirs and path injection. |
| **RBT-A1.10** | DataFusion listing pushdown | When multi, disable naive single-partition pushdown; list then filter (document perf). |
| **RBT-A1.11** | Fingerprint inclusion | Multi set must be sorted and included in fingerprint scope identity (see A4). |
| **RBT-A1.12** | Unit tests | Empty set, 1 value degenerates to Single, 3 values, file load, template error. |
| **RBT-A1.13** | Docs | Update `COMPLEX_BRONZE_AND_RUN_SCOPE.md` with multi-value section + example. |
| **RBT-A1.14** | Example fixture | Tiny lake with 3 entity partitions; one run selects 2 of 3; assert row counts. |

### Acceptance

- `rbt run --var-file entity=list.txt` materializes only listed entities.  
- Receipt stores multi vars.  
- Single-var projects unchanged.

### Error codes

`E_RBT_VAR_MULTI`, `E_RBT_VAR_FILE`, `E_RBT_VAR_LIMIT`, `E_RBT_PARTITION_FILTER`.

---

## 4. Feature RBT-A2 — Scoped part identity + replace

### Goal

When materializing **append-style** (or a new strategy), write a **deterministic part file** keyed by the active run **scope** (partition vars), and **replace** that part on re-run without deleting peer parts from other scopes.

### Non-goals

- Row-level merge across parts (A12).  
- Automatic multi-process concurrent writers without locking.

### Current gap

`incremental_append` always **adds** a new `part-*.parquet` (timestamp/uuid style) and never replaces a logical scope.

### Target behavior

```text
{model_dest}.parts/
  part-{scope_id}.parquet     # replaced on re-run of same scope
  _rbt_manifest.json          # lists parts, total_rows, updated_at, scope_id per part
```

`scope_id` = stable hash (blake3/hex16 or fnv) of canonical JSON:

```json
{"contract":"1","vars":{"report_date":"2026-08-07","run_id":"abc"},"model":"stg_x"}
```

Only vars listed in frontmatter `part_key:` (or default: all `partition_by` keys present in scope) participate.

### Microtasks

| ID | Task | Detail |
|----|------|--------|
| **RBT-A2.1** | Frontmatter `part_key: [report_date, run_id]` | Optional; default = intersection of `partition_by` and scope vars. |
| **RBT-A2.2** | `fn scope_id(model, vars_subset, contract) -> String` | Canonical sort keys; hex digest; unit tests for stability. |
| **RBT-A2.3** | Materialization name | Prefer extend `incremental_append` with `replace_scope: true` **or** new `scoped_replace` / `incremental_replace`. Document choice in CHANGELOG. **Recommended:** `materialization: scoped_replace` to avoid breaking append-only semantics. |
| **RBT-A2.4** | Parse materialization | Add to `Materialization` enum + `parse_materialization_hint`. |
| **RBT-A2.5** | Write path | `materialize_scoped_replace_stream` in `materializer/incremental.rs` (or new `scoped.rs`): write `part-{scope_id}.parquet.tmp` → rename; overwrite same name. |
| **RBT-A2.6** | Manifest update | Load `_rbt_manifest.json`; upsert entry for `scope_id`; recompute `total_rows`; atomic write. |
| **RBT-A2.7** | `ref()` listing | Reuse parts directory listing; ensure replaced part not duplicated. |
| **RBT-A2.8** | Full refresh interaction | `materialization: table` still full overwrite of single file; out of scope. |
| **RBT-A2.9** | CLI flag | Optional `--force-scope-replace` redundant if strategy is explicit. |
| **RBT-A2.10** | Multi-value scope (A1) | If multi vars in part_key → either one part containing all rows (default) or reject (`E_RBT_PART_KEY_MULTI`). Prefer **one part for whole multi-set** with scope_id hashing the sorted multi set. |
| **RBT-A2.11** | Tests | Two scopes two parts; re-run scope A updates part A only; total_rows correct. |
| **RBT-A2.12** | Measure scenario | `scoped_replace_twice` wall_ms vs full table rewrite. |
| **RBT-A2.13** | Docs | P4_CAPABILITIES + COMPLEX_BRONZE. |

### Acceptance

- Re-running same scope replaces one part; peer scopes intact.  
- Downstream `ref()` sees cumulative rows.

### Error codes

`E_RBT_PART_KEY`, `E_RBT_PART_KEY_MULTI`, `E_RBT_MANIFEST`.

---

## 5. Feature RBT-A3 — Phased publish metadata on receipts

### Goal

Receipts expose per-model outcomes + optional free-form **phase/tags** from frontmatter so hosts can distinguish “early inventory publish” vs “final product publish” without baking host vocabulary into the engine.

### Target receipt shape (extend `RunReceipt`)

```json
{
  "run_id": "...",
  "status": "success",
  "bronze_fingerprint": "content:blake3:…",
  "contract_version": "3",
  "vars": { "report_date": "2026-08-07" },
  "models": [
    {
      "name": "stg_entity_inventory",
      "status": "success",
      "row_count": 1209,
      "phase": "inventory",
      "tags": ["stage", "optional_product_absent"],
      "elapsed_ms": 42,
      "output_path": "…/stg_entity_inventory.parquet"
    }
  ],
  "total_rows": 1209,
  "skipped": false
}
```

### Microtasks

| ID | Task | Detail |
|----|------|--------|
| **RBT-A3.1** | Frontmatter fields | `phase: string` (optional), `tags: [string]` (optional) on any model layer. |
| **RBT-A3.2** | `ModelRunResult` struct | Ensure name, status, row_count, phase, tags, elapsed_ms, error optional. |
| **RBT-A3.3** | Populate during execute | Engine already tracks models; fill row_count from stream stats. |
| **RBT-A3.4** | Receipt JSON serialize | Backward compatible: old readers ignore new fields. |
| **RBT-A3.5** | CLI `--json` run summary | Print models array when `--json` / log level. |
| **RBT-A3.6** | Skip path | When fingerprint skip, models array empty or prior receipt echoed; `skipped: true`. |
| **RBT-A3.7** | Tests | phase/tags round-trip; missing phase is null/omit. |
| **RBT-A3.8** | Docs | COMPLEX_BRONZE receipts section. |

### Acceptance

Host can read `.rbt/runs/*.json` and branch on `models[].phase` without parsing logs.

---

## 6. Feature RBT-A4 — Content-addressed bronze fingerprint

### Goal

Configurable fingerprint modes so skip-if-match is correct when files are rewritten with same size/mtime or content changes without mtime reliability.

### Current behavior

`bronze_fingerprint` in `receipt.rs`: FNV over lines `model, rel, size, mtime` + contract_version → `fnv1a64:hex`.

### Target config

```yaml
# rbt_project.yml
fingerprint:
  mode: path_stat          # default (current behavior)
  # mode: content_hash
  algo: blake3             # when content_hash: blake3 | sha256
  max_bytes_per_file: 0    # 0 = full file; >0 = hash first N bytes only (escape hatch)
```

Env override: `RBT_FINGERPRINT_MODE=content_hash`.

Prefix fingerprint string:

- `path_stat:fnv1a64:…` (migrate current)  
- `content:blake3:…`  
- `content:sha256:…`

### Microtasks

| ID | Task | Detail |
|----|------|--------|
| **RBT-A4.1** | `FingerprintConfig` struct | In `project.rs`; serde defaults. |
| **RBT-A4.2** | Keep path_stat path | Refactor existing FNV logic into `fingerprint_path_stat(...)`. |
| **RBT-A4.3** | Content hash path | For each listed bronze file: stream hash; include rel path + digest in sorted manifest. |
| **RBT-A4.4** | Algorithm plug | blake3 crate (preferred) and/or sha2; feature-flag heavy deps if needed. |
| **RBT-A4.5** | Large file policy | `max_bytes_per_file`; document danger of partial hash. |
| **RBT-A4.6** | Mode mismatch | If previous receipt prefix mode ≠ current, never skip (force execute). |
| **RBT-A4.7** | CLI `--fingerprint-mode` | Optional override for one run. |
| **RBT-A4.8** | Unit tests | Same content different mtime → same content fingerprint; different content → different. |
| **RBT-A4.9** | Perf test | 100 small files content_hash vs path_stat wall_ms (measure scenario). |
| **RBT-A4.10** | Docs | COMPLEX_BRONZE + CHANGELOG migration note on prefix change. |
| **RBT-A4.11** | Backward compat | Accept old `fnv1a64:` receipts as path_stat when comparing if mode is path_stat. |

### Acceptance

`--skip-if-match` with `content_hash` skips only on true content+contract match.

### Error codes

`E_RBT_FINGERPRINT`, `E_RBT_FINGERPRINT_MODE`.

---

## 7. Feature RBT-A5 — Parts-only publish / consolidate policy

### Goal

Hosts can keep **parts directories** as the source of truth without rewriting a monolithic `model.parquet` on every run.

### Target config

```yaml
materialize:
  consolidate: never       # never | always | auto
  # auto: write single file for table materialization; parts-only for incremental_*
```

### Microtasks

| ID | Task | Detail |
|----|------|--------|
| **RBT-A5.1** | Enum `ConsolidatePolicy` | never / always / auto in `MaterializeConfig`. |
| **RBT-A5.2** | Table materialization | `always` or `auto` → single file (current). `never` → write as single-part under `.parts/` only (or error if table + never). **Recommended:** `never` + `table` → `E_RBT_CONSOLIDATE` or treat as one-part scoped. |
| **RBT-A5.3** | Incremental / scoped_replace | Default parts-only; no silent consolidate. |
| **RBT-A5.4** | Optional consolidate command | `rbt consolidate -s model` rebuilds single parquet from parts (ops). |
| **RBT-A5.5** | `ref()` | Already lists parts dir; verify stream path. |
| **RBT-A5.6** | Tests + docs | |

### Acceptance

Large incremental models never rewrite a multi-GB single file unless consolidate explicitly requested.

---

## 8. Feature RBT-A6 — Declared schema emit (stable contracts)

### Goal

When bronze is missing/empty (`on_missing: empty`) or SQL returns zero rows, the **published** table still has **all declared frontmatter columns** with correct Arrow/Parquet types (NULL values), plus any partition columns.

### Current gap

Empty frames exist for registration; ensure **materialized files** and **preview** also emit full schema; document dtype map.

### Microtasks

| ID | Task | Detail |
|----|------|--------|
| **RBT-A6.1** | Inventory `columns:` dtype parser | `parse_logical_dtype` — list supported: utf8, int64, int32, float64, bool, date32, timestamp. |
| **RBT-A6.2** | `empty_batch_for_frontmatter(fm) -> RecordBatch` | Shared helper used by bronze empty + materialize zero-row. |
| **RBT-A6.3** | Zero-row SQL result path | If stream ends with 0 rows, write empty file with declared schema (not empty schema). |
| **RBT-A6.4** | Extra SQL columns | Keep SQL columns; ensure declared cols present (add null cols if missing). |
| **RBT-A6.5** | Tests | on_missing empty → parquet schema field names/types. |
| **RBT-A6.6** | Docs | Frontmatter contract section. |

### Acceptance

Consumers can rely on physical schema stability across partial runs.

---

## 9. Feature RBT-A7 — Keyed upsert materialization (Type-1)

### Goal

Support **entity-grain** tables: one row per natural key; re-runs upsert; if non-key attributes unchanged, only update **touch** columns (e.g. watermark).

### Semantics

```yaml
---
materialization: keyed_upsert
unique_key: [entity_id]           # required, ≥1 cols
touch_columns: [last_seen_at]     # required for touch optimization; may be empty
compare_columns: [a, b, c]        # optional; default = all non-key, non-touch columns
---
SELECT entity_id, a, b, c, report_date AS last_seen_at FROM …
```

**Algorithm (per incoming row):**

1. Lookup key in existing store.  
2. If missing → insert full row.  
3. If present and all `compare_columns` equal (NULL-safe) → update only `touch_columns` from incoming.  
4. Else → replace all non-key columns from incoming (including touch).

**Batch execute:** SQL produces a batch of candidate rows; upsert set into store; write store.

### Microtasks

| ID | Task | Detail |
|----|------|--------|
| **RBT-A7.1** | Enum variant `KeyedUpsert` | `dag.rs` + parse aliases `upsert`, `scd1`, `type1`. |
| **RBT-A7.2** | Frontmatter fields | Parse unique_key, touch_columns, compare_columns; validate non-empty unique_key; disjointness checks. |
| **RBT-A7.3** | In-memory upsert engine | Given old `RecordBatch`/`Vec<Row>` + new batch → new table; NULL-safe eq. Pure function for unit tests. |
| **RBT-A7.4** | Wire execute path | After SQL stream collect **or** stream-to-temp then upsert (document memory limits); for v1 collect with max_rows guard `E_RBT_UPSERT_TOO_LARGE`. |
| **RBT-A7.5** | Default storage parquet | Read existing table if present; write full replace of single parquet via stream/atomic (v1). |
| **RBT-A7.6** | Touch-only metric | Receipt note: `rows_inserted`, `rows_updated`, `rows_touched`. |
| **RBT-A7.7** | Tests | insert; update attrs; touch-only; multi-key unique_key. |
| **RBT-A7.8** | Fail if unique_key missing in schema | `E_RBT_UPSERT_KEY`. |
| **RBT-A7.9** | Docs + example model | `examples/smoke_fixture` or new `examples/entity_registry`. |
| **RBT-A7.10** | Measure scenario | `entity_registry_upsert` synthetic N keys. |

### Acceptance

Re-run with same attrs only changes touch column; attr change overwrites.

### Error codes

`E_RBT_UPSERT_KEY`, `E_RBT_UPSERT_TOO_LARGE`, `E_RBT_UPSERT_SCHEMA`.

---

## 10. Feature RBT-A8 — Pluggable storage backend per model

### Goal

Allow a model to store its published table as **parquet**, **sqlite**, or **iceberg_fs**, especially for `keyed_upsert` hot paths.

### Target frontmatter

```yaml
storage:
  format: sqlite          # parquet (default) | sqlite | iceberg_fs
  # path: optional override; default {layer_target}/{model}.sqlite or .parquet
```

Project default:

```yaml
materialize:
  default_storage: parquet
```

### Microtasks

| ID | Task | Detail |
|----|------|--------|
| **RBT-A8.1** | `StorageFormat` enum | parquet / sqlite / iceberg_fs. |
| **RBT-A8.2** | Cargo feature `sqlite` | Optional dep `rusqlite` with bundled; default feature list decision (prefer optional to keep slim installs). |
| **RBT-A8.3** | SQLite schema map | Arrow types → SQLite affinity; create table if not exists; PK = unique_key for upsert models. |
| **RBT-A8.4** | SQLite upsert SQL | `INSERT … ON CONFLICT(pk) DO UPDATE SET …` with touch vs full branch in Rust pre-SQL or use two statements. |
| **RBT-A8.5** | SQLite read for ref() | Register as MemTable after SELECT * or use DataFusion SQLite adapter if available; v1: load to memory for ref. |
| **RBT-A8.6** | Parquet path | Existing writers. |
| **RBT-A8.7** | iceberg_fs path | Reuse existing iceberg writer for full refresh only; keyed_upsert+iceberg → `E_RBT_STORAGE` until supported. |
| **RBT-A8.8** | CLI inspect | `rbt explain -s model` shows storage format + path. |
| **RBT-A8.9** | Benches R1–R5 | See §14; implement as measure scenario + optional criterion. |
| **RBT-A8.10** | Docs honesty | “SQLite for entity registries; parquet for analytical.” |
| **RBT-A8.11** | Tests | keyed_upsert + sqlite round-trip; parquet regression. |

### Acceptance

Same SQL model can target sqlite; point upsert does not rewrite unrelated keys’ storage cost beyond index update.

---

## 11. Feature RBT-A9 — Structured per-entity failure map

### Goal

When a host runs multi-entity scope (A1), return machine-readable success/failure **per entity key** without scraping logs.

### Target API

```json
{
  "status": "partial_success",
  "entity_key": "domain",
  "ok": ["a.com", "b.com"],
  "failed": [
    { "key": "c.com", "error_code": "E_RBT_BRONZE_MISSING", "message": "…" }
  ]
}
```

### Design options (pick one and document)

**Option A (v1 recommended):** Host loops entities; rbt provides library helper:

```rust
execute_for_each_entity(dag, scope_template, entity_key, entities, |scope| …)
```

aggregating failures — **no multi-entity single SQL quarantine**.

**Option B:** Single run with multi scope; models that fail mid-way are hard — rbt runs **per-entity sub-exec** internally when `execution: per_entity` set.

### Microtasks

| ID | Task | Detail |
|----|------|--------|
| **RBT-A9.1** | Choose Option A or B | Document in ADR inside rbt docs. Prefer A for simplicity. |
| **RBT-A9.2** | `EntityRunReport` type | Serialize to JSON; write `.rbt/runs/{id}.entities.json`. |
| **RBT-A9.3** | Library helper | `execute_entities(...)` loops, isolates panics/errors per key. |
| **RBT-A9.4** | CLI | `rbt run --entity-key domain --var-file domain=list.txt --per-entity` |
| **RBT-A9.5** | Exit code | 0 all ok; 2 partial; 1 total failure (document). |
| **RBT-A9.6** | Tests | 3 entities, middle fails bronze; report lists one failed. |
| **RBT-A9.7** | Docs | |

### Acceptance

Host can quarantine failed entities and continue peers.

---

## 12. Feature RBT-A10 — Heterogeneous bronze → Arrow/Parquet adapters

> **Status:** **Implemented** (adapter trait + registry, HTML/XML/robots, matrix + guide).  
> Docs: [BRONZE_ADAPTER_MATRIX.md](BRONZE_ADAPTER_MATRIX.md) · [BRONZE_ADAPTERS.md](BRONZE_ADAPTERS.md).

### Goal

Make non-parquet bronze a **first-class, documented, testable** path: HTML, XML, JSON, JSONL, protobuf, text (robots), etc. → Arrow batches → SQL → silver.

### Microtasks

| ID | Task | Status |
|----|------|--------|
| **RBT-A10.1** | Audit matrix | Done — `docs/BRONZE_ADAPTER_MATRIX.md` |
| **RBT-A10.2** | Adapter trait | Done — `scan/adapter.rs` `BronzeAdapter` |
| **RBT-A10.3** | Registry | Done — `adapter_for` / `E_RBT_SOURCE_FORMAT` |
| **RBT-A10.4** | JSONL/JSON | Done — existing + jshift via adapter |
| **RBT-A10.5** | XML | Done — whole-file opaque; structure via pre-normalize (documented) |
| **RBT-A10.6** | HTML | Done — whole-file utf8 rows |
| **RBT-A10.7** | Protobuf | Done — opaque + docs |
| **RBT-A10.8** | Text/robots | Done — line txt + whole-file robots |
| **RBT-A10.9** | Spill path | Done — existing Arrow IPC spill |
| **RBT-A10.10** | Tests/fixtures | Done — adapter unit tests |
| **RBT-A10.11** | Docs guide | Done — `docs/BRONZE_ADAPTERS.md` |
| **RBT-A10.12** | Host-registerable adapters | Done — `register_host_adapter` / `register_named_adapter` / `NamedBronzeAdapter`; fail-closed |
| **RBT-A10.13** | Multi-file order + `_ingest_seq` | Done — `scan_order`, `inject_ingest_seq`, `inject_source_mtime` |

Also: `ModelRole` vocabulary; pipeline stages host-callable (L1.9); embed ABI guide [`docs/EMBEDDING.md`](EMBEDDING.md).

### Acceptance

A third-party host can land mixed filetypes and stage with SQL without custom Rust (for formats marked Done). Hosts can inject proprietary adapters without forking.

---

## 12b. Later — schema digest / feature gate (deferred)

> **Status:** **Consider later** (not scheduled).  
> Hosts that need a stable “gold columns match schema version X” gate can already use `contract_version` in fingerprints/receipts and declared `columns:`.

| ID | Idea | Notes |
|----|------|--------|
| **RBT-A3.x / contracts** | Optional opaque `schema_digest` (or documented `contract_version` convention) on frontmatter + receipt | Prefer **convention first** (`contract_version: "features@sha256:…"`). Add a first-class field only if convention proves noisy. Product-neutral name — not host-product-specific. |

Do **not** hardcode host vocabulary into core.

---

## 12c. L1 follow-ons (embed polish)

| ID | Task | Status |
|----|------|--------|
| **L1.6** | Single-ABI embed guide + workspace recipe | Done — `docs/EMBEDDING.md` |
| **L1.9** | Stage re-entry: register_bronze / execute_tiers / write_receipt | Done — `engine/stages.rs` + engine methods |

---

## 13. Feature RBT-A11 — Iceberg snapshot honesty

### Goal

Clarify and improve Iceberg output: either true multi-snapshot commits or explicit **FS layout only** UX so hosts do not assume OCC multi-writer.

### Microtasks

| ID | Task | Detail |
|----|------|--------|
| **RBT-A11.1** | UX audit | README/CLI: label `iceberg` as `iceberg_fs` (filesystem table layout). |
| **RBT-A11.2** | Metadata versions | On rewrite, write `v{N+1}.metadata.json` and update version-hint if easy; else document single-shot v1. |
| **RBT-A11.3** | Reader note | Document “single-writer host must serialize”. |
| **RBT-A11.4** | Optional catalog trait | Stub `IcebergCatalog` for future REST/Glue — no multi-backend sprawl. |
| **RBT-A11.5** | Tests | Round-trip read with iceberg-datafusion if in deps. |
| **RBT-A11.6** | Docs | `ICEBERG_SOR.md` status banner. |

---

## 14. Feature RBT-A12 — incremental_merge fail-closed + roadmap hook

### Goal

Keep `materialization: incremental_merge` **parseable** but execute with clear error until real MERGE exists; optional experimental flag later.

### Microtasks

| ID | Task | Detail |
|----|------|--------|
| **RBT-A12.1** | Ensure execute path | Always `E_RBT_INCREMENTAL` with message listing supported strategies. |
| **RBT-A12.2** | Frontmatter `unique_key` reserved | Parse and store for future merge; unused now. |
| **RBT-A12.3** | Design doc only | `docs/plans/incremental_merge.md` sketch (MERGE INTO via DF or delete+append by key). **Do not implement full merge in this task unless scheduled.** |
| **RBT-A12.4** | Test | merge materialization fails at run, not compile. |

---

## 15. Feature RBT-A13 — Gold band lint + topology

### Goal

Enforce silver endpoint / gold construction rules (`docs/GOLD_DEFAULT.md`) with clear errors.

### Microtasks

| ID | Task | Detail |
|----|------|--------|
| **RBT-A13.1** | Inventory existing checks | `E_RBT_LAYER_TRANSFORM_BAND` etc. |
| **RBT-A13.2** | Compile-time validate | stg_* must not ref other stg in forbidden ways per doc; gold tf only ref stg_*. |
| **RBT-A13.3** | Config escape | `layers.*.strict_band: false` for power users. |
| **RBT-A13.4** | Tests | Fixtures for allowed/denied graphs. |
| **RBT-A13.5** | Docs cross-link | GOLD_DEFAULT + validate CLI. |

---

## 16. Cross-cutting: entity registry benches (supports A7/A8)

Implement under `crates/rbt/benches/` or `measure` scenarios:

| Scenario ID | Procedure | Metrics |
|-------------|-----------|---------|
| **R1** | Build N={10k,50k,150k} registry; cold load all rows | wall_ms, peak_rss |
| **R2** | Point upsert 1 key attrs change | wall_µs |
| **R3** | Point touch only | wall_µs |
| **R4** | 100 sequential upserts | wall_ms |
| **R5** | Reload after R4; checksum | ok |

Backends: parquet full rewrite · sqlite keyed_upsert.

**Do not** publish “sqlite is faster” without checked-in numbers.

---

## 17. Definition of Done (per feature epic)

1. Code + unit tests green under `cargo test -p rbt-datalake`.  
2. `bash scripts/smoke.sh` green.  
3. CHANGELOG entry under Unreleased.  
4. Docs updated (user-facing).  
5. Public API exported in `lib.rs` if library-visible.  
6. Error codes documented in a single `docs/ERROR_CODES.md` (create if missing).  
7. No host-specific identifiers in core.

---

## 18. Suggested PR breakdown (human/agent)

| PR | Scope |
|----|--------|
| PR1 | A4 fingerprint modes |
| PR2 | A1 multi-value vars + scan filter |
| PR3 | A2 scoped_replace materialization |
| PR4 | A3 receipt models[] phase/tags |
| PR5 | A6 schema emit polish |
| PR6 | A5 consolidate policy |
| PR7 | A7 keyed_upsert + parquet |
| PR8 | A8 sqlite backend + benches R1–R5 |
| PR9 | A9 per-entity execution helper |
| PR10 | A10 adapter trait + 1–2 formats hardened |
| PR11 | A11–A13 docs + lint |

---

## 19. Out of scope for this roadmap (host systems)

- Durable orchestration (Restate/Temporal/Airflow).  
- Product lane routing (thresholds, “whale”, mime).  
- Business quarantine rules.  
- Renaming host bronze directories.

---

## 20. Quick reference — error codes to introduce

| Code | Use |
|------|-----|
| `E_RBT_VAR_MULTI` | Multi-value used where scalar required |
| `E_RBT_VAR_FILE` | `--var-file` IO/parse |
| `E_RBT_VAR_LIMIT` | Too many multi values |
| `E_RBT_PART_KEY` | Invalid part_key |
| `E_RBT_MANIFEST` | Parts manifest corrupt |
| `E_RBT_FINGERPRINT_MODE` | Unknown mode |
| `E_RBT_CONSOLIDATE` | Illegal consolidate/materialization combo |
| `E_RBT_UPSERT_KEY` | unique_key missing/invalid |
| `E_RBT_UPSERT_TOO_LARGE` | Collect path exceeded max rows |
| `E_RBT_STORAGE` | Unsupported storage/materialization pair |
| `E_RBT_SOURCE_FORMAT` | Unknown/unsupported bronze adapter |

---

## 21. File touch map (expected)

| Area | Likely files |
|------|----------------|
| Scope/vars | `core/run_scope.rs`, `main.rs` |
| Fingerprint | `core/receipt.rs`, `core/project.rs` |
| Materialize | `materializer/incremental.rs`, new `materializer/scoped.rs`, new `materializer/upsert.rs`, new `materializer/sqlite_store.rs` |
| Engine | `engine/mod.rs`, `engine/bronze.rs` |
| DAG/frontmatter | `core/dag.rs`, `core/frontmatter.rs` |
| Measure | `measure/mod.rs` |
| Docs | `docs/COMPLEX_BRONZE_AND_RUN_SCOPE.md`, `docs/P4_CAPABILITIES.md`, `CHANGELOG.md`, `README.md` |

---

*End of implementer brief. Prefer small PRs, measured claims, and fail-closed incomplete strategies.*

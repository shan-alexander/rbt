# Complex bronze landing zones, run scope, and receipts (P5)

Product goal: turn **multi-artifact Hive-ish bronze trees** into reliable **silver stage tables**, with orchestrator-friendly scope binds and job receipts.

Related brain notes: [[Complex bronze landing zones]], [[Bronze-to-silver maturity gap matrix]], [[Dual-track maturity roadmap]].

## 1. Run variables & partition binds (P5a + RBT-A1 multi-value)

```bash
rbt run -p my_project \
  --var domain=acme.com \
  --var report_date=2026-07-29 \
  --var run_id=r1
```

| Mechanism | Behavior |
|-----------|----------|
| `--var key=value` | Repeatable; also `RBT_VAR_<KEY>` and `RBT_VARS=k=v,k2=v2` |
| **Repeated `--var key=…`** | Different values promote to a **multi-value set** (A1) |
| `--var key:=["a","b"]` | Explicit JSON string array multi bind |
| `--var-file key=path` | One value per line (`#` comments / blanks skipped) |
| `{key}` / `${key}` | Expanded in `scan_path`, `path_glob`, static partition values — **scalar only** |
| Multi in path template | **`E_RBT_VAR_MULTI`** — use hive `partition_by` + IN filter instead |
| `partition_by` + vars | Scalar → `require_partitions` equality; multi → `require_partitions_in` (**IN** set) |
| `$lake` roots | Unchanged — still project `roots:` templates |

**Multi-value example** (one run, two entities):

```bash
rbt run -p my_project \
  --var entity=a.com --var entity=b.com \
  --var report_date=2026-08-07

# or
rbt run -p my_project \
  --var-file entity=entities.txt \
  --var report_date=2026-08-07

# or
rbt run -p my_project \
  --var entity:='["a.com","b.com"]' \
  --var report_date=2026-08-07
```

With frontmatter `partition_by: [entity, report_date]`, bronze listing keeps hive dirs
`entity=a.com` and `entity=b.com` only (not `entity=c.com`).

Library:

```rust
use rbt::{RunScope, TransformationEngine, RbtProjectConfig};

let scope = RunScope::new()
    .with_var("report_date", "2026-07-29")
    .with_var_multi("entity", ["a.com", "b.com"])?;
// scope.skip_if_fingerprint_match = true;

let config = RbtProjectConfig::load(project)?;
let dag = config.build_dag(project, None)?;
let engine = TransformationEngine::new();
let summary = engine
    .execute_dag_with_scope(&dag, project, output, &config, &scope)
    .await?;
```

## 2. Optional artifact families (`on_missing: empty`)

```yaml
---
source_format: jsonl
scan_path: $lake/lz/runs
path_glob: scrape.jsonl
partition_by: [domain, report_date, run_id]
on_missing: empty          # error | empty
columns:
  url: { dtype: utf8 }
  title: { dtype: utf8 }
  score: { dtype: int64 }
---
```

When the scan root is missing **or** filters match **no files**, rbt registers a **zero-row** table with the declared schema (column dtypes + any `partition_by` keys not already listed). Outer-join silver SQL stays valid under partial bronze.

`on_missing: error` (default) keeps fail-closed behavior for required families (e.g. plan inventory).

## 3. Scoped part replace (RBT-A2)

When re-running a single entity/date must **not** grow infinite append parts:

```yaml
---
materialization: scoped_replace
partition_by: [entity, report_date]
# part_key: [entity, report_date]   # optional; default = partition_by ∩ run vars
---
```

```bash
rbt run -p proj --var entity=a.com --var report_date=2026-08-07
# writes …/stg_x.parts/part-{scope_id}.parquet
rbt run -p proj --var entity=a.com --var report_date=2026-08-07
# replaces the same part; other entities' parts remain
```

`scope_id` is a 16-hex FNV of model + contract_version + sorted part_key vars
(multi vars use their canonical form). Distinct from `incremental_append` (always adds).

## 4. Stage modes (author intent)

Frontmatter `stage_mode` documents **how silver relates to bronze** (engine may use it later; SQL still owns transforms today):

| `stage_mode` | Intent |
|--------------|--------|
| `full_refresh` | Consolidated rewrite of the scoped slice into one table/file |
| `latest_only` | Keep newest landing only (`inject_source_path` + window/QUALIFY) |
| `append` | Prefer `materialization: incremental_append` |
| `mirror_bronze` | Thin 1:1 projection of one artifact family |

Iceberg MoR/CoW remain materialize format / catalog concerns (`--format iceberg`, `materialize.iceberg`), not stage_mode synonyms.

## 4. Run receipts & fingerprint skip (P5b + RBT-A3)

After a successful run (when `write_receipt` is true — CLI default):

```text
{project}/.rbt/runs/{run_id}.json
{project}/.rbt/runs/latest_{scope_key}.json
```

Receipt fields include: `vars`, `contract_version`, `bronze_fingerprint`, `models_executed`,
`total_rows`, `status`, `skipped`, and **`models[]`** (per-model outcomes).

### Per-model phase / tags (A3)

Optional frontmatter — **free-form strings**; rbt does not interpret host vocabulary:

```yaml
---
phase: inventory
tags: [stage, optional_product_absent]
---
```

Receipt shape (abbreviated):

```json
{
  "status": "ok",
  "skipped": false,
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
  ]
}
```

Skip path (`--skip-if-match`): new receipt with `status: skipped`, empty `models`, prior fingerprint.

```bash
rbt run -p my_project --var report_date=2026-07-29 --skip-if-match
# Identical bronze + contract → SKIPPED materialize; new receipt with status=skipped

# Compact run summary for hosts (A3.5) — models[] with phase/tags/elapsed_ms
rbt run -p my_project --var report_date=2026-07-29 --json

# Full on-disk receipt body to stdout (path still under .rbt/runs/)
rbt run -p my_project --var report_date=2026-07-29 --receipt-json
```

`--json` is a **summary** (`ok`, `wall_ms`, `models[]`, fingerprint, receipt_path).  
`--receipt-json` dumps the **full** written receipt file. Prefer `--json` for CI/orchestrators.

Bump `contract_version` in yml when silver SQL or bronze column meaning changes so skip does not return stale semantics.

### Fingerprint modes (RBT-A4)

```yaml
# rbt_project.yml
fingerprint:
  mode: path_stat          # default: size + mtime (fast)
  # mode: content_hash     # hash file bytes (mtime-safe)
  algo: blake3             # blake3 | sha256 (content_hash only)
  max_bytes_per_file: 0    # 0 = full file; >0 = hash first N bytes only (escape hatch)
```

```bash
# one-shot override
rbt run -p proj --skip-if-match --fingerprint-mode content_hash
# or env
RBT_FINGERPRINT_MODE=content_hash rbt run -p proj --skip-if-match
```

| Prefix | Meaning |
|--------|---------|
| `path_stat:fnv1a64:…` | Default (also accepts legacy bare `fnv1a64:…` on skip compare) |
| `content:blake3:…` | Content hash with blake3 |
| `content:sha256:…` | Content hash with sha256 |

**Mode mismatch never skips** (e.g. previous path_stat vs current content_hash → always re-execute).

## 5. Example

See [examples/complex_bronze_landing](../examples/complex_bronze_landing/): research-papers mini lakehouse — PubMed / Crossref / Europe PMC / arXiv landings → `stg_*` → gold `tf_paper_status` + Kimball star.

## 6. Measure packs (P5c)

```bash
# Stream vs collect on any project (e.g. smoke)
rbt measure -p examples/smoke_fixture --scenario stream_vs_collect --json

# Synthetic multi-file bronze (default 100k rows / 20 parts)
RBT_MEASURE_ROWS=100000 RBT_MEASURE_PARTS=20 \
  rbt measure -p . --scenario whale_synthetic --json

# Multi-artifact outer-join example
rbt measure -p examples/complex_bronze_landing --scenario complex_bronze --json
```

Reports include optional `mode_compare` (`stream_wall_ms`, `collect_wall_ms`, RSS). Linux only for RSS.

## 7. Contracts registry + contract-diff (P0)

Declare closed value sets once in `rbt_project.yml`:

```yaml
contracts:
  enums:
    works.source:
      values: [pubmed, crossref, europepmc, openalex, semanticscholar, arxiv]
      on_new: fail   # fail | warn | allow
      labels:
        openalex: "OpenAlex REST API"
      probe:
        model: stg_works
        column: source
```

Reference from staging frontmatter (no duplicated lists):

```yaml
tests:
  accepted_values:
    source: works.source          # or $contract:works.source
    topic_track: works.topic_track
```

Sample bronze and compare to the registry:

```bash
rbt validate -p examples/complex_bronze_landing --bronze-check fail --contract-diff \
  --var domain=ai-semicon-agritech --var report_date=2026-08-01 --var run_id=run…
```

- **New bronze values** not in `values` → `E_RBT_CONTRACT_NEW_VALUE` (`on_new: fail`) or
  `W_RBT_CONTRACT_NEW_VALUE` (`warn`) or notes only (`allow`).
- **Adding a source:** lander → append to `contracts.enums.*.values` (+ labels) → re-run
  `--contract-diff`. Models that reference the enum need no list edits.

# Complex bronze landing zones, run scope, and receipts (P5)

Product goal: turn **multi-artifact Hive-ish bronze trees** into reliable **silver stage tables**, with orchestrator-friendly scope binds and job receipts.

Related brain notes: [[Complex bronze landing zones]], [[Bronze-to-silver maturity gap matrix]], [[Dual-track maturity roadmap]].

## 1. Run variables & partition binds (P5a)

```bash
rbt run -p my_project \
  --var domain=acme.com \
  --var report_date=2026-07-29 \
  --var run_id=r1
```

| Mechanism | Behavior |
|-----------|----------|
| `--var key=value` | Repeatable; also `RBT_VAR_<KEY>` and `RBT_VARS=k=v,k2=v2` |
| `{key}` / `${key}` | Expanded in `scan_path`, `path_glob`, and `require_partitions` values |
| `partition_by` + vars | Vars for those keys merge into effective `require_partitions` (run wins over static frontmatter) |
| `$lake` roots | Unchanged — still project `roots:` templates |

Library:

```rust
use rbt::{RunScope, TransformationEngine, RbtProjectConfig};

let mut scope = RunScope::new()
    .with_var("report_date", "2026-07-29")
    .with_var("domain", "acme.com");
scope.skip_if_fingerprint_match = true;

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

## 3. Stage modes (author intent)

Frontmatter `stage_mode` documents **how silver relates to bronze** (engine may use it later; SQL still owns transforms today):

| `stage_mode` | Intent |
|--------------|--------|
| `full_refresh` | Consolidated rewrite of the scoped slice into one table/file |
| `latest_only` | Keep newest landing only (`inject_source_path` + window/QUALIFY) |
| `append` | Prefer `materialization: incremental_append` |
| `mirror_bronze` | Thin 1:1 projection of one artifact family |

Iceberg MoR/CoW remain materialize format / catalog concerns (`--format iceberg`, `materialize.iceberg`), not stage_mode synonyms.

## 4. Run receipts & fingerprint skip (P5b)

After a successful run (when `write_receipt` is true — CLI default):

```text
{project}/.rbt/runs/{run_id}.json
{project}/.rbt/runs/latest_{scope_key}.json
```

Receipt fields include: `vars`, `contract_version`, `bronze_fingerprint`, `models_executed`, `total_rows`, `status`, `skipped`.

Fingerprint covers filtered bronze files (path + size + mtime) + `contract_version` (`rbt_project.yml` or `--contract-version`).

```bash
rbt run -p my_project --var report_date=2026-07-29 --skip-if-match
# Identical bronze + contract → SKIPPED materialize; new receipt with status=skipped
```

Bump `contract_version` in yml when silver SQL or bronze column meaning changes so skip does not return stale semantics.

## 5. Example

See [examples/complex_bronze_landing](../examples/complex_bronze_landing/): plan + scrape + failures + optional siteinfo → `tf_unit_status` outer reconciliation.

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

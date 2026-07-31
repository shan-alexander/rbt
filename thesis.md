Thesis: dbt-shaped transforms on Iceberg, in Rust, with a byte-efficient bronze edge
Positioning (precise, not mystical)
A Rust-native project and execution layer that:

Scans bronze files (JSON/JSONL, CSV, Parquet) with selective absorption (jshift-class path work on JSON; real column projection on Parquet/CSV),
Materializes silver and star-schema tables in Apache Iceberg via the official Rust iceberg stack,
Offers a dbt-like model graph, refs, tests, and CLI,
Gives fail-fast, schema-aware SQL (validate → explain → preview → run),
Runs in-process, single-node (and optionally multi-threaded) with no JVM and no Python runtime.

Claim that is defensible
For a large class of workloads—bronze cleanup, silver typing, dimensional models, partition-incremental refreshes, team-scale lakes—this replaces Spark as the default compute, because Spark’s fixed costs (JVM, planning, shuffle machinery, cluster ops) dominate when data fits a serious Rust process and object storage is the system of record.
Claim that is not yet a measurement
“Most efficiently in the world” is a target for a proof program, not a fact on day zero. The thesis is implemented by shipping the architecture below and publishing experiment packs that beat the baselines that matter (Spark local/cluster on the same jobs, dbt+warehouse where comparable, naïve serde pipelines on JSONL).
“Byte level” in this thesis means:
Parse-avoiding structural work at the bronze edge and metadata/partition pruning via Iceberg—not “GROUP BY by splicing Parquet thrift by hand.” Relational analytics execute in a columnar engine (Polars/Arrow); Iceberg owns snapshots and table truth.

1. Why Spark is replaceable here
Spark wins when you need elastic distributed execution, huge shuffles, or an org already standardized on it.
Spark loses when:
FactorTypical lake “dbt-like” jobData sizeTens of GB to low TBs of relevant partitions, not petabyte ad-hocJob shapeProject, filter, join dims, incremental partition appendLatency to iterateEngineers/agents want validate/preview in secondsOpsNo desire for YARN/K8s/Spark Connect for every transformBronze formatFat JSONL where full parse is pure waste
Rust + Arrow + Iceberg + selective JSON extract attacks CPU waste, memory waste, and operational tax at once. That is enough to replace Spark in many cases—especially the cases this crate is for: raw → silver → star schema on object storage.

2. Product name and one-sentence pitch
Name (working): rbt — Rust lake build tool.
Pitch:
dbt-style models and tests for Iceberg, implemented in safe-leaning Rust, with a byte-efficient bronze edge and instant SQL feedback—no Spark required for the common path.

3. Architecture (implementation blueprint)
text┌──────────────────────────────────────────────────────────┐
│ CLI / library: validate | explain | preview | run | test │
└────────────────────────────┬─────────────────────────────┘
                             │
                     Project + Model DAG
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
   Bronze scanners      SQL / plans         Iceberg IO
   json(jshift)         Polars/Arrow        official `iceberg`
   csv / parquet        schema bind         catalog + tables
         │                   │                   │
         └────────────► Arrow batches ───────────┘
                             │
                    Silver + Star Iceberg tables
Crates (workspace)

CrateResponsibilityrbtFacade + CLIrbt-projectManifests, refs, DAG, selectorsrbt-bronzeFile glob/list, JSONL/CSV/Parquet extractrbt-jsonjshift integration (project/stamp/filter)rbt-sqlParse, bind to Iceberg schemas, explain/previewrbt-icebergThin typed wrappers over iceberg for create/append/replacerbt-modelsStar-schema metadata (grain, keys, relationships)rbt-testnot_null, unique, relationshipsrbt-measureBenchmark harness (thesis proof)
Depend on iceberg; do not reimplement the table format.

4. Data path: bronze files → Iceberg star schema
4.1 Bronze (files)
Expected inputs:

s3://…/bronze/.../**/*.jsonl (primary jshift path)
/**/*.csv, /**/*.parquet
TOML/YAML: project config, not fact bulk

Bronze model (declarative):
YAML# models/bronze_to_silver_events.yml
model: silver.events
kind: bronze_extract
source:
  uri: s3://lake/bronze/events/
  format: jsonl
extract:
  paths: [id, tenant_id, ts, amount, event_type]
  stamp:
    ingested_at: run_started_at
  filter:
    event_type: { neq: debug }
iceberg:
  table: silver.events
  partition_by: [tenant_id, dt]
  materialization: append   # or replace / full_refresh
Physical path:

List/select files (prefix + optional watermark).
For each JSONL object: path project + filter + stamp (jshift).
Build Arrow batches with a declared schema.
Write Iceberg data files + commit snapshot through rbt-iceberg.

4.2 Silver → star (SQL models)
SQL-- models/dim_tenant.sql
{{ config(kind = "dimension", business_key = "tenant_id") }}
select distinct tenant_id, min(ts) as first_seen_ts
from {{ ref("silver.events") }}
group by 1
SQL-- models/fact_events.sql
{{ config(kind = "fact", grain = ["event_id"]) }}
select
  e.id as event_id,
  e.tenant_id,
  e.ts,
  e.amount
from {{ ref("silver.events") }} e
Execution:

Resolve ref → Iceberg table identifiers.
Validate SQL against Iceberg schemas (unknown column → error before scan).
Plan with columnar engine; push partition filters when models/incremental specs allow.
Materialize to target Iceberg table; commit.


5. “Instant feedback” SQL loop (must-ship UX)
```text
validate  →  syntax + refs + columns/types against Iceberg schema
explain   →  logical plan + tables + partition prune summary (best effort)
preview   →  LIMIT N execution
run       →  full materialization + snapshot
test      →  grain / null / relationship assertions
```

Structured errors (agent-repairable):
textE_RBT_COLUMN_NOT_FOUND: column `tenant` not in iceberg table `silver.events`
  model: fact_events
  suggestion: did you mean `tenant_id`?
This is how the crate feels like a dev tool, not a batch submitter.

6. What “byte level” means in the implementation

StageTechniqueEngineJSONL bronzePath scan, zero-copy field slices, optional in-place stampjshiftCSV bronzeRead only needed columnsselective CSV → ArrowParquet bronzeColumn projection + row-group filteringarrow/parquetIceberg planningRead manifests, not all data filesicebergStar joins/aggsVectorized columnar executionPolars (or pinned SQL engine)
Thesis sentence for docs:
We do dbt-on-Iceberg in Rust; the bronze edge is byte-efficient and parse-avoiding; the relational core is columnar; table truth is Iceberg.

7. Proof program (how you “prove” efficiency)
Without numbers, efficiency is branding. Ship rbt-measure with locked scenarios:
Baselines

Naïve Rust: serde_json::Value project → Parquet
Spark: equivalent project + write (local mode and, if available, small cluster)
Optional: dbt + engine only where apples-to-apples

Scenarios

JSONL 50-path object → 8-field silver (1GB, 10GB)
Incremental one-day partition refresh of facts
Fact + 3 dims star build on Iceberg silver
Validate-only latency for broken vs good SQL (DX metric)

Report template

median wall time, p95, peak RSS, output row count, Iceberg snapshot id
machine class, commit SHA, dataset seed

Promotion rule: public “replaces Spark for X” claims require scenario packs checked into the repo.
Logical proof (already strong): for selective JSONL, avoiding full DOM is asymptotically less work; for single-node star schemas on pruned partitions, avoiding JVM + scheduler is constant-factor decisive. Empirical packs turn that into marketing you can defend.

8. MVP implementation order (build this, in order)
Milestone A — Skeleton

Workspace, rbt_project.toml, empty DAG loader
CLI: rbt validate-project

Milestone B — Iceberg silver write path

Open catalog (start with whatever the official crate documents best: REST/filesystem demo)
Write Arrow → Iceberg table silver.events from Parquet bronze (easiest)
rbt run --select silver.events

Milestone C — JSONL bronze via jshift

rbt-json extract spec → Arrow → same Iceberg sink
Golden tests vs serde oracle

Milestone D — SQL models

ref() resolution
validate/bind against Iceberg schema
preview + materialize via Polars reading Iceberg/Parquet files Iceberg points at

Milestone E — Star schema + tests

dimension/fact metadata
unique, not_null, relationships
sample project: events → dim_tenant + fact_events

Milestone F — Measure packs

Publish scenario 1–3 results in experiments/

Do not block MVP on multi-cluster execution, SCD2, or XML.

9. Minimal API surface (library)
Rustpub struct Project { /* root, catalog, fs */ }

impl Project {
    pub fn open(path: impl AsRef<Path>) -> Result<Self>;
    pub fn validate_sql(&self, model: &str) -> Result<Diagnostics>;
    pub fn explain(&self, model: &str) -> Result<PlanReport>;
    pub fn preview(&self, model: &str, limit: usize) -> Result<BatchFrame>;
    pub fn run(&self, select: Select) -> Result<RunReport>;
    pub fn test(&self, select: Select) -> Result<TestReport>;
}
CLI mirrors these verbs. Prost or JSON for RunReport keeps agents happy.

10. Risks and honest boundaries

Risk Response 
Spark still needed for huge shuffles -> Document “common path” vs “call Spark/Trino for elephant jobs
”Iceberg Rust catalog maturity -> Feature-detect backends; start narrow; upstream issues
SQL dialect fragmentation --> Pin one execution dialect in v0Over-
claiming byte-level analytics -> Keep byte domain at bronze + pruning; measure everything -> Scope creep to full dbt Cloud Open-source core only: compile/run/test/preview

11. Thesis statement (final form)
We implement dbt-shaped analytics engineering on Iceberg in Rust:
bronze files are absorbed with byte-efficient, selective extract (especially JSON via jshift); silver and star-schema tables are Iceberg tables via the official Rust Iceberg stack; SQL is validated against table schemas before heavy IO; execution is columnar and in-process, which replaces Spark for the default raw→dimensional path on team-scale lakes. Global “fastest possible” is earned by a public measurement program, not declared in advance.

12. Immediate next concrete steps

Freeze v0 scope: JSONL+CSV+Parquet bronze → Iceberg silver → SQL dims/facts → tests.
Pin dependencies: iceberg, arrow/parquet, polars, jshift, CLI parser.
Implement Milestone A–B so rbt run commits one Iceberg snapshot from Parquet bronze.
Add jshift JSONL path and serde differential tests.
Add validate/preview for one SQL model.
Check in measure scenario “JSONL project 1GB” with baseline comparison.

That is the implementable thesis: dbt-on-Iceberg-in-Rust, Spark-optional for the common path, byte-efficient at the bronze edge, columnar and correct through the star schema—not a slogan, a workspace you can build in milestone order.

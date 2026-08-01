---
tags: [adr, layout, layers, frontmatter, dag]
node_type: adr
aliases: [ADR-001, ADR-001 Project Layout, project structure, layer governance, zero-copy clone]
status: accepted
---
# ADR 001: Project Layout Architecture, Layer Governance, and Native Zero-Copy Cloning

## Status

**Accepted** (structure / layers / frontmatter shipped; zero-copy clone planned).  
**Date:** 2026-07-25 · **Authors:** Project maintainers  

Note: Moved from root-era path; implement zero-copy clone only after Iceberg catalog SoR or a deliberate FS pointer design.

## Context

As `rbt` scales to replace legacy dbt architectures, developers need:

1. A clean, predictable, modular project directory structure.
2. Strict DAG layer boundary enforcement (no circular or illegal upstream deps, e.g. transforms pulling from marts).
3. Soft-enforced model naming prefixes (`stg_`, `tf_`, `dim_`, `fact_`, `obt_`).
4. SQL frontmatter YAML for staging lake partition scanners.
5. Ultra-low-latency zero-copy materialization strategies.

## Decision

### Decision 1: Project directory architecture & naming

The `rbt` project configuration (`rbt_project.yml`) mandates three primary model locations by default:

```text
my_rbt_project/
├── rbt_project.yml
└── models/
    ├── staging/
    │   └── stg_<source_name>.sql
    ├── transforms/
    │   └── <data_topic_example>/
    │       └── tf_<transform_name>.sql
    └── marts/
        └── <mart_example_1>/
            ├── dim_<dimension_name>.sql
            ├── fact_<fact_name>.sql
            └── obt_<one_big_table_name>.sql
```

**Table prefix conventions**

- **Staging (`stg_`)**: Raw source ingestion and lake scanners (e.g. `stg_users.sql`).
- **Transforms (`tf_`)**: Intermediate transformations and topic aggregations (e.g. `tf_user_events.sql`).
- **Marts (`dim_`, `fact_`, `obt_`)**:
  - `dim_`: Dimension tables
  - `fact_`: Fact tables
  - `obt_`: One Big Table aggregations downstream of dims/facts

### Decision 2: Strict layer dependency boundary enforcement

1. **Staging (`stg_`)**: May reference external lake sources (`source()`). Cannot reference `tf_` or mart tables.
2. **Transforms (`tf_`)**: May only reference `stg_` or sibling `tf_`. **Cannot** reference mart tables (`dim_`, `fact_`, `obt_`).
3. **Marts (`dim_`, `fact_`, `obt_`)**: May reference `tf_` or upstream marts (e.g. `obt_` → `dim_` / `fact_`).

Violations fail at `rbt compile` / `rbt run`.

### Decision 3: Staging SQL frontmatter YAML

Staging models support YAML frontmatter on `.sql` files for lake scan parameters:

```sql
---
source_format: parquet # arrow_ipc | jsonl | parquet | csv
scan_path: "s3://my-lake/raw/events/*/*.parquet"
partition_by: ["year", "month"]
---
SELECT
    user_id,
    event_type,
    created_at
FROM {{ source('raw_lake', 'events') }}
```

### Decision 4: Native zero-copy table cloning & delta materialization

**Planned** — `Materialization::ZeroCopyClone` instead of complex Jinja macros:

1. **Metadata pointer duplication** — new catalog entry pointing at existing Parquet/Iceberg manifests without byte copy.
2. **Delta patch evaluation** — append only modified partitions or delta expressions.
3. **Targets** — filesystem lakes, Hive directories, Iceberg/DataFusion catalog paths.

Implement only after [[Iceberg system of record]] catalog proof or an explicit FS pointer design.

## Consequences

- Layer rules and prefixes make medallion DAGs reviewable and agent-checkable.
- Frontmatter contracts are the bronze edge surface ([[Bronze contracts multi-root and path_glob]]).
- Zero-copy clone remains deferred; do not document as shipped until implemented.
- Dimensional modeling of `tf_` / `dim_` / `fact_` logic placement: see [[Star schema data modeling rules]].

## Related

- Goals: [[Primary path spine]], [[Product north star]], [[Bronze contracts multi-root and path_glob]], [[Iceberg system of record]]
- Concept: [[Star schema data modeling rules]]
- Next ADRs: [[ADR-002 Thesis Alignment]], [[ADR-003 Polyglot DAG]]

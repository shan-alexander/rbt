# ADR 001: Project Layout Architecture, Layer Governance, and Native Zero-Copy Cloning

* **Status**: Accepted
* **Date**: 2026-07-25
* **Authors**: Antigravity & User Pair

---

## Context & User Intentions

As `rbt` scales to replace legacy dbt architectures, developers need:
1. A clean, predictable, and modular project directory structure.
2. Strict DAG layer boundary enforcement (preventing circular or illegal upstream dependencies, e.g. transforms pulling from marts).
3. Soft-enforced model naming prefixes (`stg_`, `tf_`, `dim_`, `fact_`, `obt_`).
4. SQL Frontmatter YAML configuration for staging lake partition scanners.
5. Ultra-low-latency zero-copy materialization strategies.

---

## Decision 1: Project Directory Architecture & Naming Conventions

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

### Table Prefix Conventions
* **Staging (`stg_`)**: Raw source ingestion and lake scanners (e.g. `stg_users.sql`).
* **Transforms (`tf_`)**: Intermediate transformations and topic aggregations (e.g. `tf_user_events.sql`).
* **Marts (`dim_`, `fact_`, `obt_`)**:
  * `dim_`: Dimension tables (e.g. `dim_customers.sql`).
  * `fact_`: Fact tables (e.g. `fact_orders.sql`).
  * `obt_`: One Big Table aggregations downstream of dims/facts (e.g. `obt_user_sales.sql`).

---

## Decision 2: Strict Layer Dependency Boundary Enforcement

To maintain a clean and maintainable DAG, `rbt` enforces the following layer directional constraints:

1. **Staging Layer (`stg_`)**: Can reference external lake sources (`source()`). Cannot reference `tf_` or mart tables.
2. **Transforms Layer (`tf_`)**: Can ONLY reference `stg_` tables or sibling `tf_` tables. **Transforms CANNOT reference Mart tables (`dim_`, `fact_`, `obt_`)**.
3. **Marts Layer (`dim_`, `fact_`, `obt_`)**: Can reference `tf_` tables or upstream mart tables (e.g. `obt_` referencing `dim_`/`fact_` tables).

Attempts to violate layer boundaries (e.g. a `tf_` model referencing a `fact_` model) will trigger a compiler error during `rbt compile` / `rbt run`.

---

## Decision 3: Staging SQL Frontmatter YAML Configuration

Staging models support YAML frontmatter at the top of the `.sql` file (`--- ... ---`) to define lake scanning and partition parameters:

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

---

## Decision 4: Native Zero-Copy Table Cloning & Delta Materialization

Instead of requiring developers to write complex Jinja macros, `rbt` introduces **Native Zero-Copy Table Cloning** (`Materialization::ZeroCopyClone`).

### Execution Mechanics
1. **Metadata Pointer Duplication**: Creates a new table entry in the catalog referencing existing Parquet/Iceberg manifest files without duplicating raw bytes on disk.
2. **Delta Patch Evaluation**: Computes and appends only modified partitions or delta SQL expressions.
3. **Target Platforms**: Native support for filesystem lakes, Hive directories, and Apache Iceberg/DataFusion catalog paths.

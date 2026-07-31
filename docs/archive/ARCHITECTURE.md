# RBT Architecture & System Design Specification

> **Project Vision**: `rbt` is a unified, high-performance Rust crate and CLI tool that replaces the PySpark + dbt dual-stack. Powered by **Apache Iceberg**, **Apache DataFusion**, **`jshift`**, and **`prost`**, `rbt` handles the complete data lifecycle—from byte-efficient raw bronze file ingestion (JSONL/CSV/Parquet) to silver cleanup, star-schema modeling, instant SQL feedback, and inline data testing.

---

## 1. Executive Summary & Design Principles

Legacy data platform architectures rely on two heavy tools:
- **PySpark**: Ingestion and heavy raw data transformations, incurring high JVM garbage collection overhead, slow cold starts, and complex cluster management.
- **dbt**: SQL model orchestration that pushes DDL queries to expensive cloud warehouses, lacking instant local feedback and charging for post-hoc quality tests.

**`rbt`** unifies both ends into a single, light-speed Rust runtime:
1. **Bronze Ingest Edge (`rbt-json` / `jshift` & `rbt-bronze`)**: Selective, parse-avoiding path extraction on raw JSONL files using `jshift`, skipping DOM parsing (`serde_json::Value`) for maximum CPU throughput.
2. **Columnar Execution Core (`rbt-engine` / Apache DataFusion)**: In-process SIMD-vectorized execution over Arrow record batches with zero serialization overhead and dynamic partition pruning.
3. **Table Truth & Snapshot Storage (`rbt-catalog` / Apache Iceberg)**: Direct Iceberg metadata catalog reads/commits (REST, Glue, Polaris, Nessie, Hive) with Write-Audit-Publish (WAP) snapshot branching.
4. **Star-Schema Native Modeling (`rbt-models`)**: First-class support for dimension keys, fact grains, and foreign key relationship assertions.
5. **Instant SQL Feedback Loop**: Instant `validate` $\rightarrow$ `explain` $\rightarrow$ `preview` $\rightarrow$ `run` developer loop with agent-repairable `prost` / JSON diagnostics.

---

## 2. Complete End-to-End Architecture

```mermaid
graph TD
    subgraph Developer & Agent Interface (Instant Feedback Loop)
        A[CLI / Library: validate | explain | preview | run | test] --> B[rbt-core: Project & Model DAG]
        B -->|Prost / JSON Diagnostics| A
    end

    subgraph Bronze Edge (Raw File Ingestion)
        C[JSONL Files] -->|jshift Selective Extract| E[Arrow RecordBatch Stream]
        D[CSV / Parquet Files] -->|Projection Pushdown| E
    end

    subgraph Transformation Engine & Catalog
        B --> F[rbt-engine: Apache DataFusion SessionContext]
        E --> F
        G[rbt-catalog: Apache Iceberg REST/Glue/Polaris] <--> F
    end

    subgraph Star Schema & Inline Testing
        F --> H[rbt-testing: Inline Arrow Quality Assertions]
        H -- Pass Validation --> I[rbt-materializer: Parquet Writer & Snapshot Committer]
        I --> G
        I --> J[(S3 / GCS / Azure Object Storage)]
    end
```

---

## 3. Subsystem Breakdown & Workspace Crate Layout

```text
rbt/
├── Cargo.toml                       # Root Cargo workspace configuration
├── docs/                            # Architectural Decision Records & thesis alignment
├── crates/
│   ├── rbt-core/                    # Project loader, AST Jinja parser (ref/source), DAG compiler
│   ├── rbt-bronze/                  # File globbing, CSV/Parquet column projection readers
│   ├── rbt-json/                    # jshift integration (parse-avoiding JSONL path extraction & stamping)
│   ├── rbt-catalog/                 # Iceberg catalog adapter (REST, Glue, Hive, Polaris, Nessie)
│   ├── rbt-engine/                  # Apache DataFusion engine context & physical optimizer rules
│   ├── rbt-materializer/            # Iceberg Parquet file encoder & WAP snapshot committer
│   ├── rbt-models/                  # Star-schema metadata (dimensions, facts, grains, relationships)
│   ├── rbt-testing/                 # Zero-copy inline Arrow quality assertion kernels
│   └── rbt-cli/                     # Binary CLI & Prost/JSON diagnostic reporter
```

### 3.1 `rbt-json` & `rbt-bronze`: High-Speed Raw Ingest Edge
- **`jshift` Parse-Avoiding JSONL Path Extraction**: Avoids allocating full JSON Document Object Models (DOM). Slices target path fields (`id`, `tenant_id`, `ts`, `amount`) directly from raw byte buffers and stamps ingestion timestamps in a single pass.
- **Selective CSV & Parquet Reader**: Applies column projection and pushdown filters at file scan time, producing aligned Arrow `RecordBatch` streams.

### 3.2 `rbt-core` & `rbt-models`: AST Parsing & Star Schema Modeling
- **Jinja SQL AST Parser**: Converts `{{ ref('silver.events') }}` and `{{ source('raw', 'users') }}` into canonical Iceberg table identifiers.
- **Star Schema Declarations**:
  ```yaml
  # models/fact_sales.yml
  model: gold.fact_sales
  kind: fact
  grain: [sale_id, tenant_id]
  relationships:
    - column: customer_id
      to: gold.dim_customer
      field: customer_id
  ```

### 3.3 `rbt-engine` & `rbt-catalog`: DataFusion & Iceberg Integration
- **In-Memory Engine**: Executes SQL transformations across DataFusion physical plan operators.
- **Manifest Pruning**: Evaluates query predicates against Iceberg manifest file min/max column bounds to skip reading irrelevant object storage data files.

### 3.4 `rbt-testing` & `rbt-materializer`: Inline Testing & WAP Protocol
- **Inline Stream Assertions**: Evaluates non-null, uniqueness, and range assertions on Arrow `RecordBatch` chunks in flight.
- **Write-Audit-Publish (WAP)**: Writes Parquet files to snapshot branch `wap_<run_id>`. Performs atomic catalog swap to `main` only after 100% test pass.

### 3.5 `rbt-cli`: Developer UX & Prost Diagnostic Engine
- **Instant SQL Loop**:
  - `rbt validate`: Validates syntax, references, and column types against Iceberg schema before reading data.
  - `rbt explain`: Generates logical execution plan and partition pruning summary.
  - `rbt preview --limit 10`: Executes fast preview on sample data chunks.
  - `rbt run`: Executes materialization and commits Iceberg snapshot.
  - `rbt test`: Runs data quality assertion suite.
- **Agent-Repairable Diagnostics (`prost`)**:
  Serialized Protobuf (`prost`) or JSON diagnostics providing exact error codes and column suggestions (e.g. `E_RBT_COLUMN_NOT_FOUND: Did you mean 'tenant_id'?`).

---

## 4. End-to-End Data Lifecycle

```text
1. RAW BRONZE (JSONL / CSV / Parquet on S3)
   │
   ▼  jshift / rbt-bronze (Path extraction & Arrow encoding)
2. SILVER ICEBERG TABLES (Typed, Partitioned, WAP Committed)
   │
   ▼  rbt-engine / DataFusion (Star joins, aggregations, grain assertions)
3. GOLD STAR SCHEMA (Dimension & Fact Iceberg Tables)
```

---

## 5. Summary of Crate Ecosystem Integration

| Crate / Technology | Primary Role in `rbt` | Performance Impact |
| :--- | :--- | :--- |
| **`apache-iceberg`** | Universal table truth, snapshot ACID commits, REST catalog | Eliminates warehouse lock-in & vendor DDL fees |
| **`jshift`** | Zero-copy selective JSON path extract & field stamping | 5x-10x faster than `serde_json::Value` full DOM parsing |
| **`prost`** | High-efficiency Protobuf diagnostic & metadata serialization | Fast, structured, machine-readable CLI error reports |
| **`datafusion`** | SIMD-vectorized in-memory SQL execution engine | 10x-50x less memory footprint than PySpark JVM |

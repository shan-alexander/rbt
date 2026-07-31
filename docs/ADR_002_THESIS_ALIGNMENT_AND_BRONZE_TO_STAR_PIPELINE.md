# ADR-002: Thesis Alignment, Bronze Edge Ingestion, and Star Schema Pipeline Design

- **Status**: Approved / Active  
- **Deciders**: Principal Data Engineering Manager, Platform Architecture Team  
- **Date**: July 2026  
- **Technical Scope**: `thesis.md` Alignment, `jshift` Raw Bronze Edge, `prost` Diagnostics, `iceberg` & `datafusion` Star Schema Pipeline  

---

## 1. Context & Gap Analysis: `thesis.md` vs. Prior Architecture

A comprehensive review of `thesis.md` against our initial architecture (`ARCHITECTURE.md` and `ADR-001`) revealed five critical design aspects that were previously understated or omitted:

1. **The Raw Bronze Edge (`jshift` & `rbt-json`)**:
   - *Previous Gap*: Our initial ADR focused primarily on Silver $\rightarrow$ Gold SQL transformations over existing Iceberg tables.
   - *Thesis Requirement*: Real enterprise pipelines start at the **Bronze edge** (raw JSONL, CSV, Parquet files on S3/GCS). Incorporating **`jshift`** provides byte-efficient, parse-avoiding selective path extraction and field stamping directly from raw byte streams, bypassing slow `serde_json::Value` DOM parsing.

2. **Machine-Readable Diagnostics & `prost` Serialization**:
   - *Previous Gap*: CLI errors were planned as basic human-readable text logs.
   - *Thesis Requirement*: To support automated AI agent repair loops and fast developer workflows, diagnostic errors (`E_RBT_COLUMN_NOT_FOUND`, field suggestions) and run reports (`RunReport`, `TestReport`) must be serialized efficiently using **`prost`** (Protobuf) and JSON.

3. **The "Instant Feedback" Developer Loop**:
   - *Previous Gap*: The CLI scope focused primarily on `run`, `compile`, and `test`.
   - *Thesis Requirement*: Developers and agents require an instant feedback loop **before** launching expensive storage scans:
     $$\text{validate} \longrightarrow \text{explain} \longrightarrow \text{preview} \longrightarrow \text{run} \longrightarrow \text{test}$$

4. **Star-Schema First-Class Metadata (`rbt-models`)**:
   - *Previous Gap*: Models were treated as generic SQL query nodes.
   - *Thesis Requirement*: Models must support explicit dimensional modeling constructs (`kind: dimension`, `business_key`, `kind: fact`, `grain: [...]`, `relationships`), allowing `rbt` to automatically validate grain uniqueness and relational integrity.

5. **Consolidated Crate Topology**:
   - *Thesis Requirement*: Align workspace crates cleanly to reflect the complete data flow:
     `rbt` (Facade/CLI), `rbt-core`, `rbt-bronze`, `rbt-json` (jshift), `rbt-sql`, `rbt-catalog` (iceberg), `rbt-engine` (datafusion), `rbt-materializer`, `rbt-models`, `rbt-testing`.

---

## 2. Core Architecture Decisions

### Decision 2.1: Ingestion Architecture with `jshift` (`rbt-json` & `rbt-bronze`)

For raw JSONL ingestion, `rbt` uses **`jshift`** to extract specified target paths (`paths: [id, tenant_id, ts, amount]`) and stamp metadata (`ingested_at: run_started_at`) directly during byte buffer iteration.

```rust
// rbt-json jshift extraction interface
pub struct BronzeJsonExtractor {
    pub target_paths: Vec<String>,
}

impl BronzeJsonExtractor {
    pub fn extract_to_arrow_batch(&self, raw_json_bytes: &[u8]) -> anyhow::Result<RecordBatch> {
        // jshift byte-level path scanning without full DOM allocations
        tracing::debug!("Extracting JSONL bytes using jshift parse-avoiding kernels");
        // Yields aligned Arrow RecordBatch
        todo!()
    }
}
```

---

### Decision 2.2: Machine-Readable Diagnostics Engine (`prost`)

All execution outputs, run summaries, diagnostic errors, and test results are backing-encoded with **`prost`** Protobuf schemas:

```protobuf
// proto/rbt_diagnostics.proto
syntax = "proto3";
package rbt.diagnostics;

enum ErrorCode {
  E_UNKNOWN = 0;
  E_RBT_COLUMN_NOT_FOUND = 1;
  E_RBT_MODEL_NOT_FOUND = 2;
  E_RBT_SCHEMA_MISMATCH = 3;
}

message DiagnosticError {
  ErrorCode code = 1;
  string message = 2;
  string model_name = 3;
  string suggestion = 4;
}

message RunReport {
  string run_id = 1;
  int64 rows_processed = 2;
  int64 bytes_written = 3;
  double duration_seconds = 4;
  string iceberg_snapshot_id = 5;
}
```

---

### Decision 2.3: Developer Feedback Loop Mechanics

`rbt` implements five CLI/library verbs to make data pipeline development smooth and immediate:

```text
1. rbt validate ──> Checks SQL syntax, ref() dependencies, and schema column types against catalog metadata (Zero I/O scan).
2. rbt explain  ──> Displays DataFusion logical execution plan and partition pruning summary.
3. rbt preview  ──> Executes query with LIMIT N on sample partition batches.
4. rbt run      ──> Executes full materialization and commits Iceberg transaction snapshot.
5. rbt test     ──> Evaluates not_null, unique, and relationship assertions.
```

---

## 3. Updated Workspace Crate Topology

We update the root workspace `Cargo.toml` to declare all 9 specialized crates:

1. **`rbt`**: Main facade and unified CLI binary (`rbt-cli`).
2. **`rbt-core`**: Manifest parsing, Jinja `ref()` / `source()` parser, topological DAG compiler.
3. **`rbt-bronze`**: File globbing, CSV/Parquet column projection readers.
4. **`rbt-json`**: `jshift` selective JSONL path extraction & field stamping.
5. **`rbt-catalog`**: Apache `iceberg` catalog bindings (REST, Glue, Polaris, Hive, Nessie).
6. **`rbt-engine`**: Apache `datafusion` in-memory SIMD execution context and optimizer rules.
7. **`rbt-materializer`**: Iceberg Parquet storage encoder & Write-Audit-Publish (WAP) committer.
8. **`rbt-models`**: Star-schema metadata models (dimension keys, fact grains, relationships).
9. **`rbt-testing`**: Zero-copy inline Arrow array data quality kernels.

---

## 4. Operational Comparison Table

| Pipeline Stage | Legacy PySpark + dbt | `rbt` Stack | Primary Tech |
| :--- | :--- | :--- | :--- |
| **Bronze Raw Ingest** | PySpark `spark.read.json()` (Full DOM parse) | Zero-copy selective path extract & stamp | `jshift` + `rbt-json` |
| **Model Resolution** | Python Jinja template compilation | Fast Rust regex/AST Jinja compiler | `rbt-core` |
| **Query Engine** | Spark Catalyst / Remote Data Warehouse | Vectorized SIMD in-memory engine | `datafusion` |
| **Table Storage** | Warehouse internal formats / Delta Lake | Universal Open Format ACID Table | `iceberg` |
| **Quality Assertions** | Post-hoc SQL queries ($$ billed) | In-flight Arrow validity bitmasks | `rbt-testing` |
| **Diagnostics & CLI** | Text logs / Python tracebacks | Machine-readable Protobuf & JSON | `prost` |

---

## 5. Decision Summary

By integrating **`jshift`** at the Bronze edge, **`prost`** for machine-readable CLI diagnostics, **`iceberg`** for snapshot table truth, and **`datafusion`** for in-memory execution, `rbt` fulfills the thesis of providing a smooth, high-performance, single-crate data pipeline engine from raw ingest to star schema.

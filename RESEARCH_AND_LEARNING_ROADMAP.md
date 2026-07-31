# RBT Research & Educational Learning Roadmap

> **Document Objective**: Provide an exhaustive inventory of competitive research, technical specification deep-dives, database engineering texts, and Rust low-level systems programming documentation required to build `rbt`.

---

## 1. Competitive & Ecosystem Landscape Research

### 1.1 dbt (Data Build Tool) Paradigm & Feature Gaps
- **dbt Feature Mapping & Deficiencies**:
  - Analyze `dbt-core` compilation model: How Jinja rendering converts model files into target SQL dialects.
  - Review `manifest.json` and `run_results.json` state schemas: Understand node selection syntax (`--select state:modified+`, tag filtering, node graph traversal).
  - Deconstruct dbt snapshot materialization (SCD Type 2 column tracking) and incremental strategies (`append`, `merge`, `delete+insert`).
  - Study limitations: Inability to execute zero-copy inline streaming tests, reliance on full table/view DDL executions, slow Python single-threaded orchestration.

### 1.2 Modern Alternatives Survey (SQLMesh, SDF, Bytewax)
- **SQLMesh**: Study Virtual Data Marts (instant environment cloning without data duplication via view pointer swaps) and static SQL analysis for incremental preview computation.
- **SDF (Semantic Data Framework)**: Analyze rust-based static AST parsing, column-level lineage extraction, and zero-warehouse model compilation.
- **Polars / Bytewax / Daft**: Evaluate how fast in-memory engines handle out-of-core execution and dataframe transformations on object storage.

### 1.3 Rust Iceberg Ecosystem Status Assessment
- **`apache-iceberg` (Official `iceberg` Rust Crate)**:
  - Audit current feature coverage: Catalog support (REST, AWS Glue, Hive, Nessie, Polaris), Reader/Writer implementation completeness, metadata commit mechanisms, and partition spec handling.
  - Benchmark Parquet writer performance and manifest file generation throughput.
- **`iceberg-datafusion` Integration**:
  - Examine `DataFusion` table provider implementations within the official Iceberg Rust repository.
  - Identify missing capabilities in predicate pushdown, partition pruning, and schema projection pushdown.

---

## 2. Technical Specification & Standards Deep-Dives

### 2.1 Apache Iceberg Specification v2 & v3
- **Format Specifications**:
  - *Metadata JSON*: Table schema history, partition spec, snapshot logs, current snapshot pointer.
  - *Manifest List & Manifest Files*: File path index, partition field summaries, min/max statistics per column chunk, null value counts, NaN counts.
  - *Delete Files*: Equality delete files and Position delete files for Merge-On-Read (MOR) operations.
  - *Puffin File Format*: Storage format for statistics (HyperLogLog sketches, Theta sketches) used in query optimization.
- **Catalog Protocols**:
  - REST Catalog Specification (OpenAPI endpoints, OAuth token exchange, transactional table commit API).
  - Optimistic Concurrency Control (OCC) protocol for atomic snapshot swaps and retry strategies upon commit collision.

### 2.2 Apache Arrow & Columnar Storage Specification
- **Arrow Memory Layout**:
  - Buffer alignment (64-byte alignment for SIMD vectorization), validity bitmaps, offset buffers for variable-length types (`StringArray`, `BinaryArray`).
  - Struct arrays, List arrays, Map arrays, and Dictionary encoding formats.
  - Zero-copy slicing (`Array::slice`) and zero-allocation memory sharing across Tokio task boundaries.
- **Parquet File Format**:
  - Column chunk pages, dictionary pages, Bloom filters, Parquet statistics (min/max), and compression codecs (Snappy, Zstd).

---

## 3. Educational Resources & Low-Level Learning Plan

### 3.1 Rust Low-Level Systems Programming
1. **The Rust Reference & The Rustonomicon**
   - *Topics*: Unsafe Rust semantics, memory layout & alignment (`#[repr(C)]`, `#[repr(align)]`), aliasing rules, interior mutability (`UnsafeCell`), variance, and subtyping.
   - *Application in `rbt`*: Writing custom zero-copy Arrow memory validators and low-level buffer operations.
2. **Tokio Async Architecture & Async Rust**
   - *Resources*: *The Async Book*, Tokio Crate Documentation, Tokio Internals Guide.
   - *Topics*: `Future` state machines, manual `Poll` and `Waker` implementations, `Pin` and `Unpin` mechanics, multi-threaded work-stealing scheduler (`tokio::spawn`), channel backpressure (`tokio::sync::mpsc::bounded`).
   - *Application in `rbt`*: Building high-performance task schedulers for execution DAG nodes without resource exhaustion or deadlocks.
3. **The Rust Performance Book & SIMD Intrinsics**
   - *Topics*: Heap allocation profiling (`jemalloc` / `mimalloc`), cache locality, zero-allocation parsing, `std::simd` and AVX-512/NEON vector operations.
   - *Application in `rbt`*: Maximizing data validation assertion throughput on multi-gigabyte Arrow record batch streams.

### 3.2 Database Internals & Query Engine Architecture
1. **Database Internals** *(by Alex Petrov)*
   - *Topics*: Columnar storage engines, execution models (Volcano iterator model vs Vectorized execution vs Code generation), join algorithms (Hash Join, Sort-Merge Join), memory pools, and spill-to-disk strategies.
2. **Apache DataFusion Architecture Documentation & Source Code**
   - *Topics*: `LogicalPlan` transformation passes, `OptimizerRule` trait, `ExecutionPlan` physical execution nodes, `RecordBatchStream` async iteration, custom aggregate (`Accumulator`) functions.
   - *Application in `rbt`*: Customizing physical execution plans to inject inline quality assertions and Iceberg statistics pushdowns.

---

## 4. Primary Research Tasks & Milestone Checklist

- [ ] **Task 1: Catalog Interoperability Testbed**
  - Spin up local Iceberg REST Catalog (via Docker Compose / Apache Polaris) and validate `iceberg` Rust crate connection, table creation, and snapshot updates.
- [ ] **Task 2: DataFusion Iceberg Read/Write Benchmark**
  - Measure throughput of reading Iceberg tables into DataFusion `RecordBatchStream` and writing modified streams back as Parquet data files.
- [ ] **Task 3: Inline Quality Validation Kernel Prototype**
  - Implement a prototype SIMD null-check and range-check on Arrow `PrimitiveArray` / `StringArray` and evaluate execution overhead relative to raw scan speed.
- [ ] **Task 4: Template & Macro Parser Design**
  - Benchmark SQL template parsing using `tera` vs custom SQL parser (`sqlparser-rs`) to establish AST-level model resolution.
- [ ] **Task 5: WAP (Write-Audit-Publish) Branch Commit Validation**
  - Verify `iceberg` Rust crate capabilities for creating uncommitted branch snapshots and committing fast-forward merges to `main`.

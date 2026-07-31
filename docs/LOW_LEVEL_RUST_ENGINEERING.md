# Low-Level Rust Systems Engineering for RBT: Zero-Copy, In-Memory Iceberg Engine

> **Document Purpose**: Technical deep-dive and educational guide on low-level Rust concepts, memory layouts, async concurrency patterns, DataFusion query engine extensions, and Iceberg spec internals required to build an ultra-low-performance dbt replacement.

---

## Module 1: Zero-Copy Memory Management & Apache Arrow Layouts

### 1.1 Arrow Memory Layout & SIMD Alignment
Apache Arrow memory arrays are backed by contiguous, 64-byte aligned binary buffers (`arrow_buffer::Buffer`). This alignment matches the cache-line and vector register width of x86_64 AVX-512 and ARM NEON SIMD instructions.

```text
Arrow Memory Buffer Layout (64-byte aligned)
┌─────────────────────────┬─────────────────────────┬─────────────────────────┐
│  Validity Bitmap        │  Offset Buffer (VarLen) │  Values Array           │
│  [1, 1, 0, 1, 1, 1...]  │  [0, 5, 12, 19, 25...]  │  "ALICE" "BOB" "CHARLIE"│
└─────────────────────────┴─────────────────────────┴─────────────────────────┘
```

#### Low-Level Rust Slicing Without Allocation
In Rust, slicing an Arrow array does **not** copy or reallocate underlying heap memory. It increments the atomic reference count of the underlying `Arc<Buffer>` and creates a new offset window pointer:

```rust
use arrow::array::{Array, Int32Array};
use std::sync::Arc;

pub fn zero_copy_slice_example(array: &Int32Array, offset: usize, length: usize) -> Int32Array {
    // Array::slice is an O(1) operation: it clones Arc<Buffer> and updates offset/len
    let sliced_data = array.to_data().slice(offset, length);
    Int32Array::from(sliced_data)
}
```

### 1.2 Bitmask Processing & $O(\lceil N/64 \rceil)$ Null Verification
Arrow represents nullability using a validity bitmap where bit `1` indicates a non-null value and bit `0` indicates a null value.

Rather than iterating row-by-row ($O(N)$), low-level Rust scans validity bitmaps using 64-bit word operations:

```rust
use arrow_buffer::Bitmap;

pub fn fast_null_count_check(bitmap: Option<&Bitmap>) -> usize {
    match bitmap {
        None => 0, // No validity bitmap means 100% non-null
        Some(bm) => {
            let total_bits = bm.len();
            let set_bits = bm.count_set_bits();
            total_bits - set_bits // O(1) or O(N/64) bitmask popcount
        }
    }
}
```

---

## Module 2: High-Performance Tokio Concurrency & Async Stream Handling

### 2.1 Async Record Batch Streams (`SendableRecordBatchStream`)
DataFusion processes data as streams of Arrow `RecordBatch`es asynchronously via `SendableRecordBatchStream`. This is defined as a pinned, thread-safe stream of results:

$$\text{Stream} = \text{Pin}\left(\text{Box}\left(\text{dyn Stream}<\text{Item} = \text{Result}<\text{RecordBatch}>> + \text{Send}\right)\right)$$

#### Custom Stream Implementation
To inject inline data quality testing or streaming metrics collection, we implement a custom wrapper stream:

```rust
use datafusion::execution::RecordBatchStream;
use datafusion::error::Result as DFResult;
use arrow::array::RecordBatch;
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct InlineQualityStream {
    pub inner: Pin<Box<dyn RecordBatchStream + Send>>,
}

impl Stream for InlineQualityStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Execute low-level SIMD assertions on RecordBatch in flight
                if let Err(e) = crate::validate_batch_inline(&batch) {
                    return Poll::Ready(Some(Err(datafusion::error::DataFusionError::Execution(
                        e.to_string(),
                    ))));
                }
                Poll::Ready(Some(Ok(batch)))
            }
            other => other,
        }
    }
}
```

### 2.2 DAG Parallelism & Tokio Semaphore Backpressure
When orchestrating hundreds of pipeline models in a DAG, launching all Tokio tasks simultaneously will exhaust CPU core allocation and memory buffers. We enforce bounded concurrency using `tokio::sync::Semaphore`:

```rust
use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct TaskScheduler {
    concurrency_limiter: Arc<Semaphore>,
}

impl TaskScheduler {
    pub fn new(max_concurrent_tasks: usize) -> Self {
        Self {
            concurrency_limiter: Arc::new(Semaphore::new(max_concurrent_tasks)),
        }
    }

    pub async fn run_model_task<F, T>(&self, task_future: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        let _permit = self.concurrency_limiter.acquire().await.unwrap();
        task_future.await
    }
}
```

---

## Module 3: DataFusion Physical Execution Plan Customization

### 3.1 Custom `ExecutionPlan` Nodes
`rbt` extends DataFusion's physical plan hierarchy by injecting custom `ExecutionPlan` nodes for materialization streaming and inline testing.

```rust
use datafusion::physical_plan::{ExecutionPlan, DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use datafusion::execution::context::TaskContext;
use datafusion::error::Result as DFResult;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct IcebergTableSinkExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub table_uri: String,
}

impl ExecutionPlan for IcebergTableSinkExec {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> arrow::datatype::SchemaRef { self.input.schema() }
    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        self.input.output_partitioning()
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> { vec![self.input.clone()] }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            table_uri: self.table_uri.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        // Wrap stream with Iceberg Parquet writer and snapshot builder
        Ok(stream)
    }
}
```

### 3.2 Partition & Predicate Pushdown Filters
DataFusion optimizes queries by pushing SQL `WHERE` predicates directly down to Iceberg manifest statistics (min/max metadata per column chunk).

- If a query includes `WHERE created_at >= '2026-07-01'`, DataFusion evaluates the predicate against Iceberg manifest min/max fields.
- Manifest files with `max_value < '2026-07-01'` are **skipped entirely** before reading any object storage byte ranges.

---

## Module 4: Apache Iceberg Spec Low-Level Serialization

### 4.1 Iceberg Avro Manifest Serialization
Iceberg metadata files (`manifest-list` and `manifest-file`) are formatted as Avro binary streams containing:
1. `status`: 0 (Existing), 1 (Added), 2 (Deleted).
2. `snapshot_id`: 64-bit integer tracking the atomic transaction snapshot.
3. `data_file`: Nested record containing file path, format (Parquet/ORC), partition data, record count, and byte size.

```text
Iceberg Snapshot Metadata Transaction Loop
┌──────────────────┐     Writes Parquet     ┌──────────────────┐
│  DataFusion      │ ─────────────────────> │ Data Files (.parquet)│
└────────┬─────────┘                        └──────────────────┘
         │
         │ Builds Avro Manifest Entries
         ▼
┌──────────────────┐     Commits Manifest   ┌──────────────────┐
│ Manifest File    │ ─────────────────────> │ Catalog REST API │
│ (.avro metadata) │                        │ (Atomic Swap OCC)│
└──────────────────┘                        └──────────────────┘
```

### 4.2 Merge-On-Read (MOR) via Equality & Position Deletes
For real-time incremental pipelines, rewriting full Parquet data files (Copy-On-Write) is prohibitively expensive. `rbt` supports Merge-On-Read (MOR):
- **Position Deletes**: A secondary Parquet file containing columns `file_path: String` and `pos: Int64` indicating exact record locations to invalidate during scan.
- **Equality Deletes**: A file containing specific deleted column key values (e.g. `user_id = 9921`) that are joined in-memory during DataFusion execution.

---

## Module 5: AST Parsing & Zero-Allocation Macro Resolution

### 5.1 `sqlparser-rs` Model AST Traversal
Rather than relying on string-regex substitution or Jinja Python runtimes, `rbt` uses `sqlparser-rs` to convert model SQL into a strongly-typed Abstract Syntax Tree (AST):

```rust
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;

pub fn parse_model_ast(sql: &str) -> anyhow::Result<Vec<Statement>> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql)?;
    Ok(ast)
}
```

### 5.2 AST Transformer for `ref('model')` Jinja Macros
The AST transformer walks `TableFactor` nodes in the parsed query tree and converts `ref('stg_orders')` calls into canonical catalog table references:

```rust
use sqlparser::ast::{ObjectName, TableFactor, TableWithJoins};

pub fn replace_model_refs(table_with_joins: &mut TableWithJoins) {
    if let TableFactor::Table { name, .. } = &mut table_with_joins.relation {
        if name.to_string().starts_with("ref(") {
            let inner_model = name.to_string()
                .replace("ref('", "")
                .replace("')", "");
            *name = ObjectName(vec![
                sqlparser::ast::Ident::new("iceberg_catalog"),
                sqlparser::ast::Ident::new("default"),
                sqlparser::ast::Ident::new(inner_model),
            ]);
        }
    }
}
```

---

## Summary of Key Concepts & Performance Rules

1. **Zero-Copy Slicing**: Use Arrow slice views instead of allocating new memory buffers.
2. **SIMD Validity Bitmask Scanning**: Inspect Arrow bitmap word counts for $O(1)$ inline quality assertions.
3. **Async Streaming Operators**: Wrap DataFusion `SendableRecordBatchStream` to execute non-blocking checks in flight.
4. **Optimistic Concurrency Control**: Leverage Iceberg REST catalog atomic snapshot updates and WAP branching.
5. **AST Macro Resolution**: Parse SQL directly into `sqlparser-rs` AST structures for zero-allocation table resolution.

---

# Advanced Systems Engineering Supplement: Peak Performance Mechanics for RBT

> **Extension Context**: Added July 2026. Deep-dive on bare-metal memory management, hardware-aware SIMD intrinsics, kernel-bypass Direct I/O, lock-free state machines, custom DataFusion physical optimizer rules, and Iceberg Puffin sketch engineering.

---

## Module 6: Bare-Metal Memory Management & Custom Allocators

### 6.1 Avoiding Heap Fragmentation with `jemalloc` / `mimalloc`
Standard glibc `malloc` experiences severe internal memory fragmentation under high-frequency Arrow batch allocation and deallocation. In multi-gigabyte data transformation streams, glibc memory allocators fail to return freed memory pages back to the operating system efficiently.

`rbt` explicitly configures `tikv-jemallocator` or `mimalloc` as the global memory allocator:

```rust
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
```

#### Arena Tuning Parameters
`rbt` configures custom `jemalloc` arena options via environment settings or low-level `mallctl` FFI calls:
- `background_thread:true`: Enables dedicated background threads to purge dirty memory pages asynchronously without stalling worker CPU execution threads.
- `dirty_decay_ms:0` and `muzzy_decay_ms:1000`: Immediately returns unused dirty memory pages to the OS kernel while delaying muzzy page decay to smooth allocation spikes.

### 6.2 Region-Based Bump Allocators for Transient Pipeline Metadata
For ephemeral AST transformations, DAG topological sorting, and model dependency resolution, standard heap allocation (`Box`, `Vec`, `String`) adds unnecessary overhead. `rbt` uses region-based bump allocation (`bumpalo::Bump`):

```rust
use bumpalo::Bump;

pub struct ArenaCompiler {
    arena: Bump,
}

impl ArenaCompiler {
    pub fn new() -> Self {
        Self {
            arena: Bump::with_capacity(1024 * 1024), // 1MB preallocated arena
        }
    }

    pub fn alloc_str<'a>(&'a self, s: &str) -> &'a str {
        self.arena.alloc_str(s)
    }

    pub fn reset(&mut self) {
        self.arena.reset(); // O(1) deallocation of all transient AST data
    }
}
```

---

## Module 7: Hardware-Aware SIMD Vectorization & Cache Topology

### 7.1 Explicit Vectorization via `std::simd` and AVX-512 Masking
For complex data assertions (e.g. `col_a > 100 AND col_b < 500`), scalar loops incur branch misprediction penalties. `rbt` utilizes Portable SIMD (`std::simd::Simd`) to process 8 or 16 64-bit integer values in a single CPU instruction clock cycle:

```rust
#![feature(portable_simd)]
use std::simd::{Simd, SimdPartialOrd, Mask};

pub fn simd_range_check_u64(values: &[u64], min: u64, max: u64) -> usize {
    let mut invalid_count = 0;
    let min_vec = Simd::splat(min);
    let max_vec = Simd::splat(max);
    
    let chunks = values.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let vec = Simd::from_slice(chunk);
        // SIMD parallel comparison generating a bitmask vector
        let out_of_bounds: Mask<i64, 8> = vec.simd_lt(min_vec) | vec.simd_gt(max_vec);
        invalid_count += out_of_bounds.to_bitmask().count_ones() as usize;
    }

    // Scalar fallback for trailing unaligned elements
    for &val in remainder {
        if val < min || val > max {
            invalid_count += 1;
        }
    }

    invalid_count
}
```

### 7.2 False Sharing Prevention & Cache Line Padding
When multiple Tokio thread workers update execution state metrics concurrently, placing atomic variables on the same L1/L2 cache line (64 bytes) causes **cache line bouncing** (false sharing), degrading multi-core scaling.

`rbt` enforces 64-byte hardware cache line alignment on worker counters:

```rust
#[repr(align(64))]
pub struct CacheAlignedCounter {
    pub value: std::sync::atomic::AtomicU64,
}
```

---

## Module 8: Kernel-Bypass Asynchronous Direct I/O (`O_DIRECT` & `io_uring`)

### 8.1 Direct I/O (`O_DIRECT`) and Page Cache Bypass
When reading multi-terabyte Iceberg Parquet files from NVMe storage or high-speed local NVMe caches, kernel page cache buffering creates duplicate memory copying (Storage Media $\rightarrow$ Kernel Page Cache $\rightarrow$ User Buffer).

`rbt` leverages `O_DIRECT` to execute Direct Memory Access (DMA) directly from storage into 4096-byte sector-aligned Arrow buffers:

```rust
#[cfg(target_os = "linux")]
use std::fs::OpenOptions;

pub fn open_direct_io_file(path: &str) -> std::io::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_DIRECT); // Bypass kernel page cache
    }
    options.open(path)
}
```

---

## Module 9: Lock-Free Concurrency for Pipeline Execution Schedulers

### 9.1 Atomic State Transitions for Model DAG Nodes
Managing model execution state (`PENDING` $\rightarrow$ `RUNNING` $\rightarrow$ `VALIDATING` $\rightarrow$ `COMMITTED` $\rightarrow$ `FAILED`) with mutexes creates lock contention. `rbt` implements a lock-free state machine using `AtomicU8` and explicit acquire/release memory orderings:

```rust
use std::sync::atomic::{AtomicU8, Ordering};

pub const STATE_PENDING: u8 = 0;
pub const STATE_RUNNING: u8 = 1;
pub const STATE_VALIDATING: u8 = 2;
pub const STATE_COMMITTED: u8 = 3;
pub const STATE_FAILED: u8 = 4;

pub struct AtomicModelNode {
    pub state: AtomicU8,
}

impl AtomicModelNode {
    pub fn try_transition_to_running(&self) -> bool {
        // Compare-And-Swap (CAS) state transition
        self.state
            .compare_exchange(
                STATE_PENDING,
                STATE_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}
```

---

## Module 10: Advanced DataFusion Optimization & Iceberg Puffin Sketches

### 10.1 Iceberg `.puffin` File Sketch Generation
For advanced Cost-Based Optimization (CBO), DataFusion requires accurate distinct count estimates (NDV - Number of Distinct Values). `rbt` generates **Puffin format files** containing HyperLogLog (HLL) and Theta sketches directly during Parquet write streams:

- **HyperLogLog (HLL++)**: Estimates column cardinality with $< 1\%$ error margin using 12KB of sketch memory per column.
- **Theta Sketches**: Enables set-intersection estimations (`COUNT(DISTINCT user_id)` across multiple partitions) directly from metadata files without reading Parquet row groups.

---

## Final Systems Architecture Takeaway

By combining **`jemalloc` arena control**, **SIMD AVX-512 array masking**, **Direct I/O page cache bypass**, **lock-free CAS state machines**, and **Iceberg Puffin sketch engineering**, `rbt` achieves bare-metal performance, squeezing maximum throughput out of modern multi-socket hardware architectures.


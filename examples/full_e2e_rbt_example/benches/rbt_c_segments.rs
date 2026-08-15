//! Segment / architecture micro-bench for RBT-C style fan-out vs mega.
//!
//! These are **host-level** timings against the full_e2e lake (when present),
//! not synthetic Criterion loops. Prefer the binary for diagnosis:
//!
//! ```bash
//! cargo run -p full-e2e-rbt-example --release -- -p examples/full_e2e_rbt_example diag -j 8
//! ```
//!
//! This bench file exists so `cargo bench -p full-e2e-rbt-example` is discoverable
//! and documents what each design measures.
//!
//! Run (from workspace root):
//! ```bash
//! cargo bench -p full-e2e-rbt-example --bench rbt_c_segments
//! ```

use std::process::Command;
use std::time::Instant;

fn project_root() -> std::path::PathBuf {
    // crates/rbt is not this package; example is examples/full_e2e_rbt_example
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
}

fn run_diag(jobs: usize) {
    let root = project_root();
    let repo = root.join("../..");
    let t0 = Instant::now();
    let status = Command::new("cargo")
        .current_dir(&repo)
        .args([
            "run",
            "-p",
            "full-e2e-rbt-example",
            "--release",
            "--",
            "-p",
            "examples/full_e2e_rbt_example",
            "diag",
            "-j",
            &jobs.to_string(),
        ])
        .status()
        .expect("spawn cargo run diag");
    assert!(status.success(), "diag failed");
    eprintln!(
        "rbt_c_segments: full diag wall_secs={:.3} jobs={jobs}",
        t0.elapsed().as_secs_f64()
    );
}

fn main() {
    let jobs: usize = std::env::var("RBT_BENCH_JOBS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    println!("=== full_e2e RBT-C segment diagnosis (delegates to `diag` subcommand) ===");
    println!("B = mega table | C = WorkUnit parts | D = per-symbol tf_* + UNION");
    println!("Set RBT_BENCH_JOBS to override workers (default 8).");
    run_diag(jobs);
}

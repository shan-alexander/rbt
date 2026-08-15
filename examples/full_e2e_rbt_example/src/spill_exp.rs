//! Experimental bronze spill wall-clock + Parquet landing materializer.
//!
//! * `spill-bench` — mega spill vs partitioned spill (IPC decode cost)
//! * `land-parquet-bronze` — write recommended hive Parquet landings from Arrow IPC
//!   so staging can use DataFusion listing (no spill).

use anyhow::{Context, Result};
use rbt::{LakeScanner, MaterializeWriteOptions, ScanRequest, SourceFormat};
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

/// Run mega + partitioned spill experiments; print wall clocks.
pub fn run_spill_experiment(project: &Path, jobs: usize) -> Result<()> {
    let bronze = project.join("lake/bronze/lz_stock_bars");
    if !bronze.is_dir() {
        anyhow::bail!("missing bronze hive {}", bronze.display());
    }

    let exp_root = project.join(".rbt/spill_exp");
    let mega_dir = exp_root.join("mega");
    let parts_dir = exp_root.join("parts");
    let _ = std::fs::remove_dir_all(&exp_root);
    std::fs::create_dir_all(&mega_dir)?;
    std::fs::create_dir_all(&parts_dir)?;

    let opts = MaterializeWriteOptions::default();

    // --- A) Mega spill (today's engine shape) ---------------------------------
    let mega_dest = mega_dir.join("bronze__ohlcv_1m.parquet");
    let mut req = base_req(project);
    // Full 1m universe, no symbol filter.
    req.require_partitions
        .insert("timeframe".into(), "1m".into());

    let t0 = Instant::now();
    let scanner = LakeScanner::from_request(&req);
    let mega_stats = scanner
        .scan_spill_to_parquet(&req, &mega_dest, &opts)
        .context("mega spill")?;
    let mega_s = t0.elapsed().as_secs_f64();
    let mega_bytes = std::fs::metadata(&mega_dest).map(|m| m.len()).unwrap_or(0);

    println!("========== SPILL EXPERIMENT ==========");
    println!(
        "MEGA     wall_secs={mega_s:.3}  rows={}  batches={}  bytes={mega_bytes}  path={}",
        mega_stats.rows,
        mega_stats.batches,
        mega_dest.display()
    );

    // --- B) Partitioned spill: one file per symbol (1m only) ------------------
    let symbols = list_1m_symbols(&bronze)?;
    println!(
        "PART     planning {} symbol parts (timeframe=1m), jobs={jobs}",
        symbols.len()
    );

    let t1 = Instant::now();
    let mut part_rows = 0usize;
    let mut part_batches = 0usize;
    let mut part_bytes = 0u64;

    if jobs <= 1 {
        for sym in &symbols {
            let (r, b, sz) = spill_one_symbol(project, &parts_dir, sym, &opts)?;
            part_rows += r;
            part_batches += b;
            part_bytes += sz;
        }
    } else {
        use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
        use std::sync::Arc;
        let rows_a = Arc::new(AtomicUsize::new(0));
        let batches_a = Arc::new(AtomicUsize::new(0));
        let bytes_a = Arc::new(AtomicU64::new(0));
        let project = project.to_path_buf();
        let parts_dir = parts_dir.clone();
        let opts = opts.clone();

        // Thread pool fan-out (blocking spill).
        let pool = std::thread::available_parallelism()
            .map(|n| n.get().min(jobs).max(1))
            .unwrap_or(jobs.max(1));
        let chunk = (symbols.len() + pool - 1) / pool;
        let mut handles = Vec::new();
        for chunk_syms in symbols.chunks(chunk.max(1)) {
            let chunk_syms: Vec<String> = chunk_syms.to_vec();
            let project = project.clone();
            let parts_dir = parts_dir.clone();
            let opts = opts.clone();
            let rows_a = Arc::clone(&rows_a);
            let batches_a = Arc::clone(&batches_a);
            let bytes_a = Arc::clone(&bytes_a);
            handles.push(std::thread::spawn(move || -> Result<()> {
                for sym in chunk_syms {
                    let (r, b, sz) = spill_one_symbol(&project, &parts_dir, &sym, &opts)?;
                    rows_a.fetch_add(r, Ordering::Relaxed);
                    batches_a.fetch_add(b, Ordering::Relaxed);
                    bytes_a.fetch_add(sz, Ordering::Relaxed);
                }
                Ok(())
            }));
        }
        for h in handles {
            h.join()
                .expect("thread join")
                .context("partitioned spill worker failed")?;
        }
        part_rows = rows_a.load(Ordering::Relaxed);
        part_batches = batches_a.load(Ordering::Relaxed);
        part_bytes = bytes_a.load(Ordering::Relaxed);
    }

    let part_s = t1.elapsed().as_secs_f64();
    let n_parts = std::fs::read_dir(&parts_dir)?.filter(|e| e.is_ok()).count();

    println!(
        "PART     wall_secs={part_s:.3}  rows={part_rows}  batches={part_batches}  bytes={part_bytes}  files={n_parts}  dir={}",
        parts_dir.display()
    );
    println!(
        "SPEEDUP  mega/part = {:.2}x  (part is faster when >1)",
        mega_s / part_s.max(1e-9)
    );
    println!("NOTE     both decode the same Arrow files; part can parallelize encode+write");
    println!("======================================");
    Ok(())
}

fn base_req(project: &Path) -> ScanRequest {
    ScanRequest {
        project_dir: project.to_path_buf(),
        scan_path: "lake/bronze/lz_stock_bars".into(),
        format: SourceFormat::ArrowIpc,
        paths: vec![],
        toml_rows_key: None,
        partition_by: vec!["symbol".into(), "timeframe".into()],
        require_partitions: HashMap::new(),
        require_partitions_in: HashMap::new(),
        path_glob: vec!["**/*.arrow".into()],
        inject_source_path: true,
        inject_ingest_seq: false,
        inject_source_mtime: false,
        file_order: rbt::ScanFileOrder::Path,
        custom_adapter: None,
        roots: {
            let mut r = HashMap::new();
            r.insert("lake".into(), "lake".into());
            r
        },
        protobuf_max_payload_bytes: 1 << 30,
        allow_empty: false,
    }
}

fn spill_one_symbol(
    project: &Path,
    parts_dir: &Path,
    sym: &str,
    opts: &MaterializeWriteOptions,
) -> Result<(usize, usize, u64)> {
    let mut req = base_req(project);
    req.require_partitions
        .insert("timeframe".into(), "1m".into());
    req.require_partitions.insert("symbol".into(), sym.into());
    let dest = parts_dir.join(format!("symbol={sym}.parquet"));
    let scanner = LakeScanner::from_request(&req);
    let stats = scanner
        .scan_spill_to_parquet(&req, &dest, opts)
        .with_context(|| format!("spill symbol={sym}"))?;
    let sz = std::fs::metadata(&dest).map(|m| m.len()).unwrap_or(0);
    Ok((stats.rows, stats.batches, sz))
}

/// Convert Arrow hive `lz_stock_bars` → Parquet hive `lz_stock_bars_parquet`
/// (`symbol=*/timeframe=1m/data.parquet`) with hive keys as columns.
///
/// After this, staging can use `source_format: parquet` without path injects → DF listing.
pub fn land_parquet_bronze_from_arrow(project: &Path, jobs: usize) -> Result<()> {
    let bronze_arrow = project.join("lake/bronze/lz_stock_bars");
    let bronze_pq = project.join("lake/bronze/lz_stock_bars_parquet");
    if !bronze_arrow.is_dir() {
        anyhow::bail!("missing {}", bronze_arrow.display());
    }
    let _ = std::fs::remove_dir_all(&bronze_pq);
    std::fs::create_dir_all(&bronze_pq)?;

    let symbols = list_1m_symbols(&bronze_arrow)?;
    let opts = MaterializeWriteOptions::default();
    println!(
        "[land-parquet-bronze] writing {} symbols → {}",
        symbols.len(),
        bronze_pq.display()
    );
    let t0 = Instant::now();
    let mut rows = 0usize;

    let write_one = |sym: &str| -> Result<usize> {
        let dest_dir = bronze_pq
            .join(format!("symbol={sym}"))
            .join("timeframe=1m");
        std::fs::create_dir_all(&dest_dir)?;
        let dest = dest_dir.join("data.parquet");
        let mut req = base_req(project);
        req.require_partitions
            .insert("timeframe".into(), "1m".into());
        req.require_partitions.insert("symbol".into(), sym.into());
        // Inject path keys so columns exist in Parquet (listing path needs them in-file).
        req.inject_source_path = false;
        let scanner = LakeScanner::from_request(&req);
        let stats = scanner.scan_spill_to_parquet(&req, &dest, &opts)?;
        Ok(stats.rows)
    };

    if jobs <= 1 {
        for sym in &symbols {
            rows += write_one(sym)?;
        }
    } else {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        let rows_a = Arc::new(AtomicUsize::new(0));
        let pool = jobs.max(1).min(
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        );
        let chunk = (symbols.len() + pool - 1) / pool.max(1);
        let mut handles = Vec::new();
        for c in symbols.chunks(chunk.max(1)) {
            let c: Vec<String> = c.to_vec();
            let project = project.to_path_buf();
            let bronze_pq = bronze_pq.clone();
            let rows_a = Arc::clone(&rows_a);
            handles.push(std::thread::spawn(move || -> Result<()> {
                let opts = MaterializeWriteOptions::default();
                for sym in c {
                    let dest_dir = bronze_pq
                        .join(format!("symbol={sym}"))
                        .join("timeframe=1m");
                    std::fs::create_dir_all(&dest_dir)?;
                    let dest = dest_dir.join("data.parquet");
                    let mut req = base_req(&project);
                    req.require_partitions
                        .insert("timeframe".into(), "1m".into());
                    req.require_partitions.insert("symbol".into(), sym.clone());
                    req.inject_source_path = false;
                    let scanner = LakeScanner::from_request(&req);
                    let stats = scanner.scan_spill_to_parquet(&req, &dest, &opts)?;
                    rows_a.fetch_add(stats.rows, Ordering::Relaxed);
                }
                Ok(())
            }));
        }
        for h in handles {
            h.join()
                .expect("join")
                .context("land-parquet-bronze worker")?;
        }
        rows = rows_a.load(Ordering::Relaxed);
    }

    println!(
        "[land-parquet-bronze] DONE wall_secs={:.3} rows={rows} layout=symbol=*/timeframe=1m/data.parquet",
        t0.elapsed().as_secs_f64()
    );
    println!(
        "[land-parquet-bronze] staging tip: source_format=parquet scan_path=$lake/bronze/lz_stock_bars_parquet (no inject_source_path)"
    );
    Ok(())
}

fn list_1m_symbols(bronze: &Path) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(bronze)? {
        let e = e?;
        let name = e.file_name();
        let name = name.to_string_lossy();
        let Some(sym) = name.strip_prefix("symbol=") else {
            continue;
        };
        let tf = e.path().join("timeframe=1m");
        if !tf.is_dir() {
            continue;
        }
        let has = std::fs::read_dir(&tf)?
            .filter_map(|x| x.ok())
            .any(|f| f.path().extension().and_then(|x| x.to_str()) == Some("arrow"));
        if has {
            out.push(sym.to_string());
        }
    }
    out.sort();
    Ok(out)
}

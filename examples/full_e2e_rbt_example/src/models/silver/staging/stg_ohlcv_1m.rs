//! Design B — pure Rust staging: Arrow hive bronze → deduped 1m OHLCV silver.
//!
//! Materializes to:
//!   `lake/rust_models_output/silver/stage/stg_ohlcv_1m.parquet`
//!
//! Bronze (shared with Design A):
//!   `lake/bronze/lz_stock_bars/symbol=*/timeframe=1m/*.arrow`
//!
//! Grain / uniqueness: `(symbol, timestamp_ns)` — **no dups**. Latest
//! `_source_path` wins when bronze re-ingests the same bar.
//!
//! Scan contract is attached via [`scan_frontmatter`] on the host `ModelSpec`
//! (engine registers `bronze.ohlcv_1m` before `execute`).

use anyhow::{Context, Result};
use async_trait::async_trait;
use rbt::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use rbt::{
    ModelTests, RustModel, RustModelContext, RustModelOutput, SourceFormat, StagingFrontmatter,
};
use std::collections::HashMap;
use std::sync::Arc;

/// DAG / registry name — must match Design A `stg_ohlcv_1m`.
pub const STG_OHLCV_1M_NAME: &str = "stg_ohlcv_1m";

/// Host Design B staging node: full-universe 1m bars, grain-unique.
pub struct StgOhlcv1m;

/// Bronze scan + grain contract for this model (attached on `ModelSpec::frontmatter`).
pub fn scan_frontmatter() -> StagingFrontmatter {
    let mut require = HashMap::new();
    require.insert("timeframe".into(), "1m".into());

    StagingFrontmatter {
        description: Some(
            "Design B: deduped 1-minute OHLCV silver from Arrow IPC hive bronze.".into(),
        ),
        context: Some(
            "Full universe of timeframe=1m under lz_stock_bars. unique_key \
             [symbol, timestamp_ns]; latest _source_path wins."
                .into(),
        ),
        source_format: Some(SourceFormat::ArrowIpc),
        scan_path: Some("$lake/bronze/lz_stock_bars".into()),
        path_glob: Some(vec!["**/*.arrow".into()]),
        partition_by: Some(vec!["symbol".into(), "timeframe".into()]),
        require_partitions: Some(require),
        inject_source_path: Some(true),
        source_name: Some("bronze".into()),
        source_table: Some("ohlcv_1m".into()),
        grain: Some(vec!["symbol".into(), "timestamp_ns".into()]),
        unique_key: Some(vec!["symbol".into(), "timestamp_ns".into()]),
        materialization: Some("table".into()),
        // Enforce no dups on grain after latest-path dedupe.
        tests: Some(ModelTests {
            not_null: Some(vec![
                "symbol".into(),
                "timestamp_ns".into(),
                "open".into(),
                "high".into(),
                "low".into(),
                "close".into(),
                "volume".into(),
            ]),
            unique: Some(vec!["symbol".into(), "timestamp_ns".into()]),
            fail_on_error: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn output_schema() -> rbt::arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("timeframe", DataType::Utf8, true),
        Field::new(
            "bar_time",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("open", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new("volume", DataType::Int64, true),
        Field::new("bronze_dup_count", DataType::Int64, true),
        Field::new("source_path", DataType::Utf8, true),
    ]))
}

/// Embedded transform SQL — lives in this `.rs` file (no Design B `.sql` tree).
/// Types, geometry filters, latest-path dedupe on grain `(symbol, timestamp_ns)`.
const STAGING_SQL: &str = r#"
WITH raw AS (
    SELECT
        symbol,
        timeframe,
        timestamp AS ts_raw,
        timestamp_ns,
        open,
        high,
        low,
        close,
        volume,
        _source_path
    FROM bronze.ohlcv_1m
),
typed AS (
    SELECT
        symbol,
        timeframe,
        ts_raw,
        CAST(timestamp_ns AS BIGINT) AS timestamp_ns,
        CAST(open AS DOUBLE) AS open,
        CAST(high AS DOUBLE) AS high,
        CAST(low AS DOUBLE) AS low,
        CAST(close AS DOUBLE) AS close,
        CAST(volume AS BIGINT) AS volume,
        _source_path
    FROM raw
    WHERE symbol IS NOT NULL
      AND TRIM(symbol) <> ''
      AND timestamp_ns IS NOT NULL
      AND timestamp_ns > 0
      AND open IS NOT NULL
      AND high IS NOT NULL
      AND low IS NOT NULL
      AND close IS NOT NULL
      AND volume IS NOT NULL
      AND volume >= 0
      AND high >= low
      AND high >= open
      AND high >= close
      AND low <= open
      AND low <= close
),
ranked AS (
    SELECT
        symbol,
        timeframe,
        ts_raw,
        timestamp_ns,
        open,
        high,
        low,
        close,
        volume,
        _source_path,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, timestamp_ns
            ORDER BY _source_path DESC
        ) AS _rn,
        COUNT(*) OVER (
            PARTITION BY symbol, timestamp_ns
        ) AS bronze_dup_count
    FROM typed
)
SELECT
    symbol,
    timeframe,
    to_timestamp_seconds(CAST(timestamp_ns / 1000000000 AS BIGINT)) AS bar_time,
    timestamp_ns,
    open,
    high,
    low,
    close,
    volume,
    bronze_dup_count,
    _source_path AS source_path
FROM ranked
WHERE _rn = 1
"#;

#[async_trait]
impl RustModel for StgOhlcv1m {
    fn name(&self) -> &str {
        STG_OHLCV_1M_NAME
    }

    fn output_schema(&self) -> rbt::arrow::datatypes::SchemaRef {
        output_schema()
    }

    async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput> {
        // Prefer stream materialize for full-universe honesty.
        let df = ctx
            .session
            .sql(STAGING_SQL)
            .await
            .context("E_DEMO: stg_ohlcv_1m bronze transform SQL")?;
        let stream = df
            .execute_stream()
            .await
            .context("E_DEMO: stg_ohlcv_1m execute_stream")?;
        Ok(RustModelOutput::Stream(stream))
    }
}

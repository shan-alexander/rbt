//! Staging from **recommended Parquet hive bronze** (no Arrow spill).
//!
//! Landings:
//!   `lake/bronze/lz_stock_bars_parquet/symbol=*/timeframe=1m/data.parquet`
//!   produce with: `land-parquet-bronze`
//!
//! Registration: DataFusion **listing** (`source_format: parquet`, no injects).
//! Grain: `(symbol, timestamp_ns)` unique — same as Arrow `StgOhlcv1m`.
//!
//! DAG name stays [`STG_OHLCV_1M_NAME`] so the same `tf_indicators_1m` / `obt_stocks_1m`
//! nodes can sit on top for fair full-DAG wall clocks vs Arrow IPC + spill.

use super::stg_ohlcv_1m::STG_OHLCV_1M_NAME;
use anyhow::{Context, Result};
use async_trait::async_trait;
use rbt::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use rbt::{
    ModelTests, RustModel, RustModelContext, RustModelOutput, SourceFormat, StagingFrontmatter,
};
use std::sync::Arc;

/// Host staging node: Parquet hive bronze → grain-unique 1m silver.
pub struct StgOhlcvBronzeparquet1m;

/// Listing-friendly scan contract (no path inject → no spill).
pub fn scan_frontmatter() -> StagingFrontmatter {
    StagingFrontmatter {
        description: Some(
            "Deduped 1m OHLCV from Parquet hive bronze (recommended landing; DF listing)."
                .into(),
        ),
        context: Some(
            "Equal scope to Arrow stg when lander is 1m-only for the same symbols. \
             No inject_source_path / require_partitions so registration stays Path A listing."
                .into(),
        ),
        source_format: Some(SourceFormat::Parquet),
        scan_path: Some("$lake/bronze/lz_stock_bars_parquet".into()),
        source_name: Some("bronze".into()),
        source_table: Some("ohlcv_parquet_1m".into()),
        grain: Some(vec!["symbol".into(), "timestamp_ns".into()]),
        unique_key: Some(vec!["symbol".into(), "timestamp_ns".into()]),
        materialization: Some("table".into()),
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
    // Same logical columns as Arrow stg (dup/source cols nullable for schema align with tf).
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

/// Geometry + grain dedupe. No `_source_path` (listing path); first row wins.
const STAGING_SQL: &str = r#"
WITH raw AS (
    SELECT
        symbol,
        timeframe,
        timestamp_ns,
        open,
        high,
        low,
        close,
        volume
    FROM bronze.ohlcv_parquet_1m
),
typed AS (
    SELECT
        symbol,
        COALESCE(timeframe, '1m') AS timeframe,
        CAST(timestamp_ns AS BIGINT) AS timestamp_ns,
        CAST(open AS DOUBLE) AS open,
        CAST(high AS DOUBLE) AS high,
        CAST(low AS DOUBLE) AS low,
        CAST(close AS DOUBLE) AS close,
        CAST(volume AS BIGINT) AS volume
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
        *,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, timestamp_ns
            ORDER BY timestamp_ns
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
    CAST(NULL AS VARCHAR) AS source_path
FROM ranked
WHERE _rn = 1
"#;

#[async_trait]
impl RustModel for StgOhlcvBronzeparquet1m {
    fn name(&self) -> &str {
        // Same registry/DAG name as Arrow staging so tf/obt wiring is identical.
        STG_OHLCV_1M_NAME
    }

    fn output_schema(&self) -> rbt::arrow::datatypes::SchemaRef {
        output_schema()
    }

    async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput> {
        let df = ctx
            .session
            .sql(STAGING_SQL)
            .await
            .context("E_DEMO: stg_ohlcv_bronzeparquet_1m SQL")?;
        let stream = df
            .execute_stream()
            .await
            .context("E_DEMO: stg bronzeparquet stream")?;
        Ok(RustModelOutput::Stream(stream))
    }
}

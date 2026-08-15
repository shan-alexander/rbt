//! Re-read already-materialized `stg_ohlcv_1m.parquet` (no bronze scan).
//!
//! Used by segment timing: measure **tf-only** and **obt-only** without redoing
//! bronze spill + staging transform.

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use rbt::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use rbt::{RustModel, RustModelContext, RustModelOutput};
use std::path::PathBuf;
use std::sync::Arc;

use super::stg_ohlcv_1m::STG_OHLCV_1M_NAME;

/// Design-B-compatible staging name that streams from a lake Parquet file.
pub struct StgLakeReader {
    pub parquet_path: PathBuf,
}

fn schema() -> rbt::arrow::datatypes::SchemaRef {
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

#[async_trait]
impl RustModel for StgLakeReader {
    fn name(&self) -> &str {
        STG_OHLCV_1M_NAME
    }

    fn output_schema(&self) -> rbt::arrow::datatypes::SchemaRef {
        schema()
    }

    async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput> {
        if !self.parquet_path.is_file() {
            bail!(
                "E_DEMO: StgLakeReader missing {}",
                self.parquet_path.display()
            );
        }
        let path = self
            .parquet_path
            .to_str()
            .context("E_DEMO: stg path not utf8")?;
        // Temp name then SQL alias into expected table for downstream refs.
        let tmp = "__rbt_stg_lake_src";
        if ctx.session.table_exist(tmp).unwrap_or(false) {
            let _ = ctx.session.deregister_table(tmp);
        }
        ctx.session
            .register_parquet(tmp, path, rbt::datafusion::prelude::ParquetReadOptions::default())
            .await
            .context("E_DEMO: register stg lake parquet")?;
        let df = ctx
            .session
            .sql(&format!(r#"SELECT * FROM "{tmp}""#))
            .await
            .context("E_DEMO: select stg lake")?;
        let stream = df.execute_stream().await.context("E_DEMO: stg lake stream")?;
        Ok(RustModelOutput::Stream(stream))
    }
}

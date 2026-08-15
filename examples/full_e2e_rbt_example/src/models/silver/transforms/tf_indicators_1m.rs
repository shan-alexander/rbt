//! Design B — pure Rust transform model (`RustModel` wiring only).
//!
//! Domain math lives in [`super::ta_kernels`] (finance-solution).
//!
//! Materializes to:
//!   `lake/rust_models_output/silver/tf/tf_indicators_1m.parquet` (or `.parts/`)
//!
//! Upstream: [`crate::models::silver::staging::stg_ohlcv_1m`].

use super::ta_kernels::{enrich_batches, indicators_schema};
use anyhow::{Context, Result};
use async_trait::async_trait;
use rbt::{
    batches_to_stream, ParallelContract, PartitionInput, PartitionKey, RustModel,
    RustModelContext, RustModelOutput, StagingFrontmatter,
};

/// DAG / registry name — must match Design A and gold OBT ref.
pub const TF_INDICATORS_NAME: &str = "tf_indicators_1m";

/// Host Design B node: silver 1m bars → TA columns (finance-solution via ta_kernels).
pub struct TfIndicators1m;

/// Optional partition contract for multi-value WorkUnit fan-out (scoped_replace).
pub fn partition_frontmatter() -> StagingFrontmatter {
    StagingFrontmatter {
        description: Some(
            "Design B: 1m OHLCV + finance-solution SMA/EMA/RSI (partition-local by symbol)."
                .into(),
        ),
        partition_by: Some(vec!["symbol".into()]),
        part_key: Some(vec!["symbol".into()]),
        materialization: Some("scoped_replace".into()),
        parallel_safe: Some(true),
        grain: Some(vec!["symbol".into(), "timestamp_ns".into()]),
        sort_within_part: Some(vec!["timestamp_ns".into()]),
        ..Default::default()
    }
}

#[async_trait]
impl RustModel for TfIndicators1m {
    fn name(&self) -> &str {
        TF_INDICATORS_NAME
    }

    fn output_schema(&self) -> rbt::arrow::datatypes::SchemaRef {
        indicators_schema()
    }

    fn parallel_contract(&self) -> ParallelContract {
        ParallelContract::PartitionLocal {
            keys: vec!["symbol".into()],
        }
    }

    async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput> {
        let df = ctx
            .session
            .sql(r#"SELECT * FROM "stg_ohlcv_1m" ORDER BY symbol, timestamp_ns"#)
            .await
            .context("E_DEMO: SQL stg_ohlcv_1m")?;
        let batches = df.collect().await.context("E_DEMO: collect stg")?;
        let out = enrich_batches(&batches)?;
        Ok(RustModelOutput::Stream(batches_to_stream(
            self.output_schema(),
            out,
        )))
    }

    async fn execute_partition(
        &self,
        ctx: &RustModelContext<'_>,
        part: &PartitionKey,
        input: PartitionInput,
    ) -> Result<RustModelOutput> {
        let symbol = part
            .get("symbol")
            .map(|s| s.to_string())
            .unwrap_or_else(|| "?".into());

        let batches = if let Some(mut stream) = input.into_stream() {
            use futures::StreamExt;
            let mut all = Vec::new();
            while let Some(item) = stream.next().await {
                all.push(item.map_err(|e| anyhow::anyhow!("E_DEMO: partition stream: {e}"))?);
            }
            all
        } else {
            let esc = symbol.replace('\'', "''");
            let sql = format!(
                r#"SELECT * FROM "stg_ohlcv_1m" WHERE symbol = '{esc}' ORDER BY timestamp_ns"#
            );
            let df = ctx
                .session
                .sql(&sql)
                .await
                .with_context(|| format!("E_DEMO: partition SQL for symbol={symbol}"))?;
            df.collect().await.context("E_DEMO: partition collect")?
        };

        if batches.is_empty() {
            return Ok(RustModelOutput::Batches(vec![]));
        }

        let out = enrich_batches(&batches)?;
        tracing::info!(
            symbol = %symbol,
            rows = out.iter().map(|b| b.num_rows()).sum::<usize>(),
            "tf_indicators_1m partition done (finance-solution SMA/EMA/RSI)"
        );
        Ok(RustModelOutput::Stream(batches_to_stream(
            self.output_schema(),
            out,
        )))
    }
}

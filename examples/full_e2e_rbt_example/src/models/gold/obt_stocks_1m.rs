//! Design B — pure Rust gold OBT (identity stream of `tf_indicators_1m`).
//!
//! Materializes to:
//!   `lake/rust_models_output/gold/obt_stocks_1m.parquet`
//!
//! Note: `materialization: alias` is a SQL/file zero-copy path. Design B uses a
//! thin Rust identity node (table) so the approach has **no `.sql` files**.

use anyhow::{Context, Result};
use async_trait::async_trait;
use rbt::{RustModel, RustModelContext, RustModelOutput};

use crate::models::silver::transforms::ta_kernels::indicators_schema;

/// DAG / registry name — matches Design A `obt_stocks_1m`.
pub const OBT_STOCKS_1M_NAME: &str = "obt_stocks_1m";

/// Gold OBT: pass-through of Design B indicators for consumers.
pub struct ObtStocks1m;

#[async_trait]
impl RustModel for ObtStocks1m {
    fn name(&self) -> &str {
        OBT_STOCKS_1M_NAME
    }

    fn output_schema(&self) -> rbt::arrow::datatypes::SchemaRef {
        indicators_schema()
    }

    async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput> {
        let df = ctx
            .session
            .sql(r#"SELECT * FROM "tf_indicators_1m""#)
            .await
            .context("E_DEMO: obt_stocks_1m ref tf_indicators_1m")?;
        let stream = df
            .execute_stream()
            .await
            .context("E_DEMO: obt_stocks_1m execute_stream")?;
        Ok(RustModelOutput::Stream(stream))
    }
}

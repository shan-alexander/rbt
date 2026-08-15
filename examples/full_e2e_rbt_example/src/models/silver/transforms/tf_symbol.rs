//! Design D — one Rust transform model **per symbol** (named node, not WorkUnit).
//!
//! Contrasts with Design C (one `tf_indicators_1m` node + RBT-C partition fan-out):
//! here the DAG itself has `tf_indicators_1m_NVDA`, `tf_indicators_1m_AMD`, …
//! which can run as **L1 concurrent models** in the same topo tier after staging.
//! Gold OBT then `UNION ALL`s every per-symbol table.

use super::ta_kernels::{enrich_batches, indicators_schema};
use anyhow::{Context, Result};
use async_trait::async_trait;
use rbt::{
    batches_to_stream, ParallelContract, RustModel, RustModelContext, RustModelOutput,
};

/// DAG name for a per-symbol indicators model (ASCII ticker).
pub fn tf_symbol_model_name(symbol: &str) -> String {
    format!("tf_indicators_1m_{symbol}")
}

/// Host model bound to a single ticker; filters silver staging then applies TA.
pub struct TfSymbolIndicators {
    /// Full DAG / registry name, e.g. `tf_indicators_1m_NVDA`.
    pub model_name: String,
    /// Equity ticker to keep from `stg_ohlcv_1m`.
    pub symbol: String,
}

impl TfSymbolIndicators {
    pub fn new(symbol: impl Into<String>) -> Self {
        let symbol = symbol.into();
        Self {
            model_name: tf_symbol_model_name(&symbol),
            symbol,
        }
    }
}

#[async_trait]
impl RustModel for TfSymbolIndicators {
    fn name(&self) -> &str {
        &self.model_name
    }

    fn output_schema(&self) -> rbt::arrow::datatypes::SchemaRef {
        indicators_schema()
    }

    /// Whole-model is one symbol — partition-local by construction (L1 safe).
    fn parallel_contract(&self) -> ParallelContract {
        ParallelContract::PartitionLocal {
            keys: vec!["symbol".into()],
        }
    }

    async fn execute(&self, ctx: &RustModelContext<'_>) -> Result<RustModelOutput> {
        let esc = self.symbol.replace('\'', "''");
        let sql = format!(
            r#"SELECT * FROM "stg_ohlcv_1m" WHERE symbol = '{esc}' ORDER BY timestamp_ns"#
        );
        let df = ctx
            .session
            .sql(&sql)
            .await
            .with_context(|| format!("E_DEMO: tf_symbol filter symbol={}", self.symbol))?;
        let batches = df
            .collect()
            .await
            .with_context(|| format!("E_DEMO: collect symbol={}", self.symbol))?;
        if batches.is_empty() {
            return Ok(RustModelOutput::Batches(vec![]));
        }
        let out = enrich_batches(&batches)?;
        tracing::info!(
            model = %self.model_name,
            symbol = %self.symbol,
            rows = out.iter().map(|b| b.num_rows()).sum::<usize>(),
            "per-symbol tf partition done"
        );
        Ok(RustModelOutput::Stream(batches_to_stream(
            self.output_schema(),
            out,
        )))
    }
}

/// Build gold SQL that unions every per-symbol transform via `ref()`.
pub fn obt_union_sql(tf_model_names: &[String]) -> String {
    let mut body = String::from(
        r#"---
description: Design D gold OBT — UNION ALL of per-symbol tf_* models
materialization: table
tags: [mart, obt, design_d, union]
---
"#,
    );
    for (i, name) in tf_model_names.iter().enumerate() {
        if i > 0 {
            body.push_str("\nUNION ALL\n");
        }
        body.push_str(&format!("SELECT * FROM {{{{ ref('{name}') }}}}"));
    }
    body.push('\n');
    body
}

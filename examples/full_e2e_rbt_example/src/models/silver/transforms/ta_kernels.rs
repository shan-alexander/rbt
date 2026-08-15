//! Domain TA kernels for Design B (`finance-solution`).
//!
//! Kept separate from the [`super::tf_indicators_1m`] `RustModel` wiring so readers
//! can see: **host owns market math**, rbt owns DAG + materialize.

use anyhow::{Context, Result};
use finance_solution::stocks::ta::{ema, rsi, sma, RsiParams};
use rbt::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringArray, StringViewArray, TimestampSecondArray,
};
use rbt::arrow::compute::cast;
use rbt::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use rbt::arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Output schema for `tf_indicators_1m` (OHLCV pass-through + TA columns).
pub fn indicators_schema() -> rbt::arrow::datatypes::SchemaRef {
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
        Field::new("sma_20", DataType::Float64, true),
        Field::new("ema_20", DataType::Float64, true),
        Field::new("rsi_14", DataType::Float64, true),
    ]))
}

/// Apply finance-solution TA **per symbol** (input may be multi-symbol on mega path).
pub fn enrich_batches(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    let schema_in = batches
        .first()
        .map(|b| b.schema())
        .ok_or_else(|| anyhow::anyhow!("E_DEMO: no batches"))?;

    let mut symbols: Vec<String> = Vec::new();
    let mut timeframes: Vec<Option<String>> = Vec::new();
    let mut bar_times: Vec<Option<i64>> = Vec::new();
    let mut ts: Vec<i64> = Vec::new();
    let mut opens: Vec<Option<f64>> = Vec::new();
    let mut highs: Vec<Option<f64>> = Vec::new();
    let mut lows: Vec<Option<f64>> = Vec::new();
    let mut closes: Vec<f64> = Vec::new();
    let mut volumes: Vec<Option<i64>> = Vec::new();

    for batch in batches {
        let sym = utf8_values(batch, "symbol")?;
        let tf = utf8_values(batch, "timeframe").unwrap_or_default();
        let bt = ts_sec_values(batch, "bar_time");
        let tsn = i64_values(batch, "timestamp_ns")?;
        let o = f64_values(batch, "open")?;
        let h = f64_values(batch, "high")?;
        let l = f64_values(batch, "low")?;
        let c = f64_values(batch, "close")?;
        let v = i64_values(batch, "volume").unwrap_or_default();

        for i in 0..batch.num_rows() {
            symbols.push(sym[i].clone());
            timeframes.push(tf.get(i).cloned());
            bar_times.push(bt.get(i).copied().flatten());
            ts.push(tsn[i].unwrap_or(0));
            opens.push(o[i]);
            highs.push(h[i]);
            lows.push(l[i]);
            closes.push(c[i].unwrap_or(f64::NAN));
            volumes.push(v.get(i).copied().flatten());
        }
    }

    let mut by_sym: std::collections::BTreeMap<String, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (i, s) in symbols.iter().enumerate() {
        by_sym.entry(s.clone()).or_default().push(i);
    }

    let mut sma20 = vec![None; closes.len()];
    let mut ema20 = vec![None; closes.len()];
    let mut rsi14 = vec![None; closes.len()];

    for (_sym, idxs) in &by_sym {
        let series: Vec<f64> = idxs.iter().map(|&i| closes[i]).collect();
        let s = sma(&series, 20).map_err(|e| anyhow::anyhow!("E_DEMO: sma: {e}"))?;
        let e = ema(&series, 20).map_err(|e| anyhow::anyhow!("E_DEMO: ema: {e}"))?;
        let r = rsi(&series, RsiParams::period_14())
            .map_err(|e| anyhow::anyhow!("E_DEMO: rsi: {e}"))?;
        for (j, &i) in idxs.iter().enumerate() {
            sma20[i] = s[j];
            ema20[i] = e[j];
            rsi14[i] = r.rsi.get(j).copied().flatten();
        }
    }

    let out_schema = indicators_schema();
    let batch = RecordBatch::try_new(
        out_schema,
        vec![
            Arc::new(StringArray::from(symbols)) as ArrayRef,
            Arc::new(StringArray::from(
                timeframes
                    .into_iter()
                    .map(|x| x.unwrap_or_else(|| "1m".into()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(TimestampSecondArray::from(bar_times)) as ArrayRef,
            Arc::new(Int64Array::from(ts)) as ArrayRef,
            Arc::new(Float64Array::from(opens)) as ArrayRef,
            Arc::new(Float64Array::from(highs)) as ArrayRef,
            Arc::new(Float64Array::from(lows)) as ArrayRef,
            Arc::new(Float64Array::from(
                closes.iter().map(|&c| Some(c)).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(volumes)) as ArrayRef,
            Arc::new(Float64Array::from(sma20)) as ArrayRef,
            Arc::new(Float64Array::from(ema20)) as ArrayRef,
            Arc::new(Float64Array::from(rsi14)) as ArrayRef,
        ],
    )
    .context("E_DEMO: build indicators batch")?;

    let _ = schema_in;
    Ok(vec![batch])
}

fn utf8_values(batch: &RecordBatch, name: &str) -> Result<Vec<String>> {
    let i = batch
        .schema()
        .index_of(name)
        .map_err(|_| anyhow::anyhow!("E_DEMO: missing column {name}"))?;
    let col = batch.column(i);
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return Ok((0..a.len()).map(|i| a.value(i).to_string()).collect());
    }
    if let Some(a) = col.as_any().downcast_ref::<StringViewArray>() {
        return Ok((0..a.len()).map(|i| a.value(i).to_string()).collect());
    }
    let casted = cast(col.as_ref(), &DataType::Utf8)
        .map_err(|e| anyhow::anyhow!("E_DEMO: cast {name} to utf8: {e}"))?;
    let a = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("E_DEMO: column {name} not utf8 after cast"))?;
    Ok((0..a.len()).map(|i| a.value(i).to_string()).collect())
}

fn f64_values(batch: &RecordBatch, name: &str) -> Result<Vec<Option<f64>>> {
    let i = batch
        .schema()
        .index_of(name)
        .map_err(|_| anyhow::anyhow!("E_DEMO: missing column {name}"))?;
    let col = batch.column(i);
    let casted = if col.data_type() == &DataType::Float64 {
        col.clone()
    } else {
        cast(col.as_ref(), &DataType::Float64)
            .map_err(|e| anyhow::anyhow!("E_DEMO: cast {name} to f64: {e}"))?
    };
    let a = casted
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("E_DEMO: column {name} not float64"))?;
    Ok((0..a.len())
        .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
        .collect())
}

fn i64_values(batch: &RecordBatch, name: &str) -> Result<Vec<Option<i64>>> {
    let i = batch
        .schema()
        .index_of(name)
        .map_err(|_| anyhow::anyhow!("E_DEMO: missing column {name}"))?;
    let col = batch.column(i);
    let casted = if col.data_type() == &DataType::Int64 {
        col.clone()
    } else {
        cast(col.as_ref(), &DataType::Int64)
            .map_err(|e| anyhow::anyhow!("E_DEMO: cast {name} to i64: {e}"))?
    };
    let a = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("E_DEMO: column {name} not int64"))?;
    Ok((0..a.len())
        .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
        .collect())
}

fn ts_sec_values(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let Ok(i) = batch.schema().index_of(name) else {
        return vec![None; batch.num_rows()];
    };
    let col = batch.column(i);
    if let Some(a) = col.as_any().downcast_ref::<TimestampSecondArray>() {
        return (0..a.len())
            .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
            .collect();
    }
    i64_values(batch, name).unwrap_or_else(|_| vec![None; batch.num_rows()])
}

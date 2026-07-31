//! Built-in DataFusion scalar UDFs (ADR-003 Design A).
//!
//! Registered on every [`TransformationEngine`] with the `rbt_` prefix so SQL models
//! can call them without a project extension crate. Project-specific UDFs can register
//! additional names the same way via [`register_scalar_udf`].

use anyhow::{Context, Result};
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionImplementation, Volatility, create_udf,
};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Names of built-in UDFs (for docs / tests).
pub const BUILTIN_UDF_NAMES: &[&str] = &[
    "rbt_upper",
    "rbt_lower",
    "rbt_nullif_empty",
    "rbt_trim",
];

/// Register all rbt built-in scalar UDFs into `ctx`.
pub fn register_builtin_udfs(ctx: &SessionContext) -> Result<()> {
    for udf in [
        make_rbt_upper(),
        make_rbt_lower(),
        make_rbt_nullif_empty(),
        make_rbt_trim(),
    ] {
        ctx.register_udf(udf);
    }
    Ok(())
}

/// Register an arbitrary [`datafusion::logical_expr::ScalarUDF`].
pub fn register_scalar_udf(
    ctx: &SessionContext,
    udf: datafusion::logical_expr::ScalarUDF,
) -> Result<()> {
    ctx.register_udf(udf);
    Ok(())
}

fn utf8_unary(
    name: &str,
    map: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
) -> datafusion::logical_expr::ScalarUDF {
    let fun: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        let arr = match &args[0] {
            ColumnarValue::Array(a) => a.clone(),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };
        let out = map_utf8_array(&arr, &map)?;
        Ok(ColumnarValue::Array(out))
    });
    create_udf(
        name,
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        fun,
    )
}

fn map_utf8_array(
    arr: &ArrayRef,
    map: &impl Fn(&str) -> Option<String>,
) -> datafusion::error::Result<ArrayRef> {
    // Accept Utf8 / Utf8View via string values
    let mut builder = arrow::array::StringBuilder::with_capacity(arr.len(), arr.len() * 8);
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = scalar_utf8_at(arr, i)?;
        match map(&s) {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn scalar_utf8_at(arr: &ArrayRef, i: usize) -> datafusion::error::Result<String> {
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(a.value(i).to_string());
    }
    if let Some(a) = arr
        .as_any()
        .downcast_ref::<arrow::array::StringViewArray>()
    {
        return Ok(a.value(i).to_string());
    }
    if let Some(a) = arr
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
    {
        return Ok(a.value(i).to_string());
    }
    // Fallback via ScalarValue
    let sv = ScalarValue::try_from_array(arr, i)?;
    Ok(sv.to_string())
}

fn make_rbt_upper() -> datafusion::logical_expr::ScalarUDF {
    utf8_unary("rbt_upper", |s| Some(s.to_uppercase()))
}

fn make_rbt_lower() -> datafusion::logical_expr::ScalarUDF {
    utf8_unary("rbt_lower", |s| Some(s.to_lowercase()))
}

fn make_rbt_trim() -> datafusion::logical_expr::ScalarUDF {
    utf8_unary("rbt_trim", |s| Some(s.trim().to_string()))
}

fn make_rbt_nullif_empty() -> datafusion::logical_expr::ScalarUDF {
    utf8_unary("rbt_nullif_empty", |s| {
        if s.is_empty() {
            None
        } else {
            Some(s.to_string())
        }
    })
}

/// Ensure builtins are present (idempotent for tests that rebuild contexts).
pub fn ensure_builtins(ctx: &SessionContext) -> Result<()> {
    register_builtin_udfs(ctx).context("E_RBT_UDF: register builtins")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn builtin_udfs_work_in_sql() -> Result<()> {
        let ctx = SessionContext::new();
        register_builtin_udfs(&ctx)?;
        let df = ctx
            .sql(
                "SELECT rbt_upper('abc') AS u, rbt_lower('AbC') AS l, \
                 rbt_trim('  x  ') AS t, rbt_nullif_empty('') IS NULL AS n",
            )
            .await?;
        let batches = df.collect().await?;
        assert_eq!(batches[0].num_rows(), 1);
        let u = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(u, "ABC");
        let l = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(l, "abc");
        Ok(())
    }
}

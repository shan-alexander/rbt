//! Built-in DataFusion scalar UDFs and host pack surface (ADR-003 Design A, RBT-L1.5).
//!
//! Built-ins register on every [`crate::TransformationEngine`] with the `rbt_` prefix.
//! Hosts add domain kernels via:
//!
//! - [`crate::RbtEngineBuilder::with_udfs`] / [`crate::RbtEngineBuilder::with_udf_pack`]
//! - [`crate::TransformationEngine::register_udfs`] / [`register_udf_pack`]
//! - [`register_scalar_udf`] for a single function
//!
//! # NULL policy
//!
//! Prefer Arrow nulls for “undefined”. Empty `Utf8` is **not** null unless your UDF
//! defines it (`rbt_nullif_empty` maps `""` → NULL). Host packs should document their
//! null semantics; SQL authors rely on that contract.
//!
//! # Ordering / windows
//!
//! Ordered domain logic belongs in SQL around the UDF:
//! `… OVER (PARTITION BY symbol ORDER BY ts)` — rbt does not invent a second ordering model.

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

/// Host UDF pack (Strategy / plugin). Implement once; register via builder or live engine.
///
/// # Example
///
/// ```rust,no_run
/// use rbt::engine::udf::{register_scalar_udf, UdfPack};
/// use rbt::datafusion::prelude::SessionContext;
/// use anyhow::Result;
///
/// struct HostPack;
/// impl UdfPack for HostPack {
///     fn register(&self, ctx: &SessionContext) -> Result<()> {
///         // register_scalar_udf(ctx, my_udf)?;
///         let _ = ctx;
///         Ok(())
///     }
/// }
/// ```
pub trait UdfPack: Send + Sync {
    /// Register all functions in this pack into `ctx`.
    fn register(&self, ctx: &SessionContext) -> Result<()>;
}

/// Register a [`UdfPack`] on a session (borrowed pack).
pub fn register_udf_pack(ctx: &SessionContext, pack: &dyn UdfPack) -> Result<()> {
    pack.register(ctx)
}

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
    use crate::engine::RbtEngineBuilder;

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

    /// Host pack: double a utf8 string (demo only).
    struct EchoPack;

    impl UdfPack for EchoPack {
        fn register(&self, ctx: &SessionContext) -> Result<()> {
            let udf = utf8_unary("host_echo", |s| Some(format!("echo:{s}")));
            register_scalar_udf(ctx, udf)
        }
    }

    #[tokio::test]
    async fn builder_with_udf_pack_registers_host_and_builtins() -> Result<()> {
        let engine = RbtEngineBuilder::new()
            .with_udf_pack(EchoPack)
            .build()
            .await?;
        let df = engine
            .ctx
            .sql("SELECT host_echo('x') AS h, rbt_upper('y') AS u")
            .await?;
        let batches = df.collect().await?;
        let h = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(h, "echo:x");
        let u = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(u, "Y");
        Ok(())
    }

    #[tokio::test]
    async fn live_register_udfs_hook() -> Result<()> {
        let engine = crate::TransformationEngine::new();
        engine.register_udfs(|ctx| {
            let udf = utf8_unary("host_live", |s| Some(s.to_string()));
            register_scalar_udf(ctx, udf)
        })?;
        let df = engine.ctx.sql("SELECT host_live('ok') AS v").await?;
        let batches = df.collect().await?;
        let v = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(v, "ok");
        Ok(())
    }
}

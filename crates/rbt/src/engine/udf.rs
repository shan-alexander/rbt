//! Built-in DataFusion scalar UDFs and host pack surface (ADR-003 Design A, RBT-L1.5).
//!
//! Built-ins register on every [`crate::TransformationEngine`]. String helpers use the
//! `rbt_` prefix; surrogate-key helpers also register ergonomic names (`sk`,
//! `surrogate_key`, `sk_unknown`) plus `rbt_*` aliases (ADR-009).
//!
//! Hosts add domain kernels via:
//!
//! - [`crate::RbtEngineBuilder::with_udfs`] / [`crate::RbtEngineBuilder::with_udf_pack`]
//! - [`crate::TransformationEngine::register_udfs`] / [`register_udf_pack`]
//! - [`register_scalar_udf`] for a single function
//!
//! # Surrogate keys
//!
//! | SQL | Notes |
//! |-----|--------|
//! | `sk(col, …)` | Default `balanced` (blake3_128 binary) |
//! | `surrogate_key(algo, col, …)` | Explicit algo (`balanced`/`fast64`/`safe256`/`compat_md5`) |
//! | `sk_unknown()` / `sk_unknown(algo)` | All-zero Unknown sentinel |
//!
//! Bare `sk()` / `surrogate_key('algo')` expand from frontmatter `grain` at compile time.
//! Algo `integer` (MIISK) is **materialize-stamp only** — not a SQL UDF.
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
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, plan_err, Result as DfResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarFunctionImplementation,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility, create_udf,
};
use datafusion::prelude::SessionContext;
use std::any::Any;
use std::sync::Arc;

use crate::engine::surrogate_key::{
    hash_batch_columns, unknown_array, SkAlgo, SkEncoding, SK_NULL_TOKEN,
};

/// Names of built-in UDFs (for docs / tests).
pub const BUILTIN_UDF_NAMES: &[&str] = &[
    "rbt_upper",
    "rbt_lower",
    "rbt_nullif_empty",
    "rbt_trim",
    "sk",
    "surrogate_key",
    "sk_unknown",
    "rbt_sk",
    "rbt_surrogate_key",
    "rbt_sk_unknown",
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
    // ADR-009 surrogate keys (ergonomic names + rbt_ aliases via ScalarUDFImpl::aliases)
    ctx.register_udf(ScalarUDF::from(SkUdf::new()));
    ctx.register_udf(ScalarUDF::from(SurrogateKeyUdf::new()));
    ctx.register_udf(ScalarUDF::from(SkUnknownUdf::new()));
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

// ─── Surrogate key UDFs (ADR-009) ───────────────────────────────────────────

fn sk_arrow_type(algo: SkAlgo) -> DataType {
    algo.arrow_type(SkEncoding::Binary)
}

fn invoke_sk_hash(algo: SkAlgo, args: &[ColumnarValue]) -> DfResult<ColumnarValue> {
    if args.is_empty() {
        return exec_err!(
            "E_RBT_SK: sk()/surrogate_key requires ≥1 grain column \
             (declare frontmatter grain for bare sk() expansion, or pass columns)"
        );
    }
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
    let out = hash_batch_columns(&refs, algo, SkEncoding::Binary)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    Ok(ColumnarValue::Array(out))
}

fn parse_algo_scalar(v: &ScalarValue) -> DfResult<SkAlgo> {
    match v {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => {
            let algo = SkAlgo::parse(s)
                .map_err(|e| datafusion::error::DataFusionError::Plan(e.to_string()))?;
            if algo.is_miisk() {
                return plan_err!(
                    "E_RBT_SK: algo 'integer' (MIISK) is materialize-stamp only — set \
                     frontmatter surrogate_key_algo: integer (durable registry). \
                     SQL UDFs support balanced|fast64|safe256|compat_md5."
                );
            }
            Ok(algo)
        }
        ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Utf8View(None) => {
            plan_err!("E_RBT_SK: surrogate_key algo argument must be a non-null string literal")
        }
        other => plan_err!(
            "E_RBT_SK: surrogate_key algo must be Utf8 literal, got {other}"
        ),
    }
}

/// `sk(col, …)` — default balanced (blake3_128 binary). Alias: `rbt_sk`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SkUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl SkUdf {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec!["rbt_sk".into()],
        }
    }
}

impl ScalarUDFImpl for SkUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sk"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> DfResult<DataType> {
        if arg_types.is_empty() {
            return plan_err!(
                "E_RBT_SK: sk() requires grain columns (set frontmatter grain for bare sk() \
                 expansion, or pass columns explicitly)"
            );
        }
        Ok(sk_arrow_type(SkAlgo::Balanced))
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        invoke_sk_hash(SkAlgo::Balanced, &args.args)
    }
}

/// `surrogate_key(algo, col, …)` — explicit algo. Alias: `rbt_surrogate_key`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SurrogateKeyUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl SurrogateKeyUdf {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec!["rbt_surrogate_key".into()],
        }
    }
}

impl ScalarUDFImpl for SurrogateKeyUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "surrogate_key"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> DfResult<DataType> {
        // Prefer return_field_from_args when algo is a literal; fallback Balanced.
        if arg_types.len() < 2 {
            return plan_err!(
                "E_RBT_SK: surrogate_key(algo, col, …) needs algo + ≥1 grain column \
                 (or surrogate_key('algo') with frontmatter grain for compile expansion)"
            );
        }
        Ok(sk_arrow_type(SkAlgo::Balanced))
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
        if args.arg_fields.len() < 2 {
            return plan_err!(
                "E_RBT_SK: surrogate_key(algo, col, …) needs algo + ≥1 grain column"
            );
        }
        let algo = match args.scalar_arguments.first().copied().flatten() {
            Some(sv) => parse_algo_scalar(sv)?,
            None => {
                return plan_err!(
                    "E_RBT_SK: surrogate_key algo must be a string literal \
                     (e.g. surrogate_key('fast64', col1, col2))"
                );
            }
        };
        let nullable = args.arg_fields.iter().skip(1).any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(
            "surrogate_key",
            sk_arrow_type(algo),
            nullable,
        )))
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        if args.args.len() < 2 {
            return exec_err!("E_RBT_SK: surrogate_key(algo, col, …) needs algo + ≥1 column");
        }
        let algo_arr = ColumnarValue::values_to_arrays(&args.args[..1])?;
        let algo_sv = ScalarValue::try_from_array(algo_arr[0].as_ref(), 0)?;
        let algo = parse_algo_scalar(&algo_sv)?;
        invoke_sk_hash(algo, &args.args[1..])
    }
}

/// `sk_unknown()` / `sk_unknown(algo)` — all-zero Unknown member. Alias: `rbt_sk_unknown`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SkUnknownUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl SkUnknownUdf {
    fn new() -> Self {
        Self {
            // 0 args or 1 Utf8 algo
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["rbt_sk_unknown".into(), "surrogate_key_unknown".into()],
        }
    }
}

impl ScalarUDFImpl for SkUnknownUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sk_unknown"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> DfResult<DataType> {
        match arg_types.len() {
            0 => Ok(sk_arrow_type(SkAlgo::Balanced)),
            1 => Ok(sk_arrow_type(SkAlgo::Balanced)), // refined in return_field_from_args
            n => plan_err!("E_RBT_SK: sk_unknown takes 0 or 1 algo arg, got {n}"),
        }
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
        let algo = match args.scalar_arguments.first().copied().flatten() {
            None if args.arg_fields.is_empty() => SkAlgo::Balanced,
            Some(sv) => parse_algo_scalar(sv)?,
            None => {
                return plan_err!("E_RBT_SK: sk_unknown(algo) requires a string literal algo");
            }
        };
        if args.arg_fields.len() > 1 {
            return plan_err!("E_RBT_SK: sk_unknown takes 0 or 1 algo arg");
        }
        Ok(Arc::new(Field::new(
            "sk_unknown",
            sk_arrow_type(algo),
            false,
        )))
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        match arg_types.len() {
            0 => Ok(vec![]),
            1 => Ok(vec![DataType::Utf8]),
            n => plan_err!("E_RBT_SK: sk_unknown takes 0 or 1 arg, got {n}"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let algo = if args.args.is_empty() {
            SkAlgo::Balanced
        } else {
            let arr = ColumnarValue::values_to_arrays(&args.args)?;
            let sv = ScalarValue::try_from_array(arr[0].as_ref(), 0)?;
            parse_algo_scalar(&sv)?
        };
        let n = args.number_rows.max(1);
        let out = unknown_array(algo, SkEncoding::Binary, n)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        Ok(ColumnarValue::Array(out))
    }
}

#[allow(dead_code)] // referenced in docs / stability note
fn _sk_null_token() -> &'static str {
    SK_NULL_TOKEN
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

    #[tokio::test]
    async fn sk_udfs_work_in_sql() -> Result<()> {
        use arrow::array::FixedSizeBinaryArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;

        let ctx = SessionContext::new();
        register_builtin_udfs(&ctx)?;
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("b", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "MSFT"])),
                Arc::new(StringArray::from(vec!["2024-01-01", "2024-01-01"])),
            ],
        )?;
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?),
        )?;

        let df = ctx
            .sql(
                "SELECT sk(a, b) AS sk_def, surrogate_key('fast64', a, b) AS sk_fast, \
                 sk_unknown() AS unk FROM t",
            )
            .await?;
        let batches = df.collect().await?;
        assert_eq!(batches[0].num_rows(), 2);
        assert!(matches!(
            batches[0].schema().field(0).data_type(),
            DataType::FixedSizeBinary(16)
        ));
        assert_eq!(batches[0].schema().field(1).data_type(), &DataType::Int64);
        let unk = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(unk.value(0), &[0u8; 16]);
        // Deterministic: same grain → same sk
        let df2 = ctx.sql("SELECT sk(a, b) AS sk FROM t").await?;
        let b2 = df2.collect().await?;
        let s1 = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0);
        let s2 = b2[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0);
        assert_eq!(s1, s2);
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

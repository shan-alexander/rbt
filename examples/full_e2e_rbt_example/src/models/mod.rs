//! Host-owned medallion models for the full e2e showcase.
//!
//! | Tree | Approach | Language |
//! |------|----------|----------|
//! | [`sql_models_approach`](sql_models_approach/) | Design A | `.sql` files (CLI + host) |
//! | [`silver`] / [`gold`] | Design B | pure `RustModel` (no `.sql`) |
//!
//! Design B materializes under `lake/rust_models_output/`.
//! Design A materializes under `lake/sql_models_output/`.
//! Shared bronze: `lake/bronze/`.

pub mod gold;
pub mod silver;

pub use gold::obt_stocks_1m::{ObtStocks1m, OBT_STOCKS_1M_NAME};
pub use silver::staging::stg_ohlcv_1m::{StgOhlcv1m, STG_OHLCV_1M_NAME};
pub use silver::staging::stg_ohlcv_bronzeparquet_1m::StgOhlcvBronzeparquet1m;
pub use silver::transforms::tf_indicators_1m::{TfIndicators1m, TF_INDICATORS_NAME};
pub use silver::transforms::tf_symbol::{
    obt_union_sql, tf_symbol_model_name, TfSymbolIndicators,
};

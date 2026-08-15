---
# =============================================================================
# Gold OBT — one wide analytics table (no separate dim/fact in this ideal demo)
# =============================================================================
# WHY OBT (not star) here
#   For bar-level research, a single denormalized OBT is often what notebooks and
#   pure-Rust consumers want. Star dims/facts remain valid for warehouse-style
#   lakes; this showcase optimizes for the quant medallion path:
#
#     bronze → stg_ohlcv_1m → tf_indicators_1m → obt_stocks_1m
#
# materialization: alias
#   When tf_indicators_1m is a pure identity pass-through or already the product
#   grain, alias avoids rewriting multi-GB Parquet. If your transform is already
#   the final schema, obt can be zero-copy.
#
# Design A output: lake/sql_models_output/gold/obt_stocks_1m.*
# Design B twin (pure Rust): src/models/gold/obt_stocks_1m.rs
#   → lake/rust_models_output/gold/
# Both ref the same logical name tf_indicators_1m (SQL vs Rust implementation).
# =============================================================================
description: Design A gold OBT of 1m bars + indicators.
context: >
  Wide table for consumers: OHLCV + TA columns. Prefer alias when identical to
  upstream transform; use table if you add gold-only columns later.

materialization: alias
alias_of: tf_indicators_1m
tags: [mart, obt, equities, timeframe_1m, gold, design_a]

# Identity SELECT — alias materialization will hardlink/symlink without re-encode.
# If you need gold-only columns, switch materialization to table and expand SQL.
---
SELECT * FROM {{ ref('tf_indicators_1m') }}

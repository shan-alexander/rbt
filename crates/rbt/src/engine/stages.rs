//! Named pipeline stages for DAG execution (systems design — thin façade over a single entry).
//!
//! Public product surface remains [`crate::TransformationEngine::execute_dag_with_scope`].
//! Internally (and for library hosts), work is staged so skip / bronze / tiers / receipts
//! are testable and re-usable without a god method body owning every concern.
//!
//! ```text
//! execute_dag_with_scope
//!   ├─ Stage 1  Fingerprint + PlanSkip   (ops::plan_skip)
//!   ├─ Stage 2  RegisterBronze           (register_bronze_sources_for_dag_scoped)
//!   ├─ Stage 3  ExecuteTiers             (topo tiers → materialize_one)
//!   └─ Stage 4  WriteReceipt             (RunReceipt)
//! ```
//!
//! Future object-store / SQLite control-plane work plugs into stages, not a parallel engine.

use crate::core::dag::ModelDag;
use crate::core::project::RbtProjectConfig;
use crate::core::run_scope::RunScope;
use crate::ops::{plan_skip, SkipPlan};
use anyhow::Result;
use std::path::Path;

/// Logical stages of a scoped DAG run (documentation + host hooks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStage {
    /// Compute bronze fingerprint and compare to latest receipt.
    PlanSkip,
    /// Register bronze sources into the DataFusion session.
    RegisterBronze,
    /// Topological tiers: SQL → materialize (table / scoped_replace / upsert / …).
    ExecuteTiers,
    /// Persist run receipt under `.rbt/runs/`.
    WriteReceipt,
}

impl PipelineStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PlanSkip => "plan_skip",
            Self::RegisterBronze => "register_bronze",
            Self::ExecuteTiers => "execute_tiers",
            Self::WriteReceipt => "write_receipt",
        }
    }
}

/// Stage 1: plan skip (same identity as CLI `--skip-if-match`).
///
/// Hosts that only need the decision call this (or [`crate::ops::plan_skip`]) without
/// running the rest of the pipeline.
pub fn stage_plan_skip(
    dag: &ModelDag,
    project_dir: &Path,
    config: &RbtProjectConfig,
    scope: &RunScope,
) -> Result<SkipPlan> {
    plan_skip(dag, project_dir, config, scope)
}

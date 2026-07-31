# Contributing to rbt

Thanks for helping build **rbt** — a lightweight Rust SQL DAG engine for medallion lakes on the filesystem (and object storage). This document is the contributor contract: product intent, how we work, and what is in vs out of scope while the primary path hardens.

For product vision and MVP order, prefer [thesis.md](./thesis.md). Architecture essays under `docs/` are historical or aspirational; if they disagree with this file or the thesis, **code + thesis win**.

---

## 1. Positioning (do not dilute)

| Pillar | Intent |
|--------|--------|
| **Niche** | Medallion bronze→silver→gold on filesystem / object storage, not warehouse dbt |
| **Stack** | Rust + Arrow + DataFusion + Iceberg + jshift |
| **UX** | dbt-shaped models/refs/tests + validate → explain → preview → run → test |
| **Claim** | Replace Spark+dbt for team-scale lakes (tens of GB → low TBs), not petabyte shuffles |

**What this means for contributions**

- Optimize for **declared models + DAG execution**, not ad-hoc scripts.
- Prefer **in-process** DataFusion over remote warehouse pushdown.
- Bronze edge should stay **selective** (column projection; jshift for JSONL) where it matters.
- Do not expand into “full dbt Cloud,” multi-cluster Spark replacement, or petabyte shuffle machinery without an explicit design discussion.

---

## 2. Product priorities (primary path first)

We deliberately ship **primary function before developer verbs**.

| Priority | Focus | Status intent |
|----------|--------|----------------|
| **P0** | Project load → DAG compile → bronze registration → SQL models → materialize silver/gold on the lake path | Must work **flawlessly** on the sample project and new projects with the same shape |
| **P1** | Correctness of materialization, bronze contracts, layer rules, fail-fast errors on the run path | Harden before new surface area |
| **P2** | Iceberg as system of record (or a deliberate pivot — see §4) | Differentiator; not a fake no-op |
| **P3** | DX verbs: `validate`, `explain`, `preview`, real `test`, `--select` | After P0/P1 are trustworthy |
| **P4** | Measure packs, incremental strategies, WAP, multi-catalog | Thesis proof / later |

**Contributor rule:** If a PR adds CLI verbs, catalogs, or polish while `compile`/`run` on the stockmarket sample is flaky or special-cased, it is out of order. Fix the spine first.

---

## 3. Current honest state (read before coding)

Rough maturity (see also internal audits under `docs/`):

| Area | Notes |
|------|--------|
| Core DAG / project / frontmatter | Strong foundation (`rbt-core`) |
| Bronze → SQL → Parquet demo path | Working vertical slice (sample stockmarket) |
| Iceberg commits / catalog | **Not** system of record yet; stubs or local Parquet only |
| DX verbs | Partial / stub; **acceptable lag** until P0 is solid |
| CI / toolchain / crates.io metadata | Expect gaps; pin toolchain when you touch build |

Do not document unfinished features as shipped. Prefer “supported today” language over marketing.

---

## 4. Is Iceberg the right system of record?

**Short answer:** Iceberg is still the **default thesis target** for silver/gold table truth, but we are **not certain enough to fake it**. Until we complete a real create → append → commit → read round-trip on the official Rust `iceberg` stack, the **working** system of record is **versioned lake files** (today: Parquet under layer `target_path`s). That is intentional honesty, not a permanent product decision.

### 4.1 What we need from “table truth”

For medallion + SQL models on object storage, the engine needs more than “dump Parquet somewhere”:

| Requirement | Why |
|-------------|-----|
| Stable table identity | `ref()` resolves to a table, not a random path string |
| Schema evolution / history | Silver typing and gold dims change over time |
| Atomic publish | Readers should not see half-written runs |
| Partition / snapshot pruning | Team-scale lakes without full scans |
| Ecosystem interoperability | Query from DataFusion, Spark, Trino, DuckDB later without lock-in |
| ACID-ish commits on object storage | Multiple runs, retries, concurrent writers eventually |

Plain hive-partitioned Parquet satisfies **some** of this with conventions. It fails or gets fragile on atomic multi-file publish, snapshot history, and cross-engine table metadata.

### 4.2 Candidate open table formats

| Option | Fit for rbt | Tradeoffs |
|--------|-------------|-----------|
| **Apache Iceberg** | Strong: snapshots, manifests, partition specs, REST/Glue catalogs, official Rust crates + `iceberg-datafusion` | Rust writer/catalog maturity still evolving; must **prove** our path, not assume Spark-level completeness |
| **Delta Lake** | Strong in Databricks ecosystems; transaction log is proven | Rust-native story weaker than Iceberg for this workspace; less aligned with “open lake, multi-engine” pitch |
| **Apache Hudi** | Good for upsert-heavy, streaming MOR | Heavier model than our default append/refresh medallion path; weaker Rust-first story |
| **Hive-style Parquet only** | Fastest MVP; what much of the demo uses today | Becomes junk DAG glue as soon as teams need time travel, concurrent runs, or multi-engine reads |
| **Lance / other columnar** | Interesting for ML/feature stores | Not the SQL analytics / medallion interchange standard |

### 4.3 Why the thesis still points at Iceberg

1. **Object-storage table format** with first-class snapshots — maps to silver/gold “tables” without a warehouse.
2. **Interop** — the lake stays queryable outside rbt (engines that already speak Iceberg).
3. **Official Rust stack** — `iceberg` + `iceberg-datafusion` match the “no JVM for the common path” claim better than bolting on a Java writer.
4. **Pruning story** — manifests + stats are the right long-term partner to DataFusion for team-scale scans.
5. **Industry default** for open lakehouse metadata (alongside Delta); Iceberg is the better open multi-engine bet for a greenfield Rust tool.

### 4.4 Why we are *not* “certain” yet

Uncertainty is **implementation and maturity**, not “Iceberg is a bad idea on paper”:

- We have **not** yet used Iceberg as system of record in this repo (no real snapshot commits on the run path).
- Rust Iceberg **catalog/writer completeness** lags the JVM stack for some backends and edge cases.
- Our demo path is **Parquet files + in-process MemTables** — that can look “done” while table truth remains undefined.
- Faking Iceberg (no-op `OutputFormat::Iceberg`, log-only WAP) is worse than an honest Parquet lake mode.

### 4.5 Decision rule (for contributors)

| Rule | Action |
|------|--------|
| **Default** | Keep Iceberg as the **target** system of record for silver/gold in designs, types, and crate layout (`rbt-catalog`, materializer hooks). |
| **Until proven** | Primary run path may materialize **Parquet** (and register for downstream SQL) without claiming production Iceberg. |
| **Proof gate** | One filesystem (or documented REST) path: create table → write data files → **commit snapshot** → read back via DataFusion/Iceberg. Prefer that over multi-catalog factories. |
| **Pivot gate** | Only reconsider Iceberg after a failed proof (blocked APIs, unusable performance, or a measured better fit). Document the pivot in an ADR; do not silently rebrand Parquet as Iceberg. |
| **Do not** | Implement WAP/branching theater, multi-catalog sprawl, or README claims of “Iceberg-native” before the proof gate. |

**Bottom line:** We are **confident Iceberg is the right *class* of solution** (open table format on object storage). We are **not certain the Rust stack is production-ready for our full wishlist** until the proof gate passes. Ship a flawless Parquet medallion DAG first; earn Iceberg next.

---

## 5. Repository layout

```text
rbt/
├── Cargo.toml                 # workspace
├── thesis.md                  # product north star
├── CONTRIBUTING.md            # this file
├── crates/
│   ├── rbt-core/              # project, frontmatter, refs, DAG
│   ├── rbt-scan/              # multi-format bronze → Arrow
│   ├── rbt-json/              # jshift JSONL extract
│   ├── rbt-engine/            # DataFusion + DAG execution + bronze registration
│   ├── rbt-materializer/      # writers (Parquet/JSONL/CSV; Iceberg TBD)
│   ├── rbt-catalog/           # Iceberg catalog adapters (thin / evolving)
│   ├── rbt-models/            # star-schema metadata (early)
│   ├── rbt-testing/           # Arrow assertion kernels
│   └── rbt-cli/               # CLI binary (currently rbt-cli)
└── examples/
    └── sample_rbt_project_stockmarket/   # golden path
```

**Naming note:** Docs sometimes say `rbt-bronze` / `rbt-sql`. In code, bronze scanning lives in **`rbt-scan`** (+ **`rbt-json`**), and SQL execution lives in **`rbt-engine`**. Prefer code names in new docs.

---

## 6. Development setup

### Prerequisites

- Rust toolchain compatible with the workspace (edition 2021). Prefer installing via [rustup](https://rustup.rs/).
- If the host has mismatched `cargo` / `rustc` versions (e.g. Nix cargo + rustup rustc), fix that **before** filing “build broken” issues. A root `rust-toolchain.toml` is welcome once a known-good version is pinned.

### Build and test

```bash
# From repository root
cargo build -p rbt-cli
cargo test --workspace

# Golden path (paths may use target/debug/rbt-cli until the binary is renamed to `rbt`)
cargo run -p rbt-cli -- compile -p examples/sample_rbt_project_stockmarket
cargo run -p rbt-cli -- run -p examples/sample_rbt_project_stockmarket --format parquet
```

### Sample project conventions

- Config: `rbt_project.yml`
- Layers: `models/staging` (`stg_`), `models/transforms` (`tf_` / `int_`), `models/marts` (`dim_` / `fact_` / …)
- Bronze: staging SQL frontmatter with `source_format` + `scan_path`
- Refs: `{{ ref('model_name') }}`, sources: `{{ source('schema', 'table') }}`

Layer boundary rules are enforced in the DAG: staging does not depend on downstream models; transforms do not depend on marts.

---

## 7. How we accept changes

### Before you open a PR

1. Run the golden path (`compile` + `run` on the stockmarket example) if you touch engine, scan, core, or CLI.
2. Add or update unit tests next to the behavior you change.
3. Keep the PR focused — one concern per PR when possible.
4. Do not expand scope into DX verbs, multi-catalog, or micro-optimizations unless that is the PR’s purpose and P0 is not regressing.

### Code style

- Match existing module style: small crates, clear module docs, `anyhow`/`thiserror` as already used.
- Prefer **fail-fast, structured errors** (model name, path, suggestion) over silent defaults.
- Avoid `todo!()` on the primary run path; gate unfinished formats with an explicit error (`not yet supported`).
- No drive-by refactors or unrelated formatting churn.
- Do not add dependencies lightly; heavy stack (DataFusion, Iceberg, Arrow) is already intentional — extra crates need justification.

### Commits and messages

- Prefer clear, imperative summaries: `engine: register bronze from frontmatter`, not `fix stuff`.
- Reference issues when applicable.

### What we will reject or defer

- Features that only exist in docs (WAP that only logs, Iceberg that writes nothing) presented as complete.
- Petabyte / distributed execution work without an ADR and capacity to maintain it.
- Warehouse-dbt parity checklists that fight the niche (e.g. deep Snowflake macros) without a user need on the lake path.
- Unmeasured performance claims in user-facing docs.

---

## 8. Design documents and ADRs

- **thesis.md** — scope, milestones, defensible claims.
- **docs/** — ADRs and research; may lag code.
- New architectural decisions (especially table-format pivot, catalog backends, incremental strategies) should land as a short ADR under `docs/` with status, decision, and consequences.

If you change the primary data path (bronze registration, materialization contract, project schema), update thesis or an ADR so contributors do not relearn from chat history.

---

## 9. Reporting bugs

Include:

1. OS and `rustc` / `cargo` versions (`rustc -V`, `cargo -V`).
2. Exact command(s).
3. Minimal project layout or a fork of the stockmarket example.
4. Expected vs actual behavior (logs welcome; redact secrets).

Security-sensitive reports: do not open a public issue with exploit details if the project later gains a security policy; for now, open a private channel with maintainers if needed.

---

## 10. License

This project is licensed under **Apache-2.0** (see workspace `Cargo.toml`). Contributions are expected under the same license.

---

## 11. Quick “should I work on X?” guide

| Idea | Do it when… |
|------|-------------|
| Harden bronze frontmatter / scan / jshift | Anytime — core niche |
| Fix DAG layer rules, compile errors, sample project | Anytime — P0 |
| Stream write instead of full `collect()` | When large runs OOM or you are hardening materialization |
| Real Iceberg snapshot commit | After/while P0 is stable; this is the table-truth proof |
| `validate` / `preview` / `--select` / real `test` | After primary run is reliable (P3) |
| Multi-catalog (Glue, Polaris, …) | After one catalog path works |
| SIMD / io_uring / custom allocators | Almost never for v0 — measure first |
| prost diagnostics | After JSON error shape stabilizes |

Welcome aboard. Make the spine boring and correct; then the verbs and Iceberg commits will have something worth standing on.

# Contributing to rbt

Thanks for helping build **rbt** — a lightweight Rust SQL DAG engine for medallion lakes on the filesystem (and object storage). This document is the contributor contract: product intent, how we work, and what is in vs out of scope while the primary path hardens.

For product vision and MVP order, prefer [thesis.md](./thesis.md). Essays under `docs/archive/` are historical; if they disagree with this file or the thesis, **code + thesis win**. Active ADRs live in [docs/adr/](./docs/adr/).

---

## 1. Positioning (do not dilute)

| Pillar | Intent |
|--------|--------|
| **Niche** | Medallion bronze→silver→gold on filesystem / object storage, not warehouse dbt |
| **Stack** | Rust + Arrow + DataFusion + Iceberg (+ jshift for JSONL) |
| **UX** | dbt-shaped models/refs/tests; long-term `validate → explain → preview → run → test` |
| **Claim** | Replace Spark+dbt for team-scale lakes (tens of GB → low TBs), not petabyte shuffles |

**What this means for contributions**

- Optimize for **declared models + DAG execution**, not ad-hoc scripts.
- Prefer **in-process** DataFusion over remote warehouse pushdown.
- Bronze edge should stay **selective** (column projection; jshift for JSONL) where it matters.
- Do not expand into “full dbt Cloud,” multi-cluster Spark replacement, or petabyte shuffle machinery without an explicit design discussion.

---

## 2. Product priorities (primary path first)

We deliberately ship **primary function before developer verbs**.

| Priority | Focus | Status (honest) |
|----------|--------|-----------------|
| **P0** | Project load → DAG compile → bronze registration → SQL models → materialize silver/gold | **Working** — smoke + full e2e examples |
| **P1** | Streaming materialize, bronze spill, assertions, atomic publish | **0.3.9** stream + Arrow IPC spill; further harden optional |
| **P2** | Iceberg as real system of record (catalog + snapshot commit), or deliberate pivot | FS layout + **versioned metadata** (0.3.9); REST/OCC still open |
| **P3** | DX verbs: `validate`, `explain`, `preview` | **0.3.9** shipped |
| **P4** | Measure packs, incremental strategies, WAP, multi-catalog, Rust models/UDFs | Thesis / later (see ADRs) |

**Contributor rule:** If a PR adds polish while `compile`/`run` on the smoke fixture is flaky, it is out of order. Fix the spine first.

---

## 3. Current honest state

| Area | Notes |
|------|--------|
| Core DAG / project / frontmatter | Solid (`rbt::core`) |
| Bronze → SQL → Parquet path | Working (smoke + full_e2e) |
| `--select` + frontmatter `test` | Working |
| Iceberg | FS table layout (`data/` + `metadata/`); **not** REST/Glue OCC SoR |
| DX verbs (`validate` / `explain` / `preview`) | **Not implemented** |
| Package surface | **One crate:** `rbt-datalake` (binary + lib import `rbt`) |

Do not document unfinished features as shipped.

---

## 4. Is Iceberg the right system of record?

**Short answer:** Iceberg remains the **default thesis target** for silver/gold table truth, but we are **not certain enough to fake full catalog product**. Working SoR today is **versioned lake files** (Parquet / FS Iceberg layout under layer `target_path`s).

### What we need from “table truth”

| Requirement | Why |
|-------------|-----|
| Stable table identity | `ref()` resolves to a table, not a random path string |
| Schema evolution / history | Silver typing and gold dims change over time |
| Atomic publish | Readers should not see half-written runs |
| Partition / snapshot pruning | Team-scale lakes without full scans |
| Ecosystem interoperability | Query from DataFusion, Spark, Trino, DuckDB later |
| ACID-ish commits on object storage | Multiple runs, retries, concurrent writers eventually |

### Decision rule

| Rule | Action |
|------|--------|
| **Default** | Keep Iceberg as the **target** SoR in designs |
| **Until proven** | Primary path may materialize **Parquet** / FS layout without claiming production multi-writer Iceberg |
| **Proof gate** | create → write data files → **commit snapshot** via official Rust `iceberg` → read back |
| **Do not** | WAP theater, multi-catalog sprawl, or “Iceberg-native” README claims before the proof gate |

Full comparison tables live in git history / older CONTRIBUTING revisions if needed; the rule above is what matters day-to-day.

---

## 5. Repository layout

```text
rbt/
├── Cargo.toml                 # workspace (member: crates/rbt only)
├── thesis.md                  # product north star
├── CONTRIBUTING.md            # this file
├── README.md
├── CHANGELOG.md
├── docs/
│   ├── README.md              # index
│   ├── PUBLISHING.md
│   ├── CRATES_IO.md
│   ├── adr/                   # active ADRs
│   └── archive/               # historical essays (not API)
├── crates/
│   └── rbt/                   # lib + bin `rbt`
│       └── src/{core,engine,scan,json,materializer,testing,main.rs}
└── examples/
    ├── smoke_fixture/         # CI golden path (tiny)
    └── full_e2e_rbt_example/  # real Arrow IPC bronze
```

Legacy dirs under `crates/rbt-*` (if present) are **not** workspace members — delete locally; do not re-publish.

---

## 6. Development setup

### Prerequisites

- Rust via [rustup](https://rustup.rs/); see `rust-toolchain.toml`.
- If host has mismatched `cargo` / `rustc` (e.g. Nix + rustup), fix that before filing build bugs.

### Build and test

```bash
cargo build -p rbt-datalake --release
cargo test -p rbt-datalake --lib
bash scripts/smoke.sh

# Benchmarks (Criterion; see crates/rbt/benches/README.md)
# cargo bench -p rbt-datalake --bench pipeline
# RBT_BENCH_FULL=0 cargo bench -p rbt-datalake --bench pipeline   # skip 9-model e2e

# Golden paths
./target/release/rbt compile -p examples/smoke_fixture --bronze-check fail
./target/release/rbt run -p examples/smoke_fixture --format parquet --select dim_ticker
./target/release/rbt test -p examples/smoke_fixture --select dim_ticker
```

### Project conventions

- Config: `rbt_project.yml`
- Layers: `models/staging` (`stg_`), `models/transforms` (`tf_` / `int_`), `models/marts` (`dim_` / `fact_` / `obt_`)
- Bronze: staging SQL frontmatter with `source_format` + `scan_path`
- Refs: `{{ ref('model_name') }}`, sources: `{{ source('schema', 'table') }}`

Layer boundary rules are enforced in the DAG: staging does not depend on downstream models; transforms do not depend on marts.

---

## 7. How we accept changes

### Before you open a PR

1. Run smoke (`bash scripts/smoke.sh`) if you touch engine, scan, core, or CLI.
2. Add or update unit tests next to the behavior you change.
3. Keep the PR focused — one concern per PR when possible.
4. Do not expand scope into DX verbs or multi-catalog unless that is the PR’s purpose and P0 is not regressing.

### Code style

- Match existing module style; clear module docs; `anyhow`/`thiserror` as already used.
- Prefer **fail-fast, structured errors** (model name, path, suggestion) over silent defaults.
- Avoid `todo!()` on the primary run path; gate unfinished formats with an explicit error.
- No drive-by refactors or unrelated formatting churn.
- Do not add dependencies lightly.

### Commits

- Prefer clear, imperative summaries: `engine: register bronze from frontmatter`.
- Reference issues when applicable.

### What we will reject or defer

- Features that only exist in docs presented as complete.
- Petabyte / distributed execution without an ADR.
- Warehouse-dbt parity checklists that fight the niche without a lake user need.
- Unmeasured performance claims in user-facing docs.

---

## 8. Design documents and ADRs

- **thesis.md** — scope, milestones, defensible claims.
- **docs/adr/** — active decisions.
- **docs/archive/** — research / old essays; may lag or overclaim.
- New architectural decisions (table-format pivot, catalog backends, incremental strategies) should land as a short ADR under `docs/adr/` with status, decision, and consequences.

---

## 9. Reporting bugs

Include:

1. OS and `rustc` / `cargo` versions.
2. Exact command(s).
3. Minimal project layout or a fork of `examples/smoke_fixture`.
4. Expected vs actual behavior.

---

## 10. License

Apache-2.0 (see `LICENSE`). Contributions under the same license.

---

## 11. Quick “should I work on X?” guide

| Idea | Do it when… |
|------|-------------|
| Harden bronze frontmatter / scan / jshift | Anytime — core niche |
| Fix DAG layer rules, compile errors, examples | Anytime — P0 |
| Stream write instead of full `collect()` | When large runs OOM or hardening materialization |
| Real Iceberg snapshot commit (proof gate) | After/while P0 stable — table-truth proof |
| `validate` / `preview` / `explain` | After primary run reliable (P3) |
| Multi-catalog (Glue, Polaris, …) | After one catalog path works |
| Rust models / UDFs (ADR-003) | After SQL spine trusted |
| `rbt-measure` packs | Before public “beats Spark” claims |
| SIMD / io_uring / custom allocators | Almost never for v0 — measure first |
| prost diagnostics | After JSON error shape stabilizes |

Welcome aboard. Make the spine boring and correct; then the verbs and Iceberg commits will have something worth standing on.

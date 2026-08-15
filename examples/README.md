# rbt examples

Showcase projects for the CLI and library. Prefer **release** builds for wall-clock:

```bash
cargo build -p rbt-datalake --release
export RBT=./target/release/rbt
```

| Example | Epic / topic | Entry |
|---------|----------------|-------|
| [smoke_fixture](smoke_fixture/) | CI baseline — tiny JSONL → stg → tf → dim | `bash scripts/smoke.sh` |
| [a1_multi_value_scope](a1_multi_value_scope/) | **A1** multi-value `--var` / IN partition filter | README |
| [a2_scoped_replace](a2_scoped_replace/) | **A2** replace one scope part; peers kept | `scripts/demo_scoped_replace.sh` |
| [entity_registry](entity_registry/) | **A7** keyed_upsert playbook (insert/touch/update/keep) | `scripts/demo_upsert.sh` |
| [complex_bronze_landing](complex_bronze_landing/) | Research mini-lake (multi-format bronze → star) | README + `scripts/fetch_bronze.py` |
| [full_e2e_rbt_example](full_e2e_rbt_example/) | **Ideal medallion showcase** — Arrow+JSONL → stg → TA (SQL or Design B + finance-solution) → OBT alias; RBT-C parallel | README + `cargo run -p full-e2e-rbt-example` |

## Feature smoke (A1 + A2 + A7)

From repo root, after building `rbt`:

```bash
bash scripts/smoke_feat_a1_a7.sh
```

Runs multi-value scope, scoped_replace peer keep, and the multi-day upsert demo.
Does **not** replace `scripts/smoke.sh` (CI baseline).

## Generated lake output

Examples write under `lake/` and `.rbt/` at runtime. Prefer `.gitignore` in each
showcase so artifacts are not committed. Re-run demos after `git clean` as needed.

## Docs

- [COMPLEX_BRONZE_AND_RUN_SCOPE.md](../docs/COMPLEX_BRONZE_AND_RUN_SCOPE.md) — run vars, upsert, receipts  
- [Feature roadmap](../docs/plans/rbt-datalake-feature-roadmap.md) — A1–A20  

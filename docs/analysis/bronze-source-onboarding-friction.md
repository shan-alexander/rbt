---
type: analysis
title: Bronze source onboarding friction (research mini-lake)
status: active
created: 2026-08-02
related:
  - examples/complex_bronze_landing
  - docs/goals/complex-bronze-landing-zones.md
---

# Bronze source onboarding friction

Exercise: add **new bibliographic sources** to the research mini-lake after the
pipeline already worked (PubMed / Crossref / Europe PMC / arXiv), then propagate
through silver → gold. Goal: treat this as a scale test for rbt DX when bronze
gains an upstream source.

## Sources considered

| Candidate | Outcome |
|-----------|---------|
| **Examine.com** | **Not added as papers.** Commercial nutrition/evidence product; free tier is not a general academic papers API. Landed as **policy failure** + robots fetch of `api.examine.com` only. |
| **OpenAlex** | **Added** (live JSON, mailto polite pool). Excellent free catalog. |
| **Semantic Scholar Graph API** | **Added** (JSON). Unauthenticated 429 common → seed fixture + optional `S2_API_KEY`. |

## Motions walked (checklist)

What actually changed for one new source (OpenAlex; S2 similar):

### 1. Lander (outside rbt)

1. Research API + politeness rules (mailto / rate / User-Agent).
2. Add query strings per `topic_track`.
3. Implement parse → normalized works row (`source`, `paper_id`, authors array, …).
4. Create `raw/<source>/` directory; write raw JSON.
5. Register origin in robots loop.
6. Plan unit `source:…` + failure path + seed path if flaky.
7. Extend `manifest.sources` / notes.
8. Re-run lander → new `run_id` + `LATEST_RUN.json`.

**Friction:** all host logic is Python. rbt does not know about sources; that is fine
for open-core (lander is project code). Pain is the *contract surface* into silver.

### 2. Silver stage (rbt models)

| Touchpoint | Required? | What happened |
|------------|-----------|---------------|
| `stg_works` column list | No (same shape) | No change if new source conforms to existing works schema |
| `stg_works` **`accepted_values.source`** | **Yes** | Hard fail until list includes `openalex`, `semanticscholar` |
| `stg_plan` / `stg_failures` / `stg_assets` | No | Free-form `source` utf8 — good |
| `stg_robots_txt` | No | New hosts appear automatically via path_glob |
| New stage model | No | Same `works.jsonl` grain |

**Friction (high):** `accepted_values` is a **closed enum** baked into YAML. Adding a
source is a deliberate contract bump, but there is **no single registry** — easy to
forget, and the failure mode is a test error only at materialize of `stg_works`.

### 3. Gold transforms / marts

| Touchpoint | Required? | What happened |
|------------|-----------|---------------|
| `tf_paper_status` | No | Unions works/failures/plan by `source` generically |
| `tf_source_run_stats` | **Partial** | Hardcoded `WHERE source IN (...)` for asset attribution — must extend |
| `dim_source` | **Partial** | `DISTINCT source` auto-picks new codes (**good**); human `CASE` labels need editing |
| `dim_paper` / `dim_venue` / `dim_topic` | No | Driven by data |
| `fact_paper_landing` | No | SK join by `source_code` |
| `fact_source_run` | No | After stats transform |
| Relationships | No | Passed once dims include new codes |

**Friction (medium):** dims auto-expand from data (Kimball Type-1 snapshot style) —
that scaled well. Human-readable `CASE` labels and any `IN (...)` filter lists did not.

### 4. Project contract / ops

| Touchpoint | Required? |
|------------|-----------|
| `contract_version` bump (`research-papers-v3`) | Should — skip/fingerprint semantics |
| README / CHANGELOG | Yes for humans |
| Measure fallback run_id / domain | Optional |
| `rbt validate` | Passed without knowing new sources exist |
| Fingerprint / receipt | New run → new fingerprint automatically |

**Friction (medium):** `validate --bronze-check fail` does not warn “new source codes
present in bronze not in accepted_values” until you run (or if tests run on empty).
No schema-diff between last receipt and current bronze.

## What worked well (keep)

1. **Conformed works grain** — one jsonl schema; new source is a new `source` value,
   not a new bronze table (unless you want one).
2. **`dim_source` from DISTINCT** — new codes appear without DDL.
3. **Hive partitions + run vars** — new lander run is a new `run_id`; no overwrite.
4. **`on_missing: empty`** + policy failure rows — partial APIs remain honest.
5. **path_glob robots/assets** — new hosts and raw files show up with zero model edits.
6. **Relationship tests on SKs** — still green after source growth.
7. **Fingerprint + receipt** — ops can see bronze changed.

## Friction ranked

| Rank | Friction | Severity |
|------|----------|----------|
| 1 | Closed `accepted_values` lists scattered in SQL frontmatter | High — silent until test; multi-file |
| 2 | Hardcoded `IN (...)` source lists in transforms | Medium — easy to miss for ops KPIs |
| 3 | Display-name `CASE` maps in dims | Low/Med — data works with raw codes |
| 4 | No first-class **source registry** in `rbt_project.yml` | Med — contracts live only in lander + tests |
| 5 | No bronze→model **impact report** (“these accepted_values will fail”) | High DX |
| 6 | Empty `failures.jsonl` previously broke `ref()` registration | High edge-case (mitigated with policy rows) |
| 7 | Multi-API lander growth → script becomes a mini ETL framework | Expected; not rbt’s job |
| 8 | Seed fixtures needed for rate-limited APIs | Ops, not rbt |

## Recommended rbt enhancements (product)

Prioritized for “bronze upstream change → silver+ adapts safely”:

### P0 — Source / enum registry — **IMPLEMENTED**

```yaml
# rbt_project.yml
contracts:
  enums:
    works.source:
      values: [pubmed, crossref, europepmc, openalex, semanticscholar, arxiv]
      on_new: fail   # fail | warn | allow
      labels: { openalex: "OpenAlex REST API" }
      probe: { model: stg_works, column: source }
```

- Single source of truth for `accepted_values`.
- Models use `accepted_values: { source: works.source }` (string = contract ref).
- Runtime assertions resolve via `config.contracts`.

### P0 — Bronze impact / contract drift report — **IMPLEMENTED**

```bash
rbt validate -p . --bronze-check fail --contract-diff \
  --var domain=… --var report_date=… --var run_id=…
```

Emits:

- distinct bronze values for each enum probe / model reference  
- `new_in_bronze` vs registry (`E_RBT_CONTRACT_NEW_VALUE` / `W_…` / notes)  
- unused registry values (informational)  
- files/rows sampled  

### P1 — Soft enums

`accepted_values` modes:

- `strict` (current)  
- `warn` (log `W_RBT_ACCEPTED_VALUES`, still materialize)  
- `open` (document known values, never fail)

Default for bronze-facing stage columns: **warn** in dev, **strict** in prod via project flag.

### P1 — Empty stage registration guarantee

0-row stages must always register a typed empty table for `ref()` (already intended via
`on_missing: empty` + `columns.*.dtype`). Harden stream path when jsonl file exists but
has zero lines (this exercise hit that earlier).

### P2 — Dim label seeds

Optional seed map in yml:

```yaml
dim_labels:
  source:
    openalex: "OpenAlex REST API"
```

Generate or inject into dim SQL so `CASE` lists stop being hand-edited.

### P2 — `rbt model touch --from-bronze works`

Scaffold: if bronze gains columns, propose frontmatter `columns:` + SELECT list diff.
Not full codegen — a dry-run patch.

### P3 — Multi-source bronze profiles

Document pattern: one works grain vs per-source bronze tables. Optional
`parts:` / source profile templates for landers (still project-owned).

## Verdict on “is rbt built for scaling sources?”

**Mostly yes for data plane, partially for control plane.**

- Data plane: conformed works + hive runs + auto dims + relationships scaled from
  4 → 6 paper sources with ~4 model touchpoints.
- Control plane: enums, label maps, and ops filters are still **copy-paste contracts**.
  That is the gap that hurts when the 7th and 15th source arrive.

Implementing P0 registry + contract-diff would make the silver+ adaptation path
feel first-class rather than “grep accepted_values and hope.”

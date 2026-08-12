---
tags: [analysis]
node_type: analysis
---
# A10 bronze-to-silver approach (adapters + silver first-class)

## Status
**Implemented** (adapter trait, registry, HTML/XML/robots, matrix + guide; A9 still skipped)

## Framing (lakehouse, not warehouse-first Kimball)

Kimball star schema was engineered for RDBMS warehouses with curated staging and controlled loads.
On a **data lake**, most of that discipline is **post-ingestion**:

| Layer | Job | Kimball analogue |
|-------|-----|------------------|
| **Bronze** | Technical landing of multi-format, multi-artifact files; partitions; optional families | Source systems / raw |
| **Silver** | Usable, typed, grain-honest tables for engineers: stage logs, current snapshots, lookups, seeds | “Staging + some integration” |
| **Gold** | Business star: dims (type1/SCD2), facts, OBTs — conformed grain and SKs | Presentation / mart |

**Rule:** Do not force dim/fact patterns into silver. Silver optimizes for **fast bronze→usable Parquet** and honest partial runs. Gold owns star contracts.

Related: [[Bronze-to-silver maturity gap matrix]] · [[Complex bronze landing zones]] · [[Star schema data modeling rules]] · [[ADR-002 Thesis Alignment]]

## A10 product goals

1. **Heterogeneous bronze → Arrow** as a first-class, documented adapter surface (not ad-hoc scanners only).
2. **Silver model roles** as first-class (beyond “just stg_”): stage, lookup/ref, seed-backed, optional delta/current — without conflating them with gold dim/fact.
3. **Ergonomics:** adding a source/format should not require core patches for Done formats; unknown format fails closed with `E_RBT_SOURCE_FORMAT`.

## Out of A10 scope

- SCD2 / full star engine (A19–A20)
- Host landers / API scrapers (project code)
- SQLite (A8 non-product)
- Per-entity failure maps (A9 skipped for now)

## Approach phases

### Phase 0 — Re-audit against current tree (A10.1)

Gap matrix was 0.5.0-era; A1–A7 closed many B-items (on_missing empty, run vars, receipts, fingerprints, schema emit, keyed_upsert). Produce **`docs/BRONZE_ADAPTER_MATRIX.md`**: format × list × hive × empty × dtype × spill × tests.

### Phase 1 — BronzeAdapter trait + registry (A10.2–3)

Thin trait over existing scan paths; registry from `SourceFormat`; no rewrite of working JSONL path.

### Phase 2 — Harden high-ROI formats (A10.4–8)

Priority: JSONL/JSON → text/log/robots → XML (or document pre-normalize) → HTML-as-utf8-rows → protobuf docs.

### Phase 3 — Silver first-class roles (extends A10 + A18)

| Role | Prefix / type | Bronze? | Materialization |
|------|---------------|---------|-----------------|
| **stage** | `stg_*` | usually | table / append / scoped_replace |
| **lookup / ref** | `ref_*` or `lkp_*` | seed or bronze | table or keyed_upsert |
| **seed** | `seed_*` | static file | table |
| **delta / current** | `tf_*` or silver tbl | from stg | table or keyed_upsert |

Seeds: small static bronze (or project `seeds/`) → silver/gold ref tables. Not Kimball dims until gold.

### Phase 4 — Docs + complex_bronze (A10.10–11)

`docs/BRONZE_ADAPTERS.md`; playbook for new format; friction notes from research mini-lake.

## Success metrics

- Format matrix truthful
- One PR adds a format via trait + tests
- Silver role vocabulary documented without forcing gold patterns into silver

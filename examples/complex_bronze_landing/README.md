# Research papers mini lakehouse

A **relatable bronze → silver → gold** demo: a small lab’s *research intelligence*
landing zone that politely collects literature on **AI × semiconductors /
neuromorphic computing** and **AI in agritech** (precision ag, crop sensing),
lands mixed filetypes, and builds a Kimball star.

| Band | Path | Role |
|------|------|------|
| **Bronze** | `lake/lz/runs/domain=…/report_date=…/run_id=…/` | Multi-API landing zone |
| **Silver stage** | `lake/silver/stage/` | `stg_*` **endpoints** |
| **Gold transforms** | `lake/gold/tf/` | recon + source-run stats (refs `stg_*` only) |
| **Gold marts** | `lake/gold/` | `dim_*` + thin facts |

Topology rules: [docs/GOLD_DEFAULT.md](../../docs/GOLD_DEFAULT.md).

---

## Story

Imagine a semiconductor–agritech research group. Each night a lander pulls
recent papers from public bibliographic APIs, drops **immutable run folders**
into bronze (XML, JSON, Atom, HTML cards, robots.txt), then rbt stages and
models:

- Which sources succeeded or rate-limited?
- Papers with abstracts vs title-only?
- Journals / venues and research tracks (`semicon` vs `agritech`)?
- Asset inventory (bytes, mime, kind) for lakehouse ops demos?

**Not used:** Google Scholar (no public API; scraping is not allowed).

---

## 1. Bronze lander (polite public APIs)

| Source | Protocol | What we store |
|--------|----------|----------------|
| [PubMed E-utilities](https://www.ncbi.nlm.nih.gov/books/NBK25501/) | esearch JSON + efetch **XML** | `works.jsonl`, `raw/pubmed/*.xml` |
| [Crossref REST](https://api.crossref.org) | JSON (mailto polite pool) | `works.jsonl`, `raw/crossref/*.json` |
| [Europe PMC REST](https://europepmc.org/RestfulWebService) | JSON core | `works.jsonl`, `raw/europepmc/*.json` |
| [OpenAlex](https://docs.openalex.org) | JSON works (mailto) | `works.jsonl`, `raw/openalex/*.json` |
| [Semantic Scholar](https://www.semanticscholar.org/product/api) | Graph JSON | `works.jsonl`, `raw/semanticscholar/*` (seed if 429) |
| [arXiv API](https://info.arxiv.org/help/api/index.html) | Atom **XML** | `works.jsonl`, `raw/arxiv/*` (seed if 429) |
| Origins | `robots.txt` | `robots/<host>.txt` (incl. examine.com api host) |
| Derived | HTML cards | `html/*.html` (title / authors / abstract body) |
| Inventory | `assets.jsonl` | every landed file (kind, mime, bytes) |

**Not free paper sources (policy failure rows):** Google Scholar; [Examine.com](https://examine.com)
(commercial nutrition evidence — request/API, not a general academic papers lander).

```bash
# from this example directory
python3 scripts/fetch_bronze.py --email YOU@example.com --retmax 5 --topic both

# prints rbt --var domain=… report_date=… run_id=…
# also writes lake/lz/LATEST_RUN.json
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--topic` | `both` | `semicon` \| `agritech` \| `both` |
| `--retmax` | `5` | Max papers per source/track slice |
| `--seed-on-fail` | on | Use `scripts/fixtures/arxiv_seed_atom.xml` if arXiv 429/timeout |
| `--domain` | `ai-semicon-agritech` | Hive `domain=` partition |

Politeness: User-Agent + From/mailto, NCBI ≤3 req/s, Crossref mailto, Europe PMC
delays, arXiv ~3s. Set a real contact email via `--email` or `RBT_BRONZE_EMAIL`.

**Crossref venue slices:** *Nature Electronics*, *IEEE TED*, *Applied Physics Letters*,
*Computers and Electronics in Agriculture*, *Precision Agriculture*.

---

## 2. Bronze layout (mixed filetypes)

```text
lake/lz/runs/domain=ai-semicon-agritech/report_date=YYYY-MM-DD/run_id=…/
  plan.jsonl           # planned API units (source × track × journal)
  works.jsonl          # papers: title, abstract, authors[], authors_json, …
  failures.jsonl       # timeouts / HTTP / rate limits
  siteinfo.jsonl       # portals + robots fetch status
  assets.jsonl         # file inventory (xml/json/html/txt/jsonl)
  manifest.json
  robots/*.txt
  raw/pubmed/*.xml
  raw/crossref/*.json
  raw/europepmc/*.json
  raw/arxiv/*
  html/*.html
```

Hive partitions: `domain=` / `report_date=` / `run_id=`.

**Works columns (normalized):** `paper_id`, `source`, `title`, `abstract`,
`authors` (JSON array in bronze), `authors_json`, `authors_joined`, `author_count`,
`abstract_chars`, `has_abstract`, `venue`, `year`, `doi`, `url`, `keywords_joined`,
`topic_track` (`semicon` \| `agritech`).

---

## 3. Models

```text
stg_plan, stg_works, stg_failures, stg_siteinfo, stg_robots_txt, stg_assets
        │  (silver endpoints)
        ▼
tf_paper_status          gold/tf — works ∪ failures ∪ planned_only
tf_source_run_stats      gold/tf — per-source KPIs (works/fails/assets)
        │
        ├─ dim_source, dim_venue, dim_paper, dim_topic   (SK + Unknown −1)
        ├─ fact_paper_landing   thin: SK FKs + row_status + author/abstract measures
        └─ fact_source_run      thin: per-source run ops KPIs
```

Authors: bronze keeps JSON **arrays**; silver exposes `authors_json` +
`authors_joined` + `author_count` for Arrow-friendly analytics.

---

## 4. Run rbt

```bash
# from repo root
cargo build -p rbt-datalake --release

# after fetch_bronze.py (use vars it prints, or LATEST_RUN.json)
./target/release/rbt validate -p examples/complex_bronze_landing --bronze-check fail

./target/release/rbt run -p examples/complex_bronze_landing --format parquet \
  --var domain=ai-semicon-agritech \
  --var report_date=2026-08-01 \
  --var run_id=<from_lander>
```

```bash
# helper: load latest pointer
python3 - <<'PY'
import json
from pathlib import Path
p = Path("examples/complex_bronze_landing/lake/lz/LATEST_RUN.json")
print(json.loads(p.read_text()))
PY
```

```bash
rbt measure -p examples/complex_bronze_landing --scenario complex_bronze --json
```

```bash
# P0: registry + bronze contract-diff (catches new source codes before run)
./target/debug/rbt validate -p examples/complex_bronze_landing --bronze-check fail \
  --contract-diff \
  --var domain=ai-semicon-agritech \
  --var report_date=2026-08-01 \
  --var run_id=<from_lander>
```

Enums live in `rbt_project.yml` → `contracts.enums`; staging uses
`accepted_values: { source: works.source }` (no duplicated lists).

---

## 5. What this demonstrates in rbt

| Feature | Where |
|---------|--------|
| Multi-root + hive partitions + `path_glob` | staging frontmatter |
| Mixed bronze formats (jsonl, txt, xml/json on disk) | lander + `stg_robots_txt` + `stg_assets` |
| `on_missing: empty` partial runs | failures / siteinfo / robots / assets |
| Gold transforms ref **only** silver stage | `tf_paper_status`, `tf_source_run_stats` |
| Dim SK + Unknown; fact SK FKs + relationships | marts |
| Lineage stamps | `lineage_stamp: true` on dims/facts |
| Run vars + `LATEST_RUN.json` | lander + measure |
| **New source onboarding** (OpenAlex + S2) | lander + `accepted_values` + dim labels |

Friction notes when bronze gains a source: [docs/analysis/bronze-source-onboarding-friction.md](../../docs/analysis/bronze-source-onboarding-friction.md).

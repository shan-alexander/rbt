# Research papers mini lakehouse

A **relatable bronze → silver → gold** demo: polite multi-API collection of
**semiconductor + machine learning** research papers, multi-format bronze, and a
small Kimball star.

| Band | Path | Role |
|------|------|------|
| **Bronze** | `lake/lz/runs/domain=…/report_date=…/run_id=…/` | PubMed / Crossref / arXiv landings |
| **Silver stage** | `lake/silver/stage/` | `stg_*` **endpoints** |
| **Gold transforms** | `lake/gold/tf/` | `tf_paper_status` (refs `stg_*` only) |
| **Gold marts** | `lake/gold/` | `dim_*` + `fact_paper_landing` |

Topology rules: [docs/GOLD_DEFAULT.md](../../docs/GOLD_DEFAULT.md).

---

## 1. Bronze lander (polite public APIs)

**Not used:** Google Scholar (no public API; scraping is not allowed).

| Source | Protocol | What we store |
|--------|----------|----------------|
| [PubMed E-utilities](https://www.ncbi.nlm.nih.gov/books/NBK25501/) | esearch JSON + efetch **XML** | `works.jsonl`, `raw/pubmed/*.xml` |
| [Crossref REST](https://api.crossref.org) | JSON (mailto polite pool) | `works.jsonl`, `raw/crossref/*.json` |
| [arXiv API](https://info.arxiv.org/help/api/index.html) | Atom **XML** | `works.jsonl`, `raw/arxiv/*` (when not rate-limited) |
| Origins | `robots.txt` | `robots/<host>.txt` |
| Derived | HTML cards | `html/*.html` (title / authors / abstract) |

```bash
# from this example directory
python3 scripts/fetch_bronze.py --email YOU@example.com --retmax 5

# prints rbt --var domain=… report_date=… run_id=…
# also writes lake/lz/LATEST_RUN.json
```

Politeness: User-Agent + From/mailto, NCBI ≤3 req/s, Crossref mailto, arXiv delays.
Set a real contact email via `--email` or `RBT_BRONZE_EMAIL`.

Crossref venues (sample slices): *Nature Electronics*, *IEEE TED*, *Applied Physics Letters*,
*Semiconductor Science and Technology*.

---

## 2. Bronze layout (mixed filetypes)

```text
lake/lz/runs/domain=semicon-ai-research/report_date=YYYY-MM-DD/run_id=…/
  plan.jsonl           # planned API units
  works.jsonl          # normalized papers (title, abstract, authors[], authors_joined, …)
  failures.jsonl       # timeouts / HTTP / rate limits (partial-run honesty)
  siteinfo.jsonl       # portals + robots fetch status
  manifest.json
  robots/*.txt
  raw/pubmed/*.xml
  raw/crossref/*.json
  raw/arxiv/*
  html/*.html
```

Hive partitions: `domain=` / `report_date=` / `run_id=`.

---

## 3. Models

```text
stg_plan, stg_works, stg_failures, stg_siteinfo, stg_robots_txt
        │  (silver endpoints)
        ▼
tf_paper_status          gold/tf — recon works ∪ failures ∪ planned_only
        │
        ├─ dim_source, dim_venue, dim_paper   (SK + Unknown −1)
        └─ fact_paper_landing                 (thin: SK FKs + row_status + author_count)
```

Authors: bronze keeps JSON **arrays**; silver exposes `authors_joined` + `author_count`
for Arrow/DataFusion-friendly analytics.

---

## 4. Run rbt

```bash
# from repo root
cargo build -p rbt-datalake --release

# after fetch_bronze.py (use vars it prints, or LATEST_RUN.json)
./target/release/rbt validate -p examples/complex_bronze_landing --bronze-check fail

./target/release/rbt run -p examples/complex_bronze_landing --format parquet \
  --var domain=semicon-ai-research \
  --var report_date=2026-08-01 \
  --var run_id=run20260802T022258Z
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

---

## 5. What this demonstrates in rbt

| Feature | Where |
|---------|--------|
| Multi-root + hive partitions + `path_glob` | staging frontmatter |
| Mixed bronze formats (jsonl, txt, xml on disk) | lander + `stg_robots_txt` |
| `on_missing: empty` partial runs | failures / siteinfo / robots |
| Gold transform refs **only** silver stage | `tf_paper_status` |
| Dim SK + Unknown; fact SK FKs + relationships | marts |
| Lineage stamps | `lineage_stamp: true` on dims/fact |

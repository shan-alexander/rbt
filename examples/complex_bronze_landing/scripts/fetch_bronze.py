#!/usr/bin/env python3
"""
Polite multi-source bronze lander for the research-papers mini lakehouse.

Sources (public APIs — NOT Google Scholar; no ToS-violating scrapes):
  • PubMed E-utilities  (NCBI)     — ≤3 req/s, tool+email
  • Crossref REST                  — polite pool (mailto)
  • arXiv Atom API                 — ≤1 req/3s recommended

Topic default: semiconductors + AI / machine learning (journal-aware queries).

Writes hive-partitioned bronze under:
  lake/lz/runs/domain=<domain>/report_date=<YYYY-MM-DD>/run_id=<id>/

Artifacts (mixed filetypes for rbt path_glob demos):
  plan.jsonl           — planned work units (source × query)
  works.jsonl          — successful normalized papers
  failures.jsonl       — failed units
  siteinfo.jsonl       — source site inventory
  robots/<host>.txt    — robots.txt from each origin
  raw/pubmed/*.xml     — PubMed efetch XML
  raw/crossref/*.json  — Crossref API JSON
  raw/arxiv/*.xml      — arXiv Atom entry XML
  html/*.html          — lightweight HTML cards (title/abstract/authors)

Usage:
  python3 scripts/fetch_bronze.py
  python3 scripts/fetch_bronze.py --email you@example.com --retmax 8
  RBT_BRONZE_EMAIL=you@example.com python3 scripts/fetch_bronze.py
"""

from __future__ import annotations

import argparse
import html as html_lib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

# ── polite defaults ──────────────────────────────────────────────────────────
TOOL = "rbt-complex-bronze-landing"
DEFAULT_EMAIL = os.environ.get("RBT_BRONZE_EMAIL", "rbt-examples@example.com")
USER_AGENT = f"{TOOL}/0.7 (+https://github.com/shan-alexander/rbt; mailto={{email}})"

# Sleeps between remote calls (seconds) — stay under published limits
SLEEP_NCBI = 0.40   # < 3/s
SLEEP_CROSSREF = 0.25
SLEEP_ARXIV = 3.5   # arXiv asks for courtesy delays; 429 → recorded in failures.jsonl
SLEEP_ROBOTS = 0.3

# Semiconductor / AI research — journals as Crossref container-title filters
QUERIES = {
    "pubmed": (
        '("semiconductor"[Title/Abstract] OR "wide bandgap"[Title/Abstract]) '
        'AND ("machine learning"[Title/Abstract] OR "deep learning"[Title/Abstract] '
        'OR "neural network"[Title/Abstract])'
    ),
    "crossref": "semiconductor machine learning",
    "arxiv": 'all:"semiconductor" AND (all:"machine learning" OR all:"neural network")',
}

# Crossref: focus on a few well-known venues (container-title partial match)
CROSSREF_JOURNALS = [
    "Nature Electronics",
    "IEEE Transactions on Electron Devices",
    "Applied Physics Letters",
    "Semiconductor Science and Technology",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def slug(s: str, max_len: int = 80) -> str:
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s.strip())
    return s[:max_len] or "x"


class PoliteClient:
    def __init__(self, email: str):
        self.email = email
        self.ua = USER_AGENT.format(email=email)
        self._last: dict[str, float] = {}

    def _throttle(self, bucket: str, delay: float) -> None:
        now = time.monotonic()
        wait = delay - (now - self._last.get(bucket, 0.0))
        if wait > 0:
            time.sleep(wait)
        self._last[bucket] = time.monotonic()

    def get(
        self,
        url: str,
        *,
        bucket: str,
        delay: float,
        accept: str | None = None,
        timeout: float = 45.0,
    ) -> tuple[int, bytes, str]:
        self._throttle(bucket, delay)
        headers = {
            "User-Agent": self.ua,
            "From": self.email,
        }
        if accept:
            headers["Accept"] = accept
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                ctype = resp.headers.get("Content-Type", "")
                return resp.status, body, ctype
        except urllib.error.HTTPError as e:
            body = e.read() if e.fp else b""
            return e.code, body, ""
        except urllib.error.URLError as e:
            return 0, str(e.reason).encode(), ""


def fetch_robots(client: PoliteClient, origin: str, dest_dir: Path) -> dict[str, Any]:
    host = urllib.parse.urlparse(origin).netloc or origin
    url = origin.rstrip("/") + "/robots.txt"
    code, body, _ = client.get(url, bucket="robots", delay=SLEEP_ROBOTS)
    path = dest_dir / f"{slug(host)}.txt"
    path.write_bytes(body if code == 200 else f"# fetch failed status={code}\n".encode())
    return {
        "host": host,
        "robots_url": url,
        "http_status": code,
        "bytes": len(body),
        "path": str(path.relative_to(dest_dir.parent.parent) if False else path.name),
        "fetched_at": utc_now_iso(),
    }


def write_html_card(path: Path, paper: dict[str, Any]) -> None:
    authors = paper.get("authors") or []
    if isinstance(authors, str):
        authors = [authors]
    authors_html = ", ".join(html_lib.escape(a) for a in authors)
    title = html_lib.escape(paper.get("title") or "")
    abstract = html_lib.escape(paper.get("abstract") or "")
    doi = html_lib.escape(paper.get("doi") or "")
    source = html_lib.escape(paper.get("source") or "")
    venue = html_lib.escape(paper.get("venue") or "")
    doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>{title}</title>
  <meta name="generator" content="{TOOL}"/>
</head>
<body>
  <article data-source="{source}" data-doi="{doi}">
    <h1>{title}</h1>
    <p class="authors">{authors_html}</p>
    <p class="venue">{venue}</p>
    <section class="abstract"><h2>Abstract</h2><p>{abstract}</p></section>
  </article>
</body>
</html>
"""
    path.write_text(doc, encoding="utf-8")


# ── PubMed ───────────────────────────────────────────────────────────────────

def pubmed_search(client: PoliteClient, term: str, retmax: int) -> list[str]:
    q = urllib.parse.urlencode(
        {
            "db": "pubmed",
            "term": term,
            "retmax": str(retmax),
            "retmode": "json",
            "tool": TOOL,
            "email": client.email,
        }
    )
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?{q}"
    code, body, _ = client.get(url, bucket="ncbi", delay=SLEEP_NCBI, accept="application/json")
    if code != 200:
        raise RuntimeError(f"PubMed esearch HTTP {code}: {body[:200]!r}")
    data = json.loads(body.decode("utf-8", errors="replace"))
    return list(data.get("esearchresult", {}).get("idlist", []))


def pubmed_fetch_xml(client: PoliteClient, pmids: list[str]) -> bytes:
    q = urllib.parse.urlencode(
        {
            "db": "pubmed",
            "id": ",".join(pmids),
            "retmode": "xml",
            "tool": TOOL,
            "email": client.email,
        }
    )
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?{q}"
    code, body, _ = client.get(url, bucket="ncbi", delay=SLEEP_NCBI, accept="application/xml")
    if code != 200:
        raise RuntimeError(f"PubMed efetch HTTP {code}")
    return body


def parse_pubmed_articles(xml_bytes: bytes) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_bytes)
    out: list[dict[str, Any]] = []
    for art in root.findall(".//PubmedArticle"):
        medline = art.find("MedlineCitation")
        if medline is None:
            continue
        pmid_el = medline.find("PMID")
        pmid = pmid_el.text if pmid_el is not None else ""
        article = medline.find("Article")
        if article is None:
            continue
        title_el = article.find("ArticleTitle")
        title = "".join(title_el.itertext()).strip() if title_el is not None else ""
        abstract_bits = []
        for ab in article.findall(".//AbstractText"):
            label = ab.get("Label")
            text = "".join(ab.itertext()).strip()
            if label:
                abstract_bits.append(f"{label}: {text}")
            else:
                abstract_bits.append(text)
        abstract = "\n".join(abstract_bits)
        authors: list[str] = []
        for au in article.findall(".//Author"):
            last = au.findtext("LastName") or ""
            fore = au.findtext("ForeName") or au.findtext("Initials") or ""
            collective = au.findtext("CollectiveName")
            if collective:
                authors.append(collective)
            elif last or fore:
                authors.append(f"{fore} {last}".strip())
        journal = article.findtext(".//Journal/Title") or article.findtext(
            ".//Journal/ISOAbbreviation"
        )
        year = article.findtext(".//JournalIssue/PubDate/Year") or article.findtext(
            ".//ArticleDate/Year"
        )
        doi = ""
        for id_el in art.findall(".//ArticleId"):
            if id_el.get("IdType") == "doi" and id_el.text:
                doi = id_el.text.strip()
        out.append(
            {
                "paper_id": f"pmid:{pmid}",
                "source": "pubmed",
                "external_id": pmid,
                "doi": doi,
                "title": title,
                "abstract": abstract,
                "authors": authors,
                "venue": journal or "",
                "year": year or "",
                "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else "",
            }
        )
    return out


# ── Crossref ─────────────────────────────────────────────────────────────────

def crossref_search(
    client: PoliteClient, query: str, journal: str, rows: int
) -> tuple[list[dict[str, Any]], bytes]:
    params = {
        "query": query,
        "query.container-title": journal,
        "rows": str(rows),
        "select": "DOI,title,author,abstract,container-title,published-print,published-online,URL,type",
        "mailto": client.email,
    }
    url = "https://api.crossref.org/works?" + urllib.parse.urlencode(params)
    code, body, _ = client.get(
        url, bucket="crossref", delay=SLEEP_CROSSREF, accept="application/json"
    )
    if code != 200:
        raise RuntimeError(f"Crossref HTTP {code}: {body[:200]!r}")
    data = json.loads(body.decode("utf-8", errors="replace"))
    items = data.get("message", {}).get("items", [])
    papers: list[dict[str, Any]] = []
    for it in items:
        title_l = it.get("title") or []
        title = title_l[0] if title_l else ""
        authors = []
        for a in it.get("author") or []:
            name = f"{a.get('given', '')} {a.get('family', '')}".strip()
            if name:
                authors.append(name)
        venue_l = it.get("container-title") or []
        venue = venue_l[0] if venue_l else journal
        # Crossref abstracts are often JATS XML-ish
        abstract = it.get("abstract") or ""
        abstract = re.sub(r"<[^>]+>", " ", abstract)
        abstract = re.sub(r"\s+", " ", abstract).strip()
        doi = it.get("DOI") or ""
        year = ""
        for key in ("published-print", "published-online"):
            parts = (it.get(key) or {}).get("date-parts") or []
            if parts and parts[0]:
                year = str(parts[0][0])
                break
        papers.append(
            {
                "paper_id": f"doi:{doi}" if doi else f"crossref:{slug(title)[:40]}",
                "source": "crossref",
                "external_id": doi,
                "doi": doi,
                "title": title,
                "abstract": abstract,
                "authors": authors,
                "venue": venue,
                "year": year,
                "url": it.get("URL") or (f"https://doi.org/{doi}" if doi else ""),
            }
        )
    return papers, body


# ── arXiv ────────────────────────────────────────────────────────────────────

def arxiv_search(client: PoliteClient, query: str, max_results: int) -> tuple[list[dict[str, Any]], bytes]:
    params = {
        "search_query": query,
        "start": "0",
        "max_results": str(max_results),
        "sortBy": "relevance",
        "sortOrder": "descending",
    }
    url = "https://export.arxiv.org/api/query?" + urllib.parse.urlencode(params)
    code, body, _ = client.get(
        url,
        bucket="arxiv",
        delay=SLEEP_ARXIV,
        accept="application/atom+xml",
        timeout=90.0,
    )
    if code != 200:
        raise RuntimeError(f"arXiv HTTP {code}: {body[:200]!r}")
    # Atom namespace
    ns = {"a": "http://www.w3.org/2005/Atom"}
    root = ET.fromstring(body)
    papers: list[dict[str, Any]] = []
    for entry in root.findall("a:entry", ns):
        id_url = (entry.findtext("a:id", default="", namespaces=ns) or "").strip()
        arxiv_id = id_url.rsplit("/", 1)[-1]
        title = " ".join((entry.findtext("a:title", default="", namespaces=ns) or "").split())
        abstract = " ".join(
            (entry.findtext("a:summary", default="", namespaces=ns) or "").split()
        )
        authors = [
            (au.findtext("a:name", default="", namespaces=ns) or "").strip()
            for au in entry.findall("a:author", ns)
        ]
        authors = [a for a in authors if a]
        published = entry.findtext("a:published", default="", namespaces=ns) or ""
        year = published[:4] if published else ""
        papers.append(
            {
                "paper_id": f"arxiv:{arxiv_id}",
                "source": "arxiv",
                "external_id": arxiv_id,
                "doi": "",
                "title": title,
                "abstract": abstract,
                "authors": authors,
                "venue": "arXiv",
                "year": year,
                "url": id_url,
            }
        )
    return papers, body


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--project-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="complex_bronze_landing project root",
    )
    ap.add_argument("--email", default=DEFAULT_EMAIL, help="Contact email for polite API pools")
    ap.add_argument("--domain", default="semicon-ai-research")
    ap.add_argument("--report-date", default=date.today().isoformat())
    ap.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("run%Y%m%dT%H%M%SZ"))
    ap.add_argument("--retmax", type=int, default=6, help="Max papers per source/journal slice")
    ap.add_argument("--skip-network", action="store_true", help="Only valid for dry structure tests")
    args = ap.parse_args()

    if args.email.endswith("@example.com"):
        print(
            "warning: using placeholder email; set --email or RBT_BRONZE_EMAIL for polite pools",
            file=sys.stderr,
        )

    root = args.project_root
    run_dir = (
        root
        / "lake"
        / "lz"
        / "runs"
        / f"domain={args.domain}"
        / f"report_date={args.report_date}"
        / f"run_id={args.run_id}"
    )
    raw_pubmed = run_dir / "raw" / "pubmed"
    raw_crossref = run_dir / "raw" / "crossref"
    raw_arxiv = run_dir / "raw" / "arxiv"
    robots_dir = run_dir / "robots"
    html_dir = run_dir / "html"
    for d in (raw_pubmed, raw_crossref, raw_arxiv, robots_dir, html_dir):
        d.mkdir(parents=True, exist_ok=True)

    client = PoliteClient(args.email)
    plan_rows: list[dict[str, Any]] = []
    works_rows: list[dict[str, Any]] = []
    fail_rows: list[dict[str, Any]] = []
    site_rows: list[dict[str, Any]] = []

    origins = [
        "https://www.ncbi.nlm.nih.gov",
        "https://api.crossref.org",
        "https://export.arxiv.org",
        "https://arxiv.org",
    ]

    print(f"[fetch_bronze] run_dir={run_dir}")
    print(f"[fetch_bronze] email={args.email} retmax={args.retmax}")

    # robots.txt
    for origin in origins:
        try:
            info = fetch_robots(client, origin, robots_dir)
            site_rows.append(
                {
                    "site_id": info["host"],
                    "origin": origin,
                    "role": "api_or_portal",
                    "robots_status": info["http_status"],
                    "robots_bytes": info["bytes"],
                    "domain": args.domain,
                    "report_date": args.report_date,
                    "run_id": args.run_id,
                    "fetched_at": info["fetched_at"],
                }
            )
            print(f"  robots {info['host']}: HTTP {info['http_status']}")
        except Exception as e:
            fail_rows.append(
                {
                    "unit_id": f"robots:{origin}",
                    "source": "robots",
                    "error": str(e),
                    "domain": args.domain,
                    "report_date": args.report_date,
                    "run_id": args.run_id,
                }
            )

    if args.skip_network:
        print("skip-network set; writing empty scaffolds only")
    else:
        # ── PubMed ───────────────────────────────────────────────────────
        unit = "pubmed:esearch"
        plan_rows.append(
            {
                "unit_id": unit,
                "source": "pubmed",
                "query": QUERIES["pubmed"],
                "planned": True,
                "domain": args.domain,
                "report_date": args.report_date,
                "run_id": args.run_id,
            }
        )
        try:
            pmids = pubmed_search(client, QUERIES["pubmed"], args.retmax)
            print(f"  pubmed: {len(pmids)} PMIDs")
            if pmids:
                xml_bytes = pubmed_fetch_xml(client, pmids)
                # also store combined + per-pmid split
                (raw_pubmed / "batch_efetch.xml").write_bytes(xml_bytes)
                papers = parse_pubmed_articles(xml_bytes)
                # split individual articles if possible
                root_xml = ET.fromstring(xml_bytes)
                for art in root_xml.findall(".//PubmedArticle"):
                    pmid_el = art.find(".//PMID")
                    if pmid_el is not None and pmid_el.text:
                        frag = ET.tostring(art, encoding="utf-8")
                        (raw_pubmed / f"{pmid_el.text}.xml").write_bytes(
                            b'<?xml version="1.0"?>\n' + frag
                        )
                for p in papers:
                    p["domain"] = args.domain
                    p["report_date"] = args.report_date
                    p["run_id"] = args.run_id
                    p["ingested_at"] = utc_now_iso()
                    p["authors_joined"] = "; ".join(p.get("authors") or [])
                    p["author_count"] = len(p.get("authors") or [])
                    works_rows.append(p)
                    write_html_card(html_dir / f"{slug(p['paper_id'])}.html", p)
        except Exception as e:
            fail_rows.append(
                {
                    "unit_id": unit,
                    "source": "pubmed",
                    "error": str(e),
                    "domain": args.domain,
                    "report_date": args.report_date,
                    "run_id": args.run_id,
                }
            )
            print(f"  pubmed FAIL: {e}", file=sys.stderr)

        # ── Crossref (per journal slice) ─────────────────────────────────
        for journal in CROSSREF_JOURNALS:
            unit = f"crossref:{slug(journal)}"
            plan_rows.append(
                {
                    "unit_id": unit,
                    "source": "crossref",
                    "query": QUERIES["crossref"],
                    "venue_filter": journal,
                    "planned": True,
                    "domain": args.domain,
                    "report_date": args.report_date,
                    "run_id": args.run_id,
                }
            )
            try:
                papers, raw = crossref_search(
                    client, QUERIES["crossref"], journal, max(2, args.retmax // 2)
                )
                raw_path = raw_crossref / f"{slug(journal)}.json"
                raw_path.write_bytes(raw)
                print(f"  crossref [{journal}]: {len(papers)} works")
                for p in papers:
                    p["domain"] = args.domain
                    p["report_date"] = args.report_date
                    p["run_id"] = args.run_id
                    p["ingested_at"] = utc_now_iso()
                    p["venue_filter"] = journal
                    p["authors_joined"] = "; ".join(p.get("authors") or [])
                    p["author_count"] = len(p.get("authors") or [])
                    works_rows.append(p)
                    write_html_card(html_dir / f"{slug(p['paper_id'])}.html", p)
                    if p.get("doi"):
                        # small per-DOI pointer file
                        (raw_crossref / f"{slug(p['doi'])}.meta.json").write_text(
                            json.dumps({"doi": p["doi"], "batch": raw_path.name}, indent=2),
                            encoding="utf-8",
                        )
            except Exception as e:
                fail_rows.append(
                    {
                        "unit_id": unit,
                        "source": "crossref",
                        "error": str(e),
                        "domain": args.domain,
                        "report_date": args.report_date,
                        "run_id": args.run_id,
                    }
                )
                print(f"  crossref [{journal}] FAIL: {e}", file=sys.stderr)

        # ── arXiv ────────────────────────────────────────────────────────
        unit = "arxiv:search"
        plan_rows.append(
            {
                "unit_id": unit,
                "source": "arxiv",
                "query": QUERIES["arxiv"],
                "planned": True,
                "domain": args.domain,
                "report_date": args.report_date,
                "run_id": args.run_id,
            }
        )
        try:
            papers, atom = arxiv_search(client, QUERIES["arxiv"], args.retmax)
            (raw_arxiv / "query_atom.xml").write_bytes(atom)
            print(f"  arxiv: {len(papers)} works")
            for p in papers:
                p["domain"] = args.domain
                p["report_date"] = args.report_date
                p["run_id"] = args.run_id
                p["ingested_at"] = utc_now_iso()
                p["authors_joined"] = "; ".join(p.get("authors") or [])
                p["author_count"] = len(p.get("authors") or [])
                works_rows.append(p)
                write_html_card(html_dir / f"{slug(p['paper_id'])}.html", p)
                # per-id stub xml
                (raw_arxiv / f"{slug(p['external_id'])}.xml").write_text(
                    f'<?xml version="1.0"?><entry id="{html_lib.escape(p["external_id"])}">'
                    f"<title>{html_lib.escape(p['title'])}</title></entry>\n",
                    encoding="utf-8",
                )
        except Exception as e:
            fail_rows.append(
                {
                    "unit_id": unit,
                    "source": "arxiv",
                    "error": str(e),
                    "domain": args.domain,
                    "report_date": args.report_date,
                    "run_id": args.run_id,
                }
            )
            print(f"  arxiv FAIL: {e}", file=sys.stderr)

    # ── write line-oriented tables ───────────────────────────────────────
    def dump_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    dump_jsonl(run_dir / "plan.jsonl", plan_rows)
    dump_jsonl(run_dir / "works.jsonl", works_rows)
    dump_jsonl(run_dir / "failures.jsonl", fail_rows)
    dump_jsonl(run_dir / "siteinfo.jsonl", site_rows)

    # manifest for humans / agents
    manifest = {
        "domain": args.domain,
        "report_date": args.report_date,
        "run_id": args.run_id,
        "topic": "semiconductors + machine learning / neural networks",
        "sources": ["pubmed", "crossref", "arxiv"],
        "journals_crossref": CROSSREF_JOURNALS,
        "counts": {
            "plan": len(plan_rows),
            "works": len(works_rows),
            "failures": len(fail_rows),
            "siteinfo": len(site_rows),
        },
        "email": args.email,
        "generated_at": utc_now_iso(),
        "tool": TOOL,
        "notes": [
            "Google Scholar is not used (no public API; scraping disallowed).",
            "PubMed ≤3 req/s; arXiv courtesy delay ~3s; Crossref polite mailto pool.",
        ],
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # pointer for rbt convenience
    pointer = {
        "domain": args.domain,
        "report_date": args.report_date,
        "run_id": args.run_id,
        "run_dir": str(run_dir.relative_to(root)),
    }
    (root / "lake" / "lz" / "LATEST_RUN.json").write_text(
        json.dumps(pointer, indent=2), encoding="utf-8"
    )

    print(
        f"[fetch_bronze] done works={len(works_rows)} failures={len(fail_rows)} "
        f"plan={len(plan_rows)} → {run_dir}"
    )
    print(
        f"[fetch_bronze] rbt vars: --var domain={args.domain} "
        f"--var report_date={args.report_date} --var run_id={args.run_id}"
    )
    return 0 if works_rows or args.skip_network else 1


if __name__ == "__main__":
    sys.exit(main())

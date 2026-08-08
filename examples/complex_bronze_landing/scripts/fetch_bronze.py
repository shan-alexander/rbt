#!/usr/bin/env python3
"""
Polite multi-source bronze lander — research-papers mini lakehouse.

Theme: a small lab's *research intelligence* landing zone for literature at the
intersection of **semiconductors / neuromorphic AI** and **AI in agritech**
(crop sensing, precision ag, edge devices).

Public APIs only (NOT Google Scholar / Examine.com — no free public papers API):

  • PubMed E-utilities (NCBI)     — esearch JSON + efetch XML     ≤3 req/s
  • Crossref REST                — JSON (mailto polite pool)
  • Europe PMC REST              — JSON core metadata + abstracts
  • arXiv Atom API               — Atom XML  (seed fixture if rate-limited)
  • OpenAlex REST                — JSON works (mailto polite pool)
  • Semantic Scholar Graph API   — JSON papers (seed if 429; optional S2_API_KEY)

Writes hive-partitioned bronze under:
  lake/lz/runs/domain=<domain>/report_date=<YYYY-MM-DD>/run_id=<id>/

Artifacts (mixed filetypes for rbt path_glob demos):
  plan.jsonl           — planned work units (source × query slice)
  works.jsonl          — normalized papers (title, abstract, authors[], …)
  failures.jsonl       — timeouts / HTTP / rate limits (partial-run honesty)
  siteinfo.jsonl       — portal inventory + robots fetch status
  assets.jsonl         — every landed file (path, kind, bytes, mime)
  manifest.json
  robots/<host>.txt
  raw/pubmed/*.xml
  raw/crossref/*.json
  raw/europepmc/*.json
  raw/openalex/*.json
  raw/semanticscholar/*.json
  raw/arxiv/*.{xml,atom}
  html/*.html          — lightweight cards (title / authors / abstract body)

Usage:
  python3 scripts/fetch_bronze.py --email you@example.com
  python3 scripts/fetch_bronze.py --retmax 5 --topic both
  RBT_BRONZE_EMAIL=you@example.com python3 scripts/fetch_bronze.py
"""

from __future__ import annotations

import argparse
import html as html_lib
import json
import mimetypes
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
USER_AGENT = f"{TOOL}/0.8 (+https://github.com/shan-alexander/rbt; mailto={{email}})"

SLEEP_NCBI = 0.40
SLEEP_CROSSREF = 0.30
SLEEP_EPMC = 0.35
SLEEP_OPENALEX = 0.25
SLEEP_S2 = 1.2  # unauthenticated S2 is heavily shared; be gentle
SLEEP_ARXIV = 3.5
SLEEP_ROBOTS = 0.30

# Dual research theme — lab monitors both tracks in one bronze domain.
TOPIC_QUERIES: dict[str, dict[str, str]] = {
    "semicon": {
        "pubmed": (
            '("semiconductor"[Title/Abstract] OR "wide bandgap"[Title/Abstract] '
            'OR neuromorphic[Title/Abstract]) AND ("machine learning"[Title/Abstract] '
            'OR "deep learning"[Title/Abstract] OR "neural network"[Title/Abstract])'
        ),
        "crossref": "semiconductor machine learning neuromorphic",
        "europepmc": (
            '(TITLE:"semiconductor" OR TITLE:neuromorphic OR ABSTRACT:"wide bandgap") '
            'AND (TITLE:"machine learning" OR TITLE:"deep learning" OR TITLE:"neural network")'
        ),
        "openalex": "semiconductor machine learning neuromorphic",
        "semanticscholar": "semiconductor machine learning neuromorphic",
        "arxiv": (
            'all:"semiconductor" AND (all:"machine learning" OR all:"neural network" '
            'OR all:neuromorphic)'
        ),
    },
    "agritech": {
        "pubmed": (
            '("precision agriculture"[Title/Abstract] OR "crop"[Title/Abstract] '
            'OR agritech[Title/Abstract] OR "plant disease"[Title/Abstract]) '
            'AND ("machine learning"[Title/Abstract] OR "deep learning"[Title/Abstract] '
            'OR "computer vision"[Title/Abstract])'
        ),
        "crossref": "precision agriculture machine learning crop",
        "europepmc": (
            '(TITLE:"precision agriculture" OR TITLE:"crop yield" OR TITLE:"plant disease" '
            'OR TITLE:agritech) AND (TITLE:"machine learning" OR TITLE:"deep learning")'
        ),
        "openalex": "precision agriculture deep learning crop",
        "semanticscholar": "precision agriculture deep learning crop sensing",
        "arxiv": (
            'all:"precision agriculture" OR (all:crop AND all:"machine learning") '
            'OR (all:agriculture AND all:"deep learning")'
        ),
    },
}

# Crossref venue slices: 2 semiconductor + 2 agritech-adjacent journals
CROSSREF_JOURNALS_SEMICON = [
    "Nature Electronics",
    "IEEE Transactions on Electron Devices",
    "Applied Physics Letters",
]
CROSSREF_JOURNALS_AGRITECH = [
    "Computers and Electronics in Agriculture",
    "Precision Agriculture",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def slug(s: str, max_len: int = 80) -> str:
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s.strip())
    return s[:max_len] or "x"


def authors_joined(authors: list[str]) -> str:
    return "; ".join(a for a in authors if a)


def enrich_paper(p: dict[str, Any], *, domain: str, report_date: str, run_id: str) -> dict[str, Any]:
    authors = p.get("authors") or []
    if not isinstance(authors, list):
        authors = [str(authors)]
    abstract = p.get("abstract") or ""
    p = dict(p)
    p["authors"] = authors
    p["authors_joined"] = authors_joined(authors)
    p["authors_json"] = json.dumps(authors, ensure_ascii=False)
    p["author_count"] = len(authors)
    p["abstract_chars"] = len(abstract)
    p["has_abstract"] = bool(abstract.strip())
    p["keywords_joined"] = p.get("keywords_joined") or ""
    p["domain"] = domain
    p["report_date"] = report_date
    p["run_id"] = run_id
    p["ingested_at"] = utc_now_iso()
    return p


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
        except TimeoutError as e:
            return 0, str(e).encode(), ""


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
        "file_name": path.name,
        "fetched_at": utc_now_iso(),
    }


def write_html_card(path: Path, paper: dict[str, Any]) -> None:
    authors = paper.get("authors") or []
    authors_html = ", ".join(html_lib.escape(a) for a in authors)
    title = html_lib.escape(paper.get("title") or "")
    abstract = html_lib.escape(paper.get("abstract") or "")
    doi = html_lib.escape(paper.get("doi") or "")
    source = html_lib.escape(paper.get("source") or "")
    venue = html_lib.escape(paper.get("venue") or "")
    year = html_lib.escape(str(paper.get("year") or ""))
    keywords = html_lib.escape(paper.get("keywords_joined") or "")
    doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>{title}</title>
  <meta name="generator" content="{TOOL}"/>
  <meta name="citation_title" content="{title}"/>
  <meta name="citation_doi" content="{doi}"/>
</head>
<body>
  <article data-source="{source}" data-doi="{doi}" data-year="{year}">
    <header>
      <h1 class="title">{title}</h1>
      <p class="authors">{authors_html}</p>
      <p class="venue">{venue} ({year})</p>
      <p class="keywords">{keywords}</p>
    </header>
    <section class="abstract">
      <h2>Abstract</h2>
      <p class="body">{abstract}</p>
    </section>
  </article>
</body>
</html>
"""
    path.write_text(doc, encoding="utf-8")


def record_asset(
    assets: list[dict[str, Any]],
    *,
    run_dir: Path,
    path: Path,
    kind: str,
    source: str,
    related_id: str = "",
    domain: str,
    report_date: str,
    run_id: str,
) -> None:
    try:
        rel = str(path.relative_to(run_dir))
    except ValueError:
        rel = str(path)
    mime, _ = mimetypes.guess_type(path.name)
    if mime is None:
        if path.suffix.lower() in {".xml", ".atom"}:
            mime = "application/xml"
        elif path.suffix.lower() == ".jsonl":
            mime = "application/x-ndjson"
        elif path.suffix.lower() == ".json":
            mime = "application/json"
        elif path.suffix.lower() == ".txt":
            mime = "text/plain"
        elif path.suffix.lower() in {".html", ".htm"}:
            mime = "text/html"
        else:
            mime = "application/octet-stream"
    size = path.stat().st_size if path.is_file() else 0
    assets.append(
        {
            "asset_id": f"{kind}:{rel}",
            "rel_path": rel,
            "kind": kind,
            "source": source,
            "related_id": related_id,
            "mime_type": mime,
            "bytes": size,
            "domain": domain,
            "report_date": report_date,
            "run_id": run_id,
        }
    )


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
            "sort": "relevance",
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
        mesh = [
            (mh.findtext("DescriptorName") or "").strip()
            for mh in medline.findall(".//MeshHeading")
        ]
        mesh = [m for m in mesh if m]
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
                "keywords_joined": "; ".join(mesh[:12]),
                "topic_track": "",
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
        "select": "DOI,title,author,abstract,container-title,published-print,published-online,URL,type,subject",
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
        subjects = it.get("subject") or []
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
                "keywords_joined": "; ".join(subjects[:12]) if subjects else "",
                "topic_track": "",
            }
        )
    return papers, body


# ── Europe PMC ───────────────────────────────────────────────────────────────

def europepmc_search(
    client: PoliteClient, query: str, page_size: int
) -> tuple[list[dict[str, Any]], bytes]:
    params = {
        "query": query,
        "resultType": "core",
        "format": "json",
        "pageSize": str(page_size),
    }
    url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search?" + urllib.parse.urlencode(
        params
    )
    code, body, _ = client.get(
        url, bucket="europepmc", delay=SLEEP_EPMC, accept="application/json"
    )
    if code != 200:
        raise RuntimeError(f"Europe PMC HTTP {code}: {body[:200]!r}")
    data = json.loads(body.decode("utf-8", errors="replace"))
    results = data.get("resultList", {}).get("result") or []
    papers: list[dict[str, Any]] = []
    for it in results:
        pmid = (it.get("pmid") or "").strip()
        doi = (it.get("doi") or "").strip()
        epmc_id = (it.get("id") or "").strip()
        src = (it.get("source") or "EPMC").strip()
        if pmid:
            paper_id = f"pmid:{pmid}"
            external_id = pmid
        elif doi:
            paper_id = f"doi:{doi}"
            external_id = doi
        else:
            paper_id = f"epmc:{src}:{epmc_id}"
            external_id = epmc_id
        author_string = (it.get("authorString") or "").strip()
        # "Last F, Last2 F2." → split on comma
        authors = [a.strip().rstrip(".") for a in author_string.split(",") if a.strip()]
        kw = it.get("keywordList", {}) or {}
        keywords = kw.get("keyword") if isinstance(kw, dict) else None
        if isinstance(keywords, str):
            keywords = [keywords]
        keywords = keywords or []
        papers.append(
            {
                "paper_id": paper_id,
                "source": "europepmc",
                "external_id": external_id,
                "doi": doi,
                "title": (it.get("title") or "").strip(),
                "abstract": (it.get("abstractText") or "").strip(),
                "authors": authors,
                "venue": (it.get("journalTitle") or it.get("bookOrReportDetails") or "")
                if isinstance(it.get("bookOrReportDetails"), str)
                else (it.get("journalTitle") or ""),
                "year": str(it.get("pubYear") or ""),
                "url": (
                    f"https://europepmc.org/article/{src}/{epmc_id}"
                    if epmc_id
                    else (f"https://doi.org/{doi}" if doi else "")
                ),
                "keywords_joined": "; ".join(keywords[:12]),
                "topic_track": "",
            }
        )
    return papers, body


# ── OpenAlex ─────────────────────────────────────────────────────────────────

def openalex_search(
    client: PoliteClient, query: str, per_page: int
) -> tuple[list[dict[str, Any]], bytes]:
    params = {
        "search": query,
        "per_page": str(per_page),
        "mailto": client.email,
    }
    url = "https://api.openalex.org/works?" + urllib.parse.urlencode(params)
    code, body, _ = client.get(
        url, bucket="openalex", delay=SLEEP_OPENALEX, accept="application/json"
    )
    if code != 200:
        raise RuntimeError(f"OpenAlex HTTP {code}: {body[:200]!r}")
    data = json.loads(body.decode("utf-8", errors="replace"))
    papers: list[dict[str, Any]] = []
    for it in data.get("results") or []:
        oa_id = (it.get("id") or "").rstrip("/").rsplit("/", 1)[-1]
        doi_url = it.get("doi") or ""
        doi = doi_url.replace("https://doi.org/", "").strip() if doi_url else ""
        authors = []
        for a in it.get("authorships") or []:
            name = ((a.get("author") or {}).get("display_name") or "").strip()
            if name:
                authors.append(name)
        # Reconstruct abstract from inverted index when present
        abstract = ""
        inv = it.get("abstract_inverted_index")
        if isinstance(inv, dict) and inv:
            positions: list[tuple[int, str]] = []
            for word, idxs in inv.items():
                for i in idxs:
                    positions.append((i, word))
            positions.sort(key=lambda x: x[0])
            abstract = " ".join(w for _, w in positions)
        venue = ""
        pl = it.get("primary_location") or {}
        src = pl.get("source") or {}
        if isinstance(src, dict):
            venue = (src.get("display_name") or "").strip()
        concepts = [
            (c.get("display_name") or "").strip()
            for c in (it.get("concepts") or [])[:12]
            if (c.get("display_name") or "").strip()
        ]
        papers.append(
            {
                "paper_id": f"openalex:{oa_id}" if oa_id else f"doi:{doi}",
                "source": "openalex",
                "external_id": oa_id or doi,
                "doi": doi,
                "title": (it.get("display_name") or it.get("title") or "").strip(),
                "abstract": abstract,
                "authors": authors,
                "venue": venue,
                "year": str(it.get("publication_year") or ""),
                "url": it.get("id") or (f"https://doi.org/{doi}" if doi else ""),
                "keywords_joined": "; ".join(concepts),
                "topic_track": "",
            }
        )
    return papers, body


# ── Semantic Scholar ─────────────────────────────────────────────────────────

def parse_semanticscholar_payload(data: dict[str, Any]) -> list[dict[str, Any]]:
    papers: list[dict[str, Any]] = []
    for it in data.get("data") or []:
        paper_key = (it.get("paperId") or "").strip()
        ext = it.get("externalIds") or {}
        doi = (ext.get("DOI") or "").strip()
        if paper_key:
            paper_id = f"s2:{paper_key}"
            external_id = paper_key
        elif doi:
            paper_id = f"doi:{doi}"
            external_id = doi
        else:
            continue
        authors = [
            (a.get("name") or "").strip()
            for a in (it.get("authors") or [])
            if (a.get("name") or "").strip()
        ]
        venue = (it.get("venue") or "").strip()
        if not venue:
            pv = it.get("publicationVenue") or {}
            if isinstance(pv, dict):
                venue = (pv.get("name") or "").strip()
        fields = it.get("fieldsOfStudy") or []
        if isinstance(fields, list):
            keywords = [str(f) for f in fields if f]
        else:
            keywords = []
        papers.append(
            {
                "paper_id": paper_id,
                "source": "semanticscholar",
                "external_id": external_id,
                "doi": doi,
                "title": (it.get("title") or "").strip(),
                "abstract": (it.get("abstract") or "").strip(),
                "authors": authors,
                "venue": venue or "Semantic Scholar",
                "year": str(it.get("year") or ""),
                "url": (it.get("url") or "").strip()
                or (f"https://www.semanticscholar.org/paper/{paper_key}" if paper_key else ""),
                "keywords_joined": "; ".join(keywords[:12]),
                "topic_track": "",
            }
        )
    return papers


def semanticscholar_search(
    client: PoliteClient, query: str, limit: int, api_key: str | None
) -> tuple[list[dict[str, Any]], bytes]:
    params = {
        "query": query,
        "limit": str(limit),
        "fields": "title,abstract,authors,year,externalIds,url,venue,publicationVenue,fieldsOfStudy",
    }
    url = "https://api.semanticscholar.org/graph/v1/paper/search?" + urllib.parse.urlencode(
        params
    )
    # Optional partner key via env; still throttle
    headers_accept = "application/json"
    # PoliteClient does not take extra headers; use raw get after throttle via client.get
    # Inject key by temporarily wrapping — use urllib with key if present
    client._throttle("s2", SLEEP_S2)
    hdrs = {
        "User-Agent": client.ua,
        "From": client.email,
        "Accept": headers_accept,
    }
    if api_key:
        hdrs["x-api-key"] = api_key
    req = urllib.request.Request(url, headers=hdrs, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=45.0) as resp:
            body = resp.read()
            code = resp.status
    except urllib.error.HTTPError as e:
        body = e.read() if e.fp else b""
        code = e.code
    except Exception as e:
        raise RuntimeError(f"Semantic Scholar error: {e}") from e
    if code != 200:
        raise RuntimeError(f"Semantic Scholar HTTP {code}: {body[:200]!r}")
    data = json.loads(body.decode("utf-8", errors="replace"))
    return parse_semanticscholar_payload(data), body


def load_semanticscholar_seed(seed_path: Path) -> tuple[list[dict[str, Any]], bytes]:
    body = seed_path.read_bytes()
    data = json.loads(body.decode("utf-8"))
    return parse_semanticscholar_payload(data), body


# ── arXiv ────────────────────────────────────────────────────────────────────

ARXIV_NS = {"a": "http://www.w3.org/2005/Atom"}


def parse_arxiv_atom(body: bytes) -> list[dict[str, Any]]:
    root = ET.fromstring(body)
    papers: list[dict[str, Any]] = []
    for entry in root.findall("a:entry", ARXIV_NS):
        id_url = (entry.findtext("a:id", default="", namespaces=ARXIV_NS) or "").strip()
        arxiv_id = id_url.rsplit("/", 1)[-1]
        title = " ".join(
            (entry.findtext("a:title", default="", namespaces=ARXIV_NS) or "").split()
        )
        abstract = " ".join(
            (entry.findtext("a:summary", default="", namespaces=ARXIV_NS) or "").split()
        )
        authors = [
            (au.findtext("a:name", default="", namespaces=ARXIV_NS) or "").strip()
            for au in entry.findall("a:author", ARXIV_NS)
        ]
        authors = [a for a in authors if a]
        published = entry.findtext("a:published", default="", namespaces=ARXIV_NS) or ""
        year = published[:4] if published else ""
        cats = [
            (c.get("term") or "")
            for c in entry.findall("a:category", ARXIV_NS)
            if c.get("term")
        ]
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
                "keywords_joined": "; ".join(cats[:12]),
                "topic_track": "",
            }
        )
    return papers


def arxiv_search(
    client: PoliteClient, query: str, max_results: int
) -> tuple[list[dict[str, Any]], bytes]:
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
    return parse_arxiv_atom(body), body


def load_arxiv_seed(seed_path: Path) -> tuple[list[dict[str, Any]], bytes]:
    body = seed_path.read_bytes()
    return parse_arxiv_atom(body), body


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--project-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="complex_bronze_landing project root",
    )
    ap.add_argument("--email", default=DEFAULT_EMAIL, help="Contact email for polite API pools")
    ap.add_argument(
        "--domain",
        default="ai-semicon-agritech",
        help="Hive partition domain (research intelligence lake)",
    )
    ap.add_argument("--report-date", default=date.today().isoformat())
    ap.add_argument(
        "--run-id", default=datetime.now(timezone.utc).strftime("run%Y%m%dT%H%M%SZ")
    )
    ap.add_argument("--retmax", type=int, default=5, help="Max papers per source/topic slice")
    ap.add_argument(
        "--topic",
        choices=["both", "semicon", "agritech"],
        default="both",
        help="Which research tracks to land",
    )
    ap.add_argument(
        "--seed-on-fail",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use scripts/fixtures/arxiv_seed_atom.xml when live arXiv fails (default: on)",
    )
    ap.add_argument("--skip-network", action="store_true", help="Only write empty scaffolds")
    args = ap.parse_args()

    if args.email.endswith("@example.com"):
        print(
            "warning: using placeholder email; set --email or RBT_BRONZE_EMAIL for polite pools",
            file=sys.stderr,
        )

    tracks = ["semicon", "agritech"] if args.topic == "both" else [args.topic]
    journals: list[tuple[str, str]] = []  # (journal, track)
    if "semicon" in tracks:
        for j in CROSSREF_JOURNALS_SEMICON:
            journals.append((j, "semicon"))
    if "agritech" in tracks:
        for j in CROSSREF_JOURNALS_AGRITECH:
            journals.append((j, "agritech"))

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
    raw_epmc = run_dir / "raw" / "europepmc"
    raw_openalex = run_dir / "raw" / "openalex"
    raw_s2 = run_dir / "raw" / "semanticscholar"
    raw_arxiv = run_dir / "raw" / "arxiv"
    robots_dir = run_dir / "robots"
    html_dir = run_dir / "html"
    for d in (
        raw_pubmed,
        raw_crossref,
        raw_epmc,
        raw_openalex,
        raw_s2,
        raw_arxiv,
        robots_dir,
        html_dir,
    ):
        d.mkdir(parents=True, exist_ok=True)
    s2_api_key = os.environ.get("S2_API_KEY") or os.environ.get("SEMANTIC_SCHOLAR_API_KEY")

    client = PoliteClient(args.email)
    plan_rows: list[dict[str, Any]] = []
    works_rows: list[dict[str, Any]] = []
    fail_rows: list[dict[str, Any]] = []
    site_rows: list[dict[str, Any]] = []
    assets: list[dict[str, Any]] = []
    seen_paper_ids: set[str] = set()

    origins = [
        ("https://www.ncbi.nlm.nih.gov", "portal"),
        ("https://api.crossref.org", "api"),
        ("https://www.ebi.ac.uk", "api"),
        ("https://api.openalex.org", "api"),
        ("https://api.semanticscholar.org", "api"),
        ("https://export.arxiv.org", "api"),
        ("https://arxiv.org", "portal"),
        ("https://europepmc.org", "portal"),
        # Examine.com: commercial nutrition evidence API — not a free papers source
        ("https://api.examine.com", "api_commercial"),
    ]

    print(f"[fetch_bronze] run_dir={run_dir}")
    print(f"[fetch_bronze] email={args.email} retmax={args.retmax} topic={args.topic}")
    print(f"[fetch_bronze] tracks={tracks}")

    def add_work(p: dict[str, Any], track: str) -> None:
        p = enrich_paper(
            p, domain=args.domain, report_date=args.report_date, run_id=args.run_id
        )
        p["topic_track"] = track
        # Dedup across sources/tracks by paper_id (prefer first landing)
        if p["paper_id"] in seen_paper_ids:
            return
        seen_paper_ids.add(p["paper_id"])
        works_rows.append(p)
        card = html_dir / f"{slug(p['paper_id'])}.html"
        write_html_card(card, p)
        record_asset(
            assets,
            run_dir=run_dir,
            path=card,
            kind="html_card",
            source=p["source"],
            related_id=p["paper_id"],
            domain=args.domain,
            report_date=args.report_date,
            run_id=args.run_id,
        )

    # robots.txt
    for origin, role in origins:
        try:
            info = fetch_robots(client, origin, robots_dir)
            site_rows.append(
                {
                    "site_id": info["host"],
                    "origin": origin,
                    "role": role,
                    "robots_status": info["http_status"],
                    "robots_bytes": info["bytes"],
                    "domain": args.domain,
                    "report_date": args.report_date,
                    "run_id": args.run_id,
                    "fetched_at": info["fetched_at"],
                }
            )
            rpath = robots_dir / info["file_name"]
            record_asset(
                assets,
                run_dir=run_dir,
                path=rpath,
                kind="robots_txt",
                source="robots",
                related_id=info["host"],
                domain=args.domain,
                report_date=args.report_date,
                run_id=args.run_id,
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
        print("skip-network set; writing scaffolds only")
    else:
        # ── PubMed (per track) ───────────────────────────────────────────
        for track in tracks:
            unit = f"pubmed:{track}"
            query = TOPIC_QUERIES[track]["pubmed"]
            plan_rows.append(
                {
                    "unit_id": unit,
                    "source": "pubmed",
                    "query": query,
                    "venue_filter": "",
                    "topic_track": track,
                    "planned": True,
                    "domain": args.domain,
                    "report_date": args.report_date,
                    "run_id": args.run_id,
                }
            )
            try:
                pmids = pubmed_search(client, query, args.retmax)
                print(f"  pubmed[{track}]: {len(pmids)} PMIDs")
                if pmids:
                    xml_bytes = pubmed_fetch_xml(client, pmids)
                    batch = raw_pubmed / f"batch_efetch_{track}.xml"
                    batch.write_bytes(xml_bytes)
                    record_asset(
                        assets,
                        run_dir=run_dir,
                        path=batch,
                        kind="pubmed_xml_batch",
                        source="pubmed",
                        related_id=unit,
                        domain=args.domain,
                        report_date=args.report_date,
                        run_id=args.run_id,
                    )
                    papers = parse_pubmed_articles(xml_bytes)
                    root_xml = ET.fromstring(xml_bytes)
                    for art in root_xml.findall(".//PubmedArticle"):
                        pmid_el = art.find(".//PMID")
                        if pmid_el is not None and pmid_el.text:
                            frag = ET.tostring(art, encoding="utf-8")
                            ppath = raw_pubmed / f"{pmid_el.text}.xml"
                            ppath.write_bytes(b'<?xml version="1.0"?>\n' + frag)
                            record_asset(
                                assets,
                                run_dir=run_dir,
                                path=ppath,
                                kind="pubmed_xml",
                                source="pubmed",
                                related_id=f"pmid:{pmid_el.text}",
                                domain=args.domain,
                                report_date=args.report_date,
                                run_id=args.run_id,
                            )
                    for p in papers:
                        add_work(p, track)
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
                print(f"  pubmed[{track}] FAIL: {e}", file=sys.stderr)

        # ── Crossref (per journal slice) ─────────────────────────────────
        per_journal = max(2, args.retmax // 2)
        for journal, track in journals:
            unit = f"crossref:{slug(journal)}"
            query = TOPIC_QUERIES[track]["crossref"]
            plan_rows.append(
                {
                    "unit_id": unit,
                    "source": "crossref",
                    "query": query,
                    "venue_filter": journal,
                    "topic_track": track,
                    "planned": True,
                    "domain": args.domain,
                    "report_date": args.report_date,
                    "run_id": args.run_id,
                }
            )
            try:
                papers, raw = crossref_search(client, query, journal, per_journal)
                raw_path = raw_crossref / f"{slug(journal)}.json"
                raw_path.write_bytes(raw)
                record_asset(
                    assets,
                    run_dir=run_dir,
                    path=raw_path,
                    kind="crossref_json",
                    source="crossref",
                    related_id=unit,
                    domain=args.domain,
                    report_date=args.report_date,
                    run_id=args.run_id,
                )
                print(f"  crossref [{journal}]: {len(papers)} works")
                for p in papers:
                    p["venue_filter"] = journal
                    add_work(p, track)
                    if p.get("doi"):
                        meta = raw_crossref / f"{slug(p['doi'])}.meta.json"
                        meta.write_text(
                            json.dumps(
                                {"doi": p["doi"], "batch": raw_path.name, "track": track},
                                indent=2,
                            ),
                            encoding="utf-8",
                        )
                        record_asset(
                            assets,
                            run_dir=run_dir,
                            path=meta,
                            kind="crossref_meta_json",
                            source="crossref",
                            related_id=p["paper_id"],
                            domain=args.domain,
                            report_date=args.report_date,
                            run_id=args.run_id,
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

        # ── Europe PMC (per track) ───────────────────────────────────────
        for track in tracks:
            unit = f"europepmc:{track}"
            query = TOPIC_QUERIES[track]["europepmc"]
            plan_rows.append(
                {
                    "unit_id": unit,
                    "source": "europepmc",
                    "query": query,
                    "venue_filter": "",
                    "topic_track": track,
                    "planned": True,
                    "domain": args.domain,
                    "report_date": args.report_date,
                    "run_id": args.run_id,
                }
            )
            try:
                papers, raw = europepmc_search(client, query, args.retmax)
                raw_path = raw_epmc / f"search_{track}.json"
                raw_path.write_bytes(raw)
                record_asset(
                    assets,
                    run_dir=run_dir,
                    path=raw_path,
                    kind="europepmc_json",
                    source="europepmc",
                    related_id=unit,
                    domain=args.domain,
                    report_date=args.report_date,
                    run_id=args.run_id,
                )
                print(f"  europepmc[{track}]: {len(papers)} works")
                for p in papers:
                    add_work(p, track)
            except Exception as e:
                fail_rows.append(
                    {
                        "unit_id": unit,
                        "source": "europepmc",
                        "error": str(e),
                        "domain": args.domain,
                        "report_date": args.report_date,
                        "run_id": args.run_id,
                    }
                )
                print(f"  europepmc[{track}] FAIL: {e}", file=sys.stderr)

        # ── OpenAlex (per track) ─────────────────────────────────────────
        for track in tracks:
            unit = f"openalex:{track}"
            query = TOPIC_QUERIES[track]["openalex"]
            plan_rows.append(
                {
                    "unit_id": unit,
                    "source": "openalex",
                    "query": query,
                    "venue_filter": "",
                    "topic_track": track,
                    "planned": True,
                    "domain": args.domain,
                    "report_date": args.report_date,
                    "run_id": args.run_id,
                }
            )
            try:
                papers, raw = openalex_search(client, query, args.retmax)
                raw_path = raw_openalex / f"search_{track}.json"
                raw_path.write_bytes(raw)
                record_asset(
                    assets,
                    run_dir=run_dir,
                    path=raw_path,
                    kind="openalex_json",
                    source="openalex",
                    related_id=unit,
                    domain=args.domain,
                    report_date=args.report_date,
                    run_id=args.run_id,
                )
                print(f"  openalex[{track}]: {len(papers)} works")
                for p in papers:
                    add_work(p, track)
            except Exception as e:
                fail_rows.append(
                    {
                        "unit_id": unit,
                        "source": "openalex",
                        "error": str(e),
                        "domain": args.domain,
                        "report_date": args.report_date,
                        "run_id": args.run_id,
                    }
                )
                print(f"  openalex[{track}] FAIL: {e}", file=sys.stderr)

        # ── Semantic Scholar (per track; seed fallback) ──────────────────
        s2_seed = Path(__file__).resolve().parent / "fixtures" / "semanticscholar_seed.json"
        for track in tracks:
            unit = f"semanticscholar:{track}"
            query = TOPIC_QUERIES[track]["semanticscholar"]
            plan_rows.append(
                {
                    "unit_id": unit,
                    "source": "semanticscholar",
                    "query": query,
                    "venue_filter": "",
                    "topic_track": track,
                    "planned": True,
                    "domain": args.domain,
                    "report_date": args.report_date,
                    "run_id": args.run_id,
                }
            )
            try:
                papers, raw = semanticscholar_search(
                    client, query, args.retmax, s2_api_key
                )
                raw_path = raw_s2 / f"search_{track}.json"
                raw_path.write_bytes(raw)
                record_asset(
                    assets,
                    run_dir=run_dir,
                    path=raw_path,
                    kind="semanticscholar_json",
                    source="semanticscholar",
                    related_id=unit,
                    domain=args.domain,
                    report_date=args.report_date,
                    run_id=args.run_id,
                )
                print(f"  semanticscholar[{track}]: {len(papers)} works (live)")
                for p in papers:
                    add_work(p, track)
            except Exception as e:
                print(f"  semanticscholar[{track}] FAIL: {e}", file=sys.stderr)
                if args.seed_on_fail and s2_seed.is_file():
                    try:
                        papers, raw = load_semanticscholar_seed(s2_seed)
                        raw_path = raw_s2 / f"seed_{track}.json"
                        raw_path.write_bytes(raw)
                        record_asset(
                            assets,
                            run_dir=run_dir,
                            path=raw_path,
                            kind="semanticscholar_json_seed",
                            source="semanticscholar",
                            related_id=unit,
                            domain=args.domain,
                            report_date=args.report_date,
                            run_id=args.run_id,
                        )
                        for p in papers:
                            p["venue"] = (p.get("venue") or "Semantic Scholar") + " (seed fixture)"
                            add_work(p, track)
                        print(
                            f"  semanticscholar[{track}]: {len(papers)} works from seed "
                            f"({s2_seed.name})"
                        )
                    except Exception as e2:
                        fail_rows.append(
                            {
                                "unit_id": unit,
                                "source": "semanticscholar",
                                "error": f"{e}; seed also failed: {e2}",
                                "domain": args.domain,
                                "report_date": args.report_date,
                                "run_id": args.run_id,
                            }
                        )
                else:
                    fail_rows.append(
                        {
                            "unit_id": unit,
                            "source": "semanticscholar",
                            "error": str(e),
                            "domain": args.domain,
                            "report_date": args.report_date,
                            "run_id": args.run_id,
                        }
                    )

        # ── arXiv (per track; seed fallback) ─────────────────────────────
        seed_path = Path(__file__).resolve().parent / "fixtures" / "arxiv_seed_atom.xml"
        for track in tracks:
            unit = f"arxiv:{track}"
            query = TOPIC_QUERIES[track]["arxiv"]
            plan_rows.append(
                {
                    "unit_id": unit,
                    "source": "arxiv",
                    "query": query,
                    "venue_filter": "",
                    "topic_track": track,
                    "planned": True,
                    "domain": args.domain,
                    "report_date": args.report_date,
                    "run_id": args.run_id,
                }
            )
            try:
                papers, atom = arxiv_search(client, query, args.retmax)
                atom_path = raw_arxiv / f"query_{track}.atom.xml"
                atom_path.write_bytes(atom)
                record_asset(
                    assets,
                    run_dir=run_dir,
                    path=atom_path,
                    kind="arxiv_atom",
                    source="arxiv",
                    related_id=unit,
                    domain=args.domain,
                    report_date=args.report_date,
                    run_id=args.run_id,
                )
                print(f"  arxiv[{track}]: {len(papers)} works (live)")
                for p in papers:
                    add_work(p, track)
                    stub = raw_arxiv / f"{slug(p['external_id'])}.xml"
                    stub.write_text(
                        f'<?xml version="1.0"?><entry id="{html_lib.escape(p["external_id"])}">'
                        f"<title>{html_lib.escape(p['title'])}</title>"
                        f"<summary>{html_lib.escape((p.get('abstract') or '')[:2000])}</summary>"
                        f"</entry>\n",
                        encoding="utf-8",
                    )
                    record_asset(
                        assets,
                        run_dir=run_dir,
                        path=stub,
                        kind="arxiv_entry_xml",
                        source="arxiv",
                        related_id=p["paper_id"],
                        domain=args.domain,
                        report_date=args.report_date,
                        run_id=args.run_id,
                    )
            except Exception as e:
                print(f"  arxiv[{track}] FAIL: {e}", file=sys.stderr)
                if args.seed_on_fail and seed_path.is_file():
                    try:
                        papers, atom = load_arxiv_seed(seed_path)
                        atom_path = raw_arxiv / f"seed_{track}.atom.xml"
                        atom_path.write_bytes(atom)
                        record_asset(
                            assets,
                            run_dir=run_dir,
                            path=atom_path,
                            kind="arxiv_atom_seed",
                            source="arxiv",
                            related_id=unit,
                            domain=args.domain,
                            report_date=args.report_date,
                            run_id=args.run_id,
                        )
                        # Tag seeded rows for honesty
                        for p in papers:
                            p["venue"] = "arXiv (seed fixture)"
                            add_work(p, track)
                        print(
                            f"  arxiv[{track}]: {len(papers)} works from seed fixture "
                            f"({seed_path.name})"
                        )
                    except Exception as e2:
                        fail_rows.append(
                            {
                                "unit_id": unit,
                                "source": "arxiv",
                                "error": f"{e}; seed also failed: {e2}",
                                "domain": args.domain,
                                "report_date": args.report_date,
                                "run_id": args.run_id,
                            }
                        )
                else:
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

    # Policy notes: commercial / no free papers API — always land failure rows
    # so partial-run / policy honesty is visible even when every free API succeeds.
    # (Also keeps stg_failures non-empty for ref() registration demos.)
    policy_skips = [
        (
            "robots:https://scholar.google.com",
            "skipped: Google Scholar has no public API; scraping disallowed (policy)",
        ),
        (
            "api:https://examine.com",
            "skipped: Examine.com is commercial nutrition evidence (request/API key); "
            "not a free academic papers source for this lake",
        ),
    ]
    for unit_id, err in policy_skips:
        if not any(r.get("unit_id") == unit_id for r in fail_rows):
            fail_rows.append(
                {
                    "unit_id": unit_id,
                    "source": "robots" if unit_id.startswith("robots:") else "policy",
                    "error": err,
                    "domain": args.domain,
                    "report_date": args.report_date,
                    "run_id": args.run_id,
                }
            )

    # ── write line-oriented tables ───────────────────────────────────────
    def dump_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        record_asset(
            assets,
            run_dir=run_dir,
            path=path,
            kind="jsonl_table",
            source="lander",
            related_id=path.name,
            domain=args.domain,
            report_date=args.report_date,
            run_id=args.run_id,
        )

    dump_jsonl(run_dir / "plan.jsonl", plan_rows)
    dump_jsonl(run_dir / "works.jsonl", works_rows)
    dump_jsonl(run_dir / "failures.jsonl", fail_rows)
    dump_jsonl(run_dir / "siteinfo.jsonl", site_rows)
    # assets last so jsonl tables are included; rewrite assets without double-count recursion
    dump_jsonl(run_dir / "assets.jsonl", assets)

    manifest = {
        "domain": args.domain,
        "report_date": args.report_date,
        "run_id": args.run_id,
        "topic": "AI × semiconductors / neuromorphic + AI in agritech",
        "topic_tracks": tracks,
        "sources": [
            "pubmed",
            "crossref",
            "europepmc",
            "openalex",
            "semanticscholar",
            "arxiv",
        ],
        "journals_crossref": [j for j, _ in journals],
        "counts": {
            "plan": len(plan_rows),
            "works": len(works_rows),
            "failures": len(fail_rows),
            "siteinfo": len(site_rows),
            "assets": len(assets),
        },
        "email": args.email,
        "generated_at": utc_now_iso(),
        "tool": TOOL,
        "notes": [
            "Google Scholar / Examine.com not used as free paper sources (policy failure rows).",
            "PubMed ≤3 req/s; arXiv ~3s + seed; Crossref/OpenAlex mailto; Europe PMC; S2 seed on 429.",
            "Authors land as JSON arrays in bronze; silver uses authors_json + authors_joined + author_count.",
            "Mixed bronze: jsonl, json, xml/atom, html, robots.txt — for path_glob demos.",
        ],
    }
    mpath = run_dir / "manifest.json"
    mpath.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

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
        f"plan={len(plan_rows)} assets={len(assets)} → {run_dir}"
    )
    print(
        f"[fetch_bronze] rbt vars: --var domain={args.domain} "
        f"--var report_date={args.report_date} --var run_id={args.run_id}"
    )
    return 0 if works_rows or args.skip_network else 1


if __name__ == "__main__":
    sys.exit(main())

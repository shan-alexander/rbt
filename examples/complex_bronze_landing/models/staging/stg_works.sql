---
description: >
  Silver stage endpoint: successful paper records from PubMed / Crossref / arXiv.
  Authors landed as authors_joined (utf8) + author_count for Arrow-friendly analytics;
  raw authors array remains in bronze JSONL for inspection.
source_format: jsonl
scan_path: $lake/lz/runs
path_glob: works.jsonl
partition_by: [domain, report_date, run_id]
columns:
  paper_id: { dtype: utf8 }
  source: { dtype: utf8 }
  external_id: { dtype: utf8 }
  doi: { dtype: utf8 }
  title: { dtype: utf8 }
  abstract: { dtype: utf8 }
  authors_joined: { dtype: utf8 }
  author_count: { dtype: int64 }
  venue: { dtype: utf8 }
  year: { dtype: utf8 }
  url: { dtype: utf8 }
  domain: { dtype: utf8 }
  report_date: { dtype: utf8 }
  run_id: { dtype: utf8 }
grain: [paper_id, report_date, run_id]
tests:
  not_null: [paper_id, source, title]
  unique: [paper_id, report_date, run_id]
  accepted_values:
    source: [pubmed, crossref, arxiv]
---
SELECT
  paper_id,
  source,
  external_id,
  doi,
  title,
  abstract,
  authors_joined,
  author_count,
  venue,
  year,
  url,
  domain,
  report_date,
  run_id
FROM {{ source('bronze', 'works') }}

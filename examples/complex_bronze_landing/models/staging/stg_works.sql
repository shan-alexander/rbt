---
description: >
  Silver stage endpoint: successful paper landings from PubMed / Crossref /
  Europe PMC / arXiv. Authors kept as authors_json (JSON array string) plus
  authors_joined and author_count for Arrow/DataFusion analytics.
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
  authors_json: { dtype: utf8 }
  author_count: { dtype: int64 }
  abstract_chars: { dtype: int64 }
  has_abstract: { dtype: bool }
  venue: { dtype: utf8 }
  year: { dtype: utf8 }
  url: { dtype: utf8 }
  keywords_joined: { dtype: utf8 }
  topic_track: { dtype: utf8 }
  domain: { dtype: utf8 }
  report_date: { dtype: utf8 }
  run_id: { dtype: utf8 }
grain: [paper_id, report_date, run_id]
tests:
  not_null: [paper_id, source, title]
  unique: [paper_id, report_date, run_id]
  accepted_values:
    # Resolved from rbt_project.yml contracts.enums (single registry)
    source: works.source
    topic_track: works.topic_track
---
SELECT
  paper_id,
  source,
  external_id,
  doi,
  title,
  abstract,
  authors_joined,
  authors_json,
  author_count,
  abstract_chars,
  has_abstract,
  venue,
  year,
  url,
  keywords_joined,
  topic_track,
  domain,
  report_date,
  run_id
FROM {{ source('bronze', 'works') }}

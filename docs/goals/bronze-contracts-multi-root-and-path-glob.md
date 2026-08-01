---
tags: [product, goal, bronze, paths, glob]
node_type: goal
aliases: [multi-root, path_glob, bronze contracts, roots]
---
# Bronze contracts multi-root and path_glob

**One-line:** Staging models declare real filesystem contracts — multi-root `$name` paths, path_glob with correct separator semantics, selective formats — without warehouse source theater.

## Goals

- Project **`roots`** map logical names to paths; resolve `$lake/...` and absolute targets consistently.
- **`path_glob`** uses path-aware matching (`literal_separator`); basename-only patterns when no `/`.
- Disable listing pushdown when globs require rbt-side filtering so DF does not over-read.
- Support bronze formats that matter for the niche: JSONL (jshift), CSV, Parquet, Arrow IPC, protobuf (with honest size limits).
- Frontmatter columns / grain / tests / partition_by as the contract surface for agents and humans.

## Non-goals

- Inventing proprietary lake product names in examples or docs.
- Full cloud inventory UIs; remote object store is a later materialize concern.

## Status

- Multi-root + path_glob + protobuf path shipped; see [docs/MULTI_ROOT_AND_PATH_GLOB.md](../MULTI_ROOT_AND_PATH_GLOB.md).
- Examples use `roots.lake` + `$lake/...` and staging `path_glob` for Arrow IPC.

## Related

- [[Primary path spine]]
- [[Product north star]]
- [[Memory-honest materialization]]
- [[Complex bronze landing zones]]
- Analysis: [[Bronze-to-silver maturity gap matrix]]

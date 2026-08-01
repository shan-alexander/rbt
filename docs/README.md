# Documentation index

| Doc | Role |
|-----|------|
| [../README.md](../README.md) | Install, CLI, project shape |
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | Priorities, Iceberg SoR policy, how to contribute |
| [../thesis.md](../thesis.md) | Product north star (wins over essays if they disagree) |
| [../CHANGELOG.md](../CHANGELOG.md) | Release notes |
| [PUBLISHING.md](PUBLISHING.md) | crates.io publish / yank notes |
| [CRATES_IO.md](CRATES_IO.md) | Why orphan crates exist; single-package policy |
| [STREAMING_MATERIALIZE_PLAN.md](STREAMING_MATERIALIZE_PLAN.md) | Stream materialize — Phases 1–3 shipped in **0.3.8**; further phases planned |
| [REF_STRATEGY.md](REF_STRATEGY.md) | `ref()` MemTable vs lake re-read config + bench tradeoffs |
| [MULTI_ROOT_AND_PATH_GLOB.md](MULTI_ROOT_AND_PATH_GLOB.md) | Absolute paths, `$roots`, `path_glob`, protobuf bronze, listing pushdown notes |
| [ICEBERG_SOR.md](ICEBERG_SOR.md) | P2 Iceberg catalog snapshot commit proof gate (0.4.0) |
| [P4_CAPABILITIES.md](P4_CAPABILITIES.md) | Measure packs, incremental_append, FS WAP, builtin UDFs (0.5.0) |
| [COMPLEX_BRONZE_AND_RUN_SCOPE.md](COMPLEX_BRONZE_AND_RUN_SCOPE.md) | P5a/P5b: run vars, on_missing empty, fingerprints, RunReceipt (0.6.0) |
| [GOLD_DEFAULT.md](GOLD_DEFAULT.md) | P6: parts sources, lineage stamps, relationships, env roots (0.7.0) |
| [../crates/rbt/benches/README.md](../crates/rbt/benches/README.md) | Criterion bench harness |

## ADRs (active)

| ADR | Topic |
|-----|--------|
| [adr/ADR_001_PROJECT_STRUCTURE.md](adr/ADR_001_PROJECT_STRUCTURE.md) | Layers, prefixes, frontmatter, zero-copy clone (planned) |
| [adr/ADR_002_THESIS_ALIGNMENT.md](adr/ADR_002_THESIS_ALIGNMENT.md) | Bronze edge, diagnostics, DX loop, star schema |
| [adr/ADR_003_UDF_RSMODELS.md](adr/ADR_003_UDF_RSMODELS.md) | SQL + UDFs and first-class Rust models (planned) |

## Archive

Historical / aspirational essays under [archive/](archive/). They may describe multi-crate layouts, WAP theater, or features not shipped. **Do not treat archive docs as current API.** Prefer thesis + CONTRIBUTING + code.

---
tags: [goal, index]
node_type: goal
---
# Goals index

Project goals live here as rustbrain `goal` notes. Bootstrap also harvests [[from-readme]] from the root README (algorithmic; edit freely).

## Product goals

| Goal | Intent |
|------|--------|
| [[Product north star]] | dbt-shaped medallion lakes in-process Rust |
| [[Honest product surface]] | Docs and claims match shipped code |
| [[Primary path spine]] | compile / run / test before polish (P0) |
| [[Memory-honest materialization]] | Stream + spill; lake-as-truth ref (P1) |
| [[Iceberg system of record]] | Official snapshot commit proof (P2) |
| [[Instant-feedback DX loop]] | validate / explain / preview (P3) |
| [[Measured claims before marketing]] | Measure packs before Spark claims (P4) |
| [[Bronze contracts multi-root and path_glob]] | Real path contracts on bronze |
| [[Honest incremental materialization]] | Append parts, not fake merge |
| [[Filesystem write-audit-publish]] | FS WAP without branch theater |
| [[Polyglot UDFs and Rust models]] | Design A now; Design B planned |
| [[Team-scale lake positioning]] | Team-scale lakes via fan-out; petabyte lakes as partitioned workers |
| [[Complex bronze landing zones]] | Multi-artifact Hive-ish bronze → honest silver |

## Maturity roadmap (rustbrain)

- Analysis: [[Bronze-to-silver maturity gap matrix]]
- Plan: [[Dual-track maturity roadmap]]

## How to work these notes

- Capture decisions that achieve a goal as ADRs under `docs/adr/`.
- After edits: `rustbrain sync` (or rely on `note new` default sync).
- Agent entry: root `AGENTS.md` + `rustbrain context "topic" --type goal,adr`.

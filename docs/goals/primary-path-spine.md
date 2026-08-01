---
tags: [product, goal, P0, spine]
node_type: goal
aliases: [P0, compile run test, primary path]
---
# Primary path spine

**One-line:** Project load → DAG compile → bronze registration → SQL models → materialize silver/gold must be boring and correct before polish.

## Goals

- Reliable `rbt compile` / `run` / `test` on smoke + full_e2e examples.
- Layer boundary enforcement (staging does not depend on marts; etc.).
- `--select` with ancestor inclusion on execute paths.
- Frontmatter-driven bronze contracts and model tests on the run path.
- Contributor rule: if polish lands while the spine is flaky, it is out of order.

## Non-goals

- Expanding into DX-only or multi-catalog work while the spine regresses.
- Drive-by refactors unrelated to spine stability.

## Status

- **Working** as of current main (smoke + full_e2e). Priority label historically **P0**.
- Continuous: every PR that touches engine/scan/core/CLI should keep `scripts/smoke.sh` green.

## Related

- [[Product north star]]
- [[Bronze contracts multi-root and path_glob]]
- [[Memory-honest materialization]]
- [[Instant-feedback DX loop]]
- ADRs: [[ADR-001 Project Layout]], [[ADR-002 Thesis Alignment]]
- Concept: [[Star schema data modeling rules]]

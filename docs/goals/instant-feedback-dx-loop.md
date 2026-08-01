---
tags: [product, goal, P3, DX, CLI]
node_type: goal
aliases: [P3, validate explain preview, DX verbs]
---
# Instant-feedback DX loop

**One-line:** Fail fast and show plans/samples before full materialization: `validate → explain → preview → run → test`.

## Goals

- **`rbt validate`** — static DAG, bronze contracts, refs; no full execute.
- **`rbt explain`** — compiled SQL, deps, bronze contract (and plan detail as available).
- **`rbt preview`** — LIMIT N sample rows; ancestors may materialize; target not published as production.
- Structured, agent-repairable errors (`E_RBT_*`, suggestions) over opaque engine dumps.
- Keep DX verbs subordinate to a working primary path (ship after / without regressing spine).

## Non-goals

- Prost/binary report formats before JSON report shape stabilizes.
- Pretending preview is free of IO when ancestors must run.
- Full warehouse-style IDE product surface.

## Status

- **Shipped** validate / explain / preview (**0.3.9**). Historical priority **P3**.
- Thesis §5 remains the UX contract for future schema-bind depth against Iceberg catalogs.

## Related

- [[Primary path spine]]
- [[Product north star]]
- [[Measured claims before marketing]]

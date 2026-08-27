---
tags: [adr, surrogate-key, grain, star-schema, RBT-A16]
node_type: adr
aliases: [ADR-009, surrogate keys, sk(), RBT-A16]
status: accepted
---
# ADR-009: First-class surrogate keys (hash SK + durable MIISK)

## Status

**Accepted** (amended 2026-08-27: productize MIISK)  
**Date:** 2026-08-27 · **Epic:** RBT-A16  
**Related:** [[rbt-datalake-feature-roadmap]] §9b · [[star-schema-data-modeling-rules]] ·
[[ADR-008 UDF host surface]]

## Context

Star-schema modeling needs stable surrogate keys for fact↔dim FK joins. Lake rebuilds should keep
**stable** SKs for the same natural grain. Hash SKs are pure/idempotent with no side state.
Classic **MIISK** (monotonically increasing integer SKs) is dramatically faster for joins and
narrower on facts, but needs a **durable assigner** the lake can own.

Users want:

- Blazing-fast Int64 SKs (MIISK / fast64) when appropriate.
- Safe defaults for large N.
- Ergonomic SQL (`sk(...)`) and optional frontmatter stamping.
- An **Unknown** member sentinel for dims (facts never emit NULL FKs).

## Decision

### 1. Identity vs surrogate

- Upsert / uniqueness match on **natural grain** (`grain` / `unique_key`) — never on SK.
- SK is optional for FK assembly only (hash of grain **or** durable integer assignment).

### 2. Algorithms (user-selectable)

| Name (aliases) | Output | Comfortable distinct N | Role |
|----------------|--------|------------------------|------|
| **`balanced`** (`blake3_128`, `blake3`) | `FixedSizeBinary(16)` | ≲ 10¹²+ | **Default** (pure, no registry) |
| **`integer`** (`miisk`, `seq`) | `Int64` | unlimited (registry-backed) | **Product MIISK** — fastest joins |
| **`fast64`** (`xxh3_64`, `xxhash64`) | `Int64` | ≲ 10⁶–10⁷ (warn ≥ 10⁸) | Hash Int64 without registry |
| **`safe256`** (`blake3_256`) | `FixedSizeBinary(32)` | Effectively unlimited | Paranoia / huge N |
| **`compat_md5`** (`md5`) | `FixedSizeBinary(16)` | like balanced | dbt parity |

### 2b. MIISK durability (productized)

- Store NK→SK map at `{project}/.rbt/sk_registry/{model}.parquet`
  (`_rbt_sk_key` Utf8 + `sk` Int64).
- **Reserved:** `sk = 0` is Unknown — never allocated to a real grain; first real SK is `1`.
- Existing grain → reuse SK; new grain → `max(sk)+1`.
- **Materialize-stamp only** (frontmatter `surrogate_key_algo: integer`). Not available as a
  pure SQL UDF (no session-local registry writes from DataFusion scalars).
- Works with `table` stream stamp and **`keyed_upsert`** (stamp after merge).
- Optional `integer_dense` (ROW_NUMBER without registry) is **not** shipped — unstable across runs.

### 3. Encoding

- Prefer **binary** (`FixedSizeBinary` / `Int64`).
- Optional `surrogate_key_encoding: hex` on frontmatter stamp only (Utf8 hex of the digest).
- SQL UDFs emit native binary/int; authors can cast/encode in SQL if needed.

### 4. Domain string (frozen `v1`)

```
rbt_sk_v1|<canonical_algo>\0<field>\0<field>\0…
```

Null fields use sentinel `_rbt_sk_null_`. Non-string Arrow values stringify via a stable
Display path. Changing the domain tag requires a new version prefix (`v2`).

### 5. Unknown member

- Reserved constant: **all zeros** for every encoding (`0i64`, 16/32 zero bytes, hex of zeros).
- Helper: `sk_unknown()` / `surrogate_key_unknown(algo?)`.
- Optional frontmatter `unknown_member: true` injects one synthetic all-zero SK row on
  full-refresh table materialize.
- Fact FK pattern: `COALESCE(dim.sk, sk_unknown())` — the hash UDF does **not** auto-map
  null grain to unknown.

### 6. SQL API

| Function | Behavior |
|----------|----------|
| `sk(col, …)` | Default = `balanced` (blake3_128 binary) |
| `surrogate_key(algo, col, …)` | Explicit algo |
| `sk_unknown()` / `surrogate_key_unknown(algo?)` | All-zero sentinel |

Prefixed aliases: `rbt_sk`, `rbt_surrogate_key`, `rbt_sk_unknown`.

### 7. Bare `sk()` / `surrogate_key(algo)` — compile-time grain expansion

**Recommendation: yes, with compile-time rewrite — not a runtime UDF side-channel.**

Reasons **not** to make the DataFusion UDF itself “read frontmatter”:

1. UDFs are session-global; they have no model/frontmatter context at invoke time.
2. Ad-hoc SQL, benches, and embedders would silently break or need hidden thread-locals.
3. Nested queries / aliases can make “which grain?” ambiguous.

**What we do instead:**

When a model declares `grain: […]`, rbt **rewrites** after Jinja/`ref` compile:

- `sk()` → `sk(grain_col1, grain_col2, …)`
- `surrogate_key('algo')` / `surrogate_key("algo")` → `surrogate_key('algo', grain_col1, …)`
- Same for `rbt_sk()` / `rbt_surrogate_key('algo')`
- Does **not** rewrite `sk_unknown()` or calls that already pass columns

If bare `sk()` appears **without** `grain`, compile fails with `E_RBT_SK` (pass columns or set grain).

Authors may still write explicit `sk(col1, col2)` (multi-grain / fact bridges / ad-hoc).

### 8. Frontmatter stamp (zero-SQL path)

```yaml
grain: [entity_id]
surrogate_key: entity_sk
# optional:
surrogate_key_algo: balanced      # default; or integer | fast64 | …
surrogate_key_encoding: binary    # or hex (hash algos only)
unknown_member: true
```

Materialize appends the SK column from grain (hash: idempotent if SQL already selected it;
MIISK: always re-applied from registry).

### 9. Defaults (verbose config not required)

| Setting | Default |
|---------|---------|
| Algo | `balanced` |
| Encoding | `binary` |
| Unknown inject | off (`unknown_member` omitted) |
| SQL shorthand | `sk()` expands from `grain` when present |

### 10. keyed_upsert × SK

- Upsert merges on NK; **then** stamps SK (hash or MIISK) before atomic write.
- Default compare columns exclude `*_sk` / `sk` and `_rbt_*` so SK stamps do not force updates.
- Receipts record insert/update/touch/**kept**.

## Consequences

- Lake-native hash SKs stay the zero-ops default; MIISK available when join width matters.
- MIISK registry is project-local state (gitignore `.rbt/` as today); losing the registry
  orphans integer FKs — document backup / treat like a sequence.
- Bare `sk()` is ergonomic for grain-declared models without hiding frontmatter inside UDFs.
- Upsert dims can carry SK without a second pass.

## Alternatives rejected

| Alt | Why not |
|-----|---------|
| Default MD5 (dbt) | Slower; cryptographically broken; offer as `compat_md5` only |
| Default xxh3_64 / default MIISK | Collision risk or registry coupling as silent default |
| Runtime UDF reads grain via TLS/context | Fragile; breaks ad-hoc SQL |
| Upsert on SK | Orphans on algo/registry change; opaque failures |
| Unknown = -1 for ints | User chose **0** / all-zeros for every encoding |
| SQL UDF for MIISK | Cannot durably allocate without engine-owned registry IO |
| ROW_NUMBER-only dense MIISK | Unstable across runs; not productized |

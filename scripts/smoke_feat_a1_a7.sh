#!/usr/bin/env bash
# Feature showcase smoke: A1 multi-value, A2 scoped_replace, A7 keyed_upsert.
# Does not replace scripts/smoke.sh (CI baseline on smoke_fixture).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BIN="${RBT_BIN:-${RBT:-./target/release/rbt}}"
if [[ ! -x "$BIN" ]]; then
  echo "[smoke_feat] building rbt CLI..."
  rustup run stable cargo build -p rbt-datalake --release 2>/dev/null \
    || cargo build -p rbt-datalake --release
  BIN=./target/release/rbt
fi
export RBT="$BIN"
export RBT_BIN="$BIN"

echo "[smoke_feat] === A1 multi-value partition scope ==="
EX1=examples/a1_multi_value_scope
rm -rf "$EX1/lake/silver" "$EX1/lake/gold" "$EX1/.rbt" 2>/dev/null || true
"$BIN" run -p "$EX1" --format parquet --bronze-check fail \
  --var entity=a.com --var entity=b.com \
  --var report_date=2026-08-07
# Expect 3 rows (a:2 + b:1); c filtered out
test -f "$EX1/lake/silver/stage/stg_events.parquet"
echo "[smoke_feat] A1 OK (stg_events materialised for a+b)"

echo "[smoke_feat] === A2 scoped_replace peer keep ==="
bash examples/a2_scoped_replace/scripts/demo_scoped_replace.sh
echo "[smoke_feat] A2 OK"

echo "[smoke_feat] === A7 keyed_upsert multi-day playbook ==="
bash examples/entity_registry/scripts/demo_upsert.sh
echo "[smoke_feat] A7 OK"

echo "[smoke_feat] ALL OK (A1 + A2 + A7)"

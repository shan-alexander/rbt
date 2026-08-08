#!/usr/bin/env bash
# A2 demo: two peer scopes, then replace entity a only — b part unchanged.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$(cd "$ROOT/../.." && pwd)"
RBT="${RBT:-${RBT_BIN:-$REPO/target/release/rbt}}"
if [[ ! -x "$RBT" ]]; then
  RBT="$REPO/target/debug/rbt"
fi
if [[ ! -x "$RBT" ]]; then
  echo "Build rbt first: cargo build -p rbt-datalake --release" >&2
  exit 1
fi

EX="$ROOT"
BRONZE_A="$EX/lake/bronze/runs/entity=a.com/report_date=2026-08-07"
PARTS="$EX/lake/silver/stage/stg_entity_events.parts"
FIX="$EX/fixtures"

mkdir -p "$FIX" "$BRONZE_A"

# Canonical fixtures (restore bronze a to 2 rows before demo)
cat > "$FIX/entity_a_v1.jsonl" <<'EOF'
{"event_id":"a1","entity":"a.com","payload":"alpha-v1","report_date":"2026-08-07"}
{"event_id":"a2","entity":"a.com","payload":"alpha-v1-b","report_date":"2026-08-07"}
EOF
cat > "$FIX/entity_a_v2.jsonl" <<'EOF'
{"event_id":"a1","entity":"a.com","payload":"alpha-v2","report_date":"2026-08-07"}
{"event_id":"a2","entity":"a.com","payload":"alpha-v2-b","report_date":"2026-08-07"}
{"event_id":"a3","entity":"a.com","payload":"alpha-v2-c","report_date":"2026-08-07"}
EOF

echo "[a2 demo] clean silver + restore bronze a (v1, 2 rows)"
rm -rf "$EX/lake/silver" "$EX/.rbt" 2>/dev/null || true
cp "$FIX/entity_a_v1.jsonl" "$BRONZE_A/events.jsonl"

echo "[a2 demo] run scope a.com"
RUST_LOG=error "$RBT" run -p "$EX" --format parquet --bronze-check fail \
  --var entity=a.com --var report_date=2026-08-07 >/tmp/rbt_a2_run.json 2>/tmp/rbt_a2_run.err

echo "[a2 demo] run scope b.com (peer)"
RUST_LOG=error "$RBT" run -p "$EX" --format parquet --bronze-check fail \
  --var entity=b.com --var report_date=2026-08-07 >/tmp/rbt_a2_run.json 2>/tmp/rbt_a2_run.err

parts_before=$(find "$PARTS" -name 'part-*.parquet' 2>/dev/null | wc -l | tr -d ' ')
test "$parts_before" -eq 2

echo "[a2 demo] re-land a.com with 3 rows (v2) — replace a part only"
cp "$FIX/entity_a_v2.jsonl" "$BRONZE_A/events.jsonl"
RUST_LOG=error "$RBT" run -p "$EX" --format parquet --bronze-check fail \
  --var entity=a.com --var report_date=2026-08-07 >/tmp/rbt_a2_run.json 2>/tmp/rbt_a2_run.err

parts_after=$(find "$PARTS" -name 'part-*.parquet' 2>/dev/null | wc -l | tr -d ' ')
test "$parts_after" -eq 2

total=$(python3 - <<PY
import json
m=json.load(open("$PARTS/_rbt_manifest.json"))
print(m.get("total_rows", 0))
print("parts", len(m.get("parts", [])), file=__import__("sys").stderr)
PY
)

# 3 (a) + 1 (b) = 4
test "$total" = "4"
echo "[a2 demo] OK: 2 parts, total_rows=4 (a replaced to 3 rows; b kept)"

# Restore v1 bronze for next clean run
cp "$FIX/entity_a_v1.jsonl" "$BRONZE_A/events.jsonl"

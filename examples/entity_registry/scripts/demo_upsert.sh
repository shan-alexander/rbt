#!/usr/bin/env bash
# Multi-day keyed_upsert playbook for examples/entity_registry.
# Proves: insert → touch+insert+keep → update+keep (not a full dim rewrite).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$(cd "$ROOT/../.." && pwd)"
RBT="${RBT:-$REPO/target/release/rbt}"
if [[ ! -x "$RBT" ]]; then
  RBT="$REPO/target/debug/rbt"
fi
if [[ ! -x "$RBT" ]]; then
  echo "Build rbt first: cargo build -p rbt-datalake --release" >&2
  exit 1
fi

BRONZE="$ROOT/lake/bronze/sightings"
FIX="$ROOT/fixtures"
OUT="$ROOT/lake/gold/dim_entity.parquet"

land() {
  local day="$1" date="$2"
  local dest="$BRONZE/report_date=${date}"
  mkdir -p "$dest"
  cp "$FIX/${day}.jsonl" "$dest/sightings.jsonl"
  echo "[demo] landed $day → $dest/sightings.jsonl"
}

run_day() {
  local label="$1"
  echo ""
  echo "========== $label =========="
  # Pure JSON on stdout (suppress tracing noise)
  RUST_LOG=error "$RBT" run -p "$ROOT" --format parquet --json \
    > /tmp/rbt_entity_registry_run.json 2>/tmp/rbt_entity_registry_run.err || {
      echo "[demo] run failed:" >&2
      cat /tmp/rbt_entity_registry_run.err >&2
      cat /tmp/rbt_entity_registry_run.json >&2
      exit 1
    }
  python3 - <<'PY'
import json
raw=open("/tmp/rbt_entity_registry_run.json").read()
# Tolerate any leading noise: take from first "{"
i=raw.find("{")
j=json.loads(raw[i:] if i>=0 else raw)
for m in j.get("models",[]):
    if m["name"]=="dim_entity":
        print(
            f"[demo] dim_entity: rows={m.get('row_count')} "
            f"inserted={m.get('rows_inserted')} "
            f"updated={m.get('rows_updated')} "
            f"touched={m.get('rows_touched')}"
        )
        break
else:
    print("[demo] warning: dim_entity not in models[]")
    print(raw[:500])
PY
}

echo "[demo] clean bronze landings + prior silver/gold"
rm -rf "$ROOT/lake/bronze/sightings" "$ROOT/lake/silver" "$ROOT/lake/gold" "$ROOT/.rbt"
mkdir -p "$BRONZE"

land day1 2026-08-01
run_day "Day 1 — candidates acme,beta → expect insert=2"

land day2 2026-08-02
run_day "Day 2 — candidates acme,gamma only → insert=1 touch=1; beta KEPT (not in candidates)"

land day3 2026-08-03
run_day "Day 3 — candidates acme only → update=1; beta+gamma KEPT"

echo ""
echo "[demo] final dim_entity parquet: $OUT"
python3 - <<PY || true
import sys
try:
    import pyarrow.parquet as pq
    t = pq.read_table("$OUT")
    print(t.to_pandas().sort_values("entity_id").to_string(index=False))
except Exception as e:
    print("(optional) pyarrow not available to print dim rows:", e)
PY

echo ""
echo "[demo] Why not materialization:table on dim_entity?"
echo "  A full-refresh table from *today's* latest candidates alone would drop beta on day 2"
echo "  when beta did not land. keyed_upsert keeps peers and only merges candidates."

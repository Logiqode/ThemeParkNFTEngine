#!/usr/bin/env bash
# =============================================================================
# run_congestion.sh — M2.5 (R15): Kafka delivery reliability under congestion.
#
# Sustained mixed load with spikes + duplicate trace_ids (10–15%), asserting:
#   - delivery success >= 99.9% (manifest uniques observed at least once)
#   - duplicate processing ~ 0 (manifest dup lines still observed <= 1 extra)
#   - lag -> 0 (recovery_sec reported; target <= 30s)
#
# Requires: healthy local stack, go, jq, bc.
# Usage:    ./scripts/bench/run_congestion.sh [BROKERS] [TOPIC]
# =============================================================================
set -euo pipefail

BROKERS="${1:-localhost:29092}"
TOPIC="${2:-ride-scans}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="$(mktemp -d /tmp/bench-congestion.XXXXXX)"
BIN="$WORK/bin"
mkdir -p "$BIN"

echo "==> Building loadgen + verifier..."
(cd "$ROOT" && go build -o "$BIN/loadgen" ./cmd/loadgen)
(cd "$ROOT" && go build -o "$BIN/verify_delivery" ./scripts/bench)

# --- Phase 1: sustained mixed load (waves) -------------------------------
echo "==> Phase 1: sustained 1,000 RPS @ 20s with 12% duplicates"
M="$WORK/phase1"; mkdir -p "$M"
"$BIN/loadgen" -rate 1000 -duration 20s -users 1000 -rides 20 -concurrency 8 \
  -dup-pct 12 -manifest "$M/manifest.jsonl" -summary "$M/summary.json" >/dev/null 2>&1
echo "    produced=$(jq -r '.total_produced' "$M/summary.json") errors=$(jq -r '.total_errors' "$M/summary.json")"

# --- Phase 2: spike burst on top -----------------------------------------
echo "==> Phase 2: 5,000 RPS burst @ 2s (spike)"
M="$WORK/phase2"; mkdir -p "$M"
"$BIN/loadgen" -rate 5000 -duration 2s -users 500 -rides 20 -concurrency 12 \
  -dup-pct 10 -manifest "$M/manifest.jsonl" -summary "$M/summary.json" >/dev/null 2>&1
echo "    produced=$(jq -r '.total_produced' "$M/summary.json") errors=$(jq -r '.total_errors' "$M/summary.json")"

# --- Verify combined manifest --------------------------------------------
echo "==> Verifying combined delivery against 99.9% target..."
cat "$WORK/phase1/manifest.jsonl" "$WORK/phase2/manifest.jsonl" > "$WORK/combined.jsonl"
OUT="$("$BIN/verify_delivery" --manifest "$WORK/combined.jsonl" --brokers "$BROKERS" --topic "$TOPIC" --timeout 120s)"
echo "    $OUT" | tee /dev/stderr

RATE=$(echo "$OUT" | jq -r '.success_rate')
DUPS=$(echo "$OUT" | jq -r '.delivery_duplicates')
UNIQ=$(echo "$OUT" | jq -r '.manifest_unique')
OBS=$(echo "$OUT" | jq -r '.observed_unique')
LINES=$(wc -l < "$WORK/combined.jsonl")
REC=$(echo "$OUT" | jq -r '.recovery_sec')

# Duplicate processing is tolerated only for the manifest's own sampled dup
# lines; the verifier counts only EXTRA observations beyond first, so 0 is the
# correct target here.
if [ "$(echo "$RATE >= 0.999" | bc -l)" = "1" ] && [ "$DUPS" = "0" ]; then
  PASS="PASS"
else
  PASS="FAIL"
fi

echo "=============================================================="
echo " M2.5 CONGESTION RESULT: $PASS"
echo "   manifest_lines      = $LINES"
echo "   manifest_unique     = $UNIQ"
echo "   observed_unique     = $OBS"
echo "   success_rate        = $RATE  (target >= 0.999)"
echo "   duplicate_process   = $DUPS (target 0)"
echo "   recovery_sec        = $REC   (target <= 30)"
echo "=============================================================="

mkdir -p "$ROOT/scripts/bench"
cat > "$ROOT/scripts/bench/RESULTS.md" <<EOF
# Benchmark Results — M2.5 Congestion (R15)

**Date:** $(date -Iseconds)
**Brokers:** $BROKERS
**Topic:** $TOPIC

| Metric | Value | Target | Status |
|---|---|---|---|
| Manifest lines (incl. dups) | $LINES | — | — |
| Manifest unique trace_ids | $UNIQ | — | — |
| Observed unique trace_ids | $OBS | == $UNIQ | $([ "$OBS" = "$UNIQ" ] && echo PASS || echo FAIL) |
| Delivery success rate | $RATE | >= 99.9% | $([ "$(echo "$RATE >= 0.999" | bc -l)" = "1" ] && echo PASS || echo FAIL) |
| Duplicate processing | $DUPS | ~0 | $([ "$DUPS" = "0" ] && echo PASS || echo FAIL) |
| Lag recovery | ${REC}s | <= 30s | $([ "$(echo "$REC <= 30" | bc -l)" = "1" ] && echo PASS || echo FAIL) |

**Overall:** $PASS
EOF

echo "Report written to scripts/bench/RESULTS.md"
rm -rf "$WORK"
[ "$PASS" = "PASS" ]
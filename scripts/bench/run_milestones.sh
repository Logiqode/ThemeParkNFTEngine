#!/usr/bin/env bash
# =============================================================================
# run_milestones.sh — Week 2 Kafka delivery milestone tests (M2.1–M2.4).
#
# Requires: a healthy local stack (`make up && make healthy`), go, jq.
# Usage:    ./scripts/bench/run_milestones.sh [BROKERS] [TOPIC]
#   BROKERS default: localhost:29092
#   TOPIC   default: ride-scans
#
# Each milestone runs loadgen → verify_delivery and asserts its criteria
# (implementation_plan.md Week 2, M2.1–M2.4). Loadgen is rate×duration driven;
# the manifest records every successfully delivered trace_id, and
# verify_delivery asserts >= 99.9% of those uniques were observable exactly once.
# =============================================================================
set -euo pipefail

BROKERS="${1:-localhost:29092}"
TOPIC="${2:-ride-scans}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="$(mktemp -d /tmp/bench-milestones.XXXXXX)"
BIN="$WORK/bin"
mkdir -p "$BIN"

echo "==> Building loadgen + verifier..."
(cd "$ROOT" && go build -o "$BIN/loadgen" ./cmd/loadgen)
(cd "$ROOT" && go build -o "$BIN/verify_delivery" ./scripts/bench)

pass=0
fail=0

report() { # name success detail
  if [ "$2" = "PASS" ]; then
    pass=$((pass+1))
    echo "  [PASS] $1 — $3"
  else
    fail=$((fail+1))
    echo "  [FAIL] $1 — $3"
  fi
}

verify() { # name manifest timeout
  local name="$1" manifest="$2" timeout="$3"
  local out
  out="$("$BIN/verify_delivery" --manifest "$manifest" --brokers "$BROKERS" --topic "$TOPIC" --timeout "$timeout")"
  echo "    $out"
  local rate dup
  rate="$(echo "$out" | jq -r '.success_rate')"
  dup="$(echo "$out" | jq -r '.delivery_duplicates')"
  if [ "$(echo "$rate >= 0.999" | bc -l)" = "1" ] && [ "$dup" = "0" ]; then
    report "$name" PASS "success=$rate dups=$dup"
  else
    report "$name" FAIL "success=$rate dups=$dup"
  fi
}

# ---------------------------------------------------------------- M2.1
echo "==> M2.1 Correctness: 1,000 events, all delivered exactly once"
M="$WORK/m21"; mkdir -p "$M"
# Deterministic count via --max-events (rate x duration pacing would under-shoot
# the exact count due to cold-start + sync acks).
"$BIN/loadgen" -rate 0 -duration 0 -max-events 1000 -users 100 -rides 5 -concurrency 4 \
  -manifest "$M/manifest.jsonl" -summary "$M/summary.json" >/dev/null 2>&1
lines=$(wc -l < "$M/manifest.jsonl")
if [ "$lines" -lt 1000 ]; then
  report "M2.1" FAIL "manifest lines $lines < 1000"
else
  verify "M2.1" "$M/manifest.jsonl" 30s
fi

# ---------------------------------------------------------------- M2.2
echo "==> M2.2 Throughput: 10,000 events, p99 < 100ms"
M="$WORK/m22"; mkdir -p "$M"
"$BIN/loadgen" -rate 0 -duration 0 -max-events 10000 -users 500 -rides 10 -concurrency 8 \
  -manifest "$M/manifest.jsonl" -summary "$M/summary.json" >/dev/null 2>&1
lines=$(wc -l < "$M/manifest.jsonl")
p99=$(jq -r '.p99_latency_ms' "$M/summary.json")
err=$(jq -r '.total_errors' "$M/summary.json")
if [ "$(echo "$p99 < 100" | bc -l)" = "1" ] && [ "$err" = "0" ]; then
  verify "M2.2" "$M/manifest.jsonl" 60s
else
  report "M2.2" FAIL "p99_ms=$p99 errors=$err lines=$lines"
fi

# ---------------------------------------------------------------- M2.3
echo "==> M2.3 Spike: 5,000 events burst, no errors, recovery <= 5s"
M="$WORK/m23"; mkdir -p "$M"
"$BIN/loadgen" -rate 0 -duration 0 -max-events 5000 -users 100 -rides 10 -concurrency 8 \
  -manifest "$M/manifest.jsonl" -summary "$M/summary.json" >/dev/null 2>&1
err=$(jq -r '.total_errors' "$M/summary.json")
if [ "$err" = "0" ]; then
  out="$("$BIN/verify_delivery" --manifest "$M/manifest.jsonl" --brokers "$BROKERS" --topic "$TOPIC" --timeout 30s)"
  echo "    $out"
  rec=$(echo "$out" | jq -r '.recovery_sec')
  if [ "$(echo "$rec <= 5" | bc -l)" = "1" ]; then
    report "M2.3" PASS "recovery=${rec}s"
  else
    report "M2.3" FAIL "recovery=${rec}s > 5s"
  fi
else
  report "M2.3" FAIL "producer errors=$err"
fi

# ---------------------------------------------------------------- M2.4
echo "==> M2.4 Graceful shutdown: SIGTERM mid-batch, in-flight flushed"
M="$WORK/m24"; mkdir -p "$M"
"$BIN/loadgen" -rate 2000 -duration 0 -users 100 -rides 10 -concurrency 8 \
  -manifest "$M/manifest.jsonl" -summary "$M/summary.json" >/dev/null 2>&1 &
LG_PID=$!
sleep 3
kill -TERM "$LG_PID"
wait "$LG_PID"
err=$(jq -r '.total_errors' "$M/summary.json")
lines=$(wc -l < "$M/manifest.jsonl")
if [ "$err" = "0" ] && [ "$lines" -gt 0 ]; then
  verify "M2.4" "$M/manifest.jsonl" 30s
else
  report "M2.4" FAIL "errors=$err manifest_lines=$lines"
fi

# ---------------------------------------------------------------- summary
echo
echo "==> Milestone results: $pass passed, $fail failed"
rm -rf "$WORK"
[ "$fail" -eq 0 ]
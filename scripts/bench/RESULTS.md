# Benchmark Results — M2.5 Congestion (R15)

**Date:** 2026-08-02T01:38:05+07:00
**Brokers:** localhost:29092
**Topic:** ride-scans

| Metric | Value | Target | Status |
|---|---|---|---|
| Manifest lines (incl. dups) | 18206 | — | — |
| Manifest unique trace_ids | 16140 | — | — |
| Observed unique trace_ids | 16140 | == 16140 | PASS |
| Delivery success rate | 1 | >= 99.9% | PASS |
| Duplicate processing | 0 | ~0 | PASS |
| Lag recovery | 3.279s | <= 30s | PASS |

**Overall:** PASS

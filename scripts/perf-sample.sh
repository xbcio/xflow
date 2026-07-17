#!/usr/bin/env bash
# CI / local perf sampling for xflow high-throughput paths (D1).
#
# Runs the perf-bench suite against the test/env docker-compose stack
# (Redis + Kafka + MySQL) and records p50/p95/p99-style bench output to a
# results file for regression monitoring.
#
# This is a SAMPLING harness, NOT a hard gate. Bench results vary with the
# host; use it to detect order-of-magnitude regressions, not to commit to
# capacity numbers. Capacity commitments require a controlled host with
# multiple samples and a separate report (see
# docs/design/HIGH-THROUGHPUT-INGESTION.md §6).
#
# Usage:
#   make env-up                  # start Redis + Kafka + MySQL
#   ./scripts/perf-sample.sh     # run perf benches, write results
#   ./scripts/perf-sample.sh -v  # verbose (stream bench output)
#
# Env:
#   XFLOW_PERF_OUT   results file (default: ./perf-sample-results.txt)
#   XFLOW_PERF_TIME  -benchtime value (default: 2s)
#   KAFKA_BROKERS    kafka brokers (default: localhost:9092)

set -euo pipefail

OUT="${XFLOW_PERF_OUT:-perf-sample-results.txt}"
BENCHTIME="${XFLOW_PERF_TIME:-2s}"
VERBOSE=0
[[ "${1:-}" == "-v" || "${1:-}" == "--verbose" ]] && VERBOSE=1

cd "$(git rev-parse --show-toplevel)"

# Stamp without relying on `date` being available in every CI image the same
# way; `date` is fine here because this runs on the host, not in the workflow
# sandbox.
stamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

{
  echo "# xflow perf sample"
  echo "# timestamp: ${stamp}"
  echo "# benchtime:  ${BENCHTIME}"
  echo "# host:       $(uname -srm 2>/dev/null || echo unknown)"
  echo "# commit:     $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo
} > "${OUT}"

# Health-check prerequisites so failures are not misread as perf regressions.
if ! redis-cli -u "${REDIS_ADDR:-redis://localhost:6379/0}" ping >/dev/null 2>&1; then
  echo "perf-sample: Redis not reachable at ${REDIS_ADDR:-redis://localhost:6379/0}; run 'make env-up'" >&2
  exit 1
fi
if ! kafka-broker-api-versions --bootstrap-server "${KAFKA_BROKERS:-localhost:9092}" >/dev/null 2>&1 \
   && ! nc -z localhost 9092 >/dev/null 2>&1; then
  echo "perf-sample: Kafka not reachable at ${KAFKA_BROKERS:-localhost:9092}; run 'make env-up'" >&2
  exit 1
fi

echo "perf-sample: running perf benches (benchtime=${BENCHTIME}); results -> ${OUT}"

# Run the perf bench suite. -benchmem for alloc reporting; -timeout bounds
# the run. Output is appended to the results file; verbose mode also streams.
if [[ "${VERBOSE}" == "1" ]]; then
  go test -tags=perf -bench=. -benchmem -benchtime="${BENCHTIME}" -timeout 30m ./test/perf/... 2>&1 \
    | tee -a "${OUT}"
else
  go test -tags=perf -bench=. -benchmem -benchtime="${BENCHTIME}" -timeout 30m ./test/perf/... 2>&1 \
    | tee -a "${OUT}" >/dev/null
fi

echo >> "${OUT}"
echo "# end of sample" >> "${OUT}"

echo "perf-sample: done. Review ${OUT} for ns/op and allocs/op trends."
echo "perf-sample: remember — sampling only, not a capacity commitment."

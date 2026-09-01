#!/usr/bin/env bash
# wait-ready.sh — waits for protocol-level readiness in all test containers.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SCRIPT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091 -- local developer configuration.
  source "$SCRIPT_DIR/.env"
  set +a
fi

TIMEOUT=${XFLOW_READY_TIMEOUT:-120}
MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD:-xflow}

find_container_cli() {
  local cli container

  if [[ -n "${XFLOW_CONTAINER_CLI:-}" ]]; then
    if ! command -v "$XFLOW_CONTAINER_CLI" >/dev/null 2>&1; then
      echo "ERROR: XFLOW_CONTAINER_CLI=$XFLOW_CONTAINER_CLI is not installed" >&2
      return 1
    fi
    for container in xflow-test-redis xflow-test-mysql xflow-test-kafka; do
      if ! "$XFLOW_CONTAINER_CLI" inspect "$container" >/dev/null 2>&1; then
        echo "ERROR: XFLOW_CONTAINER_CLI=$XFLOW_CONTAINER_CLI cannot inspect $container" >&2
        return 1
      fi
    done
    printf '%s\n' "$XFLOW_CONTAINER_CLI"
    return 0
  fi

  for cli in podman docker; do
    if ! command -v "$cli" >/dev/null 2>&1; then
      continue
    fi
    for container in xflow-test-redis xflow-test-mysql xflow-test-kafka; do
      if ! "$cli" inspect "$container" >/dev/null 2>&1; then
        break
      fi
    done
    if [[ "$container" == "xflow-test-kafka" ]] && "$cli" inspect "$container" >/dev/null 2>&1; then
      printf '%s\n' "$cli"
      return 0
    fi
  done

  echo "ERROR: neither podman nor docker can inspect all xflow test containers; run make env-up" >&2
  return 1
}

wait_for() {
  local name=$1
  local deadline=$((SECONDS + TIMEOUT))
  shift

  echo "Checking $name..."
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "ERROR: $name not ready after ${TIMEOUT}s" >&2
      return 1
    fi
    sleep 1
  done
  echo "$name ready."
}

CONTAINER_CLI="$(find_container_cli)"
wait_for Redis "$CONTAINER_CLI" exec xflow-test-redis redis-cli ping
wait_for MySQL "$CONTAINER_CLI" exec xflow-test-mysql \
  mysqladmin ping -h localhost -p"$MYSQL_ROOT_PASSWORD" --silent
wait_for Kafka "$CONTAINER_CLI" exec xflow-test-kafka \
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:29092

echo "All dependencies ready."

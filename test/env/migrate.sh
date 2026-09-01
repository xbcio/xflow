#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCHEMA="$ROOT_DIR/db/xflow_schema.sql"

if [[ -f "$SCRIPT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091 -- local developer configuration.
  source "$SCRIPT_DIR/.env"
  set +a
fi

MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-xflow}"
MYSQL_DATABASE="${MYSQL_DATABASE:-xflow}"

find_container_cli() {
  local cli

  if [[ -n "${XFLOW_CONTAINER_CLI:-}" ]]; then
    if command -v "$XFLOW_CONTAINER_CLI" >/dev/null 2>&1 &&
      "$XFLOW_CONTAINER_CLI" inspect xflow-test-mysql >/dev/null 2>&1; then
      printf '%s\n' "$XFLOW_CONTAINER_CLI"
      return 0
    fi
    echo "ERROR: XFLOW_CONTAINER_CLI=$XFLOW_CONTAINER_CLI cannot inspect xflow-test-mysql" >&2
    return 1
  fi

  for cli in podman docker; do
    if command -v "$cli" >/dev/null 2>&1 &&
      "$cli" inspect xflow-test-mysql >/dev/null 2>&1; then
      printf '%s\n' "$cli"
      return 0
    fi
  done

  echo "ERROR: neither podman nor docker can inspect xflow-test-mysql; run make env-up" >&2
  return 1
}

CONTAINER_CLI="$(find_container_cli)"
echo "waiting for mysql to be healthy via $CONTAINER_CLI..."
mysql_ready=0
for _ in $(seq 1 60); do
  if "$CONTAINER_CLI" exec xflow-test-mysql mysqladmin ping -h localhost -p"$MYSQL_ROOT_PASSWORD" --silent >/dev/null 2>&1; then
    mysql_ready=1
    break
  fi
  sleep 1
done
if [[ "$mysql_ready" -ne 1 ]]; then
  echo "ERROR: xflow-test-mysql did not become healthy after 60s" >&2
  exit 1
fi

echo "applying $SCHEMA ..."
"$CONTAINER_CLI" exec -i xflow-test-mysql mysql -h localhost -uroot -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" < "$SCHEMA"
echo "done"

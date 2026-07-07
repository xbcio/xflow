#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCHEMA="$ROOT_DIR/db/xflow_schema.sql"

MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-xflow}"
MYSQL_DATABASE="${MYSQL_DATABASE:-xflow}"

echo "waiting for mysql to be healthy..."
for i in $(seq 1 60); do
  if podman exec xflow-test-mysql mysqladmin ping -h localhost -p"$MYSQL_ROOT_PASSWORD" --silent >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "applying $SCHEMA ..."
podman exec -i xflow-test-mysql mysql -h localhost -uroot -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" < "$SCHEMA"
echo "done"

#!/usr/bin/env bash
# wait-ready.sh — polls Redis and MySQL until both are healthy or timeout.
set -euo pipefail

TIMEOUT=${XFLOW_READY_TIMEOUT:-30}
REDIS_ADDR=${XFLOW_TEST_REDIS_ADDR:-127.0.0.1:6380}
MYSQL_ADDR=${XFLOW_TEST_MYSQL_ADDR:-127.0.0.1:3306}

deadline=$((SECONDS + TIMEOUT))

echo "Checking Redis at $REDIS_ADDR..."
while ! redis-cli -h "${REDIS_ADDR%%:*}" -p "${REDIS_ADDR##*:}" ping 2>/dev/null | grep -q PONG; do
  if [ $SECONDS -ge $deadline ]; then
    echo "ERROR: Redis not ready after ${TIMEOUT}s"
    exit 1
  fi
  sleep 1
done
echo "Redis ready."

echo "Checking MySQL at $MYSQL_ADDR..."
while ! mysqladmin ping -h "${MYSQL_ADDR%%:*}" -P "${MYSQL_ADDR##*:}" --silent 2>/dev/null; do
  if [ $SECONDS -ge $deadline ]; then
    echo "ERROR: MySQL not ready after ${TIMEOUT}s"
    exit 1
  fi
  sleep 1
done
echo "MySQL ready."

echo "All dependencies ready."

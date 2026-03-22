#!/usr/bin/env bash
set -euo pipefail
CONTAINER_NAME="ks-step-120-mongo"
MONGO_PORT="27018"
MONGO_URI="mongodb://localhost:${MONGO_PORT}/kriegspiel_step120_integration"
cleanup(){ docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup
docker run -d --name "${CONTAINER_NAME}" -p "${MONGO_PORT}:27017" mongo:7 >/dev/null
for _ in $(seq 1 30); do
  if docker exec "${CONTAINER_NAME}" mongosh --quiet --eval "db.adminCommand({ ping: 1 }).ok" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
cd "$(dirname "$0")/.."
RUN_MONGO_INTEGRATION=1 MONGO_URI="${MONGO_URI}" .venv/bin/pytest src/tests/test_db.py -v -m integration

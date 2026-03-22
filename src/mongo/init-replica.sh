#!/usr/bin/env bash
set -euo pipefail

MONGO_HOST="${MONGO_HOST:-localhost}"
MONGO_PORT="${MONGO_PORT:-27017}"
REPLICA_SET_NAME="${REPLICA_SET_NAME:-rs0}"

finish() {
  local code="$1"
  if [ "${BASH_SOURCE[0]}" != "$0" ]; then
    return "$code"
  fi
  exit "$code"
}

wait_for_mongo() {
  for _ in $(seq 1 60); do
    if mongosh --host "$MONGO_HOST" --port "$MONGO_PORT" --quiet --eval "db.adminCommand({ ping: 1 }).ok" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "MongoDB did not become ready in time" >&2
  return 1
}

wait_for_mongo || finish 1

RS_OK=$(mongosh --host "$MONGO_HOST" --port "$MONGO_PORT" --quiet --eval "try { rs.status().ok } catch (e) { 0 }")
if [ "$RS_OK" = "1" ]; then
  echo "Replica set already initialized"
  finish 0
fi

mongosh --host "$MONGO_HOST" --port "$MONGO_PORT" --quiet <<EOS
rs.initiate({
  _id: "$REPLICA_SET_NAME",
  members: [{ _id: 0, host: "$MONGO_HOST:$MONGO_PORT" }]
});
EOS

for _ in $(seq 1 30); do
  PRIMARY=$(mongosh --host "$MONGO_HOST" --port "$MONGO_PORT" --quiet --eval "try { rs.isMaster().ismaster ? 1 : 0 } catch (e) { 0 }")
  if [ "$PRIMARY" = "1" ]; then
    echo "Replica set initialized"
    finish 0
  fi
  sleep 1
done

echo "Replica set init timed out" >&2
finish 1

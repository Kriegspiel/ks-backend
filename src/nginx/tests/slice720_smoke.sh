#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:18080}"

code() { curl -s -o /dev/null -w "%{http_code}" "$1"; }

echo "[slice720] nginx -t"
docker compose exec -T nginx nginx -t >/dev/null

echo "[slice720] SPA routes"
for p in / /lobby /game/abc123; do
  c="$(code "${BASE_URL}${p}")"
  echo "  ${p} -> ${c}"
  [[ "$c" == "200" || "$c" == "304" ]]
done

echo "[slice720] API health"
api_code="$(code "${BASE_URL}/api/health")"
echo "  /api/health -> ${api_code}"
[[ "$api_code" == "200" ]]

echo "[slice720] auth throttling"
mapfile -t codes < <(for i in $(seq 1 20); do curl -s -o /dev/null -w "%{http_code}\n" -X POST "${BASE_URL}/auth/login"; done)
printf  codes: %sn "$(IFS=,; echo "${codes[*]}")"
count_429=0
for c in "${codes[@]}"; do
  [[ "$c" == "429" ]] && count_429=$((count_429+1))
done
echo "  429 count: ${count_429}"
[[ "$count_429" -ge 1 ]]

echo "[slice720] PASS"

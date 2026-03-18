#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
TOKEN="${TOKEN:-}"

AUTH_HEADERS=()
if [[ -n "$TOKEN" ]]; then
  AUTH_HEADERS=(-H "Authorization: Bearer $TOKEN")
fi

curl -sS -X POST "$API_URL/score/run" \
  -H "Content-Type: application/json" \
  "${AUTH_HEADERS[@]}" \
  --data-binary @examples/requests/score_run.json

echo

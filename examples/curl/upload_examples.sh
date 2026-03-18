#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
TOKEN="${TOKEN:-}"

AUTH_HEADERS=()
if [[ -n "$TOKEN" ]]; then
  AUTH_HEADERS=(-H "Authorization: Bearer $TOKEN")
fi

curl -sS -X POST "$API_URL/datasets/upload?dataset_name=baseline_main&dataset_version=v1&format=csv&schema_type=baseline_metrics" \
  "${AUTH_HEADERS[@]}" \
  -F "file=@examples/datasets/baseline_metrics.csv"

echo

curl -sS -X POST "$API_URL/datasets/upload?dataset_name=segments_main&dataset_version=v1&format=csv&schema_type=segment_list" \
  "${AUTH_HEADERS[@]}" \
  -F "file=@examples/datasets/segment_list.csv"

echo

curl -sS -X POST "$API_URL/datasets/upload?dataset_name=funnel_main&dataset_version=v1&format=csv&schema_type=baseline_funnel_steps" \
  "${AUTH_HEADERS[@]}" \
  -F "file=@examples/datasets/baseline_funnel_steps.csv"

echo

curl -sS -X POST "$API_URL/datasets/upload?dataset_name=screen_mix_main&dataset_version=v1&format=csv&schema_type=baseline_screen_mix" \
  "${AUTH_HEADERS[@]}" \
  -F "file=@examples/datasets/baseline_screen_mix.csv"

echo

curl -sS -X POST "$API_URL/datasets/upload?dataset_name=evidence_main&dataset_version=v1&format=csv&schema_type=evidence_priors" \
  "${AUTH_HEADERS[@]}" \
  -F "file=@examples/datasets/evidence_priors.csv"

echo

curl -sS -X POST "$API_URL/datasets/upload?dataset_name=cannibalization_main&dataset_version=v1&format=csv&schema_type=cannibalization_matrix" \
  "${AUTH_HEADERS[@]}" \
  -F "file=@examples/datasets/cannibalization_matrix.csv"

echo

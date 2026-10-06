#!/usr/bin/env bash
# Phase 2 demo: register, log in, create, publish and execute a workflow, then print the execution.
# Needs a running orchestrator (docker compose up -d --build, or the app from the IDE) plus curl and jq.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
EMAIL="demo-$(date +%s)@example.com"
PASSWORD="demo-password-$(date +%s)"

json() { curl -fsS -H 'Content-Type: application/json' "$@"; }

echo "== register + login as $EMAIL"
json -X POST "$BASE_URL/api/v1/auth/register" -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" > /dev/null
TOKEN=$(json -X POST "$BASE_URL/api/v1/auth/login" -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" | jq -r .accessToken)
AUTH=(-H "Authorization: Bearer $TOKEN")

echo "== create diamond workflow a → (b, c, d) → e"
WORKFLOW_ID=$(json "${AUTH[@]}" -X POST "$BASE_URL/api/v1/workflows" -d '{
  "name": "demo-diamond-'"$(date +%s)"'",
  "description": "phase 2 demo",
  "definition": {"tasks": [
    {"key": "a", "type": "DELAY", "config": {"durationMs": 100}},
    {"key": "b", "type": "DELAY", "config": {"durationMs": 100}, "dependsOn": ["a"]},
    {"key": "c", "type": "DELAY", "config": {"durationMs": 100}, "dependsOn": ["a"]},
    {"key": "d", "type": "DELAY", "config": {"durationMs": 100}, "dependsOn": ["a"]},
    {"key": "e", "type": "DELAY", "config": {"durationMs": 100}, "dependsOn": ["b", "c", "d"]}
  ]}}' | jq -r .id)
echo "workflow $WORKFLOW_ID"

echo "== publish v1"
json "${AUTH[@]}" -X POST "$BASE_URL/api/v1/workflows/$WORKFLOW_ID/publish" | jq '{publishedVersion: .version}'

echo "== execute (twice with the same Idempotency-Key: one execution)"
KEY="demo-$(date +%s)"
for i in 1 2; do
  curl -sS -o /dev/null -w "HTTP %{http_code}\n" "${AUTH[@]}" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $KEY" -X POST "$BASE_URL/api/v1/workflows/$WORKFLOW_ID/execute" -d '{"input":{"orderId":42}}'
done
EXECUTION=$(json "${AUTH[@]}" -H "Idempotency-Key: $KEY" -X POST "$BASE_URL/api/v1/workflows/$WORKFLOW_ID/execute" \
  -d '{"input":{"orderId":42}}')

echo "== execution"
echo "$EXECUTION" | jq '{id, status, version, input, tasks: [.tasks[] | {key, status}]}'
echo "== events"
json "${AUTH[@]}" "$BASE_URL/api/v1/executions/$(echo "$EXECUTION" | jq -r .id)/events" | jq '[.items[] | {sequence, type, data}]'

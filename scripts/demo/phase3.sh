#!/usr/bin/env bash
# Demo: a monthly report (DATABASE → REPORT → HTTP notify) and a flaky HTTP task
# that fails twice and succeeds on the third attempt. Prints each execution's timeline and the flaky task's attempts.
# Needs the stack running (docker compose up -d --build) with FLOWFORGE_DISPATCH_MODE=local, plus curl and jq.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
WIREMOCK_ADMIN="${WIREMOCK_ADMIN:-http://localhost:8089}"   # WireMock as seen from this shell
WIREMOCK_FOR_TASKS="${WIREMOCK_FOR_TASKS:-http://wiremock:8080}"   # WireMock as seen from the orchestrator
EMAIL="demo-$(date +%s)@example.com"
PASSWORD="demo-password-$(date +%s)"

json() { curl -fsS -H 'Content-Type: application/json' "$@"; }

echo "== register + login as $EMAIL"
json -X POST "$BASE_URL/api/v1/auth/register" -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" > /dev/null
TOKEN=$(json -X POST "$BASE_URL/api/v1/auth/login" -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" | jq -r .accessToken)
AUTH=(-H "Authorization: Bearer $TOKEN")

# create_and_run <name> <definition-json> → prints the execution id
create_and_run() {
  local id
  id=$(json "${AUTH[@]}" -X POST "$BASE_URL/api/v1/workflows" \
    -d "{\"name\":\"$1-$(date +%s)\",\"description\":\"phase 3 demo\",\"definition\":$2}" | jq -r .id)
  json "${AUTH[@]}" -X POST "$BASE_URL/api/v1/workflows/$id/publish" > /dev/null
  json "${AUTH[@]}" -X POST "$BASE_URL/api/v1/workflows/$id/execute" -d '{}' | jq -r .id
}

# wait_for <execution-id> → waits until COMPLETED or FAILED, then prints tasks and the timeline
wait_for() {
  local status=""
  for _ in $(seq 1 60); do
    status=$(json "${AUTH[@]}" "$BASE_URL/api/v1/executions/$1" | jq -r .status)
    [[ "$status" == "COMPLETED" || "$status" == "FAILED" ]] && break
    sleep 0.5
  done
  echo "status: $status"
  json "${AUTH[@]}" "$BASE_URL/api/v1/executions/$1" | jq '[.tasks[] | {key, status, attempt}]'
  json "${AUTH[@]}" "$BASE_URL/api/v1/executions/$1/events?size=100" \
    | jq -r '.items[] | "\(.occurredAt)  \(.type)  \(.data.taskKey // "")"'
}

echo "== monthly report: orders (DATABASE) → summary (REPORT) → notify (HTTP)"
REPORT=$(create_and_run monthly-report '{"tasks": [
  {"key": "orders", "type": "DATABASE", "config": {"query": "orders_by_status", "params": {"status": "PAID"}}},
  {"key": "summary", "type": "REPORT", "dependsOn": ["orders"],
   "config": {"source": "orders", "groupBy": "customer", "metrics": [{"field": "amount", "fn": "sum"}, {"field": "id", "fn": "count"}]}},
  {"key": "notify", "type": "HTTP", "dependsOn": ["summary"],
   "config": {"method": "POST", "url": "'"$WIREMOCK_FOR_TASKS"'/api/notify", "body": {"text": "report ready"}}}]}')
wait_for "$REPORT"
json "${AUTH[@]}" "$BASE_URL/api/v1/executions/$REPORT" | jq '.tasks[] | select(.key == "summary") | .output.totals'

echo "== flaky: 503, 503, then 200 (FIXED 500 ms backoff, 3 attempts)"
curl -fsS -X POST "$WIREMOCK_ADMIN/__admin/scenarios/reset" > /dev/null
FLAKY=$(create_and_run flaky '{"tasks": [
  {"key": "call", "type": "HTTP", "config": {"method": "POST", "url": "'"$WIREMOCK_FOR_TASKS"'/api/flaky"},
   "retry": {"maxAttempts": 3, "backoff": "FIXED", "initialDelayMs": 500, "maxDelayMs": 500, "jitter": false}}]}')
wait_for "$FLAKY"
echo "== attempts of the flaky task"
json "${AUTH[@]}" "$BASE_URL/api/v1/executions/$FLAKY/tasks/call/attempts" | jq '[.[] | {attempt, outcome, errorCode}]'

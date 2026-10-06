#!/usr/bin/env bash
# Shared helpers for the chaos scripts. Needs the compose stack running, curl and jq.
# Each script prints PASS/FAIL and exits non-zero on failure.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
COMPOSE="${COMPOSE:-docker compose}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# Operator credentials for /api/v1/workers come from .env (FLOWFORGE_OPERATOR_EMAIL / _PASSWORD).
if [[ -f "$ROOT/.env" ]]; then set -a; source "$ROOT/.env"; set +a; fi

json() { curl -fsS -H 'Content-Type: application/json' "$@"; }

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }

# login_new_user → sets AUTH for a fresh user
login_new_user() {
  local email="chaos-$(uuidgen | tr "[:upper:]" "[:lower:]")@example.com" password="chaos-password-$RANDOM$RANDOM"
  json -X POST "$BASE_URL/api/v1/auth/register" -d "{\"email\":\"$email\",\"password\":\"$password\"}" > /dev/null
  TOKEN=$(json -X POST "$BASE_URL/api/v1/auth/login" -d "{\"email\":\"$email\",\"password\":\"$password\"}" | jq -r .accessToken)
  AUTH=(-H "Authorization: Bearer $TOKEN")
}

# operator_auth → sets OPAUTH (needs FLOWFORGE_OPERATOR_EMAIL/PASSWORD in .env)
operator_auth() {
  [[ -n "${FLOWFORGE_OPERATOR_EMAIL:-}" ]] || fail "set FLOWFORGE_OPERATOR_EMAIL/PASSWORD in .env"
  local token
  token=$(json -X POST "$BASE_URL/api/v1/auth/login" \
    -d "{\"email\":\"$FLOWFORGE_OPERATOR_EMAIL\",\"password\":\"$FLOWFORGE_OPERATOR_PASSWORD\"}" | jq -r .accessToken)
  OPAUTH=(-H "Authorization: Bearer $token")
}

# run_delay <ms> → prints the execution id of a one-task DELAY workflow (3 attempts, 1 s fixed backoff)
run_delay() {
  local id
  id=$(json "${AUTH[@]}" -X POST "$BASE_URL/api/v1/workflows" -d '{"name":"chaos-'"$(uuidgen | tr '[:upper:]' '[:lower:]' | cut -c1-12)"'",
    "description":"chaos","definition":{"tasks":[{"key":"job","type":"DELAY","config":{"durationMs":'"$1"'},
    "retry":{"maxAttempts":3,"backoff":"FIXED","initialDelayMs":1000,"maxDelayMs":1000,"jitter":false}}]}}' | jq -r .id)
  json "${AUTH[@]}" -X POST "$BASE_URL/api/v1/workflows/$id/publish" > /dev/null
  json "${AUTH[@]}" -X POST "$BASE_URL/api/v1/workflows/$id/execute" -d '{}' | jq -r .id
}

execution() { json "${AUTH[@]}" "$BASE_URL/api/v1/executions/$1"; }

# wait_task_running <execution> → prints the worker id once the task is RUNNING
wait_task_running() {
  for _ in $(seq 1 60); do
    local task
    task=$(execution "$1" | jq -c '.tasks[0]')
    if [[ $(jq -r .status <<<"$task") == RUNNING ]]; then jq -r .workerId <<<"$task"; return; fi
    sleep 0.5
  done
  fail "task never started"
}

# wait_status <execution> <timeout-s> → prints the final status (COMPLETED/FAILED) or TIMEOUT
wait_status() {
  for _ in $(seq 1 $(( $2 * 2 ))); do
    local status
    status=$(execution "$1" | jq -r .status)
    if [[ $status == COMPLETED || $status == FAILED ]]; then echo "$status"; return; fi
    sleep 0.5
  done
  echo TIMEOUT
}

attempts() { json "${AUTH[@]}" "$BASE_URL/api/v1/executions/$1/tasks/job/attempts" | jq -r '[.[] | "\(.attempt):\(.outcome)"] | join(",")'; }

timeline() {
  json "${AUTH[@]}" "$BASE_URL/api/v1/executions/$1/events?size=100" \
    | jq -r '.items[] | "  \(.occurredAt)  \(.type)  \(.data.workerId // "")"'
}

# container_of <worker id> → compose service name (worker-N-<8 hex> → worker-N)
service_of() { sed -E 's/-[0-9a-f]{8}$//' <<<"$1"; }

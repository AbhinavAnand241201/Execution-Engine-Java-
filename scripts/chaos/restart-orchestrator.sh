#!/usr/bin/env bash
# The orchestrator restarts while a workflow runs → it still completes (state in PostgreSQL, results in Kafka).
source "$(dirname "$0")/lib.sh"
login_new_user
id=$(run_delay 5000)
wait_task_running "$id" > /dev/null
echo "restarting the orchestrator mid-task"
$COMPOSE restart orchestrator > /dev/null
for _ in $(seq 1 120); do curl -fsS "$BASE_URL/actuator/health/readiness" > /dev/null 2>&1 && break; sleep 1; done
# The same user's token is still valid after the restart (stateless JWT, same secret).
status=$(wait_status "$id" 90)
[[ $status == COMPLETED ]] || fail "execution $status"
pass "workflow completed across an orchestrator restart"

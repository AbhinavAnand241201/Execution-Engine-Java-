#!/usr/bin/env bash
# A worker is frozen (docker pause) longer than the heartbeat TTL → recovered elsewhere → it unfreezes as a zombie
# and reports attempt 1 late → rejected as stale; the execution keeps the other worker's result.
source "$(dirname "$0")/lib.sh"
login_new_user
id=$(run_delay 3000)
worker=$(wait_task_running "$id")
service=$(service_of "$worker")
echo "task RUNNING on $worker → docker pause $service until the task completed elsewhere"
$COMPOSE pause "$service" > /dev/null
status=$(wait_status "$id" 120)   # TTL 15 s + monitor 5 s + Kafka session timeout 10 s + retry
sleep 5
$COMPOSE unpause "$service" > /dev/null
sleep 8   # let the zombie report
final=$(execution "$id")
echo "timeline:"; timeline "$id"
[[ $status == COMPLETED ]] || fail "execution $status"
[[ $(jq -r '.tasks[0].attempt' <<<"$final") == 2 ]] || fail "attempt $(jq -r '.tasks[0].attempt' <<<"$final")"
[[ $(jq -r '.tasks[0].workerId' <<<"$final") != "$worker" ]] || fail "zombie's result was applied"
pass "zombie $service ignored; result from $(jq -r '.tasks[0].workerId' <<<"$final")"

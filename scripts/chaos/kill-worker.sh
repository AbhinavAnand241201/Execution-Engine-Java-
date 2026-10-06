#!/usr/bin/env bash
# A worker is killed (SIGKILL) mid-task → OFFLINE after the heartbeat TTL → attempt WORKER_LOST → the task runs again
# on another worker → COMPLETED. Prints the timeline. Restarts the killed worker afterwards.
source "$(dirname "$0")/lib.sh"
login_new_user
id=$(run_delay 10000)
worker=$(wait_task_running "$id")
service=$(service_of "$worker")
echo "task RUNNING on $worker → docker kill $service"
$COMPOSE kill "$service" > /dev/null
status=$(wait_status "$id" 90)
echo "timeline:"; timeline "$id"
$COMPOSE start "$service" > /dev/null
[[ $status == COMPLETED ]] || fail "execution $status"
[[ $(attempts "$id") == 1:WORKER_LOST,2:SUCCEEDED ]] || fail "attempts $(attempts "$id")"
pass "killed $service mid-task; recovered on $(execution "$id" | jq -r '.tasks[0].workerId')"

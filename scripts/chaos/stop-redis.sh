#!/usr/bin/env bash
# Redis stops for 25 s (longer than the heartbeat TTL) → no worker is marked OFFLINE (the monitor skips), and
# workflows still run (dispatch doesn't need Redis). Redis back → all good.
source "$(dirname "$0")/lib.sh"
login_new_user
operator_auth
before=$(json "${OPAUTH[@]}" "$BASE_URL/api/v1/workers?status=ACTIVE&size=100" | jq -r .totalElements)
echo "ACTIVE workers before: $before → stopping redis for 25 s"
$COMPOSE stop redis > /dev/null
id=$(run_delay 500)
status=$(wait_status "$id" 60)
sleep 20
during=$(json "${OPAUTH[@]}" "$BASE_URL/api/v1/workers?status=ACTIVE&size=100" | jq -r .totalElements)
$COMPOSE start redis > /dev/null
[[ $status == COMPLETED ]] || fail "workflow during the Redis outage: $status"
[[ $during == "$before" ]] || fail "ACTIVE workers changed during the outage: $before → $during"
echo "Redis back: checking nobody is declared dead after the expired heartbeats (2 × TTL)"
sleep 35
after=$(json "${OPAUTH[@]}" "$BASE_URL/api/v1/workers?status=ACTIVE&size=100" | jq -r .totalElements)
[[ $after == "$before" ]] || fail "ACTIVE workers changed after Redis came back: $before → $after"
pass "Redis down: nobody marked OFFLINE (during or after), workflows kept running"

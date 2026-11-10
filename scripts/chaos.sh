#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
RUN_ID="chaos-$(date +%s)-$$"
source scripts/lib.sh

create_many() {
  local prefix="$1" count="$2" ids=()
  for n in $(seq 1 "$count"); do
    post_payment "$RUN_ID-$prefix-$n" "$(body 5.00 "$prefix-$n")" >/dev/null
    ids+=("$(json 'd["payment_id"]' < /tmp/e2e_body.json)")
  done
  echo "${ids[@]}"
}

all_delivered() {
  local ok=0 total=0
  for id in "$@"; do
    total=$((total + 1))
    [ "$(wait_for_webhook "$id" delivered)" = "delivered" ] && ok=$((ok + 1))
  done
  echo "$ok/$total"
}

unpublished() {
  count_payments "select count(*) from outbox where published_at is null;"
}

c1() {
  echo "== C-1 broker down: payments still accepted, delivered after recovery"
  $DC stop rabbitmq >/dev/null
  local codes=""
  for n in 1 2 3 4 5; do
    codes="$codes$(post_payment "$RUN_ID-c1-$n" "$(body 1.00 "c1-$n")")"
    IDS_C1+=("$(json 'd["payment_id"]' < /tmp/e2e_body.json)")
  done
  check "5 POSTs accepted while RabbitMQ is down" "$codes" "202202202202202"
  check "events wait in the outbox" "$(unpublished)" "5"
  $DC start rabbitmq >/dev/null
  check "all delivered after recovery" "$(all_delivered "${IDS_C1[@]}")" "5/5"
}

c2() {
  echo "== C-2 relay killed while draining a backlog"
  $DC stop outbox-relay >/dev/null
  read -r -a ids <<< "$(create_many c2 50)"
  $DC start outbox-relay >/dev/null
  sleep 1
  $DC kill outbox-relay >/dev/null
  $DC start outbox-relay >/dev/null
  check "all 50 delivered" "$(all_delivered "${ids[@]}")" "50/50"
  check "outbox fully drained" "$(unpublished)" "0"
}

c3() {
  echo "== C-3 consumer killed mid-processing"
  read -r -a ids <<< "$(create_many c3 10)"
  sleep 2
  $DC kill consumer >/dev/null
  $DC start consumer >/dev/null
  check "all 10 delivered after restart" "$(all_delivered "${ids[@]}")" "10/10"
}

c4() {
  echo "== C-4 webhook receiver failing: retries, then DLQ"
  sink_config '{"status_code": 500, "fail_times": 0}'
  post_payment "$RUN_ID-c4" "$(body 4.00 c4)" >/dev/null
  local id
  id="$(json 'd["payment_id"]' < /tmp/e2e_body.json)"
  check "webhook marked failed" "$(wait_for_webhook "$id" failed)" "failed"
  check "three attempts" "$(get_payment "$id" | json 'd["webhook_attempts"]')" "3"
  check "dead-lettered" "$(dead_letters | grep -c "$id" || true)" "1"
  sink_config '{"status_code": 200, "fail_times": 0}'
}

c5() {
  local count="${C5_COUNT:-200}"
  echo "== C-5 success rate over $count payments (expect ~90%)"
  read -r -a ids <<< "$(create_many c5 "$count")"
  all_delivered "${ids[@]}" >/dev/null
  local list
  list="$(printf "'%s'," "${ids[@]}")"
  local succeeded
  succeeded="$(count_payments "select count(*) from payments where id in (${list%,}) and status = 'succeeded';")"
  local rate
  rate="$(python3 -c "print(round($succeeded / $count * 100, 1))")"
  echo "  succeeded: $succeeded/$count ($rate%)"
  check "success rate within 85-95%" "$(python3 -c "print(85 <= $rate <= 95)")" "True"
}

IDS_C1=()
start_stack

for scenario in "${@:-c1 c2 c3 c4 c5}"; do
  for name in $scenario; do
    "$name"
  done
done

echo
if [ "$FAILURES" -eq 0 ]; then
  echo "Chaos scenarios passed ($RUN_ID)"
else
  echo "Chaos scenarios failed: $FAILURES check(s) ($RUN_ID)"
  exit 1
fi

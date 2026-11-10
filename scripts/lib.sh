#!/usr/bin/env bash

DC="${DC:-docker compose -p basis-test -f docker-compose.yml -f docker-compose.test.yml}"
API_URL="${API_URL:-http://localhost:${E2E_API_PORT:-18080}}"
SINK_URL="${SINK_URL:-http://localhost:${E2E_SINK_PORT:-19000}}"
SINK_HOOK="${SINK_HOOK:-http://webhook-sink:9000/hook}"
API_KEY="${E2E_API_KEY:-e2e-api-key}"
TIMEOUT="${E2E_TIMEOUT:-60}"
RUN_ID="${RUN_ID:-run-$(date +%s)-$$}"
FAILURES=0

pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAILURES=$((FAILURES + 1)); }
check() { if [ "$2" = "$3" ]; then pass "$1"; else fail "$1 (expected '$3', got '$2')"; fi; }
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1], {}, {'d': d}))" "$1"; }

body() {
  printf '{"amount":"%s","currency":"USD","description":"%s","metadata":{"run":"%s"},"webhook_url":"%s"}' \
    "$1" "$2" "$RUN_ID" "$SINK_HOOK"
}

post_payment() {
  curl -s -o /tmp/e2e_body.json -D /tmp/e2e_headers.txt -w '%{http_code}' \
    -X POST "$API_URL/api/v1/payments" \
    -H "X-API-Key: $API_KEY" -H "Idempotency-Key: $1" \
    -H "Content-Type: application/json" -d "$2"
}

get_payment() {
  curl -s "$API_URL/api/v1/payments/$1" -H "X-API-Key: $API_KEY"
}

wait_for_webhook() {
  local payment_id="$1" expected="$2" waited=0 state=""
  while [ "$waited" -lt "$TIMEOUT" ]; do
    state="$(get_payment "$payment_id" | json 'd["webhook_status"]')"
    [ "$state" = "$expected" ] && { echo "$state"; return; }
    sleep 1
    waited=$((waited + 1))
  done
  echo "$state"
}

sink_config() {
  curl -s -o /dev/null -X POST "$SINK_URL/config" -H "Content-Type: application/json" -d "$1"
}

count_payments() {
  $DC exec -T db psql -U "${POSTGRES_USER:-basis}" -d "${POSTGRES_DB:-basis}" -tA -c "$1"
}

start_stack() {
  if [ "${KEEP_STACK:-0}" != "1" ]; then
    trap stop_stack EXIT
  fi
  echo "== Starting the test stack ($DC)"
  $DC up -d --build --wait db rabbitmq api outbox-relay consumer webhook-sink >/dev/null
  curl -s -o /dev/null -X DELETE "$SINK_URL/calls"
  sink_config '{"status_code": 200, "fail_times": 0}'
}

stop_stack() {
  echo "== Removing the test stack and its volumes"
  $DC down -v --remove-orphans >/dev/null 2>&1 || true
}

dead_letters() {
  $DC exec -T consumer /app/.venv/bin/python -m tools.dlq_replay --dry-run 2>/dev/null
}

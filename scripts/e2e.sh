#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

RUN_ID="e2e-$(date +%s)-$$"
source scripts/lib.sh

start_stack

echo "== Authentication"
check "missing API key is rejected" \
  "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API_URL/api/v1/payments")" "401"

echo "== Create, replay and conflict"
KEY="$RUN_ID-happy"
check "create returns 202" "$(post_payment "$KEY" "$(body 10.00 happy)")" "202"
PAYMENT_ID="$(json 'd["payment_id"]' < /tmp/e2e_body.json)"
check "replay returns 202" "$(post_payment "$KEY" "$(body 10.00 happy)")" "202"
check "replay returns the same payment" "$(json 'd["payment_id"]' < /tmp/e2e_body.json)" "$PAYMENT_ID"
check "replay is flagged" \
  "$(grep -i '^idempotent-replayed:' /tmp/e2e_headers.txt | tr -d '\r' | awk '{print $2}')" "true"
check "same key with another body is 409" "$(post_payment "$KEY" "$(body 99.00 happy)")" "409"

echo "== Processing and webhook"
check "webhook delivered" "$(wait_for_webhook "$PAYMENT_ID" delivered)" "delivered"
DETAILS="$(get_payment "$PAYMENT_ID")"
check "payment reached a final status" \
  "$(echo "$DETAILS" | json 'd["status"] in ("succeeded", "failed")')" "True"
CALLS="$(curl -s "$SINK_URL/calls?payment_id=$PAYMENT_ID")"
check "sink received exactly one call" "$(echo "$CALLS" | json 'len(d)')" "1"
check "webhook signature is valid" "$(echo "$CALLS" | json 'd[0]["signature_valid"]')" "True"

echo "== Retry recovers"
sink_config '{"fail_times": 1, "fail_status_code": 503}'
post_payment "$RUN_ID-retry" "$(body 11.00 retry)" >/dev/null
RETRY_ID="$(json 'd["payment_id"]' < /tmp/e2e_body.json)"
check "webhook delivered after a 503" "$(wait_for_webhook "$RETRY_ID" delivered)" "delivered"
check "two webhook attempts recorded" "$(get_payment "$RETRY_ID" | json 'd["webhook_attempts"]')" "2"

echo "== Retries exhausted go to the DLQ"
sink_config '{"status_code": 500, "fail_times": 0}'
post_payment "$RUN_ID-dlq" "$(body 12.00 dlq)" >/dev/null
DLQ_ID="$(json 'd["payment_id"]' < /tmp/e2e_body.json)"
check "webhook marked failed" "$(wait_for_webhook "$DLQ_ID" failed)" "failed"
check "three webhook attempts recorded" "$(get_payment "$DLQ_ID" | json 'd["webhook_attempts"]')" "3"
DLQ_LIST="$(dead_letters)"
check "message is in the DLQ" "$(echo "$DLQ_LIST" | grep -c "$DLQ_ID" || true)" "1"

sink_config '{"status_code": 200, "fail_times": 0}'

echo
if [ "$FAILURES" -eq 0 ]; then
  echo "E2E passed ($RUN_ID)"
else
  echo "E2E failed: $FAILURES check(s) ($RUN_ID)"
  exit 1
fi

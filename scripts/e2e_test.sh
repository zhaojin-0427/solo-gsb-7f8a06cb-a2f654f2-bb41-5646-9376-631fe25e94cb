#!/usr/bin/env bash
# End-to-end check of the dosing-lab system (control x2 + device simulator).
# Re-runnable: each section resets the device simulator to a known state
# (POST /reset) and request keys are unique per run.
#
# Usage:
#   ./scripts/e2e_test.sh
#   C1=http://localhost:8001 C2=http://localhost:8002 DEV=http://localhost:9000 ./scripts/e2e_test.sh
#
# Optional crash-recovery section (needs a way to kill/restart control-1):
#   CRASH_KILL_CMD="docker compose kill control-1" \
#   CRASH_START_CMD="docker compose up -d control-1" ./scripts/e2e_test.sh
set -euo pipefail

C1=${C1:-http://127.0.0.1:8001}
C2=${C2:-http://127.0.0.1:8002}
DEV=${DEV:-http://127.0.0.1:9000}
RUN=${RUN:-$(date +%s)-$$}       # unique request-key prefix per run
PASS=0; FAIL=0
CODE=""; BODY=""

ok()    { PASS=$((PASS+1)); echo "  ok   - $1"; }
bad()   { FAIL=$((FAIL+1)); echo "  FAIL - $1"; }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected=$3 actual=$2)"; fi; }

jsonget() { python3 -c "import sys, json; d = json.load(sys.stdin); print($1)"; }

post() { # post <url> <json> : sets globals CODE and BODY
  local tmp
  tmp=$(mktemp)
  CODE=$(curl -s -o "$tmp" -w "%{http_code}" -X POST "$1" -H 'Content-Type: application/json' -d "$2")
  BODY=$(cat "$tmp")
  rm -f "$tmp"
}

reset_device() { post "$DEV/reset" '{}'; [ "$CODE" = "200" ] || { echo "device reset failed: $BODY"; exit 1; }; }

balance() { curl -s "$DEV/channels" | jsonget "d['$1']['remaining_ml']"; }
held()    { curl -s "$DEV/channels" | jsonget "len(d['$1']['held_op_keys'])"; }

batch_field() { # batch_field <batch_id> <python-expr-on-d> [control-base-url]
  curl -s "${3:-$C1}/batches/$1" | jsonget "$2"
}

wait_status() { # wait_status <batch_id> <wanted-status> <timeout-s> [control-base-url]
  local deadline=$((SECONDS + $3)) st
  while [ $SECONDS -lt $deadline ]; do
    st=$(batch_field "$1" "d['status']" "${4:-$C1}")
    [ "$st" = "$2" ] && return 0
    sleep 0.5
  done
  return 1
}

wait_held() { # wait_held <channel> <timeout-s>
  local deadline=$((SECONDS + $2))
  while [ $SECONDS -lt $deadline ]; do
    [ "$(held "$1")" -ge 1 ] && return 0
    sleep 0.3
  done
  return 1
}

echo "== health =="
check "control-1 health" "$(curl -s "$C1/healthz" | jsonget "d['status']")" "ok"
check "control-2 health" "$(curl -s "$C2/healthz" | jsonget "d['status']")" "ok"
check "device health"    "$(curl -s "$DEV/healthz" | jsonget "d['status']")" "ok"

echo "== validation: illegal input -> 422 =="
for body in \
  '{"request_key":"'$RUN'-bad1","channel":"A","steps":[0]}' \
  '{"request_key":"'$RUN'-bad2","channel":"A","steps":[101]}' \
  '{"request_key":"'$RUN'-bad3","channel":"A","steps":[1,2,3,4]}' \
  '{"request_key":"'$RUN'-bad4","channel":"A","steps":[]}' \
  '{"request_key":"'$RUN'-bad5","channel":"C","steps":[1]}' \
  '{"request_key":"'$RUN'-bad6","channel":"A","steps":[1.5]}' \
  '{"request_key":"'$RUN'-bad7","channel":"A","steps":[-5]}' \
  '{"channel":"A","steps":[1]}'; do
  post "$C1/batches" "$body"
  check "422 for $body" "$CODE" "422"
done

echo "== happy path: B batch [30,20,10] via control-1 =="
reset_device
post "$C1/batches" '{"request_key":"'$RUN'-b1","channel":"B","steps":[30,20,10]}'
check "create returns 202" "$CODE" "202"
BID=$(echo "$BODY" | jsonget "d['batch_id']")
check "status pending right after create" "$(echo "$BODY" | jsonget "d['status']")" "pending"
wait_status "$BID" "completed" 15 && ok "batch completed" || bad "batch completed"
check "3 confirmed receipts" "$(batch_field "$BID" "len(d['confirmed_receipts'])")" "3"
check "receipts confirmed in order" "$(batch_field "$BID" "[r['step_no'] for r in d['confirmed_receipts']]")" "[1, 2, 3]"
check "device deducted 60" "$(balance B)" "40"
check "query shows actual device balance" "$(batch_field "$BID" "d['device_remaining_ml']")" "40"

echo "== idempotent submission =="
post "$C2/batches" '{"request_key":"'$RUN'-b1","channel":"B","steps":[30,20,10]}'
check "same key + same content -> 200" "$CODE" "200"
check "same key + same content -> original batch id" "$(echo "$BODY" | jsonget "d['batch_id']")" "$BID"
post "$C1/batches" '{"request_key":"'$RUN'-b1","channel":"B","steps":[30,20]}'
check "same key + different content -> 409" "$CODE" "409"
check "balance unchanged after replays" "$(balance B)" "40"

echo "== partial completion: A batch [60,50] (A=100) =="
post "$C1/batches" '{"request_key":"'$RUN'-a1","channel":"A","steps":[60,50]}'
BID=$(echo "$BODY" | jsonget "d['batch_id']")
wait_status "$BID" "partial" 15 && ok "status partial" || bad "status partial"
check "step1 success" "$(batch_field "$BID" "d['confirmed_receipts'][0]['result']")" "success"
check "step2 rejected (insufficient)" "$(batch_field "$BID" "d['confirmed_receipts'][1]['result']")" "rejected"
check "60 deducted, not rolled back" "$(balance A)" "40"

echo "== first-step failure: A batch [50] with only 40 left =="
post "$C2/batches" '{"request_key":"'$RUN'-a2","channel":"A","steps":[50]}'
BID=$(echo "$BODY" | jsonget "d['batch_id']")
wait_status "$BID" "failed_first" 15 && ok "status failed_first" || bad "status failed_first"
check "nothing deducted" "$(balance A)" "40"

echo "== pause / resume on channel A =="
reset_device
post "$DEV/pause" '{"channel":"A"}'
check "pause armed" "$CODE" "200"
post "$C1/batches" '{"request_key":"'$RUN'-a3","channel":"A","steps":[10,10]}'
BID=$(echo "$BODY" | jsonget "d['batch_id']")
wait_held A 10 && ok "device holds the receipt after the successful deduction" || bad "device holds the receipt"
sleep 2
check "batch stays pending while held" "$(batch_field "$BID" "d['status']")" "pending"
check "no confirmed receipts while held" "$(batch_field "$BID" "len(d['confirmed_receipts'])")" "0"
check "deduction persisted on device (100->90)" "$(balance A)" "90"
post "$C2/batches" '{"request_key":"'$RUN'-b2","channel":"B","steps":[5]}'
BID2=$(echo "$BODY" | jsonget "d['batch_id']")
wait_status "$BID2" "completed" 15 && ok "B batch completes during the pause" || bad "B batch completes during the pause"
check "B deducted (100->95)" "$(balance B)" "95"
check "A batch still held" "$(batch_field "$BID" "d['status']")" "pending"
post "$DEV/release" '{"channel":"A"}'
check "release accepted" "$CODE" "200"
wait_status "$BID" "completed" 15 && ok "A batch completes after release" || bad "A batch completes after release"
check "A deducted 20 in total (100->80)" "$(balance A)" "80"

echo "== device operation-key idempotency (direct API) =="
reset_device
post "$DEV/dispense" '{"op_key":"manual-'$RUN'","batch_id":"manual","step_no":1,"channel":"A","amount_ml":1}'
check "manual dispense 200" "$CODE" "200"
FIRST_RECEIPT="$BODY"
post "$DEV/dispense" '{"op_key":"manual-'$RUN'","batch_id":"manual","step_no":1,"channel":"A","amount_ml":1}'
check "replay returns the stored receipt" "$BODY" "$FIRST_RECEIPT"
check "no double deduction" "$(balance A)" "99"
post "$DEV/dispense" '{"op_key":"manual-'$RUN'","batch_id":"manual","step_no":1,"channel":"A","amount_ml":2}'
check "same key + different params -> 409" "$CODE" "409"

echo "== both control processes work concurrently =="
reset_device
for i in 1 2 3 4; do
  if [ $((i % 2)) -eq 0 ]; then PORT="$C1"; else PORT="$C2"; fi
  ( post "$PORT/batches" '{"request_key":"'$RUN'-conc'$i'","channel":"B","steps":[1,1,1]}'
    echo "$BODY" | jsonget "d['batch_id']" > "/tmp/dosing-conc-$i" ) &
done
wait
ALLDONE=yes
for i in 1 2 3 4; do
  wait_status "$(cat /tmp/dosing-conc-$i)" "completed" 20 || ALLDONE=no
done
check "4 concurrent batches completed" "$ALLDONE" "yes"
check "B deducted 12 in total" "$(balance B)" "88"

if [ -n "${CRASH_KILL_CMD:-}" ] && [ -n "${CRASH_START_CMD:-}" ]; then
  echo "== crash recovery: kill control-1 while a receipt is held =="
  reset_device
  post "$DEV/pause" '{"channel":"A"}'
  post "$C1/batches" '{"request_key":"'$RUN'-a4","channel":"A","steps":[8,8]}'
  BID=$(echo "$BODY" | jsonget "d['batch_id']")
  wait_held A 10 && ok "receipt held (deduction persisted, 100->92)" || bad "receipt held"
  eval "$CRASH_KILL_CMD"
  sleep 1
  echo "  control-1 killed; waiting for lease expiry and takeover by control-2..."
  sleep 25
  check "no double deduction while control-1 is down" "$(balance A)" "92"
  check "batch still pending (receipt not released)" "$(batch_field "$BID" "d['status']" "$C2")" "pending"
  post "$DEV/release" '{"channel":"A"}'
  wait_status "$BID" "completed" 20 "$C2" && ok "batch completed via surviving control-2" || bad "batch completed via surviving control-2"
  check "A deducted exactly 16 (no double)" "$(balance A)" "84"
  eval "$CRASH_START_CMD"
  sleep 5
  check "control-1 back" "$(curl -s "$C1/healthz" | jsonget "d['status']")" "ok"
fi

echo
echo "passed: $PASS, failed: $FAIL"
[ "$FAIL" -eq 0 ]

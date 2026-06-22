#!/usr/bin/env bash
#
# End-to-end smoke test of the full SAGA choreography over Kafka.
#
# Brings up the infra (Postgres + Kafka), starts all three services, places
# orders via the Portfolio REST API, and asserts each order reaches the expected
# terminal state — covering successful BUY/SELL, insufficient balance/asset,
# unknown symbol, and market-unavailable handling — then checks the audit trail.
#
# Usage:  ./scripts/e2e.sh
# Requires: docker (compose) + a Rust toolchain. Exits non-zero on any failure.
set -euo pipefail
cd "$(dirname "$0")/.."

MARKET=http://127.0.0.1:8081
PORT=http://127.0.0.1:8082
AUDIT=http://127.0.0.1:8083
DEMO=11111111-1111-1111-1111-111111111111
PIDS=()
FAILED=0

log()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
pass() { printf '  \033[32mPASS\033[0m %s\n' "$*"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAILED=1; }

cleanup() {
  log "Shutting down services"
  for pid in "${PIDS[@]:-}"; do kill -TERM "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

wait_health() { # url
  for _ in $(seq 1 80); do curl -sf -o /dev/null "$1/health" && return 0; sleep 0.25; done
  return 1
}

place() { # side symbol qty -> order_id
  curl -s -X POST "$PORT/orders" -H 'content-type: application/json' \
    -d "{\"user_id\":\"$DEMO\",\"symbol\":\"$2\",\"side\":\"$1\",\"quantity\":\"$3\"}" \
    | sed -n 's/.*"order_id":"\([^"]*\)".*/\1/p'
}

final_status() { # order_id -> EXECUTED|REJECTED (waits up to ~12s)
  for _ in $(seq 1 48); do
    s=$(curl -s "$PORT/orders/$1" | sed -n 's/.*"status":"\([A-Z]*\)".*/\1/p')
    [ "$s" = EXECUTED ] || [ "$s" = REJECTED ] && { echo "$s"; return; }
    sleep 0.25
  done
  echo "$s"
}

expect() { # description side symbol qty expected_status
  local desc=$1 oid status
  oid=$(place "$2" "$3" "$4")
  [ -n "$oid" ] || { fail "$desc (no order_id returned)"; return; }
  status=$(final_status "$oid")
  if [ "$status" = "$5" ]; then pass "$desc -> $status"; else fail "$desc -> $status (expected $5)"; fi
  LAST_OID=$oid
}

# --- Infra -----------------------------------------------------------------
log "Bringing up infrastructure"
docker compose up -d postgres db-init kafka kafka-init >/dev/null
for _ in $(seq 1 60); do
  [ "$(docker inspect -f '{{.State.Health.Status}}' exchange-postgres 2>/dev/null)" = healthy ] && \
  [ "$(docker inspect -f '{{.State.Health.Status}}' exchange-kafka    2>/dev/null)" = healthy ] && break
  sleep 1
done

log "Building services"
cargo build -q

set -a; . ./.env; set +a

# --- Start services (DOGE forced to "market unavailable" for the failure test) ---
log "Starting services"
MARKET_FAIL_SYMBOLS=DOGE RUST_LOG=warn ./target/debug/market-service >/tmp/e2e-market.log 2>&1 & PIDS+=($!)
RUST_LOG=warn ./target/debug/portfolio-service >/tmp/e2e-portfolio.log 2>&1 & PIDS+=($!)
RUST_LOG=warn ./target/debug/audit-service     >/tmp/e2e-audit.log     2>&1 & PIDS+=($!)

wait_health "$MARKET" && wait_health "$PORT" && wait_health "$AUDIT" || { fail "services did not become healthy"; exit 1; }
sleep 1

# --- Scenarios -------------------------------------------------------------
log "Order scenarios"
expect "successful BUY (ETH)"            BUY  ETH  1     EXECUTED
ETH_OID=$LAST_OID
expect "successful SELL (BTC)"           SELL BTC  0.1   EXECUTED
expect "insufficient balance (BUY BTC)"  BUY  BTC  100000 REJECTED
expect "insufficient asset (SELL ADA)"   SELL ADA  5     REJECTED
expect "unknown symbol (BUY ZZZ)"        BUY  ZZZ  1     REJECTED
expect "market unavailable (BUY DOGE)"   BUY  DOGE 1     REJECTED

# --- Audit trail -----------------------------------------------------------
log "Audit trail for the executed ETH order"
sleep 2
trail=$(curl -s "$AUDIT/audit/orders/$ETH_OID")
for et in ORDER_CREATED PRICE_QUOTED ORDER_EXECUTED; do
  if echo "$trail" | grep -q "\"$et\""; then pass "audit has $et"; else fail "audit missing $et"; fi
done

# --- Result ----------------------------------------------------------------
echo
if [ "$FAILED" -eq 0 ]; then
  printf '\033[32mALL E2E CHECKS PASSED\033[0m\n'
else
  printf '\033[31mE2E CHECKS FAILED\033[0m  (see /tmp/e2e-*.log)\n'
fi
exit $FAILED

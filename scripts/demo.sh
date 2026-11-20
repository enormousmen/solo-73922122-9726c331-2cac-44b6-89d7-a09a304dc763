#!/usr/bin/env bash
# End-to-end demo against a running lease API.
#
#   docker compose up -d --build
#   ./scripts/demo.sh                  # defaults to http://127.0.0.1:8000
#   BASE=http://host:8000 ./scripts/demo.sh
#
# The second half walks the relocation-hold flow: reserve an in-pool address
# for an out-of-pool lease, confirm the move inside the maintenance window,
# and see the pool-switch guard that protects held addresses.
set -euo pipefail

BASE="${BASE:-http://127.0.0.1:8000}"
TOKEN="${OPERATOR_TOKEN:-}"
AUTH=()
if [[ -n "$TOKEN" ]]; then
  AUTH=(-H "X-Operator-Token: $TOKEN")
fi

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
api() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -sS -X "$method" "$BASE$path" -H 'Content-Type: application/json' "${AUTH[@]}" -d "$body"
  else
    curl -sS -X "$method" "$BASE$path" "${AUTH[@]}"
  fi
}
json() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)"; }
pretty() { python3 -m json.tool; }

say "Health"
api GET /health | pretty

say "Current pool"
api GET /v1/pool | pretty

say "Client laptop-01 acquires a lease"
LAPTOP=$(api POST /v1/leases '{"client_id":"laptop-01"}')
echo "$LAPTOP" | pretty
LAPTOP_ID=$(echo "$LAPTOP" | json '["lease_id"]')

say "Client laptop-01 asks again: same unexpired lease is returned"
api POST /v1/leases '{"client_id":"laptop-01"}' | pretty

say "Client tablet-02 acquires a lease"
TABLET=$(api POST /v1/leases '{"client_id":"tablet-02"}')
echo "$TABLET" | pretty
TABLET_ID=$(echo "$TABLET" | json '["lease_id"]')
TABLET_IP=$(echo "$TABLET" | json '["ip"]')

say "laptop-01 renews"
api POST "/v1/leases/$LAPTOP_ID/renew" '{"ttl_seconds":7200}' | pretty

say "Operator switches the pool (10.20.0.0/24 -> 10.30.0.0/27)"
api PUT /v1/pool '{"cidr":"10.30.0.0/27"}' | pretty

say "Out-of-pool addresses still occupied (with expiry)"
api GET /v1/admin/out-of-pool | pretty

say "Renewing laptop-01 (address now outside pool) is rejected"
api POST "/v1/leases/$LAPTOP_ID/renew" '{}' | pretty

say "A new client gets a fresh address from the new pool"
api POST /v1/leases '{"client_id":"phone-03"}' | pretty

# ------------------------------------------------------- relocation holds

say "RELOCATION: laptop-01 (out-of-pool) asks to hold the lowest free in-pool address"
HOLD=$(api POST /v1/holds "{\"lease_id\":\"$LAPTOP_ID\"}")
echo "$HOLD" | pretty
HOLD_ID=$(echo "$HOLD" | json '["hold_id"]')
HOLD_IP=$(echo "$HOLD" | json '["ip"]')
HOLD_UNTIL=$(echo "$HOLD" | json '["expires_at"]')

say "Retrying the hold request returns the SAME id, address and deadline"
RETRY=$(api POST /v1/holds "{\"lease_id\":\"$LAPTOP_ID\"}")
echo "$RETRY" | pretty
echo "reused=$(echo "$RETRY" | json '["reused"]') (expect true), same hold id: \
$(test "$(echo "$RETRY" | json '["hold_id"]')" = "$HOLD_ID" && echo yes || echo NO)"

say "Pool usage now deducts the held address (held_in_pool=1, free drops by one)"
api GET /v1/pool | pretty

say "An ordinary acquire SKIPS the held address (it must not get $HOLD_IP)"
api POST /v1/leases '{"client_id":"printer-04"}' | pretty

say "Pool switch that would strand held address $HOLD_IP is REJECTED, pool unchanged"
api PUT /v1/pool '{"cidr":"10.99.0.0/27"}' | pretty
api GET /v1/pool | pretty

say "tablet-02 also reserves an address, then releases its old lease"
HOLD2=$(api POST /v1/holds "{\"lease_id\":\"$TABLET_ID\"}")
echo "$HOLD2" | pretty
HOLD2_ID=$(echo "$HOLD2" | json '["hold_id"]')
api POST "/v1/leases/$TABLET_ID/release" | pretty

say "tablet-02's hold is invalidated immediately and its address is free again"
api GET "/v1/holds/$HOLD2_ID" | pretty
api GET /v1/pool | pretty

say "Maintenance window: confirm laptop-01's hold"
CONFIRMED=$(api POST "/v1/holds/$HOLD_ID/confirm")
echo "$CONFIRMED" | pretty
NEW_LEASE_ID=$(echo "$CONFIRMED" | json '["new_lease_id"]')

say "Old lease is ended; the new lease is active on the held address $HOLD_IP"
api GET "/v1/leases/$LAPTOP_ID" | pretty
api GET "/v1/leases/$NEW_LEASE_ID" | pretty

say "Confirming the SAME hold again only returns the original new lease id"
AGAIN=$(api POST "/v1/holds/$HOLD_ID/confirm")
echo "$AGAIN" | pretty
echo "same new lease id: \
$(test "$(echo "$AGAIN" | json '["new_lease_id"]')" = "$NEW_LEASE_ID" && echo yes || echo NO)"

# ------------------------------------------------------- misc / persistence

say "laptop-01 releases its out-of-pool address -> it is no longer occupied"
api POST "/v1/leases/$LAPTOP_ID/release" | pretty
api GET /v1/admin/out-of-pool | pretty

say "Invalid pool is rejected; the old pool keeps serving"
api PUT /v1/pool '{"cidr":"10.30.0.0/40"}' | pretty
api GET /v1/pool | pretty

if [[ "${DEMO_SKIP_RESTART:-0}" != "1" ]] && command -v docker >/dev/null 2>&1; then
  say "Restart the container; the final state comes back from the volume"
  docker compose restart >/dev/null
  sleep 2
  api GET /health | pretty
  api GET "/v1/holds/$HOLD_ID" | pretty
  api GET /v1/pool | pretty
fi

say "Done. Data persists in the 'lease-data' Docker volume across restarts."

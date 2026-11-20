# Branch Gateway — Single-Subnet IPv4 Lease API

FastAPI + SQLite lease broker for a branch gateway. One subnet (pool) is
active at a time; the operator can switch pools, and addresses that fall out
of the pool are drained by expiry or explicit release. Operators can also
pre-book an in-pool address for a device whose current lease is outside the
pool, then confirm the move inside a maintenance window.

## Run it

```bash
docker compose up -d --build
curl -s localhost:8000/health
./scripts/demo.sh          # runs the full scenario against the container
```

Interactive docs: <http://localhost:8000/docs>.

Local (no Docker):

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest httpx
uvicorn app.main:app --reload
```

Configuration (env vars, see `docker-compose.yml`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `DB_PATH` | `./data/leases.db` | SQLite file (compose: `/data/leases.db` on a volume) |
| `DEFAULT_POOL_CIDR` | `10.20.0.0/24` | Seeds a **fresh** database only |
| `DEFAULT_TTL_SECONDS` | `3600` | Lease lifetime when the request omits one |
| `MIN_TTL_SECONDS` / `MAX_TTL_SECONDS` | `60` / `604800` | Accepted `ttl_seconds` range |
| `HOLD_TTL_SECONDS` | `300` | How long a relocation hold keeps its address (5 minutes) |
| `OPERATOR_TOKEN` | empty | When set, operator endpoints require `X-Operator-Token` |

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Liveness, reports the current pool |
| `GET` | `/v1/pool` | Current pool, capacity and usage counters |
| `PUT` | `/v1/pool` | Switch pool, body `{"cidr": "10.30.0.0/27"}` (operator) |
| `POST` | `/v1/leases` | Acquire, body `{"client_id": "...", "ttl_seconds": 3600?}` |
| `GET` | `/v1/leases/{lease_id}` | Inspect any lease (active or final state) |
| `POST` | `/v1/leases/{lease_id}/renew` | Renew an active in-pool lease |
| `POST` | `/v1/leases/{lease_id}/release` | Release an active lease |
| `POST` | `/v1/holds` | Hold the lowest free in-pool address for an out-of-pool lease (operator) |
| `GET` | `/v1/holds/{hold_id}` | Inspect a relocation hold (operator) |
| `POST` | `/v1/holds/{hold_id}/confirm` | Confirm a hold: end the old lease and issue the new one (operator) |
| `GET` | `/v1/admin/out-of-pool` | Still-occupied addresses outside the pool (operator) |

Errors are structured: `{"error": {"code": "...", "message": "..."}}` with
codes such as `pool_exhausted`, `lease_not_found`, `lease_released`,
`lease_expired`, `address_out_of_pool`, `address_in_pool`, `invalid_pool`,
`invalid_ttl`, `hold_not_found`, `hold_expired`, `hold_invalidated`,
`hold_address_out_of_pool`, `unauthorized`.

## Semantics

**Acquire** allocates the lowest free address of the current pool (network
and broadcast excluded; a /31 has two usable addresses). Re-acquiring with the
same `client_id` returns that client's unexpired lease untouched with HTTP
200 (new leases are 201 + `Location`), even if the address is currently
outside the pool — the client keeps its address until the lease ends.

**Renew / release** are addressed by the current lease id. Release works for
in-pool and out-of-pool addresses; renew is refused for an address outside
the current pool (`409 address_out_of_pool`). Repeating a release is a
successful no-op (`already_released: true`).

**Pool switch** is a single transaction. Invalid CIDRs are rejected with 422
and the previous pool keeps serving; any failure inside the transaction
rolls back, so the old pool and all leases stay authoritative. Switching to
the same pool is a no-op (`changed: false`). Existing leases are *not*
deleted by a switch: addresses that fell out stay occupied until they expire
or are released, and the operator report lists them with `expires_at`.

### Relocation holds (maintenance-window moves)

A device that needs an address change cannot be re-addressed until the
maintenance window, but the operator can reserve its future address ahead
of time against the device's still-valid **out-of-pool** lease:

1. `POST /v1/holds` with `{"lease_id": "..."}` (operator auth) holds the
   **lowest free in-pool address** for five minutes (`HOLD_TTL_SECONDS`) and
   returns `hold_id`, the reserved `ip`, `pool_cidr` and `expires_at`.
2. Retrying the request while the hold is valid returns the **same hold id,
   address and deadline** unchanged (HTTP 200, `reused: true`); a fresh hold
   is 201 with a `Location` header.
3. In the maintenance window, `POST /v1/holds/{hold_id}/confirm` ends the
   source lease and issues a new lease for the same device on the held
   address with the **default lease time**, all in one transaction. If
   anything fails, the old lease record stays active and the hold remains
   confirmable. Confirming the same hold again never mints a second lease:
   it returns the original `new_lease_id`.

Hold lifecycle rules:

- A hold is only created for an active lease whose address is **outside** the
  current pool; an in-pool lease gets `409 address_in_pool`.
- While a hold is active, ordinary `POST /v1/leases` allocations skip its
  address, and `GET /v1/pool` reports it under `held_in_pool` and subtracts
  it from `free_in_pool`.
- If the source lease is **released or expires first**, the hold is marked
  `invalidated` immediately and the reserved address becomes allocatable
  again. When the hold's own five-minute deadline passes, it becomes
  `expired` with the same effect. Only a still-valid hold can be confirmed;
  a first confirmation of an ended hold fails with `409 hold_expired` /
  `409 hold_invalidated`.
- A pool switch that would leave a held address outside the new pool's
  assignable range is **rejected** (`409 hold_address_out_of_pool`) and the
  previous pool keeps serving; a switch whose range still covers the held
  address is allowed and the hold stays confirmable.
- Holds are stored in SQLite like leases, so they survive a restart; a valid
  hold can still be confirmed after the container relaunches.

**Expiry**: leases are swept to `release_reason: "expired"` at the start of
each write transaction and are reported as `expired` once past their
deadline even before a sweep. Expired rows stay in the table for auditing
and `GET /v1/leases/{id}`.

**Restart**: the pool is stored in the database's `settings` table; the
environment default only seeds a fresh file. Relaunching the container
restores the pool, all leases and the switch history (`pool_changes`).

## Concurrency

Every mutating operation runs in `BEGIN IMMEDIATE`, i.e. under SQLite's
single write lock; reads run against WAL snapshots. A pool switch is one
transaction, so a switch racing with acquire/renew/release has the same
result as one of the two serial orders — never a half-switched state and
never a lease issued from the old pool after the switch committed. The tests
in `tests/test_concurrency.py` assert this equivalence over repeated races.

## Tests

```bash
python3 -m pytest -q
```

- `tests/test_subnets.py` — CIDR parsing, usable ranges, lowest-free search
- `tests/test_store.py` — allocation, idempotency, renew/release rules, expiry,
  pool moves, restart recovery
- `tests/test_holds.py` — relocation holds: reservation, retry, free-counter
  deduction, invalidation/expiry, atomic confirmation, restart, switch guard
- `tests/test_holds_api.py` — hold HTTP contract, structured errors, operator auth
- `tests/test_api.py` — HTTP status codes, structured errors, operator auth
- `tests/test_concurrency.py` — switch-vs-operation races and address uniqueness

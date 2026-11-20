"""Store-level behaviour of relocation holds (the maintenance-window move).

A device whose lease sits outside the current pool can have the lowest free
in-pool address *held* for it.  Holds last ``hold_ttl_seconds`` (default five
minutes), keep their address out of ordinary allocations and the free
counters, and are confirmed in one atomic transaction that ends the old lease
and issues a new one with the default lease time.
"""

from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.config import Settings
from app.db import LeaseError, Store


@pytest.fixture
def hold_settings(tmp_path) -> Settings:
    return Settings(
        db_path=str(tmp_path / "holds.db"),
        default_pool_cidr="10.20.0.0/29",
        default_ttl_seconds=3600,
        min_ttl_seconds=60,
        max_ttl_seconds=86400,
        hold_ttl_seconds=300,
    )


@pytest.fixture
def hold_store(hold_settings: Settings, clock) -> Store:
    store = Store(hold_settings, clock=clock)
    store.initialize()
    return store


def _out_of_pool_lease(store: Store, client: str = "device-a") -> dict:
    lease, _ = store.acquire(client)
    store.switch_pool("10.30.0.0/29")
    return lease


def test_create_hold_returns_lowest_free_in_pool_address(hold_store: Store) -> None:
    lease = _out_of_pool_lease(hold_store)
    hold, reused = hold_store.create_hold(lease["lease_id"])
    assert reused is False
    assert hold["status"] == "active"
    assert hold["source_lease_id"] == lease["lease_id"]
    assert hold["client_id"] == lease["client_id"]
    # New pool starts empty: lowest usable address is 10.30.0.1.
    assert hold["ip"] == "10.30.0.1"
    assert hold["pool_cidr"] == "10.30.0.0/29"
    assert hold["seconds_remaining"] == 300
    assert hold["new_lease_id"] is None


def test_retry_hold_returns_same_id_address_and_deadline(hold_store: Store) -> None:
    lease = _out_of_pool_lease(hold_store)
    first, reused = hold_store.create_hold(lease["lease_id"])
    assert reused is False
    second, reused2 = hold_store.create_hold(lease["lease_id"])
    assert reused2 is True
    assert second["hold_id"] == first["hold_id"]
    assert second["ip"] == first["ip"]
    assert second["expires_at"] == first["expires_at"]


def test_hold_reserves_lowest_free_after_existing_lease(hold_store: Store) -> None:
    lease = _out_of_pool_lease(hold_store)
    # One address of the new pool is already taken by another client.
    other, _ = hold_store.acquire("device-b")
    assert other["ip"] == "10.30.0.1"
    hold, _ = hold_store.create_hold(lease["lease_id"])
    assert hold["ip"] == "10.30.0.2"


def test_ordinary_acquire_skips_held_address(hold_store: Store) -> None:
    lease = _out_of_pool_lease(hold_store)
    hold, _ = hold_store.create_hold(lease["lease_id"])
    assert hold["ip"] == "10.30.0.1"
    fresh, _ = hold_store.acquire("device-c")
    assert fresh["ip"] == "10.30.0.2"
    info = hold_store.pool_info()
    assert info["held_in_pool"] == 1
    assert info["allocated_in_pool"] == 1
    assert info["free_in_pool"] == info["usable_addresses"] - 2


def test_hold_counts_towards_pool_exhaustion(hold_store: Store) -> None:
    lease = _out_of_pool_lease(hold_store)  # /29 has six usable addresses
    for i in range(5):  # five normal leases + one hold fills the /29
        hold_store.acquire(f"filler-{i}")
    hold, _ = hold_store.create_hold(lease["lease_id"])
    assert hold["ip"] == "10.30.0.6"
    with pytest.raises(LeaseError) as exc:
        hold_store.acquire("overflow")
    assert exc.value.code == "pool_exhausted"


def test_create_hold_rejects_in_pool_lease(hold_store: Store) -> None:
    lease, _ = hold_store.acquire("device-a")  # no pool switch: still in pool
    with pytest.raises(LeaseError) as exc:
        hold_store.create_hold(lease["lease_id"])
    assert exc.value.status_code == 409
    assert exc.value.code == "address_in_pool"


def test_create_hold_rejects_unknown_released_expired(hold_store: Store, clock) -> None:
    with pytest.raises(LeaseError) as exc:
        hold_store.create_hold("missing")
    assert exc.value.status_code == 404
    assert exc.value.code == "lease_not_found"

    released = _out_of_pool_lease(hold_store, "device-r")
    hold_store.release(released["lease_id"])
    with pytest.raises(LeaseError) as exc:
        hold_store.create_hold(released["lease_id"])
    assert exc.value.code == "lease_released"

    expired = _out_of_pool_lease(hold_store, "device-e")
    clock.advance(3601)
    with pytest.raises(LeaseError) as exc:
        hold_store.create_hold(expired["lease_id"])
    assert exc.value.code == "lease_expired"


def test_hold_expires_and_address_becomes_available(hold_store: Store, clock) -> None:
    lease = _out_of_pool_lease(hold_store)
    hold, _ = hold_store.create_hold(lease["lease_id"])
    assert hold_store.pool_info()["held_in_pool"] == 1

    clock.advance(301)
    # Reads already report the deadline as expired even before a sweep.
    assert hold_store.get_hold(hold["hold_id"])["status"] == "expired"
    # A write sweeps the hold; its address is allocatable again.
    fresh, _ = hold_store.acquire("device-d")
    assert fresh["ip"] == "10.30.0.1"
    assert hold_store.pool_info()["held_in_pool"] == 0
    assert hold_store.get_hold(hold["hold_id"])["end_reason"] == "expired"


def test_source_lease_release_invalidates_hold_immediately(hold_store: Store) -> None:
    lease = _out_of_pool_lease(hold_store)
    hold, _ = hold_store.create_hold(lease["lease_id"])
    hold_store.release(lease["lease_id"])

    refreshed = hold_store.get_hold(hold["hold_id"])
    assert refreshed["status"] == "invalidated"
    assert refreshed["end_reason"] == "lease_released"
    # The held address is free for ordinary clients at once.
    fresh, _ = hold_store.acquire("device-d")
    assert fresh["ip"] == "10.30.0.1"
    with pytest.raises(LeaseError) as exc:
        hold_store.confirm_hold(hold["hold_id"])
    assert exc.value.code == "hold_invalidated"


def test_source_lease_expiry_invalidates_hold(hold_store: Store, clock) -> None:
    # A short-lived out-of-pool lease.
    lease, _ = hold_store.acquire("device-a", ttl_seconds=120)
    hold_store.switch_pool("10.30.0.0/29")
    hold, _ = hold_store.create_hold(lease["lease_id"])

    clock.advance(121)
    hold_store.acquire("trigger-sweep")  # any write triggers the sweep
    refreshed = hold_store.get_hold(hold["hold_id"])
    assert refreshed["status"] == "invalidated"
    assert refreshed["end_reason"] == "lease_expired"


def test_reads_reflect_lease_expiry_before_any_write(
    hold_store: Store, clock
) -> None:
    lease, _ = hold_store.acquire("device-a", ttl_seconds=120)
    hold_store.switch_pool("10.30.0.0/29")
    hold, _ = hold_store.create_hold(lease["lease_id"])

    clock.advance(121)
    # No write transaction runs: read-only views must already drop the hold.
    assert hold_store.get_hold(hold["hold_id"])["status"] == "invalidated"
    info = hold_store.pool_info()
    assert info["held_in_pool"] == 0
    assert info["free_in_pool"] == info["usable_addresses"]
    # And a subsequent confirmation is refused; the lease itself stays auditable.
    with pytest.raises(LeaseError) as exc:
        hold_store.confirm_hold(hold["hold_id"])
    assert exc.value.code == "hold_invalidated"


def test_confirm_hold_atomically_ends_old_and_issues_new(hold_store: Store) -> None:
    lease = _out_of_pool_lease(hold_store)
    hold, _ = hold_store.create_hold(lease["lease_id"])

    result_hold, new_lease, reused = hold_store.confirm_hold(hold["hold_id"])
    assert reused is False
    assert result_hold["status"] == "confirmed"
    assert result_hold["new_lease_id"] == new_lease["lease_id"]
    # New lease: same device, the held address, current pool, default TTL.
    assert new_lease["client_id"] == lease["client_id"]
    assert new_lease["ip"] == hold["ip"]
    assert new_lease["pool_cidr"] == "10.30.0.0/29"
    assert new_lease["status"] == "active"
    assert new_lease["seconds_remaining"] == 3600
    # The old lease is ended in the same transaction.
    assert hold_store.get_lease(lease["lease_id"])["status"] == "released"
    # The address is now an ordinary active lease, not a hold.
    assert hold_store.pool_info()["held_in_pool"] == 0
    assert hold_store.pool_info()["allocated_in_pool"] == 1


def test_first_confirmation_only_accepts_valid_hold(hold_store: Store, clock) -> None:
    lease = _out_of_pool_lease(hold_store)
    hold, _ = hold_store.create_hold(lease["lease_id"])
    clock.advance(301)
    with pytest.raises(LeaseError) as exc:
        hold_store.confirm_hold(hold["hold_id"])
    assert exc.value.code == "hold_expired"
    # The source lease survived the failed confirmation.
    assert hold_store.get_lease(lease["lease_id"])["status"] == "active"

    with pytest.raises(LeaseError) as exc:
        hold_store.confirm_hold("unknown-hold")
    assert exc.value.status_code == 404
    assert exc.value.code == "hold_not_found"


def test_repeat_confirmation_returns_original_new_lease(hold_store: Store) -> None:
    lease = _out_of_pool_lease(hold_store)
    hold, _ = hold_store.create_hold(lease["lease_id"])
    _, first_new, reused = hold_store.confirm_hold(hold["hold_id"])
    assert reused is False

    again_hold, again_lease, reused2 = hold_store.confirm_hold(hold["hold_id"])
    assert reused2 is True
    assert again_lease["lease_id"] == first_new["lease_id"]
    assert again_hold["new_lease_id"] == first_new["lease_id"]
    # No extra lease was minted.
    _, second_try, _ = hold_store.confirm_hold(hold["hold_id"])
    assert second_try["lease_id"] == first_new["lease_id"]


def test_hold_survives_restart(hold_settings: Settings, clock) -> None:
    first = Store(hold_settings, clock=clock)
    first.initialize()
    lease = _out_of_pool_lease(first)
    hold, _ = first.create_hold(lease["lease_id"])

    reopened = Store(hold_settings, clock=clock)
    reopened.initialize()
    restored = reopened.get_hold(hold["hold_id"])
    assert restored["status"] == "active"
    assert restored["ip"] == hold["ip"]
    assert restored["expires_at"] == hold["expires_at"]
    # Confirmation still works after the restart.
    _, new_lease, reused = reopened.confirm_hold(hold["hold_id"])
    assert reused is False
    assert new_lease["ip"] == hold["ip"]
    assert reopened.get_lease(lease["lease_id"])["status"] == "released"


def test_confirmed_hold_survives_restart(hold_settings: Settings, clock) -> None:
    first = Store(hold_settings, clock=clock)
    first.initialize()
    lease = _out_of_pool_lease(first)
    hold, _ = first.create_hold(lease["lease_id"])
    _, new_lease, _ = first.confirm_hold(hold["hold_id"])

    reopened = Store(hold_settings, clock=clock)
    reopened.initialize()
    _, again, reused = reopened.confirm_hold(hold["hold_id"])
    assert reused is True
    assert again["lease_id"] == new_lease["lease_id"]


def test_switch_pool_rejected_when_held_address_stranded(hold_store: Store) -> None:
    lease = _out_of_pool_lease(hold_store)  # hold will pin a 10.30.0.x address
    hold, _ = hold_store.create_hold(lease["lease_id"])

    with pytest.raises(LeaseError) as exc:
        hold_store.switch_pool("10.99.0.0/29")
    assert exc.value.status_code == 409
    assert exc.value.code == "hold_address_out_of_pool"
    # Original pool and the hold both survive.
    assert hold_store.pool_info()["cidr"] == "10.30.0.0/29"
    assert hold_store.get_hold(hold["hold_id"])["status"] == "active"


def test_switch_pool_allowed_when_held_address_still_assignable(
    hold_store: Store,
) -> None:
    lease = _out_of_pool_lease(hold_store)
    hold, _ = hold_store.create_hold(lease["lease_id"])  # 10.30.0.1
    # A superset pool that still contains the held usable address.
    result = hold_store.switch_pool("10.30.0.0/28")
    assert result["changed"] is True
    assert hold_store.get_hold(hold["hold_id"])["status"] == "active"
    _, new_lease, _ = hold_store.confirm_hold(hold["hold_id"])
    assert new_lease["ip"] == "10.30.0.1"
    assert new_lease["pool_cidr"] == "10.30.0.0/28"


def test_switch_pool_check_ignores_finished_holds(hold_store: Store) -> None:
    lease = _out_of_pool_lease(hold_store)
    hold, _ = hold_store.create_hold(lease["lease_id"])
    hold_store.release(lease["lease_id"])  # invalidates the hold
    # The switch that was blocked while the hold was active is now allowed.
    result = hold_store.switch_pool("10.99.0.0/29")
    assert result["changed"] is True


def test_confirmation_failure_keeps_source_lease(hold_store: Store) -> None:
    lease = _out_of_pool_lease(hold_store)
    hold, _ = hold_store.create_hold(lease["lease_id"])

    # Abort the new-lease insert inside the confirmation transaction.
    conn = hold_store._connect()
    try:
        conn.executescript(
            """
            CREATE TRIGGER fail_new_lease
            BEFORE INSERT ON leases
            BEGIN
                SELECT RAISE(ABORT, 'simulated disk failure');
            END;
            """
        )
    finally:
        conn.close()

    with pytest.raises(sqlite3.IntegrityError):
        hold_store.confirm_hold(hold["hold_id"])

    conn = hold_store._connect()
    try:
        conn.execute("DROP TRIGGER fail_new_lease")
    finally:
        conn.close()

    # Atomicity: the old lease record is untouched and still active.
    source = hold_store.get_lease(lease["lease_id"])
    assert source["status"] == "active"
    assert source["released_at"] is None
    # The hold is still confirmable after the rolled-back attempt.
    refreshed = hold_store.get_hold(hold["hold_id"])
    assert refreshed["status"] == "active"
    _, new_lease, reused = hold_store.confirm_hold(hold["hold_id"])
    assert reused is False
    assert new_lease["ip"] == hold["ip"]


def test_two_holds_never_share_an_address(hold_store: Store) -> None:
    # Two devices move out of the pool, then both ask for relocation holds.
    lease_a, _ = hold_store.acquire("device-a")
    lease_b, _ = hold_store.acquire("device-b")
    hold_store.switch_pool("10.30.0.0/29")
    assert lease_a["pool_cidr"] == "10.20.0.0/29"

    hold_a, _ = hold_store.create_hold(lease_a["lease_id"])
    hold_b, _ = hold_store.create_hold(lease_b["lease_id"])
    assert hold_a["ip"] == "10.30.0.1"
    assert hold_b["ip"] == "10.30.0.2"
    info = hold_store.pool_info()
    assert info["held_in_pool"] == 2
    assert info["free_in_pool"] == info["usable_addresses"] - 2


def test_concurrent_holds_and_acquires_never_share_addresses(
    hold_store: Store,
) -> None:
    leases = []
    for i in range(3):
        lease, _ = hold_store.acquire(f"device-{i}")
        leases.append(lease)
    hold_store.switch_pool("10.30.0.0/29")  # six usable addresses

    def hold_one(index: int) -> str:
        hold, _ = hold_store.create_hold(leases[index]["lease_id"])
        return hold["ip"]

    def acquire_one(index: int) -> str:
        lease, _ = hold_store.acquire(f"filler-{index}")
        return lease["ip"]

    with ThreadPoolExecutor(max_workers=6) as pool:
        hold_futures = [pool.submit(hold_one, i) for i in range(3)]
        acquire_futures = [pool.submit(acquire_one, i) for i in range(3)]
        addresses = [f.result() for f in hold_futures + acquire_futures]

    # Every writer is serialized by BEGIN IMMEDIATE: six distinct addresses.
    assert len(addresses) == len(set(addresses))
    assert hold_store.pool_info()["held_in_pool"] == 3


def test_concurrent_retry_creates_a_single_hold(hold_store: Store) -> None:
    lease = _out_of_pool_lease(hold_store)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(
            pool.map(lambda _: hold_store.create_hold(lease["lease_id"]), range(8))
        )
    hold_ids = {hold["hold_id"] for hold, _ in results}
    assert len(hold_ids) == 1
    assert hold_store.pool_info()["held_in_pool"] == 1


def test_concurrent_confirmation_mints_one_new_lease(hold_store: Store) -> None:
    lease = _out_of_pool_lease(hold_store)
    hold, _ = hold_store.create_hold(lease["lease_id"])

    def confirm() -> tuple[str, str]:
        _, new_lease, reused = hold_store.confirm_hold(hold["hold_id"])
        return new_lease["lease_id"], "reused" if reused else "fresh"

    with ThreadPoolExecutor(max_workers=4) as pool:
        outcomes = list(pool.map(lambda _: confirm(), range(6)))

    lease_ids = {lease_id for lease_id, _ in outcomes}
    assert len(lease_ids) == 1
    assert sorted(kind for _, kind in outcomes) == ["fresh"] + ["reused"] * 5
    assert hold_store.get_lease(lease["lease_id"])["status"] == "released"


def test_new_request_after_hold_expiry_creates_a_fresh_hold(
    hold_store: Store, clock
) -> None:
    lease = _out_of_pool_lease(hold_store)
    first, _ = hold_store.create_hold(lease["lease_id"])
    clock.advance(301)
    hold_store.acquire("trigger-sweep")  # sweeps the expired hold

    second, reused = hold_store.create_hold(lease["lease_id"])
    assert reused is False
    assert second["hold_id"] != first["hold_id"]
    assert second["status"] == "active"
    assert second["seconds_remaining"] == 300
    # The old row is retained for auditing in its terminal state.
    assert hold_store.get_hold(first["hold_id"])["status"] == "expired"


def test_invalidation_then_new_request_creates_a_fresh_hold(hold_store: Store) -> None:
    lease_a = _out_of_pool_lease(hold_store, "device-a")
    first, _ = hold_store.create_hold(lease_a["lease_id"])
    hold_store.release(lease_a["lease_id"])
    # The source lease ended, so no new hold can be attached to it.
    with pytest.raises(LeaseError) as exc:
        hold_store.create_hold(lease_a["lease_id"])
    assert exc.value.code == "lease_released"
    assert hold_store.get_hold(first["hold_id"])["status"] == "invalidated"


def test_confirmed_new_lease_uses_default_ttl(hold_settings: Settings, clock) -> None:
    # Explicitly prove the new lease gets the *default* lease time, not the
    # source lease's (possibly different) TTL.
    hold_settings = hold_settings.with_(default_ttl_seconds=7200)
    store = Store(hold_settings, clock=clock)
    store.initialize()
    lease, _ = store.acquire("device-a", ttl_seconds=120)
    store.switch_pool("10.30.0.0/29")
    hold, _ = store.create_hold(lease["lease_id"])
    _, new_lease, _ = store.confirm_hold(hold["hold_id"])
    assert new_lease["seconds_remaining"] == 7200
    assert new_lease["lease_id"] != lease["lease_id"]

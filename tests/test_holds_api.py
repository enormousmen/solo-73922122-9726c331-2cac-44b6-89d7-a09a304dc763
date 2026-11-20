"""HTTP-level contract tests for relocation holds."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


def _out_of_pool_lease(client: TestClient, device: str = "device-a") -> dict:
    lease = client.post("/v1/leases", json={"client_id": device}).json()
    switch = client.put("/v1/pool", json={"cidr": "10.30.0.0/29"})
    assert switch.status_code == 200
    return lease


def test_create_and_retry_hold_flow(client: TestClient) -> None:
    lease = _out_of_pool_lease(client)

    created = client.post("/v1/holds", json={"lease_id": lease["lease_id"]})
    assert created.status_code == 201
    body = created.json()
    assert created.headers["location"] == f"/v1/holds/{body['hold_id']}"
    assert body["reused"] is False
    assert body["status"] == "active"
    assert body["ip"] == "10.30.0.1"
    assert body["source_lease_id"] == lease["lease_id"]
    assert body["seconds_remaining"] > 0

    # Retrying while the hold is valid returns the same id, address, deadline.
    retry = client.post("/v1/holds", json={"lease_id": lease["lease_id"]})
    assert retry.status_code == 200
    retry_body = retry.json()
    assert retry_body["reused"] is True
    assert retry_body["hold_id"] == body["hold_id"]
    assert retry_body["ip"] == body["ip"]
    assert retry_body["expires_at"] == body["expires_at"]

    fetched = client.get(f"/v1/holds/{body['hold_id']}")
    assert fetched.status_code == 200
    assert fetched.json()["hold_id"] == body["hold_id"]


def test_hold_blocks_ordinary_acquire_and_reduces_free(client: TestClient) -> None:
    lease = _out_of_pool_lease(client)
    hold = client.post("/v1/holds", json={"lease_id": lease["lease_id"]}).json()

    fresh = client.post("/v1/leases", json={"client_id": "other"})
    assert fresh.status_code == 201
    assert fresh.json()["ip"] != hold["ip"]

    pool = client.get("/v1/pool").json()
    assert pool["held_in_pool"] == 1
    assert pool["free_in_pool"] == pool["usable_addresses"] - pool["allocated_in_pool"] - 1


def test_confirm_hold_flow(client: TestClient) -> None:
    lease = _out_of_pool_lease(client)
    hold = client.post("/v1/holds", json={"lease_id": lease["lease_id"]}).json()

    confirmed = client.post(f"/v1/holds/{hold['hold_id']}/confirm")
    assert confirmed.status_code == 200
    body = confirmed.json()
    assert body["reused"] is False
    assert body["hold"]["status"] == "confirmed"
    assert body["new_lease"]["ip"] == hold["ip"]
    assert body["new_lease"]["client_id"] == lease["client_id"]
    assert body["new_lease"]["pool_cidr"] == "10.30.0.0/29"
    assert body["new_lease_id"] == body["new_lease"]["lease_id"]

    old = client.get(f"/v1/leases/{lease['lease_id']}")
    assert old.json()["status"] == "released"

    # Second confirmation returns the original new lease, never a new one.
    again = client.post(f"/v1/holds/{hold['hold_id']}/confirm")
    assert again.status_code == 200
    again_body = again.json()
    assert again_body["reused"] is True
    assert again_body["new_lease_id"] == body["new_lease_id"]
    assert again_body["new_lease"]["lease_id"] == body["new_lease_id"]


def test_first_confirmation_rejects_expired_hold(client: TestClient) -> None:
    lease = _out_of_pool_lease(client)
    hold = client.post("/v1/holds", json={"lease_id": lease["lease_id"]}).json()

    # Reaching into the store clock mirrors how the rest of the suite drives time.
    client.app.state.store.clock.value += 301

    response = client.post(f"/v1/holds/{hold['hold_id']}/confirm")
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "hold_expired"
    # Source lease remains in place.
    assert client.get(f"/v1/leases/{lease['lease_id']}").json()["status"] == "active"


def test_released_source_lease_invalidates_hold(client: TestClient) -> None:
    lease = _out_of_pool_lease(client)
    hold = client.post("/v1/holds", json={"lease_id": lease["lease_id"]}).json()

    released = client.post(f"/v1/leases/{lease['lease_id']}/release")
    assert released.status_code == 200

    refreshed = client.get(f"/v1/holds/{hold['hold_id']}")
    assert refreshed.json()["status"] == "invalidated"
    confirm = client.post(f"/v1/holds/{hold['hold_id']}/confirm")
    assert confirm.status_code == 409
    assert confirm.json()["error"]["code"] == "hold_invalidated"
    # The address is immediately allocatable to ordinary clients.
    fresh = client.post("/v1/leases", json={"client_id": "new-kid"})
    assert fresh.json()["ip"] == hold["ip"]


def test_create_hold_rejects_in_pool_lease(client: TestClient) -> None:
    lease = client.post("/v1/leases", json={"client_id": "a"}).json()
    response = client.post("/v1/holds", json={"lease_id": lease["lease_id"]})
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "address_in_pool"


def test_hold_errors_are_structured(client: TestClient) -> None:
    missing = client.post("/v1/holds", json={"lease_id": "nope"})
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "lease_not_found"

    no_hold = client.get("/v1/holds/missing")
    assert no_hold.status_code == 404
    assert no_hold.json()["error"]["code"] == "hold_not_found"

    no_hold_confirm = client.post("/v1/holds/missing/confirm")
    assert no_hold_confirm.status_code == 404
    assert no_hold_confirm.json()["error"]["code"] == "hold_not_found"

    bad_body = client.post("/v1/holds", json={})
    assert bad_body.status_code == 422
    assert bad_body.json()["error"]["code"] == "invalid_request"
    extra = client.post("/v1/holds", json={"lease_id": "x", "bogus": 1})
    assert extra.status_code == 422


def test_pool_switch_blocked_by_active_hold(client: TestClient) -> None:
    lease = _out_of_pool_lease(client)
    hold = client.post("/v1/holds", json={"lease_id": lease["lease_id"]}).json()

    blocked = client.put("/v1/pool", json={"cidr": "10.99.0.0/29"})
    assert blocked.status_code == 409
    assert blocked.json()["error"]["code"] == "hold_address_out_of_pool"
    assert hold["ip"] in blocked.json()["error"]["message"]
    # The original pool keeps serving and the hold stays valid.
    assert client.get("/v1/pool").json()["cidr"] == "10.30.0.0/29"
    assert client.get(f"/v1/holds/{hold['hold_id']}").json()["status"] == "active"

    # A switch whose range still covers the held address is allowed.
    ok = client.put("/v1/pool", json={"cidr": "10.30.0.0/28"})
    assert ok.status_code == 200
    assert ok.json()["changed"] is True


def test_hold_endpoints_require_operator_token(tmp_path) -> None:
    from app.config import Settings
    from app.db import Store
    from app.main import create_app

    settings = Settings(
        db_path=str(tmp_path / "locked.db"),
        default_pool_cidr="10.20.0.0/29",
        operator_token="s3cret",
    )
    with TestClient(create_app(settings, store=Store(settings))) as locked:
        lease = locked.post("/v1/leases", json={"client_id": "a"}).json()
        switch = locked.put(
            "/v1/pool",
            json={"cidr": "10.30.0.0/29"},
            headers={"X-Operator-Token": "s3cret"},
        )
        assert switch.status_code == 200

        assert locked.post("/v1/holds", json={"lease_id": lease["lease_id"]}).status_code == 401
        assert locked.get("/v1/holds/anything").status_code == 401
        assert locked.post("/v1/holds/anything/confirm").status_code == 401

        created = locked.post(
            "/v1/holds",
            json={"lease_id": lease["lease_id"]},
            headers={"X-Operator-Token": "s3cret"},
        )
        assert created.status_code == 201
        hold_id = created.json()["hold_id"]
        confirmed = locked.post(
            f"/v1/holds/{hold_id}/confirm",
            headers={"X-Operator-Token": "s3cret"},
        )
        assert confirmed.status_code == 200


def test_holds_survive_restart(tmp_path) -> None:
    from app.config import Settings
    from app.db import Store
    from app.main import create_app

    settings = Settings(
        db_path=str(tmp_path / "restart.db"),
        default_pool_cidr="10.20.0.0/29",
        hold_ttl_seconds=300,
    )
    with TestClient(create_app(settings, store=Store(settings))) as first:
        lease = first.post("/v1/leases", json={"client_id": "a"}).json()
        first.put("/v1/pool", json={"cidr": "10.30.0.0/29"})
        hold = first.post("/v1/holds", json={"lease_id": lease["lease_id"]}).json()

    # Reopen the same database file with a brand new app/store.
    with TestClient(create_app(settings, store=Store(settings))) as second:
        fetched = second.get(f"/v1/holds/{hold['hold_id']}")
        assert fetched.json()["status"] == "active"
        confirmed = second.post(f"/v1/holds/{hold['hold_id']}/confirm")
        assert confirmed.status_code == 200
        assert confirmed.json()["new_lease"]["ip"] == hold["ip"]
        assert second.get(f"/v1/leases/{lease['lease_id']}").json()["status"] == "released"


@pytest.mark.parametrize("lease_id", ["", "   "])
def test_blank_lease_id_rejected(client: TestClient, lease_id: str) -> None:
    response = client.post("/v1/holds", json={"lease_id": lease_id})
    assert response.status_code == 422

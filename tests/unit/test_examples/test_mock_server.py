"""The mock-server recipe: a real app served on in-memory backends.

Statefulness is the whole claim, and it is exactly what a code read wrongly clears — so
these drive the generated routes over HTTP and assert the properties a schema faker cannot
have: `create` → `list` → `get` coherence, a real `rev` conflict, and identity that the
app's own plane enforces.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from examples.recipes.mock_server.app import build_server
from forze_fastapi.exceptions import ERROR_CODE_HEADER

pytestmark = pytest.mark.unit

_AUTH = {"X-API-Key": "dev-key"}


@pytest.fixture
def client() -> Iterator[TestClient]:
    # As a context manager, so the app's lifespan runs — that is what opens the runtime
    # scope and applies the seed, exactly as uvicorn does when serving it.
    with TestClient(build_server()) as running:
        yield running


def _list(client: TestClient) -> list[dict]:
    response = client.post("/products/list", json={}, headers=_AUTH)
    assert response.status_code == 200, response.text

    return response.json()["hits"]


class TestTheAppRunsUnchanged:
    def test_routes_are_generated_from_the_operation_catalog(self, client: TestClient) -> None:
        # The property the recipe exists for: the served routes *are* the app's catalog,
        # so there is no second contract to drift. operation_id is the operation key.
        operations = {
            operation["operationId"]
            for methods in client.app.openapi()["paths"].values()  # type: ignore[attr-defined]
            for operation in methods.values()
        }

        assert {"products.create", "products.get", "products.list"} <= operations

    def test_identity_is_the_apps_own_plane(self, client: TestClient) -> None:
        # No key → the app's own authn refuses. The mock never mints a principal.
        assert client.post("/products/list", json={}).status_code == 401
        assert (
            client.post(
                "/products/list", json={}, headers={"X-API-Key": "nope"}
            ).status_code
            == 401
        )


class TestItIsStateful:
    def test_seeded_data_is_there_to_build_against(self, client: TestClient) -> None:
        assert {row["name"] for row in _list(client)} == {"Espresso", "Cortado", "Filter"}

    def test_create_then_list_then_get_cohere(self, client: TestClient) -> None:
        created = client.post(
            "/products", json={"name": "Ristretto", "price": 240}, headers=_AUTH
        )
        assert created.status_code in (200, 201), created.text
        product_id = created.json()["id"]

        assert "Ristretto" in {row["name"] for row in _list(client)}

        fetched = client.get(f"/products/{product_id}", headers=_AUTH)
        assert fetched.status_code == 200
        assert fetched.json()["price"] == 240

    def test_a_stale_rev_fails_the_way_the_real_gateway_fails(self, client: TestClient) -> None:
        # The envelope a schema faker cannot produce: `rev` is real, so optimistic
        # concurrency fails — and with the *same* code the real persistence gateway
        # raises (`exc.precondition("Revision mismatch", code="revision_mismatch")`),
        # not merely some error. Assert the kind, not that something was raised.
        created = client.post(
            "/products", json={"name": "Lungo", "price": 300}, headers=_AUTH
        ).json()

        fresh = client.patch(
            f"/products/{created['id']}",
            params={"rev": created["rev"]},
            json={"price": 310},
            headers=_AUTH,
        )
        assert fresh.status_code == 200, fresh.text

        stale = client.patch(
            f"/products/{created['id']}",
            params={"rev": created["rev"]},
            json={"price": 999},
            headers=_AUTH,
        )
        assert stale.status_code == 400, stale.text
        assert stale.headers[ERROR_CODE_HEADER] == "revision_mismatch"

    def test_the_declared_mock_app_serves_the_same_thing(self) -> None:
        # `served.py` is what `forze mock serve` resolves. It declares no app of its own —
        # the factory, the identity wiring and the specs all come from app.py — so serving
        # through it must be indistinguishable from the hand-composed server above.
        from examples.recipes.mock_server.served import mock_app
        from forze_mock.server import build_mock_server

        with TestClient(build_mock_server(mock_app)) as client:
            assert {row["name"] for row in _list(client)} == {"Espresso", "Cortado", "Filter"}
            assert client.post("/products/list", json={}).status_code == 401
            # ...plus the control plane, which the hand-composed one does not have.
            assert client.get("/_mock/health").json()["mock"] is True

    def test_each_server_starts_from_the_same_pristine_seed(self) -> None:
        # Two servers, no shared state: a mutation in one is invisible to the other.
        with TestClient(build_server()) as first, TestClient(build_server()) as second:
            first.post("/products", json={"name": "Affogato", "price": 400}, headers=_AUTH)

            assert "Affogato" in {row["name"] for row in _list(first)}
            assert "Affogato" not in {row["name"] for row in _list(second)}

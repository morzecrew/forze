"""CRUD recipe end to end over real Postgres via the FastAPI test client (Docker)."""

from __future__ import annotations

import os

from fastapi.testclient import TestClient


def test_crud_fastapi(postgres_container) -> None:
    url = postgres_container.get_connection_url()
    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")
    os.environ["POSTGRES_DSN"] = url

    from examples.recipes.crud_fastapi.app import app

    with TestClient(app) as client:
        created = client.post("/products", json={"name": "Widget", "price": 10})
        assert created.status_code == 200, created.text
        product = created.json()
        pid = product["id"]
        assert product["name"] == "Widget" and product["price"] == 10

        got = client.get(f"/products/{pid}")
        assert got.status_code == 200 and got.json()["price"] == 10

        listed = client.get("/products")
        assert listed.status_code == 200 and any(p["id"] == pid for p in listed.json())

        updated = client.put(f"/products/{pid}?rev={product['rev']}", json={"price": 12})
        assert updated.status_code == 200, updated.text
        assert updated.json()["price"] == 12

        assert client.delete(f"/products/{pid}").status_code == 204
        assert client.get(f"/products/{pid}").status_code == 404

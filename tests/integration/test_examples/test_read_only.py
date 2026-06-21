"""Read-only document API recipe over real Postgres via the FastAPI test client."""

from __future__ import annotations

import os

from fastapi.testclient import TestClient


def test_read_only_api(postgres_container) -> None:
    url = postgres_container.get_connection_url()
    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")
    os.environ["POSTGRES_DSN"] = url

    from examples.recipes.read_only.app import app

    with TestClient(app) as client:
        listed = client.get("/articles")
        assert listed.status_code == 200
        titles = {a["title"] for a in listed.json()["hits"]}
        assert "Hexagonal architecture" in titles

        one = client.get("/articles/00000000-0000-0000-0000-000000000001")
        assert one.status_code == 200 and one.json()["title"] == "Hexagonal architecture"

        missing = client.get("/articles/00000000-0000-0000-0000-0000000000ff")
        assert missing.status_code == 404

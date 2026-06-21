"""Quickstart example, exercised end to end via the FastAPI test client (no Docker)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from examples.quickstart.app import app


def test_quickstart_crud() -> None:
    with TestClient(app) as client:
        created = client.post(
            "/users", json={"name": "Ada", "email": "ada@example.com"}
        )
        assert created.status_code == 200
        user = created.json()
        assert user["name"] == "Ada"
        assert user["email_provided"] is True
        user_id = user["id"]

        got = client.get(f"/users/{user_id}")
        assert got.status_code == 200
        assert got.json()["name"] == "Ada"

        listed = client.get("/users")
        assert listed.status_code == 200
        page = listed.json()
        assert page["count"] == 1
        assert len(page["hits"]) == 1
        assert page["hits"][0]["name"] == "Ada"

        deleted = client.delete(f"/users/{user_id}")
        assert deleted.status_code == 204

        missing = client.get(f"/users/{user_id}")
        assert missing.status_code == 404

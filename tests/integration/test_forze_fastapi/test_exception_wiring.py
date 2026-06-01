"""Integration tests for FastAPI exception handler registration.

# covers: FastAPI integration — CoreException HTTP mapping
"""

from __future__ import annotations

from fastapi import FastAPI
from starlette.testclient import TestClient

from forze.base.exceptions import exc
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers


def test_register_exception_handlers_maps_core_not_found() -> None:
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/missing")
    async def missing() -> None:
        raise exc.not_found("Document not found")

    client = TestClient(app)
    response = client.get("/missing")

    assert response.status_code == 404
    assert response.json() == {"detail": "Document not found"}
    assert response.headers.get(ERROR_CODE_HEADER) == "core.not_found"

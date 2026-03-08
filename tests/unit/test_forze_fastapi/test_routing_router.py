"""Unit tests for forze_fastapi.routing.router."""

import pytest

from fastapi import Depends
from starlette.testclient import TestClient

from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze_fastapi.routing.router import (
    ForzeAPIRouter,
    make_idem_header_dependency,
)


# ----------------------- #


def _ctx_factory() -> ExecutionContext:
    return ExecutionContext()


class TestMakeIdemHeaderDependency:
    """Tests for make_idem_header_dependency."""

    def test_raises_when_header_missing(self) -> None:
        """Dependency raises when idempotency key header is missing."""
        dep = make_idem_header_dependency("X-Idempotency-Key")
        app = __import__("fastapi").FastAPI()

        @app.post("/")
        async def route(idem=Depends(dep)) -> dict:
            return {}

        client = TestClient(app)
        response = client.post("/", json={})
        # FastAPI returns 422 for missing required Header params
        assert response.status_code == 422

    def test_passes_when_header_present(self) -> None:
        """Dependency passes when header is present."""
        dep = make_idem_header_dependency("X-Idempotency-Key")
        app = __import__("fastapi").FastAPI()

        @app.post("/")
        async def route(idem=Depends(dep)) -> dict:
            return {"ok": True}

        client = TestClient(app)
        response = client.post(
            "/",
            json={},
            headers={"X-Idempotency-Key": "key-123"},
        )
        assert response.status_code == 200
        assert response.json() == {"ok": True}


class TestForzeAPIRouter:
    """Tests for ForzeAPIRouter."""

    def test_init_requires_context_dependency(self) -> None:
        """ForzeAPIRouter requires context_dependency."""
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )
        assert router.prefix == "/api"

    def test_add_api_route_non_idempotent_works(self) -> None:
        """Non-idempotent route is added normally."""
        app = __import__("fastapi").FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )

        @router.get("/items")
        async def get_items() -> dict:
            return {"items": []}

        app.include_router(router)
        client = TestClient(app)
        response = client.get("/api/items")
        assert response.status_code == 200
        assert response.json() == {"items": []}

    def test_add_api_route_idempotent_requires_operation_id(self) -> None:
        """Idempotent POST without operation_id raises CoreError."""
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )

        with pytest.raises(CoreError, match="Operation ID is required"):

            @router.post("/create", idempotent=True)
            async def create(dto: dict) -> dict:
                return {}

    def test_post_idempotent_requires_header(self) -> None:
        """Idempotent POST returns 400 when idempotency key header is missing."""
        from pydantic import BaseModel

        class CreateDTO(BaseModel):
            name: str

        app = __import__("fastapi").FastAPI()
        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=_ctx_factory,
        )

        @router.post(
            "/create",
            idempotent=True,
            idempotency_config={"dto_param": "dto"},
            operation_id="test.create",
        )
        async def create(dto: CreateDTO) -> dict:
            return {"name": dto.name}

        app.include_router(router)
        client = TestClient(app)

        response = client.post("/api/create", json={"name": "foo"})
        assert response.status_code == 400
        assert "Idempotency key" in response.json()["detail"]

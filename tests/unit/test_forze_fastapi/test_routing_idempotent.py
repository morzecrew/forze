"""Unit tests for forze_fastapi.routing.routes.idempotent."""

from datetime import timedelta
from typing import Optional

import orjson
import pytest

from fastapi import FastAPI
from pydantic import BaseModel, TypeAdapter
from starlette.testclient import TestClient

from forze.application.contracts.idempotency import IdempotencyDepKey, IdempotencySnapshot
from forze.application.execution import Deps, ExecutionContext
from forze_fastapi.constants import IDEMPOTENCY_KEY_HEADER
from forze_fastapi.routing.routes.idempotent import IdempotentRouteConfig, _hash_payload
from forze_fastapi.routing.router import ForzeAPIRouter


# ----------------------- #


class _SpyIdempotencyPort:
    """Idempotency test double that records begin/commit calls."""

    def __init__(self) -> None:
        self.begin_calls: list[tuple[str, Optional[str], str]] = []
        self.commit_calls: list[tuple[str, Optional[str], str, IdempotencySnapshot]] = []

    async def begin(
        self,
        op: str,
        key: Optional[str],
        payload_hash: str,
    ) -> Optional[IdempotencySnapshot]:
        self.begin_calls.append((op, key, payload_hash))
        return None

    async def commit(
        self,
        op: str,
        key: Optional[str],
        payload_hash: str,
        snapshot: IdempotencySnapshot,
    ) -> None:
        self.commit_calls.append((op, key, payload_hash, snapshot))


class _SpyIdempotencyFactory:
    """Factory test double that always returns the same spy port."""

    def __init__(self, port: _SpyIdempotencyPort) -> None:
        self.port = port
        self.calls: list[timedelta] = []

    def __call__(
        self,
        context: ExecutionContext,
        ttl: timedelta = timedelta(seconds=30),
    ) -> _SpyIdempotencyPort:
        self.calls.append(ttl)
        return self.port


# ----------------------- #


class TestHashPayload:
    """Tests for _hash_payload helper."""

    def test_raises_for_invalid_json_payload(self) -> None:
        """Malformed JSON raises and is handled by the route wrapper."""

        class _CreateDTO(BaseModel):
            name: str

        config = IdempotentRouteConfig(
            operation="test.create",
            ttl=timedelta(seconds=30),
            header_key=IDEMPOTENCY_KEY_HEADER,
            adapter=TypeAdapter(_CreateDTO),
            dto_param="dto",
        )

        with pytest.raises(orjson.JSONDecodeError):
            _hash_payload(config, b'{"dto":')


class TestForzeAPIRouterIdempotency:
    """Regression tests for idempotent POST behavior."""

    def test_invalid_json_bypasses_idempotency_snapshotting(self) -> None:
        """Invalid JSON should skip idempotency begin/commit and let FastAPI validate."""
        spy_port = _SpyIdempotencyPort()
        spy_factory = _SpyIdempotencyFactory(spy_port)

        def _ctx_factory() -> ExecutionContext:
            return ExecutionContext(
                deps=Deps(
                    deps={
                        IdempotencyDepKey: spy_factory,
                    }
                )
            )

        class CreateDTO(BaseModel):
            name: str

        app = FastAPI()
        router = ForzeAPIRouter(prefix="/api", context_dependency=_ctx_factory)

        @router.post("/create", idempotent=True, operation_id="test.create")
        async def create(dto: CreateDTO) -> dict:
            return {"name": dto.name}

        app.include_router(router)
        client = TestClient(app)

        response_1 = client.post(
            "/api/create",
            data=b'{"name":',
            headers={
                IDEMPOTENCY_KEY_HEADER: "req-1",
                "Content-Type": "application/json",
            },
        )
        response_2 = client.post(
            "/api/create",
            data=b'{"name":',
            headers={
                IDEMPOTENCY_KEY_HEADER: "req-1",
                "Content-Type": "application/json",
            },
        )

        assert response_1.status_code == 422
        assert response_2.status_code == 422
        assert spy_port.begin_calls == []
        assert spy_port.commit_calls == []

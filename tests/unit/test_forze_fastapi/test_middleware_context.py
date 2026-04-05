"""Unit tests for context middleware and default call-context helpers."""

from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from starlette.requests import Request
from starlette.testclient import TestClient

from forze.application.execution import CallContext, ExecutionContext, PrincipalContext
from forze_mock import MockDepsModule, MockState

from forze_fastapi.middlewares.context.defaults import DefaultCallContextResolverInjector
from forze_fastapi.middlewares.context.middleware import ContextBindingMiddleware

# ----------------------- #


def _execution_ctx() -> ExecutionContext:
    return ExecutionContext(deps=MockDepsModule(state=MockState())())


class TestDefaultCallContextResolverInjector:
    """Tests for :class:`DefaultCallContextResolverInjector`."""

    def test_resolve_generates_ids_when_headers_missing(self) -> None:
        """Without correlation headers, new UUIDs are used."""
        inj = DefaultCallContextResolverInjector()
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [],
            }
        )
        ctx = inj.resolve(req)
        assert isinstance(ctx.execution_id, UUID)
        assert isinstance(ctx.correlation_id, UUID)
        assert ctx.causation_id is None

    def test_resolve_reads_correlation_and_causation_headers(self) -> None:
        """Valid UUID headers are parsed."""
        corr = uuid4()
        caus = uuid4()
        inj = DefaultCallContextResolverInjector()
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [
                    (b"x-correlation-id", str(corr).encode()),
                    (b"x-causation-id", str(caus).encode()),
                ],
            }
        )
        ctx = inj.resolve(req)
        assert ctx.correlation_id == corr
        assert ctx.causation_id == caus

    def test_inject_adds_execution_and_correlation_headers(self) -> None:
        """Response headers include execution and correlation ids."""
        inj = DefaultCallContextResolverInjector()
        ctx = CallContext(
            execution_id=uuid4(),
            correlation_id=uuid4(),
            causation_id=None,
        )
        headers: list[tuple[bytes, bytes]] = []
        out = inj.inject(headers, ctx)

        keys = {k.decode().lower() for k, _ in out}
        assert "x-request-id" in keys
        assert "x-correlation-id" in keys

    def test_inject_includes_causation_when_set(self) -> None:
        """Causation header is added when causation_id is not None."""
        inj = DefaultCallContextResolverInjector()
        caus = uuid4()
        ctx = CallContext(
            execution_id=uuid4(),
            correlation_id=uuid4(),
            causation_id=caus,
        )
        headers: list[tuple[bytes, bytes]] = []
        out = inj.inject(headers, ctx)

        keys = {k.decode().lower(): v.decode() for k, v in out}
        assert "x-causation-id" in keys
        assert keys["x-causation-id"] == str(caus)


class TestContextBindingMiddleware:
    """Tests for :class:`ContextBindingMiddleware`."""

    @staticmethod
    async def _ok_app(scope: object, receive: object, send: object) -> None:
        await send(  # type: ignore[misc]
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [],
            }
        )
        await send({"type": "http.response.body", "body": b"ok"})  # type: ignore[misc]

    def test_injects_call_context_headers_on_http_response(self) -> None:
        """HTTP responses get call-context headers from the injector."""
        ctx = _execution_ctx()
        mw = ContextBindingMiddleware(self._ok_app, ctx_dep=lambda: ctx)
        client = TestClient(mw)
        response = client.get("/")

        assert response.status_code == 200
        assert "x-request-id" in response.headers
        assert "x-correlation-id" in response.headers

    def test_principal_resolver_invoked_when_configured(self) -> None:
        """Optional principal resolver is called for HTTP requests."""

        class _Principal:
            called = False

            def resolve(self, request: Request) -> PrincipalContext | None:
                self.called = True
                return None

        principal = _Principal()
        ctx = _execution_ctx()
        mw = ContextBindingMiddleware(
            self._ok_app,
            ctx_dep=lambda: ctx,
            principal_ctx_resolver=principal,
        )
        client = TestClient(mw)
        client.get("/")

        assert principal.called is True

    @pytest.mark.asyncio
    async def test_non_http_scope_passthrough(self) -> None:
        """Non-HTTP scopes skip binding and forward to the inner app."""
        app = AsyncMock()
        ctx = _execution_ctx()
        mw = ContextBindingMiddleware(app, ctx_dep=lambda: ctx)

        await mw({"type": "lifespan"}, AsyncMock(), AsyncMock())

        app.assert_awaited_once()

"""Unit tests for context middleware and default call-context codec."""

from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from starlette.requests import Request
from starlette.testclient import TestClient

from forze.application.contracts.auth import AuthSpec, TokenCredentials
from forze.application.contracts.auth.deps import AuthenticationDepKey
from forze.application.execution import Deps
from forze.application.execution import AuthIdentity, CallContext, ExecutionContext
from forze_mock import MockDepsModule, MockState

from forze_fastapi.middlewares.context.auth import HeaderAuthIdentityResolver
from forze_fastapi.middlewares.context.defaults import DefaultCallContextCodec
from forze_fastapi.middlewares.context.middleware import ContextBindingMiddleware

# ----------------------- #


def _execution_ctx() -> ExecutionContext:
    return ExecutionContext(deps=MockDepsModule(state=MockState())())


class _TokenAuthPort:
    async def authenticate_with_password(self, credentials: object) -> AuthIdentity | None:
        return None

    async def authenticate_with_token(
        self,
        credentials: TokenCredentials,
    ) -> AuthIdentity | None:
        return AuthIdentity(subject_id=credentials.token)

    async def authenticate_with_api_key(self, credentials: object) -> AuthIdentity | None:
        return None


class _TokenAuthFactory:
    def __call__(self, ctx: ExecutionContext, spec: AuthSpec) -> _TokenAuthPort:
        return _TokenAuthPort()


class TestDefaultCallContextCodec:
    """Tests for :class:`DefaultCallContextCodec`."""

    def test_decode_generates_ids_when_headers_missing(self) -> None:
        """Without correlation headers, new UUIDs are used."""
        codec = DefaultCallContextCodec()
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [],
            }
        )
        ctx = codec.decode(req)
        assert isinstance(ctx.execution_id, UUID)
        assert isinstance(ctx.correlation_id, UUID)
        assert ctx.causation_id is None

    def test_decode_reads_correlation_and_causation_headers(self) -> None:
        """Valid UUID headers are parsed."""
        corr = uuid4()
        caus = uuid4()
        codec = DefaultCallContextCodec()
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
        ctx = codec.decode(req)
        assert ctx.correlation_id == corr
        assert ctx.causation_id == caus

    def test_encode_adds_execution_and_correlation_headers(self) -> None:
        """Response headers include execution and correlation ids."""
        codec = DefaultCallContextCodec()
        ctx = CallContext(
            execution_id=uuid4(),
            correlation_id=uuid4(),
            causation_id=None,
        )
        headers: list[tuple[bytes, bytes]] = []
        out = codec.encode(headers, ctx)

        keys = {k.decode().lower() for k, _ in out}
        assert "x-request-id" in keys
        assert "x-correlation-id" in keys

    def test_encode_includes_causation_when_set(self) -> None:
        """Causation header is added when causation_id is not None."""
        codec = DefaultCallContextCodec()
        caus = uuid4()
        ctx = CallContext(
            execution_id=uuid4(),
            correlation_id=uuid4(),
            causation_id=caus,
        )
        headers: list[tuple[bytes, bytes]] = []
        out = codec.encode(headers, ctx)

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
        """HTTP responses get call-context headers from the codec."""
        ctx = _execution_ctx()
        mw = ContextBindingMiddleware(self._ok_app, ctx_dep=lambda: ctx)
        client = TestClient(mw)
        response = client.get("/")

        assert response.status_code == 200
        assert "x-request-id" in response.headers
        assert "x-correlation-id" in response.headers

    def test_auth_identity_codec_invoked_when_configured(self) -> None:
        """Optional auth identity codec is called for HTTP requests."""

        class _IdentityCodec:
            called = False

            def decode(self, request: Request) -> AuthIdentity | None:
                self.called = True
                return None

        identity_codec = _IdentityCodec()
        ctx = _execution_ctx()
        mw = ContextBindingMiddleware(
            self._ok_app,
            ctx_dep=lambda: ctx,
            auth_identity_codec=identity_codec,
        )
        client = TestClient(mw)
        client.get("/")

        assert identity_codec.called is True

    def test_auth_identity_resolver_invoked_when_configured(self) -> None:
        """Async auth identity resolver is called for HTTP requests."""

        class _IdentityResolver:
            called = False

            async def resolve(
                self,
                request: Request,
                ctx: ExecutionContext,
            ) -> AuthIdentity | None:
                self.called = True
                return AuthIdentity(subject_id="sub")

        identity_resolver = _IdentityResolver()
        ctx = _execution_ctx()
        mw = ContextBindingMiddleware(
            self._ok_app,
            ctx_dep=lambda: ctx,
            auth_identity_resolver=identity_resolver,
        )
        client = TestClient(mw)
        client.get("/")

        assert identity_resolver.called is True

    @pytest.mark.asyncio
    async def test_header_auth_identity_resolver_uses_authentication_port(self) -> None:
        """Header resolver extracts bearer tokens and calls the auth contract."""

        ctx = ExecutionContext(
            deps=Deps.plain({AuthenticationDepKey: _TokenAuthFactory()})
        )
        resolver = HeaderAuthIdentityResolver(spec=AuthSpec(name="auth"))
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [(b"authorization", b"Bearer token-1")],
            }
        )

        identity = await resolver.resolve(req, ctx)

        assert identity is not None
        assert identity.subject_id == "token-1"

    @pytest.mark.asyncio
    async def test_non_http_scope_passthrough(self) -> None:
        """Non-HTTP scopes skip binding and forward to the inner app."""
        app = AsyncMock()
        ctx = _execution_ctx()
        mw = ContextBindingMiddleware(app, ctx_dep=lambda: ctx)

        await mw({"type": "lifespan"}, AsyncMock(), AsyncMock())

        app.assert_awaited_once()

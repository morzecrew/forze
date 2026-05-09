"""Unit tests for context middleware and call-context codec."""

from unittest.mock import AsyncMock
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest
from starlette.requests import Request
from starlette.testclient import TestClient

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    AuthnDepKey,
    AuthnIdentity,
    AuthnSpec,
    TokenCredentials,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import CallContext, Deps, ExecutionContext
from forze.base.errors import AuthenticationError
from forze_fastapi.middlewares.context import HeaderCallContextCodec
from forze_fastapi.middlewares.context.authn import (
    CookieAuthnIdentityResolver,
    HeaderAuthnIdentityResolver,
)
from forze_fastapi.middlewares.context.middleware import ContextBindingMiddleware
from forze_mock import MockDepsModule, MockState

# ----------------------- #


def _execution_ctx() -> ExecutionContext:
    return ExecutionContext(deps=MockDepsModule(state=MockState())())


class _NullAuthnResolver:
    """Async no-op authn resolver (middleware requires exactly one authn source)."""

    async def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
    ) -> AuthnIdentity | None:
        return None


class _NullTenantCodec:
    """Synchronous no-op tenant codec."""

    def decode(self, request: Request) -> TenantIdentity | None:
        return None


class _NullTenantResolver:
    """Async tenant resolver that yields no tenant."""

    async def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
        identity: AuthnIdentity | None,
    ) -> TenantIdentity | None:
        return None


def _mw_kwargs() -> dict[str, object]:
    return {
        "authn_identity_resolver": _NullAuthnResolver(),
        "tenant_identity_codec": _NullTenantCodec(),
    }


class _TokenAuthPort:
    async def authenticate_with_password(
        self, credentials: object
    ) -> AuthnIdentity | None:
        return None

    async def authenticate_with_token(
        self,
        credentials: TokenCredentials,
    ) -> AuthnIdentity | None:
        return AuthnIdentity(principal_id=uuid5(NAMESPACE_URL, credentials.token))

    async def authenticate_with_api_key(
        self, credentials: object
    ) -> AuthnIdentity | None:
        return None


class _TokenAuthFactory:
    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _TokenAuthPort:
        return _TokenAuthPort()


class _ApiKeyAuthPort:
    async def authenticate_with_password(
        self, credentials: object
    ) -> AuthnIdentity | None:
        return None

    async def authenticate_with_token(
        self,
        credentials: TokenCredentials,
    ) -> AuthnIdentity | None:
        return None

    async def authenticate_with_api_key(
        self, credentials: object
    ) -> AuthnIdentity | None:
        assert isinstance(credentials, ApiKeyCredentials)

        return AuthnIdentity(
            principal_id=uuid5(NAMESPACE_URL, "key:" + credentials.key)
        )


class _ApiKeyAuthFactory:
    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _ApiKeyAuthPort:
        return _ApiKeyAuthPort()


class TestHeaderCallContextCodec:
    """Tests for :class:`HeaderCallContextCodec`."""

    def test_decode_generates_ids_when_headers_missing(self) -> None:
        """Without correlation headers, new UUIDs are used."""
        codec = HeaderCallContextCodec()
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
        codec = HeaderCallContextCodec()
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
        codec = HeaderCallContextCodec()
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
        codec = HeaderCallContextCodec()
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
        mw = ContextBindingMiddleware(
            self._ok_app,
            ctx_dep=lambda: ctx,
            **_mw_kwargs(),
        )
        client = TestClient(mw)
        response = client.get("/")

        assert response.status_code == 200
        assert "x-request-id" in response.headers
        assert "x-correlation-id" in response.headers

    def test_authn_identity_resolver_invoked_when_configured(self) -> None:
        """Async authn identity resolver is called for HTTP requests."""

        class _IdentityResolver:
            def __init__(self) -> None:
                self.called = False

            async def resolve(
                self,
                request: Request,
                ctx: ExecutionContext,
            ) -> AuthnIdentity | None:
                self.called = True
                return AuthnIdentity(principal_id=uuid4())

        identity_resolver = _IdentityResolver()
        ctx = _execution_ctx()
        mw = ContextBindingMiddleware(
            self._ok_app,
            ctx_dep=lambda: ctx,
            authn_identity_resolver=identity_resolver,
            tenant_identity_resolver=_NullTenantResolver(),
        )
        client = TestClient(mw)
        client.get("/")

        assert identity_resolver.called is True

    @pytest.mark.asyncio
    async def test_header_authn_identity_resolver_uses_authentication_port(
        self,
    ) -> None:
        """Header resolver extracts bearer tokens and calls the auth contract."""

        ctx = ExecutionContext(
            deps=Deps.plain({AuthnDepKey: _TokenAuthFactory()}),
        )
        resolver = HeaderAuthnIdentityResolver(spec=AuthnSpec(name="auth"))
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
        assert identity.principal_id == uuid5(NAMESPACE_URL, "token-1")

    @pytest.mark.asyncio
    async def test_header_auth_rejects_ambiguous_credentials(self) -> None:
        class _BothPort:
            async def authenticate_with_password(self, credentials: object) -> None:
                return None

            async def authenticate_with_token(
                self,
                credentials: TokenCredentials,
            ) -> AuthnIdentity:
                return AuthnIdentity(
                    principal_id=uuid5(NAMESPACE_URL, "t:" + credentials.token)
                )

            async def authenticate_with_api_key(
                self, credentials: object
            ) -> AuthnIdentity:
                assert isinstance(credentials, ApiKeyCredentials)

                return AuthnIdentity(
                    principal_id=uuid5(NAMESPACE_URL, "k:" + credentials.key)
                )

        class _BothFactory:
            def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _BothPort:
                return _BothPort()

        ctx = ExecutionContext(deps=Deps.plain({AuthnDepKey: _BothFactory()}))
        resolver = HeaderAuthnIdentityResolver(
            spec=AuthnSpec(name="auth"),
            when_multiple_credentials="reject",
        )
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [
                    (b"authorization", b"Bearer tok"),
                    (b"x-api-key", b"secret-key"),
                ],
            }
        )

        with pytest.raises(AuthenticationError, match="Multiple"):
            await resolver.resolve(req, ctx)

    @pytest.mark.asyncio
    async def test_header_auth_api_key_first_order(self) -> None:
        class _BothPort:
            async def authenticate_with_password(self, credentials: object) -> None:
                return None

            async def authenticate_with_token(
                self,
                credentials: TokenCredentials,
            ) -> AuthnIdentity:
                return AuthnIdentity(
                    principal_id=uuid5(NAMESPACE_URL, "t:" + credentials.token)
                )

            async def authenticate_with_api_key(
                self, credentials: object
            ) -> AuthnIdentity:
                assert isinstance(credentials, ApiKeyCredentials)

                return AuthnIdentity(
                    principal_id=uuid5(NAMESPACE_URL, "k:" + credentials.key)
                )

        class _BothFactory:
            def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _BothPort:
                return _BothPort()

        ctx = ExecutionContext(deps=Deps.plain({AuthnDepKey: _BothFactory()}))
        resolver = HeaderAuthnIdentityResolver(
            spec=AuthnSpec(name="auth"),
            try_sources=("api_key", "token"),
        )
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [
                    (b"authorization", b"Bearer tok"),
                    (b"x-api-key", b"secret-key"),
                ],
            }
        )

        identity = await resolver.resolve(req, ctx)

        assert identity is not None
        assert identity.principal_id == uuid5(NAMESPACE_URL, "k:secret-key")

    @pytest.mark.asyncio
    async def test_cookie_auth_identity_resolver(self) -> None:
        ctx = ExecutionContext(deps=Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
        resolver = CookieAuthnIdentityResolver(
            spec=AuthnSpec(name="auth"), cookie_name="sid"
        )
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [(b"cookie", b"sid=cookie-token")],
            }
        )

        identity = await resolver.resolve(req, ctx)

        assert identity is not None
        assert identity.principal_id == uuid5(NAMESPACE_URL, "cookie-token")

    @pytest.mark.asyncio
    async def test_non_http_scope_passthrough(self) -> None:
        """Non-HTTP scopes skip binding and forward to the inner app."""
        app = AsyncMock()
        ctx = _execution_ctx()
        mw = ContextBindingMiddleware(
            app,
            ctx_dep=lambda: ctx,
            **_mw_kwargs(),
        )

        await mw({"type": "lifespan"}, AsyncMock(), AsyncMock())

        app.assert_awaited_once()

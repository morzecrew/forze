"""Unit tests for context middleware and call-context codec."""

from forze.base.exceptions import CoreException
from unittest.mock import AsyncMock
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest
from starlette.requests import Request
from starlette.testclient import TestClient

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    AuthnDepKey,
    AuthnIdentity,
    AuthnResult,
    AuthnSpec,
)
from forze.application.contracts.tenancy import TenantIdentity, TenantResolverDepKey
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze_fastapi.middlewares.context import (
    CookieTokenAuthnIdentityResolver,
    HeaderApiKeyAuthnIdentityResolver,
    HeaderInvocationMetadataCodec,
    HeaderTenantIdentityCodec,
    HeaderTokenAuthnIdentityResolver,
    TenantIdentityResolver,
)
from forze_fastapi.middlewares.context.middleware import ContextBindingMiddleware
from forze_mock import MockDepsModule, MockState

# ----------------------- #

def _execution_ctx() -> ExecutionContext:
    return ExecutionContext(deps=MockDepsModule(state=MockState())())

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
        authn: AuthnResult | None,
    ) -> TenantIdentity | None:
        return None

def _mw_kwargs() -> dict[str, object]:
    return {
        "tenant_identity_codec": _NullTenantCodec(),
    }

def _authn_result(
    principal_id,
    *,
    issuer_tenant_hint: str | None = None,
) -> AuthnResult:
    return AuthnResult(
        identity=AuthnIdentity(principal_id=principal_id),
        issuer_tenant_hint=issuer_tenant_hint,
    )

class _TokenAuthPort:
    async def authenticate_with_password(
        self, credentials: object
    ) -> AuthnResult | None:
        return None

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> AuthnResult | None:
        return _authn_result(uuid5(NAMESPACE_URL, credentials.token))

    async def authenticate_with_api_key(
        self, credentials: object
    ) -> AuthnResult | None:
        return None

class _TokenAuthFactory:
    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _TokenAuthPort:
        return _TokenAuthPort()

class _ApiKeyAuthPort:
    async def authenticate_with_password(
        self, credentials: object
    ) -> AuthnResult | None:
        return None

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> AuthnResult | None:
        return None

    async def authenticate_with_api_key(
        self, credentials: object
    ) -> AuthnResult | None:
        assert isinstance(credentials, ApiKeyCredentials)

        return _authn_result(
            uuid5(NAMESPACE_URL, "key:" + credentials.key)
        )

class _ApiKeyAuthFactory:
    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _ApiKeyAuthPort:
        return _ApiKeyAuthPort()

class _BothPort:
    async def authenticate_with_password(
        self, credentials: object
    ) -> AuthnResult | None:
        return None

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> AuthnResult:
        return _authn_result(
            uuid5(NAMESPACE_URL, "t:" + credentials.token),
        )

    async def authenticate_with_api_key(
        self,
        credentials: object,
    ) -> AuthnResult:
        assert isinstance(credentials, ApiKeyCredentials)

        return _authn_result(
            uuid5(NAMESPACE_URL, "k:" + credentials.key),
        )

class _BothFactory:
    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _BothPort:
        return _BothPort()

class TestHeaderInvocationMetadataCodec:
    """Tests for :class:`HeaderInvocationMetadataCodec`."""

    def test_decode_generates_ids_when_headers_missing(self) -> None:
        """Without correlation headers, new UUIDs are used."""
        codec = HeaderInvocationMetadataCodec()
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
        codec = HeaderInvocationMetadataCodec()
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
        codec = HeaderInvocationMetadataCodec()
        ctx = InvocationMetadata(
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
        codec = HeaderInvocationMetadataCodec()
        caus = uuid4()
        ctx = InvocationMetadata(
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

    def test_authn_identity_resolvers_invoked_when_configured(self) -> None:
        """Each resolver in the sequence is consulted for HTTP requests."""

        class _IdentityResolver:
            def __init__(self) -> None:
                self.called = False

            async def resolve(
                self,
                request: Request,
                ctx: ExecutionContext,
            ) -> AuthnResult | None:
                self.called = True
                return _authn_result(uuid4())

        identity_resolver = _IdentityResolver()
        ctx = _execution_ctx()
        mw = ContextBindingMiddleware(
            self._ok_app,
            ctx_dep=lambda: ctx,
            authn_identity_resolvers=(identity_resolver,),
            tenant_identity_resolver=_NullTenantResolver(),
        )
        client = TestClient(mw)
        client.get("/")

        assert identity_resolver.called is True

    def test_middleware_binds_tenant_from_authoritative_resolver(self) -> None:
        tid = uuid4()
        expected_pid = uuid5(NAMESPACE_URL, "token-1")
        captured: dict[str, object] = {}

        class _TenantResolverPort:
            async def resolve_from_principal(
                self,
                principal_id,
                *,
                requested_tenant_id=None,
            ) -> TenantIdentity | None:
                assert principal_id == expected_pid
                assert requested_tenant_id == tid
                return TenantIdentity(tenant_id=tid)

        ctx = ExecutionContext(
            deps=Deps.plain(
                {
                    AuthnDepKey: _TokenAuthFactory(),
                    TenantResolverDepKey: lambda c: _TenantResolverPort(),
                }
            )
        )

        async def _capture_app(scope, receive, send):  # type: ignore[no-untyped-def]
            captured["authn"] = ctx.inv.get_authn()
            captured["tenant"] = ctx.inv.get_tenant()
            await send(
                {"type": "http.response.start", "status": 200, "headers": []},
            )
            await send({"type": "http.response.body", "body": b"ok"})

        mw = ContextBindingMiddleware(
            _capture_app,
            ctx_dep=lambda: ctx,
            authn_identity_resolvers=(
                HeaderTokenAuthnIdentityResolver(spec=AuthnSpec(name="auth")),
            ),
            tenant_identity_resolver=TenantIdentityResolver(
                hint_codec=HeaderTenantIdentityCodec()
            ),
        )
        client = TestClient(mw)

        response = client.get(
            "/",
            headers={
                "Authorization": "Bearer token-1",
                "X-Tenant-Id": str(tid),
            },
        )

        assert response.status_code == 200
        assert captured["authn"] is not None
        assert captured["tenant"] is not None
        assert isinstance(captured["authn"], AuthnIdentity)
        assert isinstance(captured["tenant"], TenantIdentity)
        assert captured["authn"].principal_id == expected_pid
        assert captured["tenant"].tenant_id == tid

    def test_middleware_rejects_unavailable_requested_tenant(self) -> None:
        requested_tid = uuid4()

        class _TenantResolverPort:
            async def resolve_from_principal(
                self,
                principal_id,
                *,
                requested_tenant_id=None,
            ) -> TenantIdentity | None:
                assert requested_tenant_id == requested_tid
                return None

        ctx = ExecutionContext(
            deps=Deps.plain(
                {
                    AuthnDepKey: _TokenAuthFactory(),
                    TenantResolverDepKey: lambda c: _TenantResolverPort(),
                }
            )
        )

        mw = ContextBindingMiddleware(
            self._ok_app,
            ctx_dep=lambda: ctx,
            authn_identity_resolvers=(
                HeaderTokenAuthnIdentityResolver(spec=AuthnSpec(name="auth")),
            ),
            tenant_identity_resolver=TenantIdentityResolver(
                hint_codec=HeaderTenantIdentityCodec()
            ),
        )
        client = TestClient(mw)

        with pytest.raises(CoreException, match="Requested tenant"):
            client.get(
                "/",
                headers={
                    "Authorization": "Bearer token-1",
                    "X-Tenant-Id": str(requested_tid),
                },
            )

    def test_first_in_order_short_circuits(self) -> None:
        class _Always:
            def __init__(self, name: str) -> None:
                self.name = name
                self.called = False

            async def resolve(
                self,
                request: Request,
                ctx: ExecutionContext,
            ) -> AuthnResult | None:
                self.called = True
                return _authn_result(uuid5(NAMESPACE_URL, self.name))

        first = _Always("first")
        second = _Always("second")

        ctx = _execution_ctx()
        mw = ContextBindingMiddleware(
            self._ok_app,
            ctx_dep=lambda: ctx,
            authn_identity_resolvers=(first, second),
            when_multiple_credentials="first_in_order",
            tenant_identity_codec=_NullTenantCodec(),
        )
        client = TestClient(mw)
        client.get("/")

        assert first.called is True
        # In ``first_in_order`` mode, later resolvers must not run once a hit
        # is found.
        assert second.called is False

    def test_reject_raises_when_more_than_one_resolver_returns_identity(self) -> None:
        class _Always:
            def __init__(self, name: str) -> None:
                self.name = name

            async def resolve(
                self,
                request: Request,
                ctx: ExecutionContext,
            ) -> AuthnResult | None:
                return _authn_result(uuid5(NAMESPACE_URL, self.name))

        ctx = _execution_ctx()
        mw = ContextBindingMiddleware(
            self._ok_app,
            ctx_dep=lambda: ctx,
            authn_identity_resolvers=(_Always("a"), _Always("b")),
            when_multiple_credentials="reject",
            tenant_identity_codec=_NullTenantCodec(),
        )
        client = TestClient(mw)

        with pytest.raises(CoreException, match="Multiple"):
            client.get("/")

    @pytest.mark.asyncio
    async def test_header_token_resolver_uses_authentication_port(
        self,
    ) -> None:
        """Header resolver extracts bearer tokens and calls the auth contract."""

        ctx = ExecutionContext(
            deps=Deps.plain({AuthnDepKey: _TokenAuthFactory()}),
        )
        resolver = HeaderTokenAuthnIdentityResolver(spec=AuthnSpec(name="auth"))
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [(b"authorization", b"Bearer token-1")],
            }
        )

        authn = await resolver.resolve(req, ctx)

        assert authn is not None
        assert authn.identity.principal_id == uuid5(NAMESPACE_URL, "token-1")

    @pytest.mark.asyncio
    async def test_header_token_resolver_required_missing(self) -> None:
        ctx = ExecutionContext(deps=Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
        resolver = HeaderTokenAuthnIdentityResolver(
            spec=AuthnSpec(name="auth"),
            required=True,
        )
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [],
            }
        )

        with pytest.raises(CoreException, match="required"):
            await resolver.resolve(req, ctx)

    @pytest.mark.asyncio
    async def test_header_api_key_resolver_uses_authentication_port(
        self,
    ) -> None:
        ctx = ExecutionContext(
            deps=Deps.plain({AuthnDepKey: _ApiKeyAuthFactory()}),
        )
        resolver = HeaderApiKeyAuthnIdentityResolver(spec=AuthnSpec(name="auth"))
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [(b"x-api-key", b"secret-key")],
            }
        )

        authn = await resolver.resolve(req, ctx)

        assert authn is not None
        assert authn.identity.principal_id == uuid5(NAMESPACE_URL, "key:secret-key")

    @pytest.mark.asyncio
    async def test_cookie_token_resolver(self) -> None:
        ctx = ExecutionContext(deps=Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
        resolver = CookieTokenAuthnIdentityResolver(
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

        authn = await resolver.resolve(req, ctx)

        assert authn is not None
        assert authn.identity.principal_id == uuid5(NAMESPACE_URL, "cookie-token")

    def test_header_tenant_codec_rejects_malformed_uuid(self) -> None:
        codec = HeaderTenantIdentityCodec()
        req = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/",
                "headers": [(b"x-tenant-id", b"not-a-uuid")],
            }
        )

        with pytest.raises(CoreException, match="Invalid tenant hint"):
            codec.decode(req)

    @pytest.mark.asyncio
    async def test_middleware_rejects_ambiguous_credentials_via_resolvers(
        self,
    ) -> None:
        """Two resolvers each producing an identity should be rejected when policy is reject."""

        ctx = ExecutionContext(deps=Deps.plain({AuthnDepKey: _BothFactory()}))

        token_resolver = HeaderTokenAuthnIdentityResolver(spec=AuthnSpec(name="auth"))
        api_key_resolver = HeaderApiKeyAuthnIdentityResolver(spec=AuthnSpec(name="auth"))

        async def _ok(scope, receive, send):  # type: ignore[no-untyped-def]
            await send(
                {"type": "http.response.start", "status": 200, "headers": []},
            )
            await send({"type": "http.response.body", "body": b"ok"})

        mw = ContextBindingMiddleware(
            _ok,
            ctx_dep=lambda: ctx,
            authn_identity_resolvers=(token_resolver, api_key_resolver),
            when_multiple_credentials="reject",
            tenant_identity_codec=_NullTenantCodec(),
        )

        client = TestClient(mw)

        with pytest.raises(CoreException, match="Multiple"):
            client.get(
                "/",
                headers={
                    "Authorization": "Bearer tok",
                    "X-API-Key": "secret-key",
                },
            )

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

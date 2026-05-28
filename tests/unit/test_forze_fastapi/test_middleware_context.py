"""Unit tests for FastAPI invocation and security middleware."""

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
from forze.base.exceptions import CoreException
from forze_fastapi.middlewares import InvocationMetadataMiddleware, SecurityContextMiddleware
from forze_fastapi.security import (
    AuthnRequirement,
    CookieTokenAuthn,
    HeaderApiKeyAuthn,
    HeaderTokenAuthn,
    resolve_authn_ingress,
)
from forze_mock import MockDepsModule, MockState


def _execution_ctx() -> ExecutionContext:
    return ExecutionContext(deps=MockDepsModule(state=MockState())())


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
    async def authenticate_with_password(self, credentials: object) -> AuthnResult | None:
        return None

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> AuthnResult | None:
        return _authn_result(uuid5(NAMESPACE_URL, credentials.token))

    async def authenticate_with_api_key(self, credentials: object) -> AuthnResult | None:
        return None


class _TokenAuthFactory:
    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _TokenAuthPort:
        return _TokenAuthPort()


class _ApiKeyAuthPort:
    async def authenticate_with_password(self, credentials: object) -> AuthnResult | None:
        return None

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> AuthnResult | None:
        return None

    async def authenticate_with_api_key(self, credentials: object) -> AuthnResult | None:
        assert isinstance(credentials, ApiKeyCredentials)
        return _authn_result(uuid5(NAMESPACE_URL, "key:" + credentials.key))


class _ApiKeyAuthFactory:
    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _ApiKeyAuthPort:
        return _ApiKeyAuthPort()


class _BothPort:
    async def authenticate_with_password(self, credentials: object) -> AuthnResult | None:
        return None

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> AuthnResult:
        return _authn_result(uuid5(NAMESPACE_URL, "t:" + credentials.token))

    async def authenticate_with_api_key(self, credentials: object) -> AuthnResult:
        assert isinstance(credentials, ApiKeyCredentials)
        return _authn_result(uuid5(NAMESPACE_URL, "k:" + credentials.key))


class _BothFactory:
    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _BothPort:
        return _BothPort()


_TOKEN_SPEC = AuthnSpec(name="auth", enabled_methods=frozenset({"token"}))
_API_KEY_SPEC = AuthnSpec(name="auth", enabled_methods=frozenset({"api_key"}))


class TestInvocationMetadataMiddleware:
    @staticmethod
    async def _ok_app(scope: object, receive: object, send: object) -> None:
        await send(  # type: ignore[misc]
            {"type": "http.response.start", "status": 200, "headers": []}
        )
        await send({"type": "http.response.body", "body": b"ok"})  # type: ignore[misc]

    def test_decode_generates_ids_when_headers_missing(self) -> None:
        mw = InvocationMetadataMiddleware(self._ok_app, ctx_dep=_execution_ctx)
        req = Request({"type": "http", "path": "/", "method": "GET", "headers": []})

        metadata = mw._decode_metadata(req)

        assert isinstance(metadata.execution_id, UUID)
        assert isinstance(metadata.correlation_id, UUID)
        assert metadata.causation_id is None

    def test_decode_reads_correlation_and_causation_headers(self) -> None:
        corr = uuid4()
        caus = uuid4()
        mw = InvocationMetadataMiddleware(self._ok_app, ctx_dep=_execution_ctx)
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

        metadata = mw._decode_metadata(req)

        assert metadata.correlation_id == corr
        assert metadata.causation_id == caus

    def test_encode_adds_execution_and_correlation_headers(self) -> None:
        mw = InvocationMetadataMiddleware(self._ok_app, ctx_dep=_execution_ctx)
        metadata = InvocationMetadata(
            execution_id=uuid4(),
            correlation_id=uuid4(),
            causation_id=None,
        )

        out = mw._encode_metadata([], metadata)

        keys = {k.decode().lower() for k, _ in out}
        assert "x-request-id" in keys
        assert "x-correlation-id" in keys

    def test_injects_metadata_headers_and_binds_metadata(self) -> None:
        ctx = _execution_ctx()
        captured: dict[str, InvocationMetadata | None] = {}

        async def _capture_app(scope, receive, send):  # type: ignore[no-untyped-def]
            captured["metadata"] = ctx.inv_ctx.get_metadata()
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        mw = InvocationMetadataMiddleware(_capture_app, ctx_dep=lambda: ctx)
        response = TestClient(mw).get("/")

        assert response.status_code == 200
        assert "x-request-id" in response.headers
        assert "x-correlation-id" in response.headers
        assert captured["metadata"] is not None

    @pytest.mark.asyncio
    async def test_non_http_scope_passthrough(self) -> None:
        app = AsyncMock()
        mw = InvocationMetadataMiddleware(app, ctx_dep=_execution_ctx)

        await mw({"type": "lifespan"}, AsyncMock(), AsyncMock())

        app.assert_awaited_once()


class TestResolveAuthnIngress:
    @pytest.mark.asyncio
    async def test_header_token_uses_authentication_port(self) -> None:
        ctx = ExecutionContext(deps=Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [(b"authorization", b"Bearer token-1")],
            }
        )

        authn = await resolve_authn_ingress(
            HeaderTokenAuthn(authn_spec=_TOKEN_SPEC, header_name="Authorization"),
            request=req,
            ctx=ctx,
        )

        assert authn is not None
        assert authn.identity.principal_id == uuid5(NAMESPACE_URL, "token-1")

    @pytest.mark.asyncio
    async def test_header_token_required_missing_raises(self) -> None:
        ctx = ExecutionContext(deps=Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
        req = Request({"type": "http", "path": "/", "method": "GET", "headers": []})

        with pytest.raises(CoreException, match="required"):
            await resolve_authn_ingress(
                HeaderTokenAuthn(
                    authn_spec=_TOKEN_SPEC,
                    header_name="Authorization",
                    required=True,
                ),
                request=req,
                ctx=ctx,
            )

    @pytest.mark.asyncio
    async def test_header_api_key_uses_authentication_port(self) -> None:
        ctx = ExecutionContext(deps=Deps.plain({AuthnDepKey: _ApiKeyAuthFactory()}))
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [(b"x-api-key", b"secret-key")],
            }
        )

        authn = await resolve_authn_ingress(
            HeaderApiKeyAuthn(authn_spec=_API_KEY_SPEC, header_name="X-API-Key"),
            request=req,
            ctx=ctx,
        )

        assert authn is not None
        assert authn.identity.principal_id == uuid5(NAMESPACE_URL, "key:secret-key")

    @pytest.mark.asyncio
    async def test_cookie_token_uses_authentication_port(self) -> None:
        ctx = ExecutionContext(deps=Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [(b"cookie", b"sid=cookie-token")],
            }
        )

        authn = await resolve_authn_ingress(
            CookieTokenAuthn(authn_spec=_TOKEN_SPEC, cookie_name="sid"),
            request=req,
            ctx=ctx,
        )

        assert authn is not None
        assert authn.identity.principal_id == uuid5(NAMESPACE_URL, "cookie-token")


class TestSecurityContextMiddleware:
    @staticmethod
    async def _ok_app(scope: object, receive: object, send: object) -> None:
        await send(  # type: ignore[misc]
            {"type": "http.response.start", "status": 200, "headers": []}
        )
        await send({"type": "http.response.body", "body": b"ok"})  # type: ignore[misc]

    def test_binds_authn_identity_from_token_ingress(self) -> None:
        ctx = ExecutionContext(deps=Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
        captured: dict[str, object] = {}

        async def _capture_app(scope, receive, send):  # type: ignore[no-untyped-def]
            captured["authn"] = ctx.inv_ctx.get_authn()
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        mw = SecurityContextMiddleware(
            _capture_app,
            ctx_dep=lambda: ctx,
            authn=AuthnRequirement(
                ingress=(HeaderTokenAuthn(authn_spec=_TOKEN_SPEC, header_name="Authorization"),)
            ),
            when_multiple_credentials="first_in_order",
        )
        response = TestClient(mw).get("/", headers={"Authorization": "Bearer token-1"})

        assert response.status_code == 200
        assert isinstance(captured["authn"], AuthnIdentity)
        assert captured["authn"].principal_id == uuid5(NAMESPACE_URL, "token-1")

    def test_binds_tenant_from_authoritative_resolver(self) -> None:
        tid = uuid4()

        class _TenantResolver:
            async def resolve_from_principal(self, principal_id, *, requested_tenant_id=None):
                _ = principal_id, requested_tenant_id
                return TenantIdentity(tenant_id=tid)

        ctx = ExecutionContext(
            deps=Deps.plain(
                {
                    AuthnDepKey: _TokenAuthFactory(),
                    TenantResolverDepKey: lambda c: _TenantResolver(),
                }
            )
        )
        captured: dict[str, object] = {}

        async def _capture_app(scope, receive, send):  # type: ignore[no-untyped-def]
            captured["tenant"] = ctx.inv_ctx.get_tenant()
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        mw = SecurityContextMiddleware(
            _capture_app,
            ctx_dep=lambda: ctx,
            authn=AuthnRequirement(
                ingress=(HeaderTokenAuthn(authn_spec=_TOKEN_SPEC, header_name="Authorization"),)
            ),
            when_multiple_credentials="first_in_order",
        )
        response = TestClient(mw).get("/", headers={"Authorization": "Bearer token-1"})

        assert response.status_code == 200
        assert isinstance(captured["tenant"], TenantIdentity)
        assert captured["tenant"].tenant_id == tid

    def test_first_in_order_short_circuits(self) -> None:
        ctx = ExecutionContext(deps=Deps.plain({AuthnDepKey: _BothFactory()}))
        mw = SecurityContextMiddleware(
            self._ok_app,
            ctx_dep=lambda: ctx,
            authn=AuthnRequirement(
                ingress=(
                    HeaderTokenAuthn(authn_spec=_TOKEN_SPEC, header_name="Authorization"),
                    HeaderApiKeyAuthn(authn_spec=_API_KEY_SPEC, header_name="X-API-Key"),
                )
            ),
            when_multiple_credentials="first_in_order",
        )
        response = TestClient(mw).get(
            "/",
            headers={"Authorization": "Bearer tok", "X-API-Key": "secret-key"},
        )

        assert response.status_code == 200

    def test_reject_raises_when_more_than_one_ingress_matches(self) -> None:
        ctx = ExecutionContext(deps=Deps.plain({AuthnDepKey: _BothFactory()}))
        mw = SecurityContextMiddleware(
            self._ok_app,
            ctx_dep=lambda: ctx,
            authn=AuthnRequirement(
                ingress=(
                    HeaderTokenAuthn(authn_spec=_TOKEN_SPEC, header_name="Authorization"),
                    HeaderApiKeyAuthn(authn_spec=_API_KEY_SPEC, header_name="X-API-Key"),
                )
            ),
            when_multiple_credentials="reject",
        )
        client = TestClient(mw)

        with pytest.raises(CoreException, match="Multiple"):
            client.get(
                "/",
                headers={"Authorization": "Bearer tok", "X-API-Key": "secret-key"},
            )

    @pytest.mark.asyncio
    async def test_non_http_scope_passthrough(self) -> None:
        app = AsyncMock()
        mw = SecurityContextMiddleware(
            app,
            ctx_dep=_execution_ctx,
            authn=AuthnRequirement(
                ingress=(HeaderTokenAuthn(authn_spec=_TOKEN_SPEC, header_name="Authorization"),)
            ),
            when_multiple_credentials="first_in_order",
        )

        await mw({"type": "lifespan"}, AsyncMock(), AsyncMock())

        app.assert_awaited_once()

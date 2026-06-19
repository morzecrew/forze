"""Unit tests for FastAPI invocation and security middleware."""

from unittest.mock import AsyncMock
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest
from fastapi import FastAPI
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
from tests.support.execution_context import context_from_deps
from forze.application.contracts.tenancy import (
    TENANT_ID_HEADER,
    TenantIdentity,
    TenantResolverDepKey,
)
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze.base.exceptions import CoreException, exc
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
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
    return context_from_deps(MockDepsModule(state=MockState())())


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


class _TokenAuthPortWithTenantHint:
    def __init__(self, *, issuer_tenant_hint: str) -> None:
        self._issuer_tenant_hint = issuer_tenant_hint

    async def authenticate_with_password(self, credentials: object) -> AuthnResult | None:
        return None

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> AuthnResult | None:
        return _authn_result(
            uuid5(NAMESPACE_URL, credentials.token),
            issuer_tenant_hint=self._issuer_tenant_hint,
        )

    async def authenticate_with_api_key(self, credentials: object) -> AuthnResult | None:
        return None


class _TokenAuthFactoryWithTenantHint:
    def __init__(self, *, issuer_tenant_hint: str) -> None:
        self._issuer_tenant_hint = issuer_tenant_hint

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _TokenAuthPortWithTenantHint:
        return _TokenAuthPortWithTenantHint(issuer_tenant_hint=self._issuer_tenant_hint)


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

    def test_garbage_correlation_header_falls_back_to_generated_id(self) -> None:
        # The headers are advisory: malformed values must not fail the request.
        ctx = _execution_ctx()
        mw = InvocationMetadataMiddleware(self._ok_app, ctx_dep=lambda: ctx)

        response = TestClient(mw).get(
            "/",
            headers={
                "X-Correlation-ID": "definitely-not-a-uuid",
                "X-Causation-ID": "also-garbage",
            },
        )

        assert response.status_code == 200
        generated = UUID(response.headers["x-correlation-id"])
        assert isinstance(generated, UUID)
        assert "x-causation-id" not in response.headers

    def test_binds_idempotency_key_from_header(self) -> None:
        ctx = _execution_ctx()
        captured: dict[str, str | None] = {}

        async def _capture_app(scope, receive, send):  # type: ignore[no-untyped-def]
            captured["key"] = ctx.inv_ctx.get_idempotency_key()
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        mw = InvocationMetadataMiddleware(_capture_app, ctx_dep=lambda: ctx)
        response = TestClient(mw).get("/", headers={"Idempotency-Key": "req-123"})

        assert response.status_code == 200
        assert captured["key"] == "req-123"

    def test_no_idempotency_key_when_header_absent(self) -> None:
        ctx = _execution_ctx()
        captured: dict[str, str | None] = {}

        async def _capture_app(scope, receive, send):  # type: ignore[no-untyped-def]
            captured["key"] = ctx.inv_ctx.get_idempotency_key()
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        mw = InvocationMetadataMiddleware(_capture_app, ctx_dep=lambda: ctx)
        response = TestClient(mw).get("/")

        assert response.status_code == 200
        assert captured["key"] is None

    @pytest.mark.asyncio
    async def test_non_http_scope_passthrough(self) -> None:
        app = AsyncMock()
        mw = InvocationMetadataMiddleware(app, ctx_dep=_execution_ctx)

        await mw({"type": "lifespan"}, AsyncMock(), AsyncMock())

        app.assert_awaited_once()


class TestResolveAuthnIngress:
    @pytest.mark.asyncio
    async def test_header_token_uses_authentication_port(self) -> None:
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
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
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
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
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _ApiKeyAuthFactory()}))
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
    async def test_header_api_key_splits_prefix_on_colon(self) -> None:
        # ``prefix:secret`` must split on the first colon (matching forze_mcp),
        # so the verifier receives the bare secret, not ``prefix:secret``.
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _ApiKeyAuthFactory()}))
        req = Request(
            {
                "type": "http",
                "path": "/",
                "method": "GET",
                "headers": [(b"x-api-key", b"sk_live:secret-key")],
            }
        )

        authn = await resolve_authn_ingress(
            HeaderApiKeyAuthn(authn_spec=_API_KEY_SPEC, header_name="X-API-Key"),
            request=req,
            ctx=ctx,
        )

        assert authn is not None
        # key == "secret-key" (prefix "sk_live" stripped), not the whole header.
        assert authn.identity.principal_id == uuid5(NAMESPACE_URL, "key:secret-key")

    @pytest.mark.asyncio
    async def test_cookie_token_uses_authentication_port(self) -> None:
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
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
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
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

        ctx = context_from_deps(Deps.plain(
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

    def test_binds_tenant_from_issuer_hint_via_resolver(self) -> None:
        tid = uuid4()

        class _TenantResolver:
            async def resolve_from_principal(self, principal_id, *, requested_tenant_id=None):
                assert requested_tenant_id == tid
                return TenantIdentity(tenant_id=tid)

        ctx = context_from_deps(
            Deps.plain(
                {
                    AuthnDepKey: _TokenAuthFactoryWithTenantHint(
                        issuer_tenant_hint=str(tid),
                    ),
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

    def _run_with_tenant_header(self, tid: UUID, *, trust_tenant_header: bool):
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
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
            trust_tenant_header=trust_tenant_header,
        )
        response = TestClient(mw).get(
            "/",
            headers={
                "Authorization": "Bearer token-1",
                TENANT_ID_HEADER: str(tid),
            },
        )

        assert response.status_code == 200
        return captured["tenant"]

    def test_header_tenant_denied_by_default_without_resolver(self) -> None:
        # Unvalidated X-Tenant-Id header is not trusted unless opted in.
        tenant = self._run_with_tenant_header(uuid4(), trust_tenant_header=False)
        assert tenant is None

    def test_binds_header_tenant_when_trust_tenant_header_opted_in(self) -> None:
        tid = uuid4()
        tenant = self._run_with_tenant_header(tid, trust_tenant_header=True)
        assert isinstance(tenant, TenantIdentity)
        assert tenant.tenant_id == tid

    def test_first_in_order_short_circuits(self) -> None:
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _BothFactory()}))
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

    def test_reject_returns_401_when_more_than_one_ingress_matches(self) -> None:
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _BothFactory()}))
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

        response = client.get(
            "/",
            headers={"Authorization": "Bearer tok", "X-API-Key": "secret-key"},
        )

        assert response.status_code == 401
        assert response.json() == {"detail": "Multiple authentication credentials present"}
        assert response.headers.get(ERROR_CODE_HEADER) == "ambiguous_credentials"

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


class _RejectingAuthPort:
    async def authenticate_with_password(self, credentials: object) -> AuthnResult | None:
        return None

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> AuthnResult | None:
        raise exc.authentication("Invalid authentication credentials")

    async def authenticate_with_api_key(self, credentials: object) -> AuthnResult | None:
        return None


class _RejectingAuthFactory:
    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _RejectingAuthPort:
        return _RejectingAuthPort()


class TestMiddlewareStackErrorHandling:
    """CoreExceptions raised in middlewares must yield the standard JSON error
    response instead of escaping above Starlette's ExceptionMiddleware as a 500."""

    def _build_app(
        self,
        ctx: ExecutionContext,
        *,
        required: bool = False,
        when_multiple_credentials: str = "first_in_order",
    ) -> FastAPI:
        app = FastAPI()

        @app.get("/")
        def ok() -> dict[str, bool]:
            return {"ok": True}

        @app.get("/raise")
        def raise_core_error() -> None:
            raise exc.not_found("Document not found")

        register_exception_handlers(app)

        app.add_middleware(
            SecurityContextMiddleware,  # type: ignore[arg-type]
            ctx_dep=lambda: ctx,
            authn=AuthnRequirement(
                ingress=(
                    HeaderTokenAuthn(
                        authn_spec=_TOKEN_SPEC,
                        header_name="Authorization",
                        required=required,
                    ),
                    HeaderApiKeyAuthn(authn_spec=_API_KEY_SPEC, header_name="X-API-Key"),
                )
            ),
            when_multiple_credentials=when_multiple_credentials,
        )
        app.add_middleware(
            InvocationMetadataMiddleware,  # type: ignore[arg-type]
            ctx_dep=lambda: ctx,
        )

        return app

    def test_invalid_bearer_token_returns_401_not_500(self) -> None:
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _RejectingAuthFactory()}))
        app = self._build_app(ctx)
        client = TestClient(app, raise_server_exceptions=True)

        response = client.get("/", headers={"Authorization": "Bearer not-a-valid-token"})

        assert response.status_code == 401
        assert response.json() == {"detail": "Invalid authentication credentials"}
        assert response.headers.get(ERROR_CODE_HEADER) == "core.authentication"

    def test_missing_required_credentials_returns_401(self) -> None:
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _RejectingAuthFactory()}))
        app = self._build_app(ctx, required=True)
        client = TestClient(app, raise_server_exceptions=True)

        response = client.get("/")

        assert response.status_code == 401
        assert response.json() == {"detail": "Authentication credentials are required"}
        assert response.headers.get(ERROR_CODE_HEADER) == "core.authentication"

    def test_ambiguous_credentials_returns_mapped_status(self) -> None:
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _BothFactory()}))
        app = self._build_app(ctx, when_multiple_credentials="reject")
        client = TestClient(app, raise_server_exceptions=True)

        response = client.get(
            "/",
            headers={"Authorization": "Bearer tok", "X-API-Key": "secret-key"},
        )

        assert response.status_code == 401
        assert response.json() == {"detail": "Multiple authentication credentials present"}
        assert response.headers.get(ERROR_CODE_HEADER) == "ambiguous_credentials"

    def test_garbage_correlation_header_still_succeeds(self) -> None:
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
        app = self._build_app(ctx)
        client = TestClient(app, raise_server_exceptions=True)

        response = client.get(
            "/",
            headers={
                "Authorization": "Bearer token-1",
                "X-Correlation-ID": "definitely-not-a-uuid",
            },
        )

        assert response.status_code == 200
        assert response.json() == {"ok": True}
        assert isinstance(UUID(response.headers["x-correlation-id"]), UUID)

    def test_core_exception_in_route_handler_still_handled(self) -> None:
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
        app = self._build_app(ctx)
        client = TestClient(app, raise_server_exceptions=True)

        response = client.get("/raise", headers={"Authorization": "Bearer token-1"})

        assert response.status_code == 404
        assert response.json() == {"detail": "Document not found"}
        assert response.headers.get(ERROR_CODE_HEADER) == "core.not_found"

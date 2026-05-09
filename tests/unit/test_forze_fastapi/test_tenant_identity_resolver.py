"""Tests for FastAPI tenant identity merge resolver."""

from uuid import uuid4

import pytest
from starlette.requests import Request

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity, TenantResolverDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import AuthenticationError
from forze_fastapi.middlewares.context import (
    HeaderTenantIdentityCodec,
    TenantIdentityResolver,
)


@pytest.mark.asyncio
async def test_prefers_authn_tenant_over_resolver() -> None:
    tid_cred = uuid4()
    tid_res = uuid4()
    pid = uuid4()

    class _Tr:
        async def resolve_from_principal(self, principal_id):
            return TenantIdentity(tenant_id=tid_res)

    ctx = ExecutionContext(
        deps=Deps.plain({TenantResolverDepKey: lambda c: _Tr()}),
    )

    resolver = TenantIdentityResolver()
    req = Request({"type": "http", "method": "GET", "path": "/", "headers": []})

    out = await resolver.resolve(
        req,
        ctx,
        AuthnIdentity(principal_id=pid, tenant_id=tid_cred),
    )

    assert out is not None
    assert out.tenant_id == tid_cred


@pytest.mark.asyncio
async def test_strict_conflict_raises() -> None:
    tid_a = uuid4()
    tid_b = uuid4()
    pid = uuid4()

    class _Tr:
        async def resolve_from_principal(self, principal_id):
            return TenantIdentity(tenant_id=tid_b)

    ctx = ExecutionContext(
        deps=Deps.plain({TenantResolverDepKey: lambda c: _Tr()}),
    )

    resolver = TenantIdentityResolver(strict_tenant_sources=True)
    req = Request({"type": "http", "method": "GET", "path": "/", "headers": []})

    with pytest.raises(AuthenticationError, match="Conflicting"):
        await resolver.resolve(
            req,
            ctx,
            AuthnIdentity(principal_id=pid, tenant_id=tid_a),
        )


@pytest.mark.asyncio
async def test_hint_used_when_authn_has_no_tenant() -> None:
    tid_hint = uuid4()
    pid = uuid4()

    ctx = ExecutionContext(deps=Deps.plain({}))

    resolver = TenantIdentityResolver(hint_codec=HeaderTenantIdentityCodec())
    req = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(b"x-tenant-id", str(tid_hint).encode())],
        }
    )

    out = await resolver.resolve(req, ctx, AuthnIdentity(principal_id=pid))

    assert out is not None
    assert out.tenant_id == tid_hint

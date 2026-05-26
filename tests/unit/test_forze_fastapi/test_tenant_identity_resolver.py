"""Tests for FastAPI tenant resolution helpers."""

from uuid import uuid4

import pytest
from starlette.requests import Request

from forze.application.contracts.authn import AuthnIdentity, AuthnResult
from forze.application.contracts.tenancy import TenantIdentity, TenantResolverDepKey
from forze.application.execution import Deps, ExecutionContext
from forze_fastapi.security import resolve_tenant_identity


def _authn(pid) -> AuthnResult:
    return AuthnResult(identity=AuthnIdentity(principal_id=pid))


@pytest.mark.asyncio
async def test_resolve_tenant_identity_uses_authoritative_resolver() -> None:
    tid = uuid4()
    pid = uuid4()

    class _TenantResolver:
        async def resolve_from_principal(self, principal_id, *, requested_tenant_id=None):
            assert principal_id == pid
            assert requested_tenant_id is None
            return TenantIdentity(tenant_id=tid)

    ctx = ExecutionContext(
        deps=Deps.plain({TenantResolverDepKey: lambda c: _TenantResolver()}),
    )
    req = Request({"type": "http", "method": "GET", "path": "/", "headers": []})

    out = await resolve_tenant_identity(_authn(pid), request=req, ctx=ctx)

    assert out is not None
    assert out.tenant_id == tid


@pytest.mark.asyncio
async def test_resolve_tenant_identity_returns_none_without_authn() -> None:
    ctx = ExecutionContext(deps=Deps.plain({}))
    req = Request({"type": "http", "method": "GET", "path": "/", "headers": []})

    out = await resolve_tenant_identity(None, request=req, ctx=ctx)

    assert out is None


@pytest.mark.asyncio
async def test_resolve_tenant_identity_returns_none_without_tenant_resolver() -> None:
    pid = uuid4()
    ctx = ExecutionContext(deps=Deps.plain({}))
    req = Request({"type": "http", "method": "GET", "path": "/", "headers": []})

    out = await resolve_tenant_identity(_authn(pid), request=req, ctx=ctx)

    assert out is None

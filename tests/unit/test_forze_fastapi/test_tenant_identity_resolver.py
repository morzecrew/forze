"""Tests for FastAPI tenant identity merge resolver."""

from uuid import uuid4

import pytest
from starlette.requests import Request

from forze.application.contracts.authn import AuthnIdentity, AuthnResult
from forze.application.contracts.tenancy import TenantIdentity, TenantResolverDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import AuthenticationError
from forze_fastapi.middlewares.context import (
    HeaderTenantIdentityCodec,
    TenantIdentityResolver,
)


def _authn(pid, *, issuer_tenant_hint: str | None = None) -> AuthnResult:
    return AuthnResult(
        identity=AuthnIdentity(principal_id=pid),
        issuer_tenant_hint=issuer_tenant_hint,
    )


@pytest.mark.asyncio
async def test_matching_hint_is_validated_by_authoritative_resolver() -> None:
    tid_hint = uuid4()
    pid = uuid4()

    class _Tr:
        async def resolve_from_principal(self, principal_id, *, requested_tenant_id=None):
            assert principal_id == pid
            assert requested_tenant_id == tid_hint
            return TenantIdentity(tenant_id=tid_hint)

    ctx = ExecutionContext(
        deps=Deps.plain({TenantResolverDepKey: lambda c: _Tr()}),
    )

    resolver = TenantIdentityResolver(hint_codec=HeaderTenantIdentityCodec())
    req = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(b"x-tenant-id", str(tid_hint).encode())],
        }
    )

    out = await resolver.resolve(
        req,
        ctx,
        _authn(pid),
    )

    assert out is not None
    assert out.tenant_id == tid_hint


@pytest.mark.asyncio
async def test_conflicting_issuer_and_request_hints_raise() -> None:
    tid_header = uuid4()
    tid_issuer = uuid4()
    pid = uuid4()
    ctx = ExecutionContext(deps=Deps.plain({}))

    resolver = TenantIdentityResolver(hint_codec=HeaderTenantIdentityCodec())
    req = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(b"x-tenant-id", str(tid_header).encode())],
        }
    )

    with pytest.raises(AuthenticationError, match="Conflicting"):
        await resolver.resolve(
            req,
            ctx,
            _authn(pid, issuer_tenant_hint=str(tid_issuer)),
        )


@pytest.mark.asyncio
async def test_requested_tenant_without_membership_raises_conflict() -> None:
    tid_hint = uuid4()
    pid = uuid4()

    class _Tr:
        async def resolve_from_principal(self, principal_id, *, requested_tenant_id=None):
            assert principal_id == pid
            assert requested_tenant_id == tid_hint
            return None

    ctx = ExecutionContext(
        deps=Deps.plain({TenantResolverDepKey: lambda c: _Tr()}),
    )

    resolver = TenantIdentityResolver(hint_codec=HeaderTenantIdentityCodec())
    req = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(b"x-tenant-id", str(tid_hint).encode())],
        }
    )

    with pytest.raises(AuthenticationError, match="Requested tenant"):
        await resolver.resolve(req, ctx, _authn(pid))


@pytest.mark.asyncio
async def test_hints_are_ignored_without_authoritative_resolver() -> None:
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

    out = await resolver.resolve(req, ctx, _authn(pid))

    assert out is None

"""Tests for FastAPI tenant resolution helpers."""

from uuid import uuid4

import pytest
from starlette.requests import Request

from forze.application.contracts.authn import AuthnIdentity, AuthnResult
from forze.application.contracts.tenancy import (
    TENANT_ID_HEADER,
    TenantIdentity,
    TenantResolverDepKey,
)
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from tests.support.execution_context import context_from_deps
from forze_fastapi.security import resolve_tenant_identity


def _authn(
    pid,
    *,
    issuer_tenant_hint: str | None = None,
) -> AuthnResult:
    return AuthnResult(
        identity=AuthnIdentity(principal_id=pid),
        issuer_tenant_hint=issuer_tenant_hint,
    )


def _request(*, headers: list[tuple[bytes, bytes]] | None = None) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": headers or [],
        }
    )


def _tenant_header(tid) -> tuple[bytes, bytes]:
    return (TENANT_ID_HEADER.lower().encode(), str(tid).encode())


@pytest.mark.asyncio
async def test_resolve_tenant_identity_uses_authoritative_resolver() -> None:
    tid = uuid4()
    pid = uuid4()

    class _TenantResolver:
        async def resolve_from_principal(self, principal_id, *, requested_tenant_id=None):
            assert principal_id == pid
            assert requested_tenant_id is None
            return TenantIdentity(tenant_id=tid)

    ctx = context_from_deps(
        Deps.plain({TenantResolverDepKey: lambda c: _TenantResolver()}),
    )
    req = _request()

    out = await resolve_tenant_identity(_authn(pid), request=req, ctx=ctx)

    assert out is not None
    assert out.tenant_id == tid


@pytest.mark.asyncio
async def test_resolve_tenant_identity_passes_issuer_hint_to_resolver() -> None:
    tid = uuid4()
    pid = uuid4()

    class _TenantResolver:
        async def resolve_from_principal(self, principal_id, *, requested_tenant_id=None):
            assert principal_id == pid
            assert requested_tenant_id == tid
            return TenantIdentity(tenant_id=tid)

    ctx = context_from_deps(
        Deps.plain({TenantResolverDepKey: lambda c: _TenantResolver()}),
    )
    req = _request()

    out = await resolve_tenant_identity(
        _authn(pid, issuer_tenant_hint=str(tid)),
        request=req,
        ctx=ctx,
    )

    assert out is not None
    assert out.tenant_id == tid


@pytest.mark.asyncio
async def test_resolve_tenant_identity_passes_header_hint_to_resolver() -> None:
    tid = uuid4()
    pid = uuid4()

    class _TenantResolver:
        async def resolve_from_principal(self, principal_id, *, requested_tenant_id=None):
            assert principal_id == pid
            assert requested_tenant_id == tid
            return TenantIdentity(tenant_id=tid)

    ctx = context_from_deps(
        Deps.plain({TenantResolverDepKey: lambda c: _TenantResolver()}),
    )
    req = _request(headers=[_tenant_header(tid)])

    out = await resolve_tenant_identity(_authn(pid), request=req, ctx=ctx)

    assert out is not None
    assert out.tenant_id == tid


@pytest.mark.asyncio
async def test_resolve_tenant_identity_tenant_conflict() -> None:
    tid = uuid4()
    other = uuid4()
    pid = uuid4()

    class _TenantResolver:
        async def resolve_from_principal(self, principal_id, *, requested_tenant_id=None):
            raise AssertionError("should not reach resolver")

    ctx = context_from_deps(
        Deps.plain({TenantResolverDepKey: lambda c: _TenantResolver()}),
    )
    req = _request(headers=[_tenant_header(other)])

    with pytest.raises(CoreException, match="Conflicting") as ei:
        await resolve_tenant_identity(
            _authn(pid, issuer_tenant_hint=str(tid)),
            request=req,
            ctx=ctx,
        )

    assert ei.value.code == "tenant_conflict"


@pytest.mark.asyncio
async def test_resolve_tenant_identity_malformed_hint_ignored() -> None:
    pid = uuid4()

    class _TenantResolver:
        async def resolve_from_principal(self, principal_id, *, requested_tenant_id=None):
            assert principal_id == pid
            assert requested_tenant_id is None
            return None

    ctx = context_from_deps(
        Deps.plain({TenantResolverDepKey: lambda c: _TenantResolver()}),
    )
    req = _request()

    out = await resolve_tenant_identity(
        _authn(pid, issuer_tenant_hint="not-a-uuid"),
        request=req,
        ctx=ctx,
    )

    assert out is None


@pytest.mark.asyncio
async def test_resolve_tenant_identity_verified_issuer_trusted_without_resolver() -> None:
    # A tenant from a verified credential (issuer hint) is honored even with no resolver.
    tid = uuid4()
    pid = uuid4()
    ctx = context_from_deps(Deps.plain({}))
    req = _request()

    out = await resolve_tenant_identity(
        _authn(pid, issuer_tenant_hint=str(tid)),
        request=req,
        ctx=ctx,
    )

    assert out == TenantIdentity(tenant_id=tid)


@pytest.mark.asyncio
async def test_resolve_tenant_identity_header_only_denied_by_default() -> None:
    # A header-only tenant is unauthenticated input: denied unless explicitly trusted.
    tid = uuid4()
    pid = uuid4()
    ctx = context_from_deps(Deps.plain({}))
    req = _request(headers=[_tenant_header(tid)])

    assert await resolve_tenant_identity(_authn(pid), request=req, ctx=ctx) is None
    assert await resolve_tenant_identity(None, request=req, ctx=ctx) is None


@pytest.mark.asyncio
async def test_resolve_tenant_identity_header_trusted_when_opted_in() -> None:
    tid = uuid4()
    ctx = context_from_deps(Deps.plain({}))
    req = _request(headers=[_tenant_header(tid)])

    out = await resolve_tenant_identity(
        None,
        request=req,
        ctx=ctx,
        trust_tenant_header=True,
    )

    assert out == TenantIdentity(tenant_id=tid)


@pytest.mark.asyncio
async def test_trust_tenant_header_ignored_for_anonymous_when_resolver_configured() -> None:
    # A resolver is the tenancy authority. An anonymous request it can't validate must NOT get an
    # attacker-settable header tenant, even with trust_tenant_header=True — that flag is only the
    # no-resolver (gateway) fallback.
    tid = uuid4()

    class _TenantResolver:
        async def resolve_from_principal(self, principal_id, *, requested_tenant_id=None):
            return TenantIdentity(tenant_id=tid)

    ctx = context_from_deps(
        Deps.plain({TenantResolverDepKey: lambda c: _TenantResolver()}),
    )
    req = _request(headers=[_tenant_header(tid)])

    out = await resolve_tenant_identity(
        None,  # anonymous
        request=req,
        ctx=ctx,
        trust_tenant_header=True,
    )

    assert out is None


@pytest.mark.asyncio
async def test_resolve_tenant_identity_returns_none_without_authn() -> None:
    ctx = context_from_deps(Deps.plain({}))
    req = _request()

    out = await resolve_tenant_identity(None, request=req, ctx=ctx)

    assert out is None


@pytest.mark.asyncio
async def test_resolve_tenant_identity_returns_none_without_tenant_resolver() -> None:
    pid = uuid4()
    ctx = context_from_deps(Deps.plain({}))
    req = _request()

    out = await resolve_tenant_identity(_authn(pid), request=req, ctx=ctx)

    assert out is None

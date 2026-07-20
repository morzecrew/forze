"""Tests for authn identity and tenant identity on ExecutionContext."""

from uuid import uuid4

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, InvocationMetadata
from tests.support.execution_context import (
    context_from_deps,
)


def test_get_tenant_without_binding_returns_none() -> None:
    ctx = context_from_deps(Deps())
    assert ctx.inv_ctx.get_tenant() is None


def test_get_tenant_returns_bound_tenant() -> None:
    ctx = context_from_deps(Deps())
    tid = uuid4()
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with ctx.inv_ctx.bind(
        metadata=metadata,
        authn=AuthnIdentity(principal_id=uuid4()),
        tenant=TenantIdentity(tenant_id=tid),
    ):
        ten = ctx.inv_ctx.get_tenant()
        assert ten is not None and ten.tenant_id == tid


def test_get_authn_identity_roundtrip() -> None:
    ctx = context_from_deps(Deps())
    tid = uuid4()
    pid = uuid4()
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthnIdentity(principal_id=pid)

    with ctx.inv_ctx.bind(
        metadata=metadata,
        authn=ident,
        tenant=TenantIdentity(tenant_id=tid),
    ):
        got = ctx.inv_ctx.get_authn()
        assert got is not None
        assert got.principal_id == pid
        assert not hasattr(got, "tenant_id")
        assert ctx.inv_ctx.get_tenant() is not None
        assert ctx.inv_ctx.get_tenant().tenant_id == tid


def test_bind_clears_identity_after_exit() -> None:
    ctx = context_from_deps(Deps())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthnIdentity(principal_id=uuid4())

    with ctx.inv_ctx.bind(
        metadata=metadata,
        authn=ident,
        tenant=TenantIdentity(tenant_id=uuid4()),
    ):
        assert ctx.inv_ctx.get_authn() is not None

    assert ctx.inv_ctx.get_authn() is None
    assert ctx.inv_ctx.get_tenant() is None


def test_identity_without_tenant_yields_no_tenancy_identity() -> None:
    ctx = context_from_deps(Deps())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthnIdentity(principal_id=uuid4())

    with ctx.inv_ctx.bind(metadata=metadata, authn=ident):
        assert ctx.inv_ctx.get_tenant() is None

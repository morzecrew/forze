"""Tests for authn identity and tenant identity on ExecutionContext."""

from uuid import uuid4

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import (
    CallContext,
    Deps,
    ExecutionContext,
)


def test_get_tenancy_identity_without_binding_returns_none() -> None:
    ctx = ExecutionContext(deps=Deps())
    assert ctx.get_tenancy_identity() is None


def test_get_tenant_id_returns_bound_tenant() -> None:
    ctx = ExecutionContext(deps=Deps())
    tid = uuid4()
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())

    with ctx.bind_call(
        call=call,
        identity=AuthnIdentity(principal_id=uuid4()),
        tenancy=TenantIdentity(tenant_id=tid),
    ):
        ten = ctx.get_tenancy_identity()
        assert ten is not None and ten.tenant_id == tid


def test_get_authn_identity_roundtrip() -> None:
    ctx = ExecutionContext(deps=Deps())
    tid = uuid4()
    pid = uuid4()
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthnIdentity(principal_id=pid)

    with ctx.bind_call(
        call=call,
        identity=ident,
        tenancy=TenantIdentity(tenant_id=tid),
    ):
        got = ctx.get_authn_identity()
        assert got is not None
        assert got.principal_id == pid
        assert ctx.get_tenancy_identity() is not None
        assert ctx.get_tenancy_identity().tenant_id == tid


def test_bind_call_clears_identity_after_exit() -> None:
    ctx = ExecutionContext(deps=Deps())
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthnIdentity(principal_id=uuid4())

    with ctx.bind_call(
        call=call,
        identity=ident,
        tenancy=TenantIdentity(tenant_id=uuid4()),
    ):
        assert ctx.get_authn_identity() is not None

    assert ctx.get_authn_identity() is None
    assert ctx.get_tenancy_identity() is None


def test_identity_without_tenant_yields_no_tenancy_identity() -> None:
    ctx = ExecutionContext(deps=Deps())
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthnIdentity(principal_id=uuid4())

    with ctx.bind_call(call=call, identity=ident):
        assert ctx.get_tenancy_identity() is None

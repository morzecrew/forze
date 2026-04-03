"""Tests for principal / tenant / actor context on ExecutionContext."""

from uuid import UUID, uuid4

from forze.application.execution import (
    CallContext,
    Deps,
    ExecutionContext,
    PrincipalContext,
)


def test_get_tenant_id_without_principal_returns_none() -> None:
    ctx = ExecutionContext(deps=Deps())
    assert ctx.get_tenant_id() is None


def test_get_tenant_id_returns_bound_tenant() -> None:
    ctx = ExecutionContext(deps=Deps())
    tid = uuid4()
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())
    principal = PrincipalContext(tenant_id=tid)

    with ctx.bind_call(call=call, principal=principal):
        assert ctx.get_tenant_id() == tid


def test_get_principal_ctx_roundtrip() -> None:
    ctx = ExecutionContext(deps=Deps())
    tid = uuid4()
    aid = uuid4()
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())
    principal = PrincipalContext(tenant_id=tid, actor_id=aid)

    with ctx.bind_call(call=call, principal=principal):
        p = ctx.get_principal_ctx()
        assert p is not None
        assert p.tenant_id == tid
        assert p.actor_id == aid


def test_bind_call_clears_principal_after_exit() -> None:
    ctx = ExecutionContext(deps=Deps())
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())
    principal = PrincipalContext(tenant_id=uuid4())

    with ctx.bind_call(call=call, principal=principal):
        assert ctx.get_principal_ctx() is not None

    assert ctx.get_principal_ctx() is None

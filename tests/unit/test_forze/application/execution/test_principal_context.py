"""Tests for auth identity and tenant context on ExecutionContext."""

from uuid import uuid4

from forze.application.execution import (
    AuthIdentity,
    CallContext,
    Deps,
    ExecutionContext,
)


def test_get_tenant_id_without_identity_returns_none() -> None:
    ctx = ExecutionContext(deps=Deps())
    assert ctx.get_tenant_id() is None


def test_get_tenant_id_returns_bound_tenant() -> None:
    ctx = ExecutionContext(deps=Deps())
    tid = uuid4()
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthIdentity(subject_id="sub", tenant_id=tid)

    with ctx.bind_call(call=call, identity=ident):
        assert ctx.get_tenant_id() == tid


def test_get_auth_identity_roundtrip() -> None:
    ctx = ExecutionContext(deps=Deps())
    tid = uuid4()
    aid = uuid4()
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthIdentity(subject_id="s", tenant_id=tid, actor_id=aid)

    with ctx.bind_call(call=call, identity=ident):
        got = ctx.get_auth_identity()
        assert got is not None
        assert got.subject_id == "s"
        assert got.tenant_id == tid
        assert got.actor_id == aid


def test_bind_call_clears_identity_after_exit() -> None:
    ctx = ExecutionContext(deps=Deps())
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthIdentity(subject_id="x", tenant_id=uuid4())

    with ctx.bind_call(call=call, identity=ident):
        assert ctx.get_auth_identity() is not None

    assert ctx.get_auth_identity() is None


def test_identity_without_tenant_yields_no_tenant_id() -> None:
    ctx = ExecutionContext(deps=Deps())
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthIdentity(subject_id="sub")

    with ctx.bind_call(call=call, identity=ident):
        assert ctx.get_tenant_id() is None

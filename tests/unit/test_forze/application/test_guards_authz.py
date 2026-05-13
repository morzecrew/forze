"""Tests for usecase-level authorization guards."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import AuthzDepKey, AuthzSpec
from forze.application.execution import CallContext, Deps, ExecutionContext
from forze.application.guards.authz import (
    AuthzPermissionRequirement,
    authz_permission_capability_keys,
    authz_permission_guard_factory,
)
from forze.base.errors import AuthenticationError, AuthorizationError


class _AuthzPort:
    def __init__(self) -> None:
        self.permits = AsyncMock(return_value=True)


class _AuthzF:
    def __init__(self, port: _AuthzPort) -> None:
        self._port = port

    def __call__(self, ctx: ExecutionContext, spec: AuthzSpec) -> _AuthzPort:
        return self._port


def _ctx_with_authz(port: _AuthzPort) -> ExecutionContext:
    return ExecutionContext(deps=Deps.plain({AuthzDepKey: _AuthzF(port)}))


@pytest.mark.unit
async def test_authz_permission_guard_allows() -> None:
    port = _AuthzPort()
    ctx = _ctx_with_authz(port)
    spec = AuthzSpec(name="api")
    req = AuthzPermissionRequirement(permission_key="x.read")
    guard = authz_permission_guard_factory(spec, req)(ctx)
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())

    with ctx.bind_call(call=call, identity=AuthnIdentity(principal_id=uuid4())):
        await guard(None)

    port.permits.assert_awaited_once()


@pytest.mark.unit
async def test_authz_permission_guard_denies() -> None:
    port = _AuthzPort()
    port.permits = AsyncMock(return_value=False)
    ctx = _ctx_with_authz(port)
    spec = AuthzSpec(name="api")
    req = AuthzPermissionRequirement(permission_key="x.write")
    guard = authz_permission_guard_factory(spec, req)(ctx)
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())

    with (
        ctx.bind_call(call=call, identity=AuthnIdentity(principal_id=uuid4())),
        pytest.raises(AuthorizationError),
    ):
        await guard(None)


@pytest.mark.unit
async def test_authz_permission_guard_requires_identity_by_default() -> None:
    port = _AuthzPort()
    ctx = _ctx_with_authz(port)
    spec = AuthzSpec(name="api")
    req = AuthzPermissionRequirement(permission_key="x.read")
    guard = authz_permission_guard_factory(spec, req)(ctx)
    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())

    with ctx.bind_call(call=call, identity=None), pytest.raises(AuthenticationError):
        await guard(None)

    port.permits.assert_not_called()


def test_authz_permission_capability_keys() -> None:
    req = AuthzPermissionRequirement(permission_key="doc.write")
    r, p = authz_permission_capability_keys(req)
    assert "authn.principal" in r
    assert "authz.permits:doc.write" in p

    req2 = AuthzPermissionRequirement(
        permission_key="doc.read",
        require_authn_identity=False,
    )
    r2, p2 = authz_permission_capability_keys(req2)
    assert r2 == frozenset()
    assert "authz.permits:doc.read" in p2

"""Tests for :mod:`forze_kits.authn.facades`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from forze_kits.authn import AuthnFacade, AuthnKernelOp, build_authn_registry
from forze.application.contracts.authn import AuthnSpec
from forze.application.execution.operations.facade import facade_op
from forze.application.handlers.authn import AuthnPasswordLogin
from forze.base.exceptions import exc

# ----------------------- #


def _authn_spec() -> AuthnSpec:
    return AuthnSpec(name="app")


class TestAuthnFacade:
    def test_facade_op_descriptors(self) -> None:
        assert isinstance(AuthnFacade.password_login, facade_op)
        assert AuthnFacade.password_login.op == AuthnKernelOp.PASSWORD_LOGIN
        assert AuthnFacade.password_login.uc is AuthnPasswordLogin

    def test_resolve_password_login_operation(self) -> None:
        spec = _authn_spec()
        reg = build_authn_registry(spec).freeze()
        ctx = MagicMock()
        ctx.deps.resolve_configurable = MagicMock(
            return_value=AsyncMock(),
        )
        facade = AuthnFacade(ctx=ctx, registry=reg, namespace=spec.default_namespace)
        op = facade.password_login
        assert op is not None

    def test_namespace_required(self) -> None:
        spec = _authn_spec()
        reg = build_authn_registry(spec).freeze()
        ctx = MagicMock()
        with pytest.raises(exc, match="requires namespace"):
            AuthnFacade(ctx=ctx, registry=reg)

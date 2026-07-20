"""Tests for :mod:`forze_kits.aggregates.authn.facades`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.application.contracts.authn import AuthnSpec
from forze.application.execution.operations.facade import facade_op
from forze.base.exceptions import exc
from forze_kits.aggregates.authn import AuthnFacade, AuthnKernelOp, build_authn_registry
from forze_kits.aggregates.authn.handlers import (
    AuthnPasswordLogin,
    AuthnRequestPasswordReset,
    AuthnResetPassword,
    DeactivatePrincipalHandler,
)

# ----------------------- #


def _authn_spec() -> AuthnSpec:
    return AuthnSpec(name="app")


class TestAuthnFacade:
    def test_facade_op_descriptors(self) -> None:
        assert isinstance(AuthnFacade.password_login, facade_op)
        assert AuthnFacade.password_login.op == AuthnKernelOp.PASSWORD_LOGIN
        assert AuthnFacade.password_login.uc is AuthnPasswordLogin

    def test_deactivate_principal_facade_op(self) -> None:
        assert isinstance(AuthnFacade.deactivate_principal, facade_op)
        assert AuthnFacade.deactivate_principal.op == AuthnKernelOp.DEACTIVATE_PRINCIPAL
        assert AuthnFacade.deactivate_principal.uc is DeactivatePrincipalHandler

    def test_password_reset_facade_ops(self) -> None:
        assert isinstance(AuthnFacade.request_password_reset, facade_op)
        assert (
            AuthnFacade.request_password_reset.op
            == AuthnKernelOp.REQUEST_PASSWORD_RESET
        )
        assert AuthnFacade.request_password_reset.uc is AuthnRequestPasswordReset

        assert isinstance(AuthnFacade.reset_password, facade_op)
        assert AuthnFacade.reset_password.op == AuthnKernelOp.RESET_PASSWORD
        assert AuthnFacade.reset_password.uc is AuthnResetPassword

    def test_resolve_deactivate_principal_operation(self) -> None:
        spec = _authn_spec()
        reg = build_authn_registry(spec).freeze()
        ctx = MagicMock()
        ctx.deps.resolve_configurable = MagicMock(
            return_value=AsyncMock(),
        )
        facade = AuthnFacade(ctx=ctx, registry=reg, namespace=spec.default_namespace)
        op = facade.deactivate_principal
        assert op is not None

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

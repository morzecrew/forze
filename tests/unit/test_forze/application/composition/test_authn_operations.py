"""Tests for :mod:`forze.application.composition.authn.operations`."""

from forze.application.composition.authn import AuthnKernelOp


class TestAuthnKernelOp:
    def test_password_login_suffix(self) -> None:
        assert AuthnKernelOp.PASSWORD_LOGIN == "password_login"

    def test_refresh_tokens_suffix(self) -> None:
        assert AuthnKernelOp.REFRESH_TOKENS == "refresh_tokens"

    def test_logout_suffix(self) -> None:
        assert AuthnKernelOp.LOGOUT == "logout"

    def test_change_password_suffix(self) -> None:
        assert AuthnKernelOp.CHANGE_PASSWORD == "change_password"

    def test_deactivate_principal_suffix(self) -> None:
        assert AuthnKernelOp.DEACTIVATE_PRINCIPAL == "deactivate_principal"

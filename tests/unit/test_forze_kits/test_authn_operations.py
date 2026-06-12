"""Tests for :mod:`forze_kits.aggregates.authn.operations`."""

from forze_kits.aggregates.authn import AuthnKernelOp


class TestAuthnKernelOp:
    def test_password_login_suffix(self) -> None:
        assert AuthnKernelOp.PASSWORD_LOGIN == "password_login"

    def test_refresh_tokens_suffix(self) -> None:
        assert AuthnKernelOp.REFRESH_TOKENS == "refresh_tokens"

    def test_logout_suffix(self) -> None:
        assert AuthnKernelOp.LOGOUT == "logout"

    def test_change_password_suffix(self) -> None:
        assert AuthnKernelOp.CHANGE_PASSWORD == "change_password"

    def test_request_password_reset_suffix(self) -> None:
        assert AuthnKernelOp.REQUEST_PASSWORD_RESET == "request_password_reset"

    def test_reset_password_suffix(self) -> None:
        assert AuthnKernelOp.RESET_PASSWORD == "reset_password"

    def test_deactivate_principal_suffix(self) -> None:
        assert AuthnKernelOp.DEACTIVATE_PRINCIPAL == "deactivate_principal"

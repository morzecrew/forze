import attrs

from forze.application.execution import UsecasesFacade, facade_op
from forze.application.usecases.authn import (
    AuthnChangePassword,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
)

from .operations import AuthnOperation

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnUsecasesFacade(UsecasesFacade):
    """Typed facade for authentication usecases."""

    password_login = facade_op(
        AuthnOperation.PASSWORD_LOGIN,
        uc=AuthnPasswordLogin,
    )
    """Password login usecase."""

    refresh_tokens = facade_op(
        AuthnOperation.REFRESH_TOKENS,
        uc=AuthnRefreshTokens,
    )
    """Refresh tokens usecase."""

    logout = facade_op(
        AuthnOperation.LOGOUT,
        uc=AuthnLogout,
    )
    """Logout (revoke session tokens) usecase."""

    change_password = facade_op(
        AuthnOperation.CHANGE_PASSWORD,
        uc=AuthnChangePassword,
    )
    """Change-password usecase."""

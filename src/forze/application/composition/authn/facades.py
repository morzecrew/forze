import attrs

from forze.application.execution import (
    FacadeOperationDescriptor,
    UsecasesFacade,
    namespaced_facade,
)
from forze.application.usecases.authn import (
    AuthnChangePassword,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
)

from .operations import AuthnKernelOp

# ----------------------- #


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnUsecasesFacade(UsecasesFacade):
    """Typed facade for authentication usecases."""

    password_login = FacadeOperationDescriptor(
        AuthnKernelOp.PASSWORD_LOGIN,
        uc=AuthnPasswordLogin,
    )
    """Password login usecase."""

    refresh_tokens = FacadeOperationDescriptor(
        AuthnKernelOp.REFRESH_TOKENS,
        uc=AuthnRefreshTokens,
    )
    """Refresh tokens usecase."""

    logout = FacadeOperationDescriptor(
        AuthnKernelOp.LOGOUT,
        uc=AuthnLogout,
    )
    """Logout (revoke session tokens) usecase."""

    change_password = FacadeOperationDescriptor(
        AuthnKernelOp.CHANGE_PASSWORD,
        uc=AuthnChangePassword,
    )
    """Change-password usecase."""

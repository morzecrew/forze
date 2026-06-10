import attrs

from forze.application.execution.operations.facade import (
    OperationFacade,
    facade_op,
    namespaced_facade,
)
from .handlers import (
    AuthnChangePassword,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
    DeactivatePrincipalHandler,
)

from .operations import AuthnKernelOp

# ----------------------- #


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnFacade(OperationFacade):
    """Typed facade for authentication operations."""

    password_login = facade_op(
        AuthnKernelOp.PASSWORD_LOGIN,
        uc=AuthnPasswordLogin,
    )
    """Password login operation."""

    refresh_tokens = facade_op(
        AuthnKernelOp.REFRESH_TOKENS,
        uc=AuthnRefreshTokens,
    )
    """Refresh tokens operation."""

    logout = facade_op(
        AuthnKernelOp.LOGOUT,
        uc=AuthnLogout,
    )
    """Logout (revoke session tokens) usecase."""

    change_password = facade_op(
        AuthnKernelOp.CHANGE_PASSWORD,
        uc=AuthnChangePassword,
    )
    """Change-password usecase."""

    deactivate_principal = facade_op(
        AuthnKernelOp.DEACTIVATE_PRINCIPAL,
        uc=DeactivatePrincipalHandler,
    )
    """Deactivate-principal (cascaded offboarding) usecase."""

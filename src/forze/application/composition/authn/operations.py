"""Authn operation kernel suffixes for usecase registration and resolution."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class AuthnKernelOp(StrEnum):
    """Kernel segments (suffix only) for authentication usecase keys."""

    PASSWORD_LOGIN = "password_login"  # nosec B105
    """Authenticate with password credentials and issue a fresh token pair."""

    REFRESH_TOKENS = "refresh_tokens"
    """Rotate a refresh token into a fresh access/refresh pair."""

    LOGOUT = "logout"
    """Revoke all sessions for the currently authenticated identity."""

    CHANGE_PASSWORD = "change_password"  # nosec B105
    """Change the password of the currently authenticated identity."""

    DEACTIVATE_PRINCIPAL = "deactivate_principal"
    """Deactivate a principal for the application (policy, sessions, credentials)."""

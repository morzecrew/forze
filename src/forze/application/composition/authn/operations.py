"""Authn operation identifiers for usecase registration and resolution."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class AuthnOperation(StrEnum):
    """Logical operation identifiers for authentication usecases."""

    PASSWORD_LOGIN = "authn.password_login"  # nosec B105
    """Authenticate with password credentials and issue a fresh token pair."""

    REFRESH_TOKENS = "authn.refresh_tokens"
    """Rotate a refresh token into a fresh access/refresh pair."""

    LOGOUT = "authn.logout"
    """Revoke all sessions for the currently authenticated identity."""

    CHANGE_PASSWORD = "authn.change_password"  # nosec B105
    """Change the password of the currently authenticated identity."""

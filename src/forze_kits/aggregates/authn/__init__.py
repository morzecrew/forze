"""Authn composition: facades, factories, and operation identifiers."""

from .dto import (
    AuthnChangePasswordRequestDTO,
    AuthnLoginRequestDTO,
    AuthnPasswordResetAckDTO,
    AuthnRefreshRequestDTO,
    AuthnRequestPasswordResetDTO,
    AuthnResetPasswordDTO,
    AuthnTokenResponseDTO,
)
from .events import (
    AUTHN_PASSWORD_RESET_REQUESTED,
    AuthnPasswordResetRequestedPayload,
)
from .facades import AuthnFacade
from .factories import build_authn_registry
from .handlers import (
    AuthnChangePassword,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
    AuthnRequestPasswordReset,
    AuthnResetPassword,
    DeactivatePrincipalHandler,
    DeactivatePrincipalRequestDTO,
)
from .operations import AuthnKernelOp

# ----------------------- #

__all__ = [
    "AUTHN_PASSWORD_RESET_REQUESTED",
    "AuthnKernelOp",
    "AuthnFacade",
    "build_authn_registry",
    "AuthnChangePasswordRequestDTO",
    "AuthnLoginRequestDTO",
    "AuthnPasswordResetAckDTO",
    "AuthnPasswordResetRequestedPayload",
    "AuthnRefreshRequestDTO",
    "AuthnRequestPasswordResetDTO",
    "AuthnResetPasswordDTO",
    "AuthnTokenResponseDTO",
    "AuthnChangePassword",
    "AuthnLogout",
    "AuthnPasswordLogin",
    "AuthnRefreshTokens",
    "AuthnRequestPasswordReset",
    "AuthnResetPassword",
    "DeactivatePrincipalHandler",
    "DeactivatePrincipalRequestDTO",
]

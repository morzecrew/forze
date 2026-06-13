"""Authn composition: facades, factories, and operation identifiers."""

from .dto import (
    AuthnApiKeyListDTO,
    AuthnApiKeyListItemDTO,
    AuthnChangePasswordRequestDTO,
    AuthnIssueApiKeyRequestDTO,
    AuthnIssuedApiKeyDTO,
    AuthnLoginRequestDTO,
    AuthnPasswordResetAckDTO,
    AuthnRefreshRequestDTO,
    AuthnRequestPasswordResetDTO,
    AuthnResetPasswordDTO,
    AuthnRevokeApiKeyRequestDTO,
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
    AuthnIssueApiKey,
    AuthnListApiKeys,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
    AuthnRequestPasswordReset,
    AuthnResetPassword,
    AuthnRevokeApiKey,
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
    "AuthnApiKeyListDTO",
    "AuthnApiKeyListItemDTO",
    "AuthnChangePasswordRequestDTO",
    "AuthnIssueApiKeyRequestDTO",
    "AuthnIssuedApiKeyDTO",
    "AuthnLoginRequestDTO",
    "AuthnPasswordResetAckDTO",
    "AuthnPasswordResetRequestedPayload",
    "AuthnRefreshRequestDTO",
    "AuthnRequestPasswordResetDTO",
    "AuthnResetPasswordDTO",
    "AuthnRevokeApiKeyRequestDTO",
    "AuthnTokenResponseDTO",
    "AuthnChangePassword",
    "AuthnIssueApiKey",
    "AuthnListApiKeys",
    "AuthnLogout",
    "AuthnPasswordLogin",
    "AuthnRefreshTokens",
    "AuthnRequestPasswordReset",
    "AuthnResetPassword",
    "AuthnRevokeApiKey",
    "DeactivatePrincipalHandler",
    "DeactivatePrincipalRequestDTO",
]

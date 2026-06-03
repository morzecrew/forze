"""Authn composition: facades, factories, and operation identifiers."""

from .dto import (
    AuthnChangePasswordRequestDTO,
    AuthnLoginRequestDTO,
    AuthnRefreshRequestDTO,
    AuthnTokenResponseDTO,
)
from .facades import AuthnFacade
from .factories import build_authn_registry
from .handlers import (
    AuthnChangePassword,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
)
from .operations import AuthnKernelOp

# ----------------------- #

__all__ = [
    "AuthnKernelOp",
    "AuthnFacade",
    "build_authn_registry",
    "AuthnChangePasswordRequestDTO",
    "AuthnLoginRequestDTO",
    "AuthnRefreshRequestDTO",
    "AuthnTokenResponseDTO",
    "AuthnChangePassword",
    "AuthnLogout",
    "AuthnPasswordLogin",
    "AuthnRefreshTokens",
]

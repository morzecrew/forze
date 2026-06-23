"""Authn value objects, split by concern.

Importing from :mod:`forze.application.contracts.authn.value_objects` continues to work
as before; submodules expose narrower seams when needed (e.g. resolvers consume only
:class:`~forze.application.contracts.authn.value_objects.assertion.VerifiedAssertion`).
"""

from .assertion import ACT_CLAIM, VerifiedAssertion
from .client import ClientIdentity
from .credentials import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    PasswordCredentials,
    RefreshTokenCredentials,
)
from .identity import AuthnIdentity
from .lifetime import CredentialLifetime
from .result import AuthnResult
from .tokens import (
    ApiKeyInfo,
    IssuedAccessToken,
    IssuedApiKey,
    IssuedInvite,
    IssuedPasswordReset,
    IssuedRefreshToken,
    IssuedTokens,
)

# ----------------------- #

__all__ = [
    "AccessTokenCredentials",
    "ApiKeyCredentials",
    "AuthnIdentity",
    "AuthnResult",
    "ClientIdentity",
    "CredentialLifetime",
    "IssuedAccessToken",
    "ApiKeyInfo",
    "IssuedApiKey",
    "IssuedInvite",
    "IssuedPasswordReset",
    "IssuedRefreshToken",
    "IssuedTokens",
    "PasswordCredentials",
    "RefreshTokenCredentials",
    "ACT_CLAIM",
    "VerifiedAssertion",
]

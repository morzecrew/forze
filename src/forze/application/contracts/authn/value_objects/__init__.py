"""Authn value objects, split by concern.

Importing from :mod:`forze.application.contracts.authn.value_objects` continues to work
as before; submodules expose narrower seams when needed (e.g. resolvers consume only
:class:`~forze.application.contracts.authn.value_objects.assertion.VerifiedAssertion`).
"""

from .assertion import VerifiedAssertion
from .credentials import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    PasswordCredentials,
    RefreshTokenCredentials,
)
from .identity import AuthnIdentity
from .lifetime import CredentialLifetime
from .tokens import (
    IssuedAccessToken,
    IssuedApiKey,
    IssuedRefreshToken,
    IssuedTokens,
)

# ----------------------- #

__all__ = [
    "AccessTokenCredentials",
    "ApiKeyCredentials",
    "AuthnIdentity",
    "CredentialLifetime",
    "IssuedAccessToken",
    "IssuedApiKey",
    "IssuedRefreshToken",
    "IssuedTokens",
    "PasswordCredentials",
    "RefreshTokenCredentials",
    "VerifiedAssertion",
]

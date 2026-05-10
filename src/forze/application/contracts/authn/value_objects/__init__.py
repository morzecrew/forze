"""Authn value objects, split by concern.

Importing from :mod:`forze.application.contracts.authn.value_objects` continues to work
as before; submodules expose narrower seams when needed (e.g. resolvers consume only
:class:`~forze.application.contracts.authn.value_objects.assertion.VerifiedAssertion`).
"""

from .assertion import VerifiedAssertion
from .credentials import ApiKeyCredentials, PasswordCredentials, TokenCredentials
from .identity import AuthnIdentity
from .lifetime import CredentialLifetime
from .tokens import ApiKeyResponse, OAuth2Tokens, OAuth2TokensResponse, TokenResponse

# ----------------------- #

__all__ = [
    "ApiKeyCredentials",
    "ApiKeyResponse",
    "AuthnIdentity",
    "CredentialLifetime",
    "OAuth2Tokens",
    "OAuth2TokensResponse",
    "PasswordCredentials",
    "TokenCredentials",
    "TokenResponse",
    "VerifiedAssertion",
]

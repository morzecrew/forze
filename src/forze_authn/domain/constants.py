from typing import Final

# ----------------------- #

ACCESS_TOKEN_SCHEME: Final[str] = "Bearer"
"""Access token scheme hint."""

# ....................... #
# Issuer identifiers used in :class:`~forze.application.contracts.authn.value_objects.assertion.VerifiedAssertion`.

ISSUER_FORZE_JWT: Final[str] = "forze:jwt"
"""Stable issuer label for first-party Forze access tokens."""

ISSUER_FORZE_PASSWORD: Final[str] = "forze:password_account"
"""Stable issuer label for assertions produced by the Argon2 password verifier."""

ISSUER_FORZE_API_KEY: Final[str] = "forze:api_key"
"""Stable issuer label for assertions produced by the HMAC API-key verifier."""

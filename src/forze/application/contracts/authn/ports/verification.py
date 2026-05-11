from typing import Awaitable, Protocol

from ..value_objects import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    PasswordCredentials,
    VerifiedAssertion,
)

# ----------------------- #


class PasswordVerifierPort(Protocol):
    """Verify password credentials and emit a :class:`VerifiedAssertion`.

    The output assertion identifies the authority and subject (e.g. the password account
    record) without coercing them into a Forze :class:`UUID`. A
    :class:`~forze.application.contracts.authn.ports.resolution.PrincipalResolverPort`
    completes the mapping to ``AuthnIdentity``.
    """

    def verify_password(
        self,
        credentials: PasswordCredentials,  # noqa: F841
    ) -> Awaitable[VerifiedAssertion]:
        """Verify password credentials and return a :class:`VerifiedAssertion`."""
        ...


# ....................... #


class TokenVerifierPort(Protocol):
    """Verify access-token credentials (JWT, opaque, OIDC, etc.) and emit a :class:`VerifiedAssertion`.

    External IdP integrations (Firebase, Casdoor, generic OIDC) implement this port; their
    :attr:`~forze.application.contracts.authn.value_objects.assertion.VerifiedAssertion.issuer`
    becomes the discriminator inside the principal resolver. Refresh tokens are handled
    by :class:`~forze.application.contracts.authn.ports.lifecycle.TokenLifecyclePort` and
    never reach a verifier.
    """

    def verify_token(
        self,
        credentials: AccessTokenCredentials,  # noqa: F841
    ) -> Awaitable[VerifiedAssertion]:
        """Verify access-token credentials and return a :class:`VerifiedAssertion`."""
        ...


# ....................... #


class ApiKeyVerifierPort(Protocol):
    """Verify API-key credentials and emit a :class:`VerifiedAssertion`."""

    def verify_api_key(
        self,
        credentials: ApiKeyCredentials,  # noqa: F841
    ) -> Awaitable[VerifiedAssertion]:
        """Verify API key credentials and return a :class:`VerifiedAssertion`."""
        ...

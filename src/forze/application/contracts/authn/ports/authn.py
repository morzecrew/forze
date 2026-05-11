from typing import Awaitable, Protocol

from ..value_objects import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    AuthnIdentity,
    PasswordCredentials,
)

# ----------------------- #


class AuthnPort(Protocol):
    """Orchestration facade for authenticating requests by credential type.

    Implementations dispatch to the matching verifier and resolver pair to produce a
    canonical :class:`~forze.application.contracts.authn.value_objects.identity.AuthnIdentity`.
    The three methods stay separate (rather than collapsing into a single ``authenticate``)
    to keep call-site type narrowing explicit and to make adding new credential families
    (e.g. mTLS, WebAuthn) a non-breaking addition.
    """

    def authenticate_with_password(
        self,
        credentials: PasswordCredentials,  # noqa: F841
    ) -> Awaitable[AuthnIdentity]:
        """Authenticate with password credentials and return the subject."""
        ...

    def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,  # noqa: F841
    ) -> Awaitable[AuthnIdentity]:
        """Authenticate with access-token credentials and return the subject."""
        ...

    def authenticate_with_api_key(
        self,
        credentials: ApiKeyCredentials,  # noqa: F841
    ) -> Awaitable[AuthnIdentity]:
        """Authenticate with API key credentials and return the subject."""
        ...

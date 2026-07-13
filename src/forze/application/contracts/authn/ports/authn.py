from collections.abc import Awaitable
from typing import Protocol

from ..value_objects import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    AuthnResult,
    PasswordCredentials,
)

# ----------------------- #


class AuthnPort(Protocol):
    """Orchestration facade for authenticating requests by credential type.

    Implementations dispatch to the matching verifier and resolver pair to produce a
    canonical
    :class:`~forze.application.contracts.authn.value_objects.result.AuthnResult`.
    The three methods stay separate (rather than collapsing into a single ``authenticate``)
    to keep call-site type narrowing explicit and to make adding new credential families
    (e.g. mTLS, WebAuthn) a non-breaking addition.
    """

    def authenticate_with_password(
        self,
        credentials: PasswordCredentials,
    ) -> Awaitable[AuthnResult]:
        """Authenticate with password credentials and return the boundary authn result."""
        ...

    def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> Awaitable[AuthnResult]:
        """Authenticate with access-token credentials and return the boundary authn result."""
        ...

    def authenticate_with_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> Awaitable[AuthnResult]:
        """Authenticate with API key credentials and return the boundary authn result."""
        ...

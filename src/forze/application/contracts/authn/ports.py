from typing import Awaitable, Protocol

from .value_objects import (
    ApiKeyCredentials,
    AuthnIdentity,
    OAuth2Tokens,
    OAuth2TokensResponse,
    PasswordCredentials,
    TokenCredentials,
)

# ----------------------- #


class AuthnPort(Protocol):
    """Port for authenticating with different credentials."""

    def authenticate_with_password(
        self,
        credentials: PasswordCredentials,  # noqa: F841
    ) -> Awaitable[AuthnIdentity]:
        """Authenticate with password credentials and return the subject."""
        ...

    def authenticate_with_token(
        self,
        credentials: TokenCredentials,  # noqa: F841
    ) -> Awaitable[AuthnIdentity]:
        """Authenticate with token credentials and return the subject."""
        ...

    def authenticate_with_api_key(
        self,
        credentials: ApiKeyCredentials,  # noqa: F841
    ) -> Awaitable[AuthnIdentity]:
        """Authenticate with API key credentials and return the subject."""
        ...


# ....................... #


class PasswordLifecyclePort(Protocol):  # pragma: no cover
    """Port for managing the lifecycle of password accounts."""

    def change_password(
        self,
        identity: AuthnIdentity,  # noqa: F841
        new_password: str,  # noqa: F841
    ) -> Awaitable[None]: ...


# ....................... #


class TokenLifecyclePort(Protocol):  # pragma: no cover
    """Port for managing the lifecycle of authentication tokens."""

    def issue_tokens(
        self,
        identity: AuthnIdentity,  # noqa: F841
    ) -> Awaitable[OAuth2TokensResponse]: ...

    def refresh_tokens(
        self,
        credentials: OAuth2Tokens,  # noqa: F841
    ) -> Awaitable[OAuth2TokensResponse]: ...

    def revoke_tokens(
        self,
        identity: AuthnIdentity,  # noqa: F841
    ) -> Awaitable[None]: ...

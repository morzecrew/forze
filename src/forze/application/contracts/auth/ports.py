from typing import Awaitable, Protocol, Sequence

from .value_objects import (
    ApiKeyCredentials,
    ApiKeyResponse,
    AuthIdentity,
    AuthorizationRequest,
    OAuth2Tokens,
    OAuth2TokensResponse,
    PasswordCredentials,
    TokenCredentials,
)

# ----------------------- #


class AuthenticationPort(Protocol):  # pragma: no cover
    """Port for authenticating with different credentials."""

    def authenticate_with_password(
        self,
        credentials: PasswordCredentials,  # noqa: F841
    ) -> Awaitable[AuthIdentity | None]:
        """Authenticate with password credentials and return the identity."""
        ...

    def authenticate_with_token(
        self,
        credentials: TokenCredentials,  # noqa: F841
    ) -> Awaitable[AuthIdentity | None]:
        """Authenticate with token credentials and return the identity."""
        ...

    def authenticate_with_api_key(
        self,
        credentials: ApiKeyCredentials,  # noqa: F841
    ) -> Awaitable[AuthIdentity | None]:
        """Authenticate with API key credentials and return the identity."""
        ...


# ....................... #


class TokenLifecyclePort(Protocol):  # pragma: no cover
    """Port for managing the lifecycle of authentication tokens."""

    def issue_tokens(
        self,
        identity: AuthIdentity,  # noqa: F841
    ) -> Awaitable[OAuth2TokensResponse | None]: ...

    def refresh_tokens(
        self,
        credentials: OAuth2Tokens,  # noqa: F841
    ) -> Awaitable[OAuth2TokensResponse | None]: ...

    def revoke_token(self, token_id: str) -> Awaitable[None]: ...  # noqa: F841

    def revoke_many_tokens(
        self,
        token_ids: Sequence[str],  # noqa: F841
    ) -> Awaitable[None]: ...


# ....................... #


class ApiKeyLifecyclePort(Protocol):  # pragma: no cover
    """Port for managing the lifecycle of API keys."""

    def issue_api_key(
        self,
        identity: AuthIdentity,  # noqa: F841
    ) -> Awaitable[ApiKeyResponse | None]: ...

    def refresh_api_key(
        self,
        credentials: ApiKeyCredentials,  # noqa: F841
    ) -> Awaitable[ApiKeyResponse | None]: ...

    def revoke_api_key(self, key_id: str) -> Awaitable[None]: ...  # noqa: F841

    def revoke_many_api_keys(
        self,
        key_ids: Sequence[str],  # noqa: F841
    ) -> Awaitable[None]: ...


# ....................... #


class AuthorizationPort(Protocol):  # pragma: no cover
    """Port for authorizing access to resources."""

    def authorize(
        self,
        identity: AuthIdentity,  # noqa: F841
        request: AuthorizationRequest,  # noqa: F841
    ) -> Awaitable[bool]: ...

    def authorize_many(
        self,
        identity: AuthIdentity,  # noqa: F841
        requests: Sequence[AuthorizationRequest],  # noqa: F841
    ) -> Awaitable[bool]: ...

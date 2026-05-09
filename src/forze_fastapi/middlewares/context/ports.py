from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Awaitable, Protocol

from fastapi import Request

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import CallContext, ExecutionContext

# ----------------------- #


class CallContextCodecPort(Protocol):
    """Codec for encoding and decoding the call context."""

    def decode(self, request: Request) -> CallContext: ...  # pragma: no cover

    def encode(
        self,
        headers: list[tuple[bytes, bytes]],
        ctx: CallContext,
    ) -> list[tuple[bytes, bytes]]: ...  # pragma: no cover


# ....................... #


class AuthnIdentityResolverPort(Protocol):
    """Async resolver for authenticating a request into an identity."""

    def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
    ) -> Awaitable[AuthnIdentity | None]: ...  # pragma: no cover


# ....................... #


class TenantIdentityCodecPort(Protocol):
    """Codec for decoding the tenant identity from a request."""

    def decode(self, request: Request) -> TenantIdentity | None: ...  # pragma: no cover


# ....................... #


class TenantIdentityResolverPort(Protocol):
    """Async resolver for resolving the tenant identity from a request."""

    def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
        identity: AuthnIdentity | None,
    ) -> Awaitable[TenantIdentity | None]: ...  # pragma: no cover

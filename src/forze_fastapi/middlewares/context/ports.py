from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Awaitable, Protocol

from fastapi import Request

from forze.application.contracts.authn import AuthnResult
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext, InvocationMetadata

# ----------------------- #


class InvocationMetadataCodecPort(Protocol):
    """Codec for encoding and decoding the invocation metadata."""

    def decode(self, request: Request) -> InvocationMetadata: ...  # pragma: no cover

    def encode(
        self,
        headers: list[tuple[bytes, bytes]],
        metadata: InvocationMetadata,
    ) -> list[tuple[bytes, bytes]]: ...  # pragma: no cover


# ....................... #


class AuthnIdentityResolverPort(Protocol):
    """Async resolver for authenticating a request into a boundary authn result."""

    def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
    ) -> Awaitable[AuthnResult | None]: ...  # pragma: no cover


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
        authn: AuthnResult | None,
    ) -> Awaitable[TenantIdentity | None]: ...  # pragma: no cover

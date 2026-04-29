from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Awaitable, Protocol

from fastapi import Request

from forze.application.execution import AuthIdentity, CallContext, ExecutionContext

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


class AuthIdentityCodecPort(Protocol):
    """Codec for decoding the authenticated identity from a request."""

    def decode(self, request: Request) -> AuthIdentity | None: ...  # pragma: no cover


# ....................... #


class AuthIdentityResolverPort(Protocol):
    """Async resolver for authenticating a request into an identity."""

    def resolve(
        self,
        request: Request,
        ctx: ExecutionContext,
    ) -> Awaitable[AuthIdentity | None]: ...  # pragma: no cover

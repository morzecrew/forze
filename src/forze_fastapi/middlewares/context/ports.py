from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Protocol

from fastapi import Request

from forze.application.execution import CallContext, PrincipalContext

# ----------------------- #


class CallContextResolverPort(Protocol):
    """Resolve the call context from the FastAPI request."""

    def resolve(self, request: Request) -> CallContext: ...


# ....................... #


class CallContextInjectorPort(Protocol):
    """Inject the call context headers into the FastAPI response."""

    def inject(
        self, headers: list[tuple[bytes, bytes]], ctx: CallContext
    ) -> list[tuple[bytes, bytes]]: ...


# ....................... #


class PrincipalContextResolverPort(Protocol):
    """Resolve the principal context from the FastAPI request."""

    def resolve(self, request: Request) -> PrincipalContext | None: ...

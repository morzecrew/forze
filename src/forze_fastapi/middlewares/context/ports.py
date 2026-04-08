from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Protocol

from fastapi import Request

from forze.application.execution import CallContext, PrincipalContext

# ----------------------- #


class CallContextCodecPort(Protocol):
    """Codec for encoding and decoding the call context."""

    def decode(self, request: Request) -> CallContext: ...

    def encode(
        self,
        headers: list[tuple[bytes, bytes]],
        ctx: CallContext,
    ) -> list[tuple[bytes, bytes]]: ...


# ....................... #


class PrincipalContextCodecPort(Protocol):
    """Codec for decoding the principal context."""

    def decode(self, request: Request) -> PrincipalContext | None: ...

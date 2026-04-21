from typing import Protocol, runtime_checkable

from fastapi import Response

from .context import HttpEndpointContext
from .typevars import B, C, F, H, In, P, Q, R, Raw

# ----------------------- #


@runtime_checkable
class HttpEndpointHandlerPort(Protocol[Q, P, H, C, B, In, Raw, R, F]):
    async def __call__(
        self,
        ctx: HttpEndpointContext[Q, P, H, C, B, In, Raw, R, F],
    ) -> R | Response: ...


# ....................... #


@runtime_checkable
class HttpEndpointFeaturePort(Protocol[Q, P, H, C, B, In, Raw, R, F]):
    def wrap(
        self,
        handler: HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R, F],
    ) -> HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R, F]: ...

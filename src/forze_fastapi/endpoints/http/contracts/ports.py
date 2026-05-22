from typing import Protocol, runtime_checkable

from fastapi import Response

from .context import HttpEndpointContext
from .typevars import B, C, H, In, P, Q, R, Raw

# ----------------------- #


@runtime_checkable
class HttpEndpointHandlerPort(Protocol[Q, P, H, C, B, In, Raw, R]):
    async def __call__(
        self,
        ctx: HttpEndpointContext[Q, P, H, C, B, In, Raw, R],
    ) -> R | Response: ...


# ....................... #


@runtime_checkable
class HttpEndpointFeaturePort(Protocol[Q, P, H, C, B, In, Raw, R]):
    def wrap(
        self,
        handler: HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R],
    ) -> HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R]: ...

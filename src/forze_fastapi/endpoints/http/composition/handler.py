from typing import final

import attrs

from ..contracts import HttpEndpointContext, HttpEndpointHandlerPort
from ..contracts.typevars import B, C, F, H, In, P, Q, R

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class UsecaseHttpEndpointHandler(HttpEndpointHandlerPort[Q, P, H, C, B, In, R, F]):
    async def __call__(self, ctx: HttpEndpointContext[Q, P, H, C, B, In, R, F]) -> R:
        uc = ctx.spec.call.bind(ctx.facade)

        return await uc(ctx.input)

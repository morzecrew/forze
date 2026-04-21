from typing import final

import attrs

from ..contracts import HttpEndpointContext, HttpEndpointHandlerPort
from ..contracts.typevars import B, C, F, H, In, P, Q, R, Raw

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class UsecaseHttpEndpointHandler(HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R, F]):
    async def __call__(
        self,
        ctx: HttpEndpointContext[Q, P, H, C, B, In, Raw, R, F],
    ) -> R:
        uc = ctx.spec.call.bind(ctx.facade)
        raw = await uc(ctx.input)
        mapper = ctx.spec.response_mapper
        if mapper is None:
            return raw  # type: ignore[return-value]
        return await mapper(raw, ctx=ctx.exec_ctx)

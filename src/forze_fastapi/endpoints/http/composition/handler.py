from typing import cast, final

import attrs

from forze.application.execution import OperationRef, UsecasesFacade

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
        call = cast(OperationRef[In, Raw], getattr(ctx.spec, "call"))
        facade = cast(UsecasesFacade, ctx.facade)
        uc = facade.resolve(call)
        raw = await uc(ctx.input)
        mapper = ctx.spec.response_mapper
        if mapper is None:
            return raw  # type: ignore[return-value]
        return await mapper(raw, ctx=ctx.exec_ctx)

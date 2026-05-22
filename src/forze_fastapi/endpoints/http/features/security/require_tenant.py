from typing import final

import attrs
from fastapi import HTTPException, Response

from ...contracts import (
    HttpEndpointContext,
    HttpEndpointFeaturePort,
    HttpEndpointHandlerPort,
)
from ...contracts.typevars import B, C, H, In, P, Q, R, Raw

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RequireTenantFeature(HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, R]):
    """Require a bound tenant identity before the handler runs."""

    # ....................... #

    def wrap(
        self,
        handler: HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R],
    ) -> HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R]:

        async def wrapped(
            ctx: HttpEndpointContext[Q, P, H, C, B, In, Raw, R],
        ) -> R | Response:
            ten = ctx.exec_ctx.inv.get_tenant()

            if ten is None:
                raise HTTPException(status_code=401, detail="Tenant context required")

            return await handler(ctx)

        return wrapped

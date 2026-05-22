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
class RequireAuthnFeature(HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, R]):
    """Require a bound :class:`~forze.application.contracts.authn.AuthnIdentity` before the handler runs."""

    def wrap(
        self,
        handler: HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R],
    ) -> HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R]:

        async def wrapped(
            ctx: HttpEndpointContext[Q, P, H, C, B, In, Raw, R],
        ) -> R | Response:
            ident = ctx.exec_ctx.inv.get_authn()

            if ident is None:
                raise HTTPException(status_code=401, detail="Authentication required")

            return await handler(ctx)

        return wrapped

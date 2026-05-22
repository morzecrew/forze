from typing import Any, final

import attrs

from forze.application.execution.registry import FrozenOperationRegistry

from ..contracts import HttpEndpointContext, HttpEndpointHandlerPort

# ----------------------- #


def build_http_endpoint_handler(
    registry: FrozenOperationRegistry,
) -> HttpEndpointHandlerPort[Any, Any, Any, Any, Any, Any, Any, Any]:
    """Build a handler that resolves and runs the spec operation from a frozen registry."""

    @final
    @attrs.define(slots=True, frozen=True, kw_only=True)
    class _Handler(HttpEndpointHandlerPort[Any, Any, Any, Any, Any, Any, Any, Any]):
        async def __call__(
            self,
            ctx: HttpEndpointContext[Any, Any, Any, Any, Any, Any, Any, Any],
        ) -> Any:
            resolved = registry.resolve(ctx.spec.operation, ctx.exec_ctx)
            raw = await resolved(ctx.input)
            mapper = ctx.spec.response_mapper

            if mapper is None:
                return raw

            return await mapper(raw)

    return _Handler()

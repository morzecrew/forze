from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, Callable

from fastapi import APIRouter, Depends, Request

from forze.application.execution import ExecutionContext, UsecaseRegistry

from ..._utils import facade_dependency
from ..contracts import HttpEndpointContext, HttpEndpointSpec
from ..contracts.typevars import B, C, F, H, In, P, Q, R
from .dto import build_request_dto
from .handler import UsecaseHttpEndpointHandler
from .helpers import compose_endpoint_features
from .signature import build_http_endpoint_signature

# ----------------------- #


def attach_http_endpoint(
    router: APIRouter,
    *,
    spec: HttpEndpointSpec[Q, P, H, C, B, In, R, F],
    registry: UsecaseRegistry,
    ctx_dep: Callable[[], ExecutionContext],
) -> APIRouter:
    facade_dep = facade_dependency(
        facade=spec.facade_type,
        reg=registry,
        ctx_dep=ctx_dep,
    )
    operation_id = registry.qualify_operation(spec.call.op)
    base_handler = UsecaseHttpEndpointHandler[Q, P, H, C, B, In, R, F]()
    handler = compose_endpoint_features(base_handler, spec.features)

    async def endpoint(
        request: Request,
        ctx: ExecutionContext = Depends(ctx_dep),
        ucs: F = Depends(facade_dep),
        **kwargs: Any,
    ) -> Any:
        dto = build_request_dto(kwargs=kwargs, spec=spec.request)
        input_ = await spec.mapper(dto)

        call_ctx = HttpEndpointContext(
            raw_request=request,
            raw_kwargs=kwargs,
            exec_ctx=ctx,
            facade=ucs,
            dto=dto,
            input=input_,
            spec=spec,
            operation_id=operation_id,
        )

        return await handler(call_ctx)

    endpoint.__name__ = operation_id.replace(".", "_")
    endpoint.__qualname__ = endpoint.__name__
    endpoint.__signature__ = (  # type: ignore[attr-defined]
        build_http_endpoint_signature(
            spec=spec,
            facade_dep=facade_dep,
            ctx_dep=ctx_dep,
        )
    )

    metadata = spec.metadata or {}
    description = metadata.get("description")

    if description is not None:
        # Only docstring hack allows to use Markdown formatting
        endpoint.__doc__ = description

    router.add_api_route(
        spec.http["path"],
        endpoint,
        methods=[spec.http["method"]],
        response_model=spec.response,
        status_code=spec.http.get("status_code"),
        operation_id=operation_id,
        summary=metadata.get("summary"),
    )

    return router

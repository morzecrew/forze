from fastapi.routing import APIRoute

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, Callable, Sequence

from fastapi import APIRouter, Depends, Request

from forze.application.execution import ExecutionContext, UsecaseRegistry
from forze.base.errors import CoreError

from ..._utils import facade_dependency
from ..contracts import HttpEndpointContext, HttpEndpointSpec
from ..contracts.typevars import B, C, F, H, In, P, Q, R
from .dto import build_request_dto
from .handler import UsecaseHttpEndpointHandler
from .helpers import compose_endpoint_features, validate_http_features
from .signature import build_http_endpoint_signature

# ----------------------- #


def _join_paths(prefix: str, path: str) -> str:
    if not prefix:
        return path

    if not path:
        return prefix

    return f"{prefix.rstrip('/')}/{path.lstrip('/')}"


# ....................... #


def _has_route(router: APIRouter, *, path: str, method: str) -> bool:
    method = method.upper()
    full_path = _join_paths(router.prefix, path)

    for r in router.routes:
        if isinstance(r, APIRoute) and r.path == full_path and method in r.methods:
            return True

    return False


# ....................... #


def attach_http_endpoint(
    router: APIRouter,
    *,
    spec: HttpEndpointSpec[Q, P, H, C, B, In, R, F],
    registry: UsecaseRegistry,
    ctx_dep: Callable[[], ExecutionContext],
) -> APIRouter:
    # Fail fast if route already exists
    path = spec.http["path"]
    method = spec.http["method"]

    if _has_route(router, path=path, method=method):
        raise CoreError(f"Route already exists: {path} {method}")

    # Fail fast if features are invalid
    validate_http_features(spec.http, spec.features)

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
        path,
        endpoint,
        methods=[method],
        response_model=spec.response,
        status_code=spec.http.get("status_code"),
        operation_id=operation_id,
        summary=metadata.get("summary"),
    )

    return router


# ....................... #


def attach_http_endpoints(
    router: APIRouter,
    *,
    specs: Sequence[HttpEndpointSpec[Any, Any, Any, Any, Any, Any, Any, Any]],
    registry: UsecaseRegistry,
    ctx_dep: Callable[[], ExecutionContext],
) -> APIRouter:
    for spec in specs:
        attach_http_endpoint(router, spec=spec, registry=registry, ctx_dep=ctx_dep)

    return router

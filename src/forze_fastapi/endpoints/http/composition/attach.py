from fastapi.routing import APIRoute

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, Callable, Sequence

from fastapi import APIRouter, Depends, Request

from forze.application.execution import ExecutionContext
from forze.application.execution.registry import FrozenOperationRegistry
from forze.base.errors import CoreError

from ..contracts import HttpEndpointContext, HttpEndpointSpec
from ..contracts.typevars import B, C, H, In, P, Q, R, Raw
from .dto import build_request_dto
from .handler import build_http_endpoint_handler
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
    spec: HttpEndpointSpec[Q, P, H, C, B, In, Raw, R],
    registry: FrozenOperationRegistry,
    ctx_dep: Callable[[], ExecutionContext],
    exclude_none: bool = True,
) -> APIRouter:
    path = spec.http["path"]
    method = spec.http["method"]

    if _has_route(router, path=path, method=method):
        raise CoreError(f"Route already exists: {path} {method}")

    validate_http_features(spec.http, spec.features)

    operation_id = str(spec.operation)
    base_handler = build_http_endpoint_handler(registry)  # type: ignore
    handler = compose_endpoint_features(base_handler, spec.features)  # type: ignore

    async def endpoint(
        request: Request,
        ctx: ExecutionContext = Depends(ctx_dep),
        **kwargs: Any,
    ) -> Any:
        dto = build_request_dto(kwargs=kwargs, spec=spec.request)
        input_ = await spec.request_mapper(dto)

        call_ctx = HttpEndpointContext(
            raw_request=request,
            raw_kwargs=kwargs,
            exec_ctx=ctx,
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
            ctx_dep=ctx_dep,
        )
    )

    metadata = spec.metadata or {}
    description = metadata.get("description")

    if description is not None:
        endpoint.__doc__ = description

    rsp: type[R] | type[None] | None = spec.response

    if rsp is type(None):
        rsp = None  # type: ignore[assignment]

    route_kwargs: dict[str, Any] = {
        "path": path,
        "endpoint": endpoint,
        "methods": [method],
        "response_model": rsp,
        "status_code": spec.http.get("status_code"),
        "operation_id": operation_id,
        "summary": metadata.get("summary"),
        "response_model_exclude_none": exclude_none,
    }

    if "dependencies" in metadata:
        route_kwargs["dependencies"] = list(metadata["dependencies"])

    if "openapi_extra" in metadata:
        route_kwargs["openapi_extra"] = dict(metadata["openapi_extra"])

    if "responses" in metadata:
        route_kwargs["responses"] = dict(metadata["responses"])

    if "include_in_schema" in metadata:
        route_kwargs["include_in_schema"] = bool(metadata["include_in_schema"])

    router.add_api_route(**route_kwargs)

    return router


# ....................... #


def attach_http_endpoints(
    router: APIRouter,
    *,
    specs: Sequence[HttpEndpointSpec[Any, Any, Any, Any, Any, Any, Any, Any]],
    registry: FrozenOperationRegistry,
    ctx_dep: Callable[[], ExecutionContext],
    exclude_none: bool = True,
) -> APIRouter:
    for spec in specs:
        attach_http_endpoint(
            router,
            spec=spec,
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    return router

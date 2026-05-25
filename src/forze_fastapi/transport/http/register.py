"""Register generated routes on a standard :class:`fastapi.APIRouter`."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from fastapi import APIRouter
from fastapi.routing import APIRoute

from forze_fastapi.transport.http.policies import merge_policies
from forze_fastapi.transport.http.policies.base import Policy
from forze_fastapi.transport.http.router import ForzeAPIRoute, HttpMethod

# ----------------------- #


@dataclass(frozen=True, slots=True)
class RouteRegistration:
    """Description of one route to attach via :func:`register_route`."""

    method: HttpMethod
    path: str
    operation_id: str
    endpoint: Callable[..., Any]
    response_model: type[Any] | None = None
    status_code: int | None = None
    dependencies: Sequence[Any] = field(default_factory=tuple)
    policies: Sequence[Policy] = field(default_factory=tuple)
    route_class: type | None = None
    openapi_extra: dict[str, Any] | None = None
    include_in_schema: bool = True


# ....................... #


def register_route(router: APIRouter, reg: RouteRegistration) -> None:
    """Merge policies and call ``router.add_api_route``."""

    merged = merge_policies(reg.policies)
    deps = list(reg.dependencies) + merged.dependencies
    openapi_extra = dict(merged.openapi_extra)
    if reg.openapi_extra:
        from forze_fastapi.transport.http.policies.base import deep_merge_openapi_extra

        openapi_extra = deep_merge_openapi_extra(openapi_extra, reg.openapi_extra)

    route_class = reg.route_class or merged.route_class or APIRoute

    add_kwargs: dict[str, Any] = {
        "methods": [reg.method],
        "response_model": reg.response_model,
        "status_code": reg.status_code,
        "dependencies": deps,
        "operation_id": reg.operation_id,
        "include_in_schema": reg.include_in_schema,
    }

    if openapi_extra:
        add_kwargs["openapi_extra"] = openapi_extra

    if route_class is ForzeAPIRoute:
        route = ForzeAPIRoute(
            router.prefix + reg.path,
            reg.endpoint,
            forze_policies=merged.policies,
            **add_kwargs,
        )
        router.routes.append(route)
        return

    router.add_api_route(
        reg.path,
        reg.endpoint,
        route_class_override=route_class,
        **add_kwargs,
    )

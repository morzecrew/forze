from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, Callable

from fastapi import APIRouter

from forze.application.composition.search import SearchDTOs
from forze.application.execution import ExecutionContext, UsecaseRegistry

from ..http import attach_http_endpoint
from .endpoints import (
    build_raw_search_endpoint_spec,
    build_typed_search_endpoint_spec,
)
from .specs import SearchEndpointsSpec

# ----------------------- #


def attach_search_endpoints(
    router: APIRouter,
    *,
    dtos: SearchDTOs[Any],
    registry: UsecaseRegistry,
    ctx_dep: Callable[[], ExecutionContext],
    endpoints: SearchEndpointsSpec | None = None,
) -> APIRouter:
    endpoints = endpoints or {}

    search_endpoint = endpoints.get("search", {})
    raw_search_endpoint = endpoints.get("raw_search", {})

    if not search_endpoint.get("disable", False):
        search_endpoint_spec = build_typed_search_endpoint_spec(
            dtos=dtos,
            path_override=search_endpoint.get("path_override", None),
            metadata=search_endpoint.get("metadata", None),
        )
        attach_http_endpoint(
            router=router,
            spec=search_endpoint_spec,
            registry=registry,
            ctx_dep=ctx_dep,
        )

    if not raw_search_endpoint.get("disable", False):
        raw_search_endpoint_spec = build_raw_search_endpoint_spec(
            dtos=dtos,
            path_override=raw_search_endpoint.get("path_override", None),
            metadata=raw_search_endpoint.get("metadata", None),
        )
        attach_http_endpoint(
            router=router,
            spec=raw_search_endpoint_spec,
            registry=registry,
            ctx_dep=ctx_dep,
        )

    return router

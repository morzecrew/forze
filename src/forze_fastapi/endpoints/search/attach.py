from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, Callable

from fastapi import APIRouter

from forze.application.composition.search import SearchDTOs
from forze.application.execution import ExecutionContext, UsecaseRegistry

from ..http import SimpleHttpEndpointSpec, attach_http_endpoint
from .endpoints import (
    build_raw_search_cursor_endpoint_spec,
    build_raw_search_endpoint_spec,
    build_typed_search_cursor_endpoint_spec,
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
    exclude_none: bool = True,
) -> APIRouter:
    endpoints = endpoints or {}

    search_endpoint = endpoints.get("search", False)
    raw_search_endpoint = endpoints.get("raw_search", False)
    search_cursor_endpoint = endpoints.get("search_cursor", False)
    raw_search_cursor_endpoint = endpoints.get("raw_search_cursor", False)

    if search_endpoint is not False:
        _search = (
            search_endpoint if search_endpoint is not True else SimpleHttpEndpointSpec()
        )

        search_endpoint_spec = build_typed_search_endpoint_spec(
            dtos=dtos,
            path_override=_search.get("path_override"),
            metadata=_search.get("metadata"),
        )
        attach_http_endpoint(
            router=router,
            spec=search_endpoint_spec,
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if raw_search_endpoint is not False:
        _raw_search = (
            raw_search_endpoint
            if raw_search_endpoint is not True
            else SimpleHttpEndpointSpec()
        )

        raw_search_endpoint_spec = build_raw_search_endpoint_spec(
            dtos=dtos,
            path_override=_raw_search.get("path_override"),
            metadata=_raw_search.get("metadata"),
        )
        attach_http_endpoint(
            router=router,
            spec=raw_search_endpoint_spec,
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if search_cursor_endpoint is not False:
        _search_c = (
            search_cursor_endpoint
            if search_cursor_endpoint is not True
            else SimpleHttpEndpointSpec()
        )

        search_cursor_endpoint_spec = build_typed_search_cursor_endpoint_spec(
            dtos=dtos,
            path_override=_search_c.get("path_override"),
            metadata=_search_c.get("metadata"),
        )
        attach_http_endpoint(
            router=router,
            spec=search_cursor_endpoint_spec,
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if raw_search_cursor_endpoint is not False:
        _raw_search_c = (
            raw_search_cursor_endpoint
            if raw_search_cursor_endpoint is not True
            else SimpleHttpEndpointSpec()
        )

        raw_search_cursor_endpoint_spec = build_raw_search_cursor_endpoint_spec(
            dtos=dtos,
            path_override=_raw_search_c.get("path_override"),
            metadata=_raw_search_c.get("metadata"),
        )
        attach_http_endpoint(
            router=router,
            spec=raw_search_cursor_endpoint_spec,
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    return router

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, Callable, Sequence

from fastapi import APIRouter

from forze.application.composition.search import SearchDTOs
from forze.application.execution import ExecutionContext, UsecaseRegistry

from ..http import HttpEndpointSpec, SimpleHttpEndpointSpec, attach_http_endpoint
from ..http.policy import (
    AnyFeature,
    apply_authn_requirement,
    with_default_http_features,
)
from .endpoints import (
    build_raw_search_cursor_endpoint_spec,
    build_raw_search_endpoint_spec,
    build_typed_search_cursor_endpoint_spec,
    build_typed_search_endpoint_spec,
)
from .specs import SearchEndpointsSpec

# ----------------------- #


HttpEndSpec = HttpEndpointSpec[Any, Any, Any, Any, Any, Any, Any, Any, Any]


def attach_search_endpoints(
    router: APIRouter,
    *,
    dtos: SearchDTOs[Any],
    registry: UsecaseRegistry,
    ctx_dep: Callable[[], ExecutionContext],
    endpoints: SearchEndpointsSpec | None = None,
    exclude_none: bool = True,
    default_http_features: Sequence[AnyFeature] | None = None,
) -> APIRouter:
    endpoints = endpoints or {}

    def _apply_defaults(
        spec: HttpEndSpec,
        simple: SimpleHttpEndpointSpec | None = None,
    ) -> HttpEndSpec:
        with_defaults = with_default_http_features(spec, default_http_features)
        if simple is None:
            return with_defaults
        return apply_authn_requirement(with_defaults, simple.get("authn"))

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
            spec=_apply_defaults(search_endpoint_spec, _search),
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
            spec=_apply_defaults(raw_search_endpoint_spec, _raw_search),
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
            spec=_apply_defaults(search_cursor_endpoint_spec, _search_c),
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
            spec=_apply_defaults(raw_search_cursor_endpoint_spec, _raw_search_c),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    return router

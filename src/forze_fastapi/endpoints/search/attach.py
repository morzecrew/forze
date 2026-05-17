from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, Callable, Sequence

from fastapi import APIRouter

from forze.application.composition.search import SearchDTOs
from forze.application.contracts.search import SearchSpec
from forze.application.execution import (
    ExecutionContext,
    UsecaseRegistry,
    operation_namespace_for,
)

from ..http import (
    AuthnRequirement,
    HttpEndpointSpec,
    SimpleHttpEndpointSpec,
    attach_http_endpoint,
)
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
    search: SearchSpec[Any],
    dtos: SearchDTOs[Any],
    registry: UsecaseRegistry,
    ctx_dep: Callable[[], ExecutionContext],
    endpoints: SearchEndpointsSpec | None = None,
    exclude_none: bool = True,
    default_http_features: Sequence[AnyFeature] | None = None,
) -> APIRouter:
    endpoints = endpoints or {}
    base_authn: AuthnRequirement | None = endpoints.get("authn")
    _search_namespace = registry.namespace or operation_namespace_for(search)
    _search_facade_init: dict[str, Any] = {"namespace": _search_namespace}

    def _resolve_authn(
        simple: SimpleHttpEndpointSpec | None,
    ) -> AuthnRequirement | None:
        if simple is not None:
            per_endpoint = simple.get("authn")
            if per_endpoint is not None:
                return per_endpoint
        return base_authn

    def _apply_defaults(
        spec: HttpEndSpec,
        simple: SimpleHttpEndpointSpec | None = None,
    ) -> HttpEndSpec:
        with_defaults = with_default_http_features(spec, default_http_features)
        return apply_authn_requirement(with_defaults, _resolve_authn(simple))

    search_endpoint = endpoints.get("search", False)
    raw_search_endpoint = endpoints.get("raw_search", False)
    search_cursor_endpoint = endpoints.get("search_cursor", False)
    raw_search_cursor_endpoint = endpoints.get("raw_search_cursor", False)

    if search_endpoint is not False:
        _search = (
            search_endpoint if search_endpoint is not True else SimpleHttpEndpointSpec()
        )

        search_endpoint_spec = build_typed_search_endpoint_spec(
            namespace=_search_namespace,
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
            facade_init_kwargs=_search_facade_init,
        )

    if raw_search_endpoint is not False:
        _raw_search = (
            raw_search_endpoint
            if raw_search_endpoint is not True
            else SimpleHttpEndpointSpec()
        )

        raw_search_endpoint_spec = build_raw_search_endpoint_spec(
            namespace=_search_namespace,
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
            facade_init_kwargs=_search_facade_init,
        )

    if search_cursor_endpoint is not False:
        _search_c = (
            search_cursor_endpoint
            if search_cursor_endpoint is not True
            else SimpleHttpEndpointSpec()
        )

        search_cursor_endpoint_spec = build_typed_search_cursor_endpoint_spec(
            namespace=_search_namespace,
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
            facade_init_kwargs=_search_facade_init,
        )

    if raw_search_cursor_endpoint is not False:
        _raw_search_c = (
            raw_search_cursor_endpoint
            if raw_search_cursor_endpoint is not True
            else SimpleHttpEndpointSpec()
        )

        raw_search_cursor_endpoint_spec = build_raw_search_cursor_endpoint_spec(
            namespace=_search_namespace,
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
            facade_init_kwargs=_search_facade_init,
        )

    return router

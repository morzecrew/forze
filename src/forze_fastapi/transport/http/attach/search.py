"""Attach search routes to an :class:`fastapi.APIRouter`."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import logging
from collections.abc import Callable, Mapping, Sequence

from fastapi import APIRouter
from pydantic import BaseModel

from forze.application.composition._attach import normalize_enable
from forze.application.composition.search import SEARCH_OPERATIONS, SearchDTOs, SearchPreset
from forze.application.composition.search.facades import SearchFacade
from forze.application.contracts.search import SearchSpec
from forze.application.execution import ExecutionContext
from forze.base.primitives import StrKeyNamespace
from forze_fastapi.transport.http.attach._common import build_route_policies, route_opts_from_entry
from forze_fastapi.transport.http.attach._loop import (
    iter_catalog_operations,
    resolve_include_in_schema,
    resolve_route_path,
)
from forze_fastapi.transport.http.auth import AuthnRequirement
from forze_fastapi.transport.http.bindings.search import make_search_endpoint, search_binding_for
from forze_fastapi.transport.http.options import RouteOpts
from forze_fastapi.transport.http.policies import Policy
from forze_fastapi.transport.http.register import RouteRegistration, register_route

logger = logging.getLogger(__name__)

# ----------------------- #


def attach_search_routes[M: BaseModel](
    router: APIRouter,
    *,
    search: SearchSpec[M],
    dtos: SearchDTOs[M],
    facade_dep: Callable[..., SearchFacade[M]],
    ctx_dep: Callable[[], ExecutionContext],
    enable: Sequence[str] | tuple[str, ...] | None = None,
    paths: Mapping[str, str] | None = None,
    policies: Sequence[Policy] = (),
    authn: AuthnRequirement | None = None,
    per_route: Mapping[str, RouteOpts] | None = None,
    strict: bool = True,
    namespace: StrKeyNamespace | None = None,
) -> APIRouter:
    enabled = normalize_enable(enable, default=SearchPreset.TYPED)
    per_route = per_route or {}
    paths = paths or {}
    search_namespace = namespace or search.default_namespace

    for name, catalog_entry in iter_catalog_operations(
        enabled,
        SEARCH_OPERATIONS,
        strict=strict,
        logger=logger,
        domain_label="search",
    ):
        http = search_binding_for(catalog_entry)
        route_opts = route_opts_from_entry(per_route.get(name))
        path = resolve_route_path(
            name,
            paths=paths,
            per_route=per_route,
            default_path=http.default_path,
        )

        route_policies = build_route_policies(
            base_policies=policies,
            authn=authn,
            ctx_dep=ctx_dep,
            route_opts=route_opts,
        )

        reg = RouteRegistration(
            method=http.method,
            path=path,
            operation_id=str(search_namespace.key(catalog_entry.kernel_op)),
            endpoint=make_search_endpoint(
                catalog_entry.body_type,
                catalog_entry.facade_attr,
                facade_dep,
            ),
            response_model=http.response_factory(dtos),
            policies=route_policies,
            include_in_schema=resolve_include_in_schema(route_opts),
        )
        register_route(router, reg)

    return router

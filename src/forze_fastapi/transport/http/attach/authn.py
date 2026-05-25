"""Attach authn routes to an :class:`fastapi.APIRouter`."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

from fastapi import APIRouter

from forze.application.composition._attach import normalize_enable
from forze.application.composition.authn import AUTHN_OPERATIONS, AuthnPreset
from forze.application.composition.authn.facades import AuthnFacade
from forze.application.contracts.authn import AuthnSpec
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze.base.primitives import StrKeyNamespace
from forze_fastapi.transport.http.attach._common import build_route_policies, route_opts_from_entry
from forze_fastapi.transport.http.attach._loop import (
    iter_catalog_operations,
    resolve_include_in_schema,
    resolve_route_path,
)
from forze_fastapi.transport.http.auth import AuthnRequirement
from forze_fastapi.transport.http.bindings.authn import AUTHN_HTTP_BINDINGS, build_authn_registration
from forze_fastapi.transport.http.options import AuthnConfigSpec, RouteOpts
from forze_fastapi.transport.http.policies import Policy
from forze_fastapi.transport.http.register import register_route
from forze_fastapi.transport.http.wire.authn import (
    TokenTransportSpec,
    default_access_transport,
    default_refresh_transport,
)

logger = logging.getLogger(__name__)

# ----------------------- #


def _derive_authn_requirement(
    spec: AuthnSpec,
    access_transport: TokenTransportSpec,
) -> AuthnRequirement:
    transport_dict: dict[str, Any] = dict(access_transport)

    if transport_dict.get("kind") == "cookie":
        cookie_name = transport_dict.get("cookie_name")
        if not cookie_name:
            raise CoreError("cookie_name is required for cookie access transport")
        return AuthnRequirement(authn_route=spec.name, token_cookie=str(cookie_name))

    header_name = transport_dict.get("header_name")
    if not header_name:
        raise CoreError("header_name is required for header access transport")
    return AuthnRequirement(authn_route=spec.name, token_header=str(header_name))


def attach_authn_routes(
    router: APIRouter,
    *,
    spec: AuthnSpec,
    facade_dep: Callable[..., AuthnFacade],
    ctx_dep: Callable[[], ExecutionContext],
    enable: Sequence[str] | tuple[str, ...] | None = None,
    paths: Mapping[str, str] | None = None,
    policies: Sequence[Policy] = (),
    per_route: Mapping[str, RouteOpts] | None = None,
    config: AuthnConfigSpec | None = None,
    strict: bool = True,
    namespace: StrKeyNamespace | None = None,
) -> APIRouter:
    enabled = normalize_enable(enable, default=AuthnPreset.ALL)
    per_route = per_route or {}
    paths = paths or {}
    config = config or {}
    authn_namespace = namespace or spec.default_namespace

    access_transport = cast(
        TokenTransportSpec,
        config.get("access_token_transport", default_access_transport()),
    )
    refresh_transport = cast(
        TokenTransportSpec,
        config.get("refresh_token_transport", default_refresh_transport()),
    )
    default_authn = _derive_authn_requirement(spec, access_transport)

    for name, catalog_entry in iter_catalog_operations(
        enabled,
        AUTHN_OPERATIONS,
        strict=strict,
        logger=logger,
        domain_label="authn",
    ):
        http = AUTHN_HTTP_BINDINGS[name]
        route_opts = route_opts_from_entry(per_route.get(name))
        path = resolve_route_path(
            name,
            paths=paths,
            per_route=per_route,
            default_path=http.default_path,
        )

        authn = route_opts.get("authn") if route_opts else None
        if authn is None and name in ("logout", "change_password"):
            authn = default_authn

        route_policies = build_route_policies(
            base_policies=policies,
            authn=authn,
            ctx_dep=ctx_dep,
            route_opts=route_opts,
        )

        reg = build_authn_registration(
            name,
            path=path,
            operation_id=str(authn_namespace.key(catalog_entry.kernel_op)),
            namespace=authn_namespace,
            facade_dep=facade_dep,
            access_transport=access_transport,
            refresh_transport=refresh_transport,
            policies=route_policies,
            include_in_schema=resolve_include_in_schema(route_opts),
        )
        if reg is None:
            if strict:
                raise CoreError(f"Unknown authn route '{name}'")
            continue

        register_route(router, reg)

    return router

"""Attach storage routes to an :class:`fastapi.APIRouter`."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import logging
from collections.abc import Callable, Mapping, Sequence

from fastapi import APIRouter

from forze.application.composition._attach import normalize_enable
from forze.application.composition.storage import STORAGE_OPERATIONS, StoragePreset
from forze.application.composition.storage.facades import StorageFacade
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze.base.primitives import StrKeyNamespace
from forze_fastapi.transport.http.attach._common import (
    build_route_policies,
    default_idempotency_ttl,
    route_opts_from_entry,
)
from forze_fastapi.transport.http.attach._loop import (
    iter_catalog_operations,
    resolve_include_in_schema,
    resolve_route_path,
)
from forze_fastapi.transport.http.auth import AuthnRequirement
from forze_fastapi.transport.http.bindings.storage import (
    STORAGE_HTTP_BINDINGS,
    build_storage_registration,
)
from forze_fastapi.transport.http.options import RouteOpts, StorageConfigSpec
from forze_fastapi.transport.http.policies import IdempotentPolicy, Policy
from forze_fastapi.transport.http.register import register_route

logger = logging.getLogger(__name__)

# ----------------------- #


def attach_storage_routes(
    router: APIRouter,
    *,
    storage: StorageSpec,
    facade_dep: Callable[..., StorageFacade],
    ctx_dep: Callable[[], ExecutionContext],
    enable: Sequence[str] | tuple[str, ...] | None = None,
    paths: Mapping[str, str] | None = None,
    policies: Sequence[Policy] = (),
    authn: AuthnRequirement | None = None,
    per_route: Mapping[str, RouteOpts] | None = None,
    config: StorageConfigSpec | None = None,
    strict: bool = True,
    namespace: StrKeyNamespace | None = None,
) -> APIRouter:
    enabled = normalize_enable(enable, default=StoragePreset.ALL)
    per_route = per_route or {}
    paths = paths or {}
    config = config or {}
    storage_namespace = namespace or storage.default_namespace

    idempotency_spec: IdempotencySpec | None = None
    if config.get("enable_idempotency", False):
        idempotency_spec = IdempotencySpec(
            name=str(storage.name),
            ttl=default_idempotency_ttl(config),
        )

    for name, _ in iter_catalog_operations(
        enabled,
        STORAGE_OPERATIONS,
        strict=strict,
        logger=logger,
        domain_label="storage",
    ):
        http = STORAGE_HTTP_BINDINGS[name]
        route_opts = route_opts_from_entry(per_route.get(name))
        path = resolve_route_path(
            name,
            paths=paths,
            per_route=per_route,
            default_path=http.default_path,
        )

        extra: list[Policy] = []
        if name == "upload" and idempotency_spec is not None:
            extra.append(IdempotentPolicy())

        route_policies = build_route_policies(
            base_policies=policies,
            authn=authn,
            ctx_dep=ctx_dep,
            route_opts=route_opts,
            extra=extra,
        )

        reg = build_storage_registration(
            name,
            path=path,
            namespace=storage_namespace,
            facade_dep=facade_dep,
            ctx_dep=ctx_dep,
            policies=route_policies,
            idempotency_spec=idempotency_spec if name == "upload" else None,
            include_in_schema=resolve_include_in_schema(route_opts),
        )
        if reg is None:
            if strict:
                raise CoreError(f"Unknown storage route '{name}'")
            continue

        register_route(router, reg)

    return router

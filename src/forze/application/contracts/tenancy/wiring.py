"""Shared tenancy wiring validation for integration deps modules."""

from typing import Any, Callable, Literal, Protocol, Sequence, TypeVar

import attrs

from forze.application._logger import logger
from forze.application.contracts.resolution import (
    NamedResourceSpec,
    RelationSpec,
    is_static_named_resource,
    is_static_relation,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, StrKeyMapping

# ----------------------- #

TenantIsolationMode = Literal["none", "row", "relation", "database"]
"""Derived isolation mode for docs and diagnostics (not configured directly)."""

# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class TenancyRouteSpec:
    """One registered integration route and its row-level tenant flag."""

    name: StrKey
    """Route name (document, search, or analytics spec key)."""

    tenant_aware: bool
    """Whether the route applies row-level tenant filtering."""

    kind: str
    """Resource kind for log messages (e.g. ``document``, ``search``)."""


# ....................... #


def derive_tenant_isolation_mode(
    *,
    client_is_routed: bool,
    routes: Sequence[TenancyRouteSpec],
    has_relation_resolvers: bool = False,
) -> TenantIsolationMode:
    """Return the effective isolation mode implied by client and route flags."""

    if client_is_routed:
        return "database"

    if any(r.tenant_aware for r in routes):
        return "row"

    if has_relation_resolvers:
        return "relation"

    return "none"


# ....................... #

ConfigT = TypeVar("ConfigT")

# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class IntegrationRouteWarning[ConfigT]:
    """Descriptor for batch tenant-aware route warnings in integration deps modules."""

    kind: str
    """Resource kind for log messages (e.g. ``document``, ``storage``)."""

    tenant_aware: Callable[[ConfigT], bool]
    """Return whether the route applies row-level tenant filtering."""

    relation_fields: Callable[
        [ConfigT],
        Sequence[tuple[str, RelationSpec | None]],
    ] = lambda _config: ()
    """Return relation fields to inspect for dynamic resolvers."""

    named_fields: Callable[
        [ConfigT],
        Sequence[tuple[str, NamedResourceSpec | None]],
    ] = lambda _config: ()
    """Return named resource fields to inspect for dynamic resolvers."""


# ....................... #


class _NamespacedRouteConfig(Protocol):
    """Structural config exposing a base namespace and a row-level tenant flag."""

    @property
    def tenant_aware(self) -> bool: ...

    @property
    def namespace(self) -> Any:
        # ``Any`` (not ``NamedResourceSpec``) so attrs ``converter=`` fields, which
        # type checkers model as an opaque descriptor, still satisfy the protocol.
        ...


# ....................... #


def namespace_route_warning[C: _NamespacedRouteConfig](
    config_type: type[C],
    *,
    kind: str,
) -> IntegrationRouteWarning[C]:
    """Build a route warning for a namespaced, tenant-aware integration config.

    Shared by namespace-based integrations (Redis, SQS, RabbitMQ); *config_type*
    only pins the generic config type for the returned descriptor.
    """

    _ = config_type

    return IntegrationRouteWarning[C](
        kind=kind,
        tenant_aware=lambda config: config.tenant_aware,
        named_fields=lambda config: [("namespace", config.namespace)],
    )


# ....................... #


def warn_integration_routes[ConfigT](
    *,
    integration: str,
    routes: StrKeyMapping[ConfigT] | None,
    warning: IntegrationRouteWarning[ConfigT],
    log_warning: Callable[..., None] | None = None,
) -> None:
    """Log tenant-aware dynamic resolver warnings for every route in a mapping."""

    if not routes:
        return

    for name, config in routes.items():
        warn_dynamic_relation_with_tenant_aware(
            integration=integration,
            route_name=str(name),
            kind=warning.kind,
            tenant_aware=warning.tenant_aware(config),
            relation_fields=warning.relation_fields(config),
            named_fields=warning.named_fields(config),
            log_warning=log_warning,
        )


# ....................... #


def warn_dynamic_relation_with_tenant_aware(
    *,
    integration: str,
    route_name: str,
    kind: str,
    tenant_aware: bool,
    relation_fields: Sequence[tuple[str, RelationSpec | None]] = (),
    named_fields: Sequence[tuple[str, NamedResourceSpec | None]] = (),
    log_warning: Callable[..., None] | None = None,
) -> None:
    """Log when a route combines row filters with per-tenant resource resolvers."""

    if not tenant_aware:
        return

    for field_name, rel_spec in relation_fields:
        if rel_spec is None or is_static_relation(rel_spec):
            continue

        _emit_dynamic_warn(
            integration=integration,
            kind=kind,
            route_name=route_name,
            field_name=field_name,
            spec_kind="RelationSpec",
            log_warning=log_warning,
        )

    for field_name, named_spec in named_fields:
        if named_spec is None or is_static_named_resource(named_spec):
            continue

        _emit_dynamic_warn(
            integration=integration,
            kind=kind,
            route_name=route_name,
            field_name=field_name,
            spec_kind="NamedResourceSpec",
            log_warning=log_warning,
        )


# ....................... #


def validate_routed_client_tenancy_wiring(
    *,
    integration: str,
    client_is_routed: bool,
    partition_key_set: bool,
    routes: Sequence[TenancyRouteSpec],
    partition_key_detail: str,
    validation_failed_code: str,
    log_warning: Callable[..., None] | None = None,
) -> None:
    """Fail or warn when a routed client and per-route ``tenant_aware`` disagree."""

    if client_is_routed and not partition_key_set:
        raise exc.configuration(
            f"{integration} tenancy validation failed: routed client requires a "
            f"cache partition key so metadata caches are partitioned by tenant. "
            f"{partition_key_detail}",
            code=validation_failed_code,
            details={"client_is_routed": True},
        )

    if not client_is_routed:
        return

    tenant_aware_routes = [r for r in routes if r.tenant_aware]

    if not tenant_aware_routes:
        return

    for route in tenant_aware_routes:
        message = (
            f"{integration} tenancy for {route.kind} route {route.name!r}: routed client "
            "already scopes connections per tenant; tenant_aware=True adds redundant "
            "row-level filtering (defense-in-depth is acceptable)."
        )

        if log_warning is not None:
            log_warning(message)

        else:
            logger.warning(message)


# ....................... #


def _emit_dynamic_warn(
    *,
    integration: str,
    kind: str,
    route_name: str,
    field_name: str,
    spec_kind: str,
    log_warning: Callable[..., None] | None,
) -> None:
    message = (
        f"{integration} {kind} route {route_name!r}: {field_name} uses a dynamic "
        f"{spec_kind} resolver with tenant_aware=True; prefer tenant_aware=False for "
        "relation-level isolation (row filter is usually redundant)."
    )

    if log_warning is not None:
        log_warning(message)

    else:
        logger.warning(message)

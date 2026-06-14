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

TenantIsolationMode = Literal["none", "row", "relation", "schema", "database"]
"""Derived isolation tier for docs, diagnostics, and the ``required_isolation`` floor.

The physical-strength ladder (weakest → strongest) is ``none < relation < row < schema <
database``. The three tenant-discriminator tiers map to a deployment's mechanism:

- ``row`` — shared resource, tenant discriminator embedded (column / key prefix / path
  prefix / graph property).
- ``schema`` — separate logical namespace per tenant on a shared instance/connection
  (DB schema, warehouse dataset/database, object-store bucket) resolved from the tenant.
- ``database`` — separate instance/credentials per tenant (a routed client). The only
  model safe for untrusted raw or self-scoping query paths.

Derived from the config an integration already carries (it is not configured directly).
"""

# ....................... #

_ISOLATION_RANK: dict[TenantIsolationMode, int] = {
    "none": 0,
    "relation": 1,
    "row": 2,
    "schema": 3,
    "database": 4,
}
"""Strength ordering for isolation modes (weakest → strongest).

``relation`` and ``row`` are both logical (shared-store) isolation; ``row`` outranks
``relation`` because every row physically carries the tenant tag, whereas relation-level
scoping depends on a join/grant being applied. ``schema`` (a per-tenant namespace on a
shared instance) is physically stronger than ``row`` but weaker than ``database`` (a
separate instance/credentials per tenant), which is the only model safe for untrusted raw
or self-scoping query paths.
"""


def isolation_satisfies(
    *,
    derived: TenantIsolationMode,
    required: TenantIsolationMode,
) -> bool:
    """Return whether *derived* isolation is at least as strong as *required*."""

    return _ISOLATION_RANK[derived] >= _ISOLATION_RANK[required]


def validate_required_isolation(
    *,
    integration: str,
    derived: TenantIsolationMode,
    required: TenantIsolationMode | None,
    code: str,
    max_supported: TenantIsolationMode | None = None,
) -> None:
    """Fail closed when the wired isolation is weaker than the declared requirement.

    A deployment declares the *minimum* isolation it accepts (``required``); this refuses
    to wire any combination whose ``derived`` mode is weaker. Pass ``required=None`` to opt
    out (no declared floor — the historical behavior).

    ``max_supported`` is the strongest tier the integration can ever provide (its
    capability ceiling — e.g. an in-process backend caps at ``row``, an object store at
    ``database``). When ``required`` exceeds it, the failure is reported as a capability
    mismatch (the floor is unreachable by configuration) rather than a wiring gap.
    """

    if required is None:
        return

    if max_supported is not None and not isolation_satisfies(
        derived=max_supported, required=required
    ):
        raise exc.configuration(
            f"{integration} supports at most {max_supported!r} tenant isolation, but the "
            f"deployment declares required_isolation={required!r}, which it cannot provide. "
            "Lower the declared requirement or use a backend that supports it.",
            code=code,
            details={
                "required_isolation": required,
                "max_supported_isolation": max_supported,
            },
        )

    if isolation_satisfies(derived=derived, required=required):
        return

    raise exc.configuration(
        f"{integration} tenancy validation failed: deployment declares "
        f"required_isolation={required!r} but the wired isolation is {derived!r}, which "
        "is weaker. Strengthen the wiring (mark routes tenant_aware, route a per-tenant "
        "namespace, or route the client per tenant) or lower the declared requirement.",
        code=code,
        details={"required_isolation": required, "derived_isolation": derived},
    )


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
    has_namespace_routing: bool = False,
) -> TenantIsolationMode:
    """Return the effective isolation tier implied by an integration's wiring.

    Strongest applicable tier wins: a per-tenant routed *client* → ``database``; a
    per-tenant *namespace* resolver (schema / dataset / bucket) → ``schema``; a row-level
    ``tenant_aware`` route → ``row``; a dynamic relation resolver → ``relation``; else
    ``none``.
    """

    if client_is_routed:
        return "database"

    if has_namespace_routing:
        return "schema"

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
    required_isolation: TenantIsolationMode | None = None,
    has_relation_resolvers: bool = False,
    has_namespace_routing: bool = False,
    max_supported_isolation: TenantIsolationMode | None = None,
    log_warning: Callable[..., None] | None = None,
) -> None:
    """Fail or warn when a routed client and per-route ``tenant_aware`` disagree.

    When ``required_isolation`` is set, also fail closed if the derived isolation tier
    (from ``client_is_routed`` / ``has_namespace_routing`` / ``routes`` /
    ``has_relation_resolvers``) is weaker than the declared floor, or exceeds
    ``max_supported_isolation`` — see :func:`validate_required_isolation`.
    """

    if client_is_routed and not partition_key_set:
        raise exc.configuration(
            f"{integration} tenancy validation failed: routed client requires a "
            f"cache partition key so metadata caches are partitioned by tenant. "
            f"{partition_key_detail}",
            code=validation_failed_code,
            details={"client_is_routed": True},
        )

    validate_required_isolation(
        integration=integration,
        derived=derive_tenant_isolation_mode(
            client_is_routed=client_is_routed,
            routes=routes,
            has_relation_resolvers=has_relation_resolvers,
            has_namespace_routing=has_namespace_routing,
        ),
        required=required_isolation,
        code=validation_failed_code,
        max_supported=max_supported_isolation,
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

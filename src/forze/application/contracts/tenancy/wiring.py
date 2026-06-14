"""Shared tenancy wiring validation for integration deps modules."""

from typing import Any, Callable, Literal, Protocol, Sequence

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

TenantIsolationMode = Literal["none", "tagged", "namespace", "dedicated"]
"""Derived isolation tier for docs, diagnostics, and the ``required_isolation`` floor.

The physical-strength ladder (weakest → strongest) is ``none < tagged < namespace <
dedicated``. The names are storage-agnostic (the model spans SQL, document, object, queue,
cache and graph backends); each tier maps to a deployment's mechanism:

- ``tagged`` — shared resource, tenant marker embedded that operations must filter on
  (column / key prefix / path prefix / graph property). Per-tenant *table partitioning* is
  this tier too — a forgotten predicate still scans every partition, so the guarantee is
  the same as a plain discriminator.
- ``namespace`` — a separate per-tenant container on a shared instance/connection (DB
  schema, warehouse dataset/database, object-store bucket, per-tenant collection) resolved
  from the tenant via a dynamic namespace/relation resolver. A name-resolution boundary, so
  a forgotten predicate cannot cross tenants.
- ``dedicated`` — a separate instance/credentials per tenant (a routed client). The only
  model safe for untrusted raw or self-scoping query paths.

Derived from the config an integration already carries (it is not configured directly).
"""

# ....................... #

_ISOLATION_RANK: dict[TenantIsolationMode, int] = {
    "none": 0,
    "tagged": 1,
    "namespace": 2,
    "dedicated": 3,
}
"""Strength ordering for isolation modes (weakest → strongest).

``tagged`` is shared-store isolation (every item carries an embedded tenant marker that
operations filter on). ``namespace`` (a separate per-tenant container on a shared instance,
resolved from the tenant) is physically stronger — a name-resolution boundary rather than a
filter — and ``dedicated`` (a separate instance/credentials per tenant) is the strongest,
the only model safe for untrusted raw or self-scoping query paths.
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
    capability ceiling — e.g. an in-process backend caps at ``tagged``, an object store at
    ``dedicated``). When ``required`` exceeds it, the failure is reported as a capability
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
    """One registered integration route and its tagged-tier tenant flag."""

    name: StrKey
    """Route name (document, search, or analytics spec key)."""

    tenant_aware: bool
    """Whether the route applies tagged-tier (tenant-marker) filtering."""

    kind: str
    """Resource kind for log messages (e.g. ``document``, ``search``)."""


# ....................... #


def derive_tenant_isolation_mode(
    *,
    client_is_routed: bool,
    routes: Sequence[TenancyRouteSpec],
    has_namespace_routing: bool = False,
) -> TenantIsolationMode:
    """Return the effective isolation tier implied by an integration's wiring.

    Strongest applicable tier wins: a per-tenant routed *client* → ``dedicated``; a dynamic
    per-tenant *namespace* resolver (schema / dataset / bucket / collection) → ``namespace``;
    a ``tenant_aware`` route (embedded tenant marker) → ``tagged``; else ``none``.
    """

    if client_is_routed:
        return "dedicated"

    if has_namespace_routing:
        return "namespace"

    if any(r.tenant_aware for r in routes):
        return "tagged"

    return "none"


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class IntegrationRouteWarning[ConfigT]:
    """Descriptor for batch tenant-aware route warnings in integration deps modules."""

    kind: str
    """Resource kind for log messages (e.g. ``document``, ``storage``)."""

    tenant_aware: Callable[[ConfigT], bool]
    """Return whether the route applies tagged-tier (tenant-marker) filtering."""

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
    """Structural config exposing a base namespace and a tagged-tier tenant flag."""

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
    has_namespace_routing: bool = False,
    max_supported_isolation: TenantIsolationMode | None = None,
    log_warning: Callable[..., None] | None = None,
) -> None:
    """Fail or warn when a routed client and per-route ``tenant_aware`` disagree.

    When ``required_isolation`` is set, also fail closed if the derived isolation tier
    (from ``client_is_routed`` / ``has_namespace_routing`` / ``routes``) is weaker than the
    declared floor, or exceeds ``max_supported_isolation`` — see
    :func:`validate_required_isolation`.
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
            "tenant-marker filtering (defense-in-depth is acceptable)."
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


# ----------------------- #
# Consolidated per-module tenancy validation


@attrs.define(slots=True, frozen=True, kw_only=True)
class TenancyRouteGroup[ConfigT]:
    """One group of same-kind routes plus how to read tenancy from each config.

    Lets a deps module declare its routes once (the config mapping + accessors) and hand
    them to :func:`validate_module_tenancy`, instead of hand-building ``TenancyRouteSpec``
    lists and recomputing the ``namespace``-tier (namespace-routing) signal in every module.
    """

    kind: str
    """Resource kind for diagnostics (e.g. ``document``, ``analytics``, ``storage``)."""

    configs: StrKeyMapping[ConfigT] | None
    """Route-name → config mapping for this group (``None`` / empty = no routes)."""

    tenant_aware: Callable[[ConfigT], bool]
    """Return whether a route applies tagged-tier (tenant_aware) filtering."""

    namespace_resolver: Callable[[ConfigT], NamedResourceSpec | RelationSpec | None] = (
        lambda _config: None
    )
    """Return the route's per-tenant namespace spec — a ``NamedResourceSpec`` (bucket /
    index / dataset) or a ``RelationSpec`` (schema/collection pair), or ``None``. A
    *dynamic* (callable) spec marks the ``namespace`` isolation tier."""


# ....................... #


def validate_module_tenancy(
    *,
    integration: str,
    client_is_routed: bool,
    groups: Sequence[TenancyRouteGroup[Any]],
    required_isolation: TenantIsolationMode | None,
    max_supported_isolation: TenantIsolationMode,
    validation_failed_code: str,
    partition_key_set: bool = True,
    partition_key_detail: str = "",
    log_warning: Callable[..., None] | None = None,
) -> None:
    """Derive an integration's isolation tier from its route groups and enforce the floor.

    The single entry point every deps module uses: it flattens the groups into routes,
    detects ``namespace``-tier namespace routing (any *dynamic* per-tenant namespace/relation
    resolver — a callable spec, whether a ``NamedResourceSpec`` bucket/index or a
    ``RelationSpec`` collection), and delegates to
    :func:`validate_routed_client_tenancy_wiring`.

    ``max_supported_isolation`` is the strongest tier this integration's mechanisms can ever
    reach (``dedicated`` for a backend with a routed per-tenant client; ``tagged`` for an
    in-process or single-client one). The integration declares its own ceiling here — it is
    the sole authority on its capability — so a declared ``required_isolation`` it can never
    meet fails closed as a capability mismatch rather than a wiring gap.
    """

    routes: list[TenancyRouteSpec] = []
    has_namespace_routing = False

    for group in groups:
        for name, config in (group.configs or {}).items():
            routes.append(
                TenancyRouteSpec(
                    name=str(name),
                    tenant_aware=group.tenant_aware(config),
                    kind=group.kind,
                )
            )

            # A *dynamic* (callable) namespace/relation resolver scopes the resource per
            # tenant → namespace tier. A static name (str) or static relation (tuple) does not.
            namespace = group.namespace_resolver(config)

            if callable(namespace):
                has_namespace_routing = True

    validate_routed_client_tenancy_wiring(
        integration=integration,
        client_is_routed=client_is_routed,
        partition_key_set=partition_key_set,
        routes=routes,
        partition_key_detail=partition_key_detail,
        validation_failed_code=validation_failed_code,
        required_isolation=required_isolation,
        has_namespace_routing=has_namespace_routing,
        max_supported_isolation=max_supported_isolation,
        log_warning=log_warning,
    )

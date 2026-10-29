"""Shared tenancy wiring validation for integration deps modules."""

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Final, Literal, Protocol, get_args

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

from ..tiers import TierLattice
from .routed_client_base import RoutedTenantClientBase

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

_ISOLATION_LATTICE: TierLattice[TenantIsolationMode] = TierLattice(
    field="isolation",
    validation_label="tenancy",
    wired_noun="isolation",
    ceiling_noun="tenant isolation",
    floor_remediation=(
        "Strengthen the wiring (mark routes tenant_aware, route a per-tenant namespace, or "
        "route the client per tenant) or lower the declared requirement."
    ),
    ranks={"none": 0, "tagged": 1, "namespace": 2, "dedicated": 3},
)
"""Strength ordering for isolation modes (weakest → strongest), with its floor check.

``tagged`` is shared-store isolation (every item carries an embedded tenant marker that
operations filter on). ``namespace`` (a separate per-tenant container on a shared instance,
resolved from the tenant) is physically stronger — a name-resolution boundary rather than a
filter — and ``dedicated`` (a separate instance/credentials per tenant) is the strongest,
the only model safe for untrusted raw or self-scoping query paths.
"""

# ....................... #


def isolation_satisfies(
    *,
    derived: TenantIsolationMode,
    required: TenantIsolationMode,
) -> bool:
    """Return whether *derived* isolation is at least as strong as *required*."""

    return _ISOLATION_LATTICE.satisfies(derived=derived, required=required)


# ----------------------- #
# Statement origin — the second axis of the floor

ORIGIN_ISOLATION_FLOOR_CODE = "statement_origin_isolation_floor"
"""Error code for an origin whose floor outruns the route's wired isolation.

One code for every integration, unlike the per-integration ``*_tenancy_validation_failed``
codes beside it: the origin floor is a framework-wide rule with one remediation, and an
operator grepping for it is asking "where am I running unbuilt text without a container",
which is not an integration-scoped question.
"""

# ....................... #

StatementOrigin = Literal["structured", "compiled", "raw"]
"""Who authored the statement text reaching a backend.

The isolation ladder above answers *how strong is the container*. This answers *what kind
of process produced the text that runs inside it*, and the two together decide whether a
route is safe — which is why the ladder's own docstring already carries a sentence about
query text (``dedicated`` is "the only model safe for untrusted raw or self-scoping query
paths"). That sentence recognises two kinds; this names the three that exist.

- ``structured`` — the framework builds the statement from typed spec elements (document,
  search, analytics named queries, procedures). The adapter places every predicate itself,
  so the text cannot address a container the adapter did not name. Safe at any tier, which
  is why every shipped plane is this and none of them says so.
- ``compiled`` — the text is generated per request by a trusted compiler, and the adapter
  can check a declared read set against it before executing. Stronger than ``raw`` because
  the claim is checkable; weaker than ``structured`` because the text is still not ours.
- ``raw`` — an engine-specific string the framework can neither rewrite nor verify, the
  shape of a whole-query hatch like ``GraphRawQueryPort``. Naming the shape is not the
  same as governing it: no shipped route declares an origin, so that hatch is still
  governed by ``allow_raw_query`` and the deployment's own declared floor.

Distinct from author *trust*, which asks how much we trust whoever produced the text and
is spelled ``provenance`` on the routes that carry it. The two compose: an untrusted
author emitting into a ``compiled`` surface is a different route from a trusted one, and
the effective floor is the strongest requirement either axis makes.
"""

# ....................... #

_ORIGIN_FLOORS: Final[Mapping[StatementOrigin, TenantIsolationMode]] = {
    "structured": "none",
    "compiled": "namespace",
    "raw": "dedicated",
}
"""The weakest isolation tier at which each origin is safe.

``compiled`` sits at ``namespace`` rather than ``tagged`` on purpose, and the reason
generalizes past this table: **verification raises confidence in a claim, it does not
create a boundary.** A read-set assertion is a check over generated text, so a compiler
bug or a construct the checker renders imprecisely produces a statement that passes the
check and reads what it should not. At ``tagged`` that outcome is a silent cross-tenant
read in a correctly-rendered report; at ``namespace`` the same defect either names a
relation that does not exist or stays inside the tenant's own container. The floor is
chosen by what happens *when the check is wrong*, not by how good the check is.

The consequence is architectural rather than incidental: ``compiled`` at ``namespace``
means per-tenant containers, which makes whatever names those containers a
security-relevant component rather than a readability one.
"""

# ....................... #


def _check_origin_floors(floors: Mapping[StatementOrigin, TenantIsolationMode]) -> None:
    """Refuse an origin ladder and a floor table that name different rungs.

    Not decoration: a ``dict`` literal missing a key is invisible to a type checker even
    when the key type is a ``Literal`` — verified against both mypy and pyright — so a
    fourth rung added without a floor would pass every gate in the repository and first
    surface as a ``KeyError`` at wiring time. That is a tenancy floor failing open, which
    is the one outcome this module exists to prevent. The rung set is derived from the
    literal so the two cannot drift.

    Both directions are checked. A *missing* floor fails open, which is the dangerous
    half; a *stale* floor for a rung the literal no longer has is harmless at runtime but
    is a table that documents a tier nothing can reach, and a reader who trusts it is
    reasoning about a ladder that does not exist.

    Raises rather than asserts: ``assert`` is stripped under ``-O``, which would remove
    the guard from exactly the deployments that run optimized.
    """

    rungs = frozenset(get_args(StatementOrigin))

    if rungs == floors.keys():  # pyright: ignore[reportUnnecessaryComparison]
        return

    missing = sorted(rungs - floors.keys())
    stale = sorted(floors.keys() - rungs)

    raise exc.internal(
        "StatementOrigin and the origin floor table disagree — "
        f"origins with no floor: {missing}; floors for no origin: {stale}. Every origin "
        "needs exactly one floor: a missing entry fails open as a KeyError at wiring time "
        "instead of refusing the route, and a stale one documents a rung nothing can "
        "declare.",
        code="origin_floors_incomplete",
        details={"missing": missing, "stale": stale},
    )


_check_origin_floors(_ORIGIN_FLOORS)

# ....................... #


def required_isolation_for_origin(origin: StatementOrigin) -> TenantIsolationMode:
    """Return the weakest isolation tier at which *origin* is safe.

    Refuses an unrecognised origin rather than letting the lookup fail. The import guard
    keeps the *table* honest, but the origin itself arrives from a wiring-supplied callable
    (``TenancyRouteGroup.origin``) that nothing checks at runtime, so a typo — ``"Compiled"``,
    a stray space — reaches here as a value no floor covers. A bare ``KeyError`` out of a
    tenancy validator names neither the route nor the fix.
    """

    floor = _ORIGIN_FLOORS.get(origin)

    if floor is None:
        raise exc.configuration(
            f"Unknown statement origin {origin!r}: expected one of "
            f"{sorted(_ORIGIN_FLOORS)}. An origin decides a route's tenant-isolation "
            "floor, so an unrecognised one cannot be defaulted.",
            code="statement_origin_unknown",
            details={"origin": repr(origin), "known": sorted(_ORIGIN_FLOORS)},
        )

    return floor


# ....................... #


def validate_origin_isolation(
    *,
    origin: StatementOrigin,
    derived: TenantIsolationMode,
    route: str,
    integration: str,
) -> None:
    """Fail closed when a route runs an origin its isolation tier cannot carry.

    The origin floor is **intrinsic**, not declared: it comes from the kind of text the
    route executes, so unlike ``required_isolation`` there is nothing to lower. A route
    that needs a weaker tier has to stop executing that kind of text.

    The comparison goes through :func:`isolation_satisfies` rather than re-reading the
    ranks, so this axis can never fork the ordering the declared floor uses.
    """

    required = required_isolation_for_origin(origin)

    if isolation_satisfies(derived=derived, required=required):
        return

    raise exc.configuration(
        f"{integration} {route!r} statement-origin validation failed: origin "
        f"{origin!r} requires at least {required!r} tenant isolation, but the route wires "
        f"{derived!r}, which is weaker. Text the framework did not build cannot be confined "
        "by a tenant marker the statement is free to omit — give the route a per-tenant "
        "container (schema / database / dataset / bucket) or a per-tenant routed client. "
        "This floor comes from the origin itself and cannot be lowered; a route that must "
        f"stay at {derived!r} has to stop executing {origin!r} statements.",
        code=ORIGIN_ISOLATION_FLOOR_CODE,
        details={
            "route": route,
            "origin": origin,
            "required_isolation": required,
            "derived_isolation": derived,
        },
    )


# ....................... #


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

    _ISOLATION_LATTICE.validate(
        integration=integration,
        derived=derived,
        required=required,
        code=code,
        max_supported=max_supported,
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

    has_namespace_routing: bool = False
    """Whether every namespace or relation *this* route names resolves per tenant (a dynamic
    resolver) — the ``namespace`` tier; see :func:`resolves_per_tenant`. Per-route so a
    declared floor is enforced route by route."""

    has_per_tenant_relation: bool = False
    """Whether *any* namespace or relation this route names resolves per tenant. Weaker than
    :attr:`has_namespace_routing`, and read only to refuse a :attr:`tenant_global` route,
    whose resolvers would run with no tenant bound."""

    origin: StatementOrigin = "structured"
    """What kind of process authored the statements this route executes.

    Defaults to ``structured`` because every shipped plane is, so nothing that wires today
    has to say so; only a route that generates or passes through text declares otherwise.
    See :data:`StatementOrigin`."""

    tenant_global: bool = False
    """Whether the route is the deployment's rather than a tenant's — a fleet, a lifecycle
    lock, an outbox wake — used where no tenant is bound. It stays out of the declared
    floor, and may not be scoped per tenant."""


# ....................... #


def _route_isolation_mode(
    route: TenancyRouteSpec,
    *,
    client_is_routed: bool,
) -> TenantIsolationMode:
    """The isolation tier one route actually reaches.

    A routed client scopes every connection per tenant, so it lifts every route to
    ``dedicated`` regardless of what the route itself declares; otherwise the strongest
    per-route mechanism wins.
    """

    if client_is_routed:
        return "dedicated"

    if route.has_namespace_routing:
        return "namespace"

    if route.tenant_aware:
        return "tagged"

    return "none"


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
    a ``tenant_aware`` route (embedded tenant marker) → ``tagged``; else ``none``. The
    namespace signal is read per route (``TenancyRouteSpec.has_namespace_routing``); the
    module-level ``has_namespace_routing`` arg is an optional override for callers that have
    not yet populated it per route.
    """

    if client_is_routed:
        return "dedicated"

    if has_namespace_routing or any(r.has_namespace_routing for r in routes):
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


def _refuse_tenant_scoped_global(
    route: TenancyRouteSpec,
    *,
    integration: str,
    client_is_routed: bool,
    code: str,
) -> None:
    if client_is_routed:
        reason = "its client is routed per tenant"

    elif route.tenant_aware:
        reason = "it is tenant-aware"

    elif route.has_namespace_routing or route.has_per_tenant_relation:
        reason = "it resolves its namespace per tenant"

    else:
        return

    raise exc.configuration(
        f"{integration} {route.kind} route {route.name!r} is tenant-global, but {reason}. "
        "A tenant-global route is used where no tenant is bound, so scoped per tenant it "
        "would refuse every call. Wire it with tenant_aware=False and a static namespace "
        "on a client that is not routed per tenant.",
        code=code,
        details={"route": str(route.name), "kind": route.kind},
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
    max_supported_isolation: TenantIsolationMode | None = None,
    log_warning: Callable[..., None] | None = None,
) -> None:
    """Fail or warn when a routed client and per-route ``tenant_aware`` disagree.

    Two floors are enforced here, and a route must clear both:

    * the **origin floor** — intrinsic to the kind of statement text a route executes, so it
      applies whether or not the deployment declared anything (see
      :func:`validate_origin_isolation`);
    * the **declared floor** — ``required_isolation``, enforced **per route**, because the
      weakest route is the module's real isolation and an unscoped sibling must not slip
      through under a stronger one. The capability ceiling
      (``max_supported_isolation``) is enforced with it — see
      :func:`validate_required_isolation`. A ``tenant_global`` route is left out of it: it
      holds no tenant's data and is used where no tenant is bound, so a module whose only
      routes are tenant-global meets any floor within its ceiling. Such a route is refused
      when it is tenant-aware, resolves its namespace per tenant, or sits on a routed
      client — each would refuse every call it exists for.

    Either way a route's tier comes from :func:`_route_isolation_mode`. The origin floor
    runs first: it is the one a deployment cannot resolve by revising its own declaration,
    so reporting it ahead of a declared floor points at the fix that exists.
    """

    if client_is_routed and not partition_key_set:
        raise exc.configuration(
            f"{integration} tenancy validation failed: routed client requires a "
            f"cache partition key so metadata caches are partitioned by tenant. "
            f"{partition_key_detail}",
            code=validation_failed_code,
            details={"client_is_routed": True},
        )

    for route in routes:
        validate_origin_isolation(
            origin=route.origin,
            derived=_route_isolation_mode(route, client_is_routed=client_is_routed),
            route=str(route.name),
            integration=f"{integration} {route.kind} route",
        )

        if route.tenant_global:
            _refuse_tenant_scoped_global(
                route,
                integration=integration,
                client_is_routed=client_is_routed,
                code=validation_failed_code,
            )

    floored = [route for route in routes if not route.tenant_global]

    if required_isolation is not None:
        if client_is_routed:
            # Routed client → every route (and a module with no routes) is dedicated; only
            # the capability ceiling can fail.
            validate_required_isolation(
                integration=integration,
                derived="dedicated",
                required=required_isolation,
                code=validation_failed_code,
                max_supported=max_supported_isolation,
            )

        elif not floored:
            # No routes → no tenant isolation at all. Only tenant-global routes → nothing
            # falls short of the floor; the capability ceiling still applies.
            validate_required_isolation(
                integration=integration,
                derived=required_isolation if routes else "none",
                required=required_isolation,
                code=validation_failed_code,
                max_supported=max_supported_isolation,
            )

        else:
            for route in floored:
                validate_required_isolation(
                    integration=f"{integration} {route.kind} route {route.name!r}",
                    derived=_route_isolation_mode(route, client_is_routed=False),
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


def resolves_per_tenant(
    namespace: NamedResourceSpec | RelationSpec | None,
    *others: NamedResourceSpec | RelationSpec | None,
) -> bool:
    """Return whether a route's storage resolves per tenant — the ``namespace`` tier.

    A static relation holds every tenant's data however the route's others are split, so
    the route reaches the tier only when every one is a dynamic (callable) resolver.

    :param namespace: The route's own namespace or relation. ``None`` is a static default,
        so it does not resolve per tenant.
    :param others: Every other namespace or relation the route reads or writes tenant data
        through. ``None`` is an unset optional one and is skipped.
    :returns: ``True`` when *namespace* and every set one of *others* are resolvers.
    """

    return callable(namespace) and all(callable(spec) for spec in others if spec is not None)


# ....................... #


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

    other_namespaces: Callable[
        [ConfigT],
        Sequence[NamedResourceSpec | RelationSpec | None],
    ] = lambda _config: ()
    """Return every other namespace or relation a route reads or writes tenant data through
    (``None`` = an unset optional one). The route reaches the ``namespace`` tier only when
    :attr:`namespace_resolver` and each of these resolve per tenant; see
    :func:`resolves_per_tenant`."""

    origin: Callable[[ConfigT], StatementOrigin] = lambda _config: "structured"
    """Return what kind of process authored the statements a route executes.

    Left alone by every plane whose statements the framework builds — which is all of them
    today. A group overrides it only when its routes generate or pass through text; see
    :data:`StatementOrigin`."""

    tenant_global: Callable[[ConfigT], bool] = lambda _config: False
    """Return whether a route is the deployment's, outside the declared floor; see
    :attr:`TenancyRouteSpec.tenant_global`."""


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
    detects ``namespace``-tier namespace routing (every namespace/relation a route names is
    a *dynamic* per-tenant resolver — a callable spec, whether a ``NamedResourceSpec``
    bucket/index or a ``RelationSpec`` collection; see :func:`resolves_per_tenant`), and
    delegates to :func:`validate_routed_client_tenancy_wiring`.

    ``max_supported_isolation`` is the strongest tier this integration's mechanisms can ever
    reach (``dedicated`` for a backend with a routed per-tenant client; ``tagged`` for an
    in-process or single-client one). The integration declares its own ceiling here — it is
    the sole authority on its capability — so a declared ``required_isolation`` it can never
    meet fails closed as a capability mismatch rather than a wiring gap.

    Each group also reports its routes' :data:`StatementOrigin`, whose floor is enforced
    here too and independently of ``required_isolation``. Groups that leave it alone —
    every shipped plane — declare ``structured``, whose floor is ``none``, so nothing that
    wires today changes.
    """

    routes: list[TenancyRouteSpec] = []

    for group in groups:
        for name, config in (group.configs or {}).items():
            namespace = group.namespace_resolver(config)
            others = group.other_namespaces(config)

            routes.append(
                TenancyRouteSpec(
                    name=str(name),
                    tenant_aware=group.tenant_aware(config),
                    kind=group.kind,
                    has_namespace_routing=resolves_per_tenant(namespace, *others),
                    has_per_tenant_relation=any(map(callable, (namespace, *others))),
                    origin=group.origin(config),
                    tenant_global=group.tenant_global(config),
                )
            )

    validate_routed_client_tenancy_wiring(
        integration=integration,
        client_is_routed=client_is_routed,
        partition_key_set=partition_key_set,
        routes=routes,
        partition_key_detail=partition_key_detail,
        validation_failed_code=validation_failed_code,
        required_isolation=required_isolation,
        max_supported_isolation=max_supported_isolation,
        log_warning=log_warning,
    )


# ----------------------- #
# Ports used where no tenant is bound

TENANT_GLOBAL_REQUIRED_CODE: Final[str] = "tenant_global_required"
"""Error code for a tenant-scoped port used where no tenant is bound."""

_GATEWAYS: Final[tuple[str, ...]] = ("read_gw", "write_gw")
"""Where a document adapter keeps the parts that reach its backend."""


def _tenant_scope(part: object) -> str | None:
    if getattr(part, "tenant_aware", False) is True:
        return "is tenant-aware"

    client = part if isinstance(part, RoutedTenantClientBase) else getattr(part, "client", None)

    # A routed client asks its own provider, which a deployment may point at a default
    # tenant; only one with no tenant to route by fails here.
    if isinstance(client, RoutedTenantClientBase) and client.tenant_provider() is None:
        return "reaches its backend through a client routed per tenant, with no tenant to route by"

    return None


def require_tenant_global(port: object, *, what: str) -> None:
    """Refuse a port that cannot serve a call made with no tenant bound.

    Lifecycle steps and background loops run outside any request, so no tenant is bound
    there. A port that is tenant-aware, or that reaches its backend through a client routed
    per tenant whose provider has no tenant here, refuses each such call with
    ``tenant_required``, an authentication error that no retry clears. This refuses it once,
    where the step starts, as the wiring error it is.

    Reads ``tenant_aware`` and ``client`` on *port*, and on a document adapter's gateways. A
    per-tenant namespace resolver is not refused: it is called with ``None``, and what that
    means is the resolver's.

    :param port: A resolved port, or a client.
    :param what: What uses the port, for the error.
    :raises CoreException: ``configuration`` (``tenant_global_required``).
    """

    for part in (port, *(getattr(port, name, None) for name in _GATEWAYS)):
        if part is None or (reason := _tenant_scope(part)) is None:
            continue

        raise exc.configuration(
            f"{what} runs with no tenant bound, but its port {reason}, so every call it "
            "makes would be refused. Wire the route tenant-global: tenant_aware=False on a "
            "client that is not routed per tenant. A step that takes tenants= can bind them "
            "instead.",
            code=TENANT_GLOBAL_REQUIRED_CODE,
            details={"what": what, "reason": reason},
        )


# ....................... #

TENANT_AWARE_REQUIRED_CODE: Final[str] = "tenant_aware_required"
"""Error code for a tenant-global port used by one loop per bound tenant."""


def require_tenant_aware(port: object, *, what: str) -> None:
    """Refuse a port that does not keep each bound tenant's data apart.

    A consumer that runs one loop per tenant, each bound to its tenant, treats what a loop
    reads as that tenant's. That holds only when the port reads a separate key or partition
    per bound tenant. Over a tenant-global port every loop reads the same data, so each
    would deliver the others' as its own. A client routed per tenant is not enough: a
    deployment may route several tenants to one client, where the key is still shared.

    :param port: A resolved port.
    :param what: What uses the port, for the error.
    :raises CoreException: ``configuration`` (``tenant_aware_required``).
    """

    if getattr(port, "tenant_aware", False) is True:
        return

    raise exc.configuration(
        f"{what} runs one loop per tenant and treats what each loop reads as that "
        "tenant's, but its port is not tenant-aware, so every loop would read the same "
        "data. Wire the route tenant_aware=True.",
        code=TENANT_AWARE_REQUIRED_CODE,
        details={"what": what},
    )

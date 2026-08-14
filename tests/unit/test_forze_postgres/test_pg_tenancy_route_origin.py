"""Postgres routes must be able to carry a statement origin to the floor.

Postgres is the one integration that does **not** go through ``validate_module_tenancy``.
It keeps its own ``PostgresTenancyRouteSpec`` and converts to the contract type in
``to_contract()``, so every field the contract grew has to be forwarded by hand — and a
field that is not forwarded does not fail, it silently takes its default.

For an isolation floor that default is the weakest one, which makes a dropped field worse
than a missing feature: the mechanism looks wired, the docs say the floor is enforced, and
the check quietly compares ``structured`` against every route. This pins both the
forwarding and the converter itself, because the next field added to the contract will
land in exactly the same trap.
"""

from __future__ import annotations

import attrs
import pytest

from forze.application.contracts.tenancy import ORIGIN_ISOLATION_FLOOR_CODE
from forze.application.contracts.tenancy.wiring import TenancyRouteSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres.kernel.catalog.validation.validate_tenancy import (
    PostgresTenancyRouteSpec,
    validate_postgres_tenancy_wiring,
)

# ----------------------- #


def _route(
    *,
    origin: str = "structured",
    tenant_aware: bool = False,
    has_namespace_routing: bool = False,
) -> PostgresTenancyRouteSpec:
    return PostgresTenancyRouteSpec(
        name="widgets",
        kind="analytics",
        tenant_aware=tenant_aware,
        has_namespace_routing=has_namespace_routing,
        origin=origin,  # type: ignore[arg-type]
    )


def _wire(
    route: PostgresTenancyRouteSpec,
    *,
    client_is_routed: bool = False,
) -> None:
    validate_postgres_tenancy_wiring(
        client_is_routed=client_is_routed,
        introspector_cache_partition_key_set=True,
        routes=[route],
    )


# ....................... #


def test_a_compiled_postgres_route_on_the_tagged_tier_is_refused() -> None:
    """The floor has to reach Postgres, which is where the compiled plane lands first.

    A tenant-marker column is the tier a catalog-authored statement is free to not filter
    on, so this is the combination the floor exists to refuse.
    """

    with pytest.raises(CoreException) as err:
        _wire(_route(origin="compiled", tenant_aware=True))

    assert err.value.kind is ExceptionKind.CONFIGURATION
    assert err.value.code == ORIGIN_ISOLATION_FLOOR_CODE
    assert err.value.details["origin"] == "compiled"
    assert err.value.details["required_isolation"] == "namespace"


def test_a_compiled_postgres_route_passes_on_a_per_tenant_schema() -> None:
    """A per-tenant schema is a name boundary, which is what ``compiled`` needs."""

    _wire(_route(origin="compiled", has_namespace_routing=True))
    _wire(_route(origin="compiled"), client_is_routed=True)


def test_postgres_routes_still_default_to_structured() -> None:
    """Every shipped Postgres route declares nothing and must keep wiring."""

    assert PostgresTenancyRouteSpec(
        name="widgets", kind="document", tenant_aware=True
    ).origin == "structured"

    _wire(_route(tenant_aware=True))


def test_the_converter_forwards_every_contract_field() -> None:
    """The ratchet, not the fix.

    ``to_contract()`` is a hand-written field-by-field copy, so the failure mode is not
    this one dropped field — it is that dropping a field is invisible. A contract field
    that the Postgres spec cannot express, or expresses and fails to forward, silently
    substitutes the contract's default; for anything feeding a floor that default is the
    permissive end.

    So this compares the converter's *output* against a spec whose every field is set to a
    non-default value: a field left behind shows up as a default in the result, whatever
    the field is and whenever it was added.
    """

    probe = PostgresTenancyRouteSpec(
        name="widgets",
        kind="analytics",
        tenant_aware=True,
        has_namespace_routing=True,
        origin="raw",
    )
    contract = probe.to_contract()

    contract_fields = {f.name for f in attrs.fields(TenancyRouteSpec)}
    postgres_fields = {f.name for f in attrs.fields(PostgresTenancyRouteSpec)}

    missing = contract_fields - postgres_fields
    assert not missing, (
        f"PostgresTenancyRouteSpec cannot express {sorted(missing)}, so those fields take "
        "the contract default for every Postgres route — silently, and permissively for "
        "anything that gates on them."
    )

    defaults = {
        f.name: f.default
        for f in attrs.fields(TenancyRouteSpec)
        if f.default is not attrs.NOTHING
    }
    dropped = [
        name
        for name, default in defaults.items()
        if getattr(contract, name) == default and getattr(probe, name) != default
    ]
    assert not dropped, f"to_contract() did not forward {dropped}"

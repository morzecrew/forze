"""The statement-origin axis: which text may run inside which container.

The isolation ladder answers how strong a route's container is. This axis answers what
kind of process authored the text that runs inside it, and the two together decide whether
a route wires at all. The battery pins three things, in descending order of how quietly
they could go wrong:

* the floor table itself, over the whole origin × tier grid — the semantics, not a sample;
* that a rung without a floor is an **import** failure, because a missing dict key is
  invisible to every static gate in the repository and would otherwise fail open;
* that the comparison still goes through the isolation lattice, so this axis cannot fork
  the ordering the declared floor uses.
"""

from __future__ import annotations

import importlib.util
import typing
from collections.abc import Iterator
from typing import Any

import pytest

from forze.application.contracts.tenancy import wiring
from forze.application.contracts.tenancy.wiring import (
    ORIGIN_ISOLATION_FLOOR_CODE,
    StatementOrigin,
    TenancyRouteGroup,
    TenancyRouteSpec,
    TenantIsolationMode,
    required_isolation_for_origin,
    validate_module_tenancy,
    validate_origin_isolation,
    validate_routed_client_tenancy_wiring,
)
from forze.base.exceptions import CoreException

# ----------------------- #

TIERS: tuple[TenantIsolationMode, ...] = ("none", "tagged", "namespace", "dedicated")

FLOORS: dict[StatementOrigin, TenantIsolationMode] = {
    "structured": "none",
    "compiled": "namespace",
    "raw": "dedicated",
}
"""The table restated independently of the module, so a silent edit to one shows up here."""


def _route(
    *,
    origin: StatementOrigin = "structured",
    tenant_aware: bool = False,
    has_namespace_routing: bool = False,
) -> TenancyRouteSpec:
    return TenancyRouteSpec(
        name="widgets",
        kind="dynamic_read",
        tenant_aware=tenant_aware,
        has_namespace_routing=has_namespace_routing,
        origin=origin,
    )


def _wire(
    route: TenancyRouteSpec,
    *,
    client_is_routed: bool = False,
    required_isolation: TenantIsolationMode | None = None,
) -> None:
    validate_routed_client_tenancy_wiring(
        integration="X",
        client_is_routed=client_is_routed,
        partition_key_set=True,
        routes=[route],
        partition_key_detail="",
        validation_failed_code="x_tenancy_failed",
        required_isolation=required_isolation,
        max_supported_isolation="dedicated",
    )


# ....................... #
# 1 — the floor table


@pytest.mark.parametrize("origin", sorted(FLOORS))
def test_every_origin_has_its_documented_floor(origin: StatementOrigin) -> None:
    assert required_isolation_for_origin(origin) == FLOORS[origin]


def test_the_literal_and_the_floor_table_cover_the_same_rungs() -> None:
    """Exactly three rungs, each with a floor — the shape the import guard defends."""

    assert frozenset(typing.get_args(StatementOrigin)) == FLOORS.keys()


@pytest.mark.parametrize("origin", sorted(FLOORS))
@pytest.mark.parametrize("derived", TIERS)
def test_the_whole_origin_by_tier_grid(
    origin: StatementOrigin,
    derived: TenantIsolationMode,
) -> None:
    """Twelve cells, so the semantics are pinned rather than sampled.

    A route passes exactly when its tier is at least its origin's floor. Sampling a few
    pairs would leave the interesting diagonal — ``compiled`` at ``tagged`` — as the only
    tested refusal, and a floor table that had drifted one rung in either direction would
    still satisfy it.
    """

    expected_ok = TIERS.index(derived) >= TIERS.index(FLOORS[origin])

    if expected_ok:
        validate_origin_isolation(
            origin=origin,
            derived=derived,
            route="widgets",
            integration="X dynamic_read route",
        )
        return

    with pytest.raises(CoreException) as err:
        validate_origin_isolation(
            origin=origin,
            derived=derived,
            route="widgets",
            integration="X dynamic_read route",
        )

    assert err.value.code == ORIGIN_ISOLATION_FLOOR_CODE
    assert err.value.details["origin"] == origin
    assert err.value.details["derived_isolation"] == derived
    assert err.value.details["required_isolation"] == FLOORS[origin]


# ....................... #
# 2, 3, 4 — the floor through the wiring validator


def test_a_compiled_route_on_the_tagged_tier_is_refused() -> None:
    """Guard 1 of the dynamic-read plane, expressed once for every plane that follows."""

    with pytest.raises(CoreException) as err:
        _wire(_route(origin="compiled", tenant_aware=True))

    message = str(err.value)

    # Actionable means: which route, which origin, and the tier it needs — a message that
    # forces the reader back to the config to find out what failed is a worse gate.
    assert "widgets" in message
    assert "compiled" in message
    assert "namespace" in message
    assert err.value.code == ORIGIN_ISOLATION_FLOOR_CODE


def test_a_compiled_route_passes_at_namespace_and_at_dedicated() -> None:
    _wire(_route(origin="compiled", has_namespace_routing=True))
    _wire(_route(origin="compiled"), client_is_routed=True)


def test_a_raw_route_needs_a_routed_client() -> None:
    """The shipped ``GraphRawQueryPort`` doctrine, now data rather than prose."""

    with pytest.raises(CoreException) as err:
        _wire(_route(origin="raw", has_namespace_routing=True))

    assert err.value.details["required_isolation"] == "dedicated"

    _wire(_route(origin="raw"), client_is_routed=True)


def test_a_routed_client_lifts_every_route_to_dedicated() -> None:
    """A routed client scopes the connection, so the route's own markers stop mattering.

    Worth its own case because the per-route tier is computed in one place now: if that
    helper ever forgot ``client_is_routed``, a ``raw`` route on a routed client would be
    refused and the only symptom would be a deployment that used to wire and no longer
    does.
    """

    for origin in sorted(FLOORS):
        _wire(_route(origin=origin), client_is_routed=True)


# ....................... #
# 5 — composition with the declared floor


def test_the_declared_floor_still_bites_above_the_origin_floor() -> None:
    """Two floors, and a route clears both — the stronger one is what refuses it."""

    with pytest.raises(CoreException) as err:
        _wire(
            _route(origin="compiled", has_namespace_routing=True),
            required_isolation="dedicated",
        )

    # The origin floor is satisfied here (namespace ≥ namespace), so this must be the
    # *declared* floor talking — and the message has to say so, or an operator lowers the
    # wrong knob.
    assert err.value.code == "x_tenancy_failed"
    assert err.value.details["required_isolation"] == "dedicated"


def test_the_origin_floor_bites_below_a_satisfied_declared_floor() -> None:
    """The mirror: a declared floor a route meets does not license its origin."""

    with pytest.raises(CoreException) as err:
        _wire(_route(origin="compiled", tenant_aware=True), required_isolation="tagged")

    assert err.value.code == ORIGIN_ISOLATION_FLOOR_CODE


def test_the_origin_floor_applies_with_no_declared_floor_at_all() -> None:
    """The reason this is not just a preset for ``required_isolation``.

    Most modules declare nothing. If the origin floor only ran alongside a declared one it
    would be absent from exactly the deployments least likely to have thought about it.
    """

    with pytest.raises(CoreException) as err:
        _wire(_route(origin="compiled", tenant_aware=True), required_isolation=None)

    assert err.value.code == ORIGIN_ISOLATION_FLOOR_CODE


def test_when_both_floors_fail_the_unfixable_one_is_reported() -> None:
    """A deployment can lower its own declaration; it cannot lower an origin's floor.

    So when both are unmet, reporting the declared floor would offer a remediation that
    resolves nothing — the route would fail again on the origin. Pinned because the order
    is a deliberate diagnostic choice, not an accident of statement order.
    """

    with pytest.raises(CoreException) as err:
        _wire(_route(origin="raw", tenant_aware=True), required_isolation="dedicated")

    assert err.value.code == ORIGIN_ISOLATION_FLOOR_CODE
    assert "cannot be lowered" in str(err.value)


# ....................... #
# 6 — structured stays invisible


@pytest.mark.parametrize("derived", TIERS)
def test_a_route_that_declares_no_origin_wires_exactly_as_before(
    derived: TenantIsolationMode,
) -> None:
    """The "no shipped wiring changes" claim, as a test rather than an intention."""

    _wire(
        TenancyRouteSpec(
            name="widgets",
            kind="document",
            tenant_aware=derived == "tagged",
            has_namespace_routing=derived == "namespace",
        ),
        client_is_routed=derived == "dedicated",
    )


def test_a_route_group_defaults_to_structured() -> None:
    """A deps module that never mentions origin gets the invisible default.

    The default lives on ``TenancyRouteGroup`` as well as ``TenancyRouteSpec``; a default
    on only one of them would leave every module going through ``validate_module_tenancy``
    — which is all of them — reading an unset attribute.
    """

    group: TenancyRouteGroup[dict[str, Any]] = TenancyRouteGroup(
        kind="document",
        configs={"widgets": {}},
        tenant_aware=lambda _config: True,
    )

    assert group.origin({}) == "structured"

    validate_module_tenancy(
        integration="X",
        client_is_routed=False,
        groups=[group],
        required_isolation=None,
        max_supported_isolation="dedicated",
        validation_failed_code="x_tenancy_failed",
    )


def test_a_group_carries_its_origin_through_to_the_floor() -> None:
    """The path a real deps module takes: group accessor → route spec → refusal.

    Tested end-to-end because the accessor is the seam a plane declares through, and a
    ``TenancyRouteGroup.origin`` that was read nowhere would leave every direct test above
    passing while no module could actually opt in.
    """

    group: TenancyRouteGroup[dict[str, Any]] = TenancyRouteGroup(
        kind="dynamic_read",
        configs={"widgets": {}},
        tenant_aware=lambda _config: True,
        origin=lambda _config: "compiled",
    )

    with pytest.raises(CoreException) as err:
        validate_module_tenancy(
            integration="X",
            client_is_routed=False,
            groups=[group],
            required_isolation=None,
            max_supported_isolation="dedicated",
            validation_failed_code="x_tenancy_failed",
        )

    assert err.value.code == ORIGIN_ISOLATION_FLOOR_CODE
    assert "widgets" in str(err.value)


# ....................... #
# 7 — one lattice, not two


def test_the_verdict_goes_through_the_isolation_lattice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A future refactor must not be able to fork the ordering.

    Re-deriving the comparison from the ranks would work today and drift the first time a
    tier is inserted — the two axes would disagree about what ``namespace`` outranks, and
    the disagreement would surface as a route that one floor allows and the other refuses.
    """

    calls: list[tuple[str, str]] = []
    real = wiring.isolation_satisfies

    def spy(*, derived: TenantIsolationMode, required: TenantIsolationMode) -> bool:
        calls.append((derived, required))
        return real(derived=derived, required=required)

    monkeypatch.setattr(wiring, "isolation_satisfies", spy)

    wiring.validate_origin_isolation(
        origin="compiled",
        derived="namespace",
        route="widgets",
        integration="X",
    )

    assert calls == [("namespace", "namespace")]


# ....................... #
# The import guard


@pytest.fixture
def reexecuted_wiring() -> Iterator[Any]:
    """Execute ``wiring.py`` from source into a fresh namespace.

    ``importlib.reload`` would rebind the real module's names — including the lattice other
    modules imported by value — leaving the rest of the session holding objects that are no
    longer the ones under test. A fresh module object runs the same source with the same
    relative imports (the dotted name gives it a package to resolve them against) and
    touches nothing.
    """

    spec = importlib.util.spec_from_file_location(
        "forze.application.contracts.tenancy._wiring_import_probe",
        wiring.__file__,
    )

    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        pytest.fail("could not build a spec for the wiring module")

    yield lambda: spec.loader.exec_module(  # type: ignore[union-attr]
        importlib.util.module_from_spec(spec)
    )


def test_the_probe_executes_the_real_module_cleanly(reexecuted_wiring: Any) -> None:
    """The control: unmodified, the source imports fine through this fixture.

    Without it, the failure test below could pass because the probe is broken rather than
    because the guard fired — the classic way an import-failure test becomes vacuous.
    """

    reexecuted_wiring()


def test_a_rung_without_a_floor_fails_at_import(
    monkeypatch: pytest.MonkeyPatch,
    reexecuted_wiring: Any,
) -> None:
    """A missing floor must never reach a wiring — and cannot be caught any later.

    A ``dict`` literal that omits a key of its ``Literal`` key type is not a type error, so
    a fourth rung added without a floor passes mypy, pyright and every gate in the repo. Its
    first symptom would be a ``KeyError`` inside a tenancy check — a floor that fails open.
    The rung is injected by widening what the module sees as the literal's members, which is
    the same thing adding a rung to the ``Literal`` does.
    """

    monkeypatch.setattr(
        typing,
        "get_args",
        lambda _tp: ("structured", "compiled", "raw", "ephemeral"),
    )

    with pytest.raises(CoreException) as err:
        reexecuted_wiring()

    assert err.value.code == "origin_floors_incomplete"
    assert "ephemeral" in str(err.value)

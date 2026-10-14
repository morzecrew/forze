"""The in-memory fleet against the shared battery — the oracle's leg."""

from __future__ import annotations

from datetime import timedelta

import attrs
import pytest

from forze.application.contracts.fleet import FleetPort, FleetSpec
from forze.base.exceptions import CoreException
from forze_mock import MockDepsModule
from forze_mock.adapters import MockFleetAdapter
from forze_mock.execution import MockRouteConfig
from forze_mock.state import MockState
from tests.support.execution_context import context_from_modules
from tests.support.fleet_conformance import FLEET_BATTERY, Check, FleetHarness

# ----------------------- #

pytestmark = pytest.mark.asyncio


@attrs.define
class _Clock:
    now: float = 1_000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def harness() -> FleetHarness:
    state = MockState()
    clock = _Clock()

    def fleet(spec: FleetSpec) -> FleetPort:
        return MockFleetAdapter(spec=spec, state=state, namespace=str(spec.name), clock=clock)

    async def wait(delta: timedelta) -> None:
        clock.now += delta.total_seconds()

    return FleetHarness(backend="mock", fleet=fleet, wait=wait)


@pytest.mark.conformance(plane="fleet", engine="mock")
@pytest.mark.parametrize("check", FLEET_BATTERY, ids=lambda check: check.__name__)
async def test_fleet_battery(check: Check, harness: FleetHarness) -> None:
    await check(harness)


async def test_the_module_wires_the_fleet_port() -> None:
    ctx = context_from_modules(MockDepsModule())
    spec = FleetSpec(name="gateways")

    view = await ctx.fleet(spec).beat("a", "serving", {})

    assert isinstance(ctx.fleet(spec), MockFleetAdapter)
    assert [m.id for m in (await ctx.fleet(spec).view()).members] == ["a"]
    assert view.members[0].since == view.at


@pytest.mark.parametrize(
    "route",
    [MockRouteConfig(tenant_aware=True), MockRouteConfig(namespace=lambda tenant_id: "x")],
)
async def test_a_tenant_scoped_fleet_route_is_refused(route: MockRouteConfig) -> None:
    ctx = context_from_modules(MockDepsModule(routes={"gateways": route}))

    with pytest.raises(CoreException) as caught:
        ctx.fleet(FleetSpec(name="gateways"))

    assert caught.value.code == "fleet.tenant_scoped_route"

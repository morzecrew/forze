"""Two-phase pricing recipe — prepare prices outside the tx, apply writes inside it."""

from __future__ import annotations

from examples.recipes.two_phase_pricing.app import place_priced_order
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_mock import MockDepsModule


async def test_place_priced_order_prices_then_writes() -> None:
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze()
    )
    async with runtime.scope():
        order = await place_priced_order(runtime.get_context())

    assert order.item == "widget"
    assert order.price == len("widget") * 100  # priced by the (mock) external service

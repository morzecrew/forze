"""Runs the order-fulfillment example end to end — proves the stack composes.

Aggregate `@event_emitter` → dispatch-on-persist (in a saga step's tx) → outbox → relay →
inbox dedup → downstream aggregate, all in-process via `forze_mock`.
"""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException, ExceptionKind

from examples.recipes.order_fulfillment.app import (
    INVENTORY_SPEC,
    ORDER_SPEC,
    SHIPMENT_SPEC,
    build_context,
    deliver,
    place_order,
    relay_once,
    run_checkout,
)

# ----------------------- #


class TestOrderFulfillmentExample:
    async def test_happy_path_confirms_relays_and_ships(self) -> None:
        ctx = build_context()

        order_id, inventory_id = await place_order(ctx)
        await run_checkout(ctx, order_id, inventory_id)
        messages = await relay_once(ctx)

        assert len(messages) == 1  # exactly one order.confirmed staged + claimed
        assert await deliver(ctx, messages[0]) is True

        order = await ctx.document.query(ORDER_SPEC).get(order_id)
        inventory = await ctx.document.query(INVENTORY_SPEC).get(inventory_id)
        shipments = await ctx.document.query(SHIPMENT_SPEC).find_many()

        assert order.status == "confirmed"
        assert inventory.reserved == 2
        assert len(shipments.hits) == 1
        assert shipments.hits[0].order_id == order_id

    async def test_redelivery_is_deduped_by_the_inbox(self) -> None:
        ctx = build_context()

        order_id, inventory_id = await place_order(ctx)
        await run_checkout(ctx, order_id, inventory_id)
        messages = await relay_once(ctx)
        assert len(messages) == 1

        first = await deliver(ctx, messages[0])
        second = await deliver(ctx, messages[0])  # same event id redelivered

        assert first is True
        assert second is False  # skipped as duplicate

        shipments = await ctx.document.query(SHIPMENT_SPEC).find_many()
        assert len(shipments.hits) == 1  # still exactly one shipment

    async def test_pivot_failure_compensates_and_emits_nothing(self) -> None:
        ctx = build_context()

        order_id, inventory_id = await place_order(ctx)

        with pytest.raises(CoreException) as ei:
            await run_checkout(ctx, order_id, inventory_id, simulate_failure=True)

        assert ei.value.kind is ExceptionKind.DOMAIN  # step_failed -> compensated

        # reserve was compensated; order never confirmed; nothing staged/relayed/shipped.
        inventory_after = await ctx.document.query(INVENTORY_SPEC).get(inventory_id)
        order_after = await ctx.document.query(ORDER_SPEC).get(order_id)
        shipments = await ctx.document.query(SHIPMENT_SPEC).find_many()

        assert inventory_after.reserved == 0
        assert order_after.status == "pending"
        assert await relay_once(ctx) == []
        assert len(shipments.hits) == 0

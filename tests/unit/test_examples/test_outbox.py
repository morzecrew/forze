"""Transactional-outbox recipe — staged event is relayed to the queue (mock, no Docker)."""

from __future__ import annotations

from forze.application.contracts.queue import QueueQueryDepKey
from forze.application.execution import DepsRegistry, ExecutionContext
from forze_mock import MockDepsModule

from examples.recipes.outbox.app import ORDER_EVENTS, ORDERS_QUEUE, place_order, relay


async def test_outbox_relays_to_queue() -> None:
    ctx = ExecutionContext(deps=DepsRegistry.from_modules(MockDepsModule()).freeze().resolve())

    await place_order(ctx, "order-1")
    published = await relay(ctx)
    assert published == 1

    queue = ctx.deps.resolve_configurable(ctx, QueueQueryDepKey, ORDERS_QUEUE, route=ORDERS_QUEUE.name)
    messages = await queue.receive("orders")
    assert len(messages) == 1
    assert messages[0].type == "order.placed"

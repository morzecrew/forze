"""Hexagon example — the README slice round-trips through the port (mock, no Docker)."""

from __future__ import annotations

from examples.hexagon.app import place_order, read_order
from forze import build_runtime
from forze_mock import MockDepsModule


async def test_place_and_read_order_through_the_port() -> None:
    runtime = build_runtime(MockDepsModule())
    async with runtime.scope():
        ctx = runtime.get_context()
        placed = await place_order(ctx, item="widget")
        order = await read_order(ctx, placed.id)

    assert order.id == placed.id
    assert order.item == "widget"

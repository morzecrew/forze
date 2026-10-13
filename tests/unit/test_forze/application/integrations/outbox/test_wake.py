"""A flush publishes the route's wake once its rows commit, and only then."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from forze.application.contracts.inventory import SpecPlane, SpecRegistry
from forze.application.contracts.outbox import (
    DEFAULT_OUTBOX_WAKE_TOPIC,
    OutboxSpec,
    OutboxWake,
)
from forze.application.execution import ExecutionContext
from forze.base.serialization import PydanticModelCodec
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters.pubsub import MockPubSubAdapter
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _Payload(BaseModel):
    note: str


WAKE = OutboxWake(route="outbox-wake")
SPEC = OutboxSpec(name="orders-events", codec=PydanticModelCodec(_Payload), wake=WAKE)


class _Boom(RuntimeError):
    pass


def _wakes(state: MockState) -> list[str]:
    return [
        message.payload.route
        for topics in state.pubsub_logs.values()
        for message in topics.get(DEFAULT_OUTBOX_WAKE_TOPIC, [])
    ]


def _rows(state: MockState) -> int:
    return len(state.outbox_rows.get("orders-events", []))


async def _stage_and_flush(ctx: ExecutionContext, spec: OutboxSpec[_Payload] = SPEC) -> int:
    outbox = ctx.outbox.command(spec)
    await outbox.stage("order.placed", _Payload(note="n"))
    return await outbox.flush()


# ....................... #


async def test_a_flush_wakes_the_relay_once_its_transaction_commits() -> None:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state))
    seen: list[tuple[int, int]] = []
    publish = MockPubSubAdapter.publish

    async def _observed(self: MockPubSubAdapter[Any], topic: str, payload: Any, **kw: Any) -> None:
        # What a relay hearing this wake could see: the transaction depth and the rows.
        seen.append((ctx.tx_ctx.depth(), _rows(state)))
        await publish(self, topic, payload, **kw)

    with patch.object(MockPubSubAdapter, "publish", _observed):
        async with ctx.tx_ctx.scope("mock"):
            assert await _stage_and_flush(ctx) == 1
            assert _wakes(state) == []

    assert _wakes(state) == ["orders-events"]
    assert seen == [(0, 1)]


async def test_a_rolled_back_flush_publishes_no_wake() -> None:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state))

    with pytest.raises(_Boom):
        async with ctx.tx_ctx.scope("mock"):
            await _stage_and_flush(ctx)
            raise _Boom

    assert _wakes(state) == []
    assert _rows(state) == 0


async def test_a_flush_outside_a_transaction_wakes_at_once() -> None:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state))

    await _stage_and_flush(ctx)

    assert _wakes(state) == ["orders-events"]


async def test_a_flush_that_wrote_nothing_publishes_no_wake() -> None:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state))

    async with ctx.tx_ctx.scope("mock"):
        assert await ctx.outbox.command(SPEC).flush() == 0

    assert _wakes(state) == []


async def test_a_route_without_a_wake_publishes_none() -> None:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state))
    plain = OutboxSpec(name="orders-events", codec=PydanticModelCodec(_Payload))

    async with ctx.tx_ctx.scope("mock"):
        await _stage_and_flush(ctx, plain)

    assert _rows(state) == 1
    assert state.pubsub_logs == {}


async def test_a_failed_wake_neither_fails_nor_undoes_the_flush() -> None:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state))

    async def _down(*_: Any, **__: Any) -> None:
        raise ConnectionError("broker down")

    with patch.object(MockPubSubAdapter, "publish", _down):
        async with ctx.tx_ctx.scope("mock"):
            await _stage_and_flush(ctx)

        # Outside a transaction the wake runs at once, inside the flush call.
        await _stage_and_flush(context_from_modules(MockDepsModule(state=state)))

    assert _rows(state) == 2


# ....................... #


def test_cataloguing_a_route_with_a_wake_catalogues_its_pubsub_route() -> None:
    entries = SpecRegistry().register(SPEC).freeze().entries

    assert {(entry.plane, entry.name) for entry in entries} == {
        (SpecPlane.OUTBOX, "orders-events"),
        (SpecPlane.PUBSUB, "outbox-wake"),
    }

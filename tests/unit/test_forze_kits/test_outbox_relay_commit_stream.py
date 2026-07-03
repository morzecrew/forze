"""The outbox → relay → offset-log → inbox path, end-to-end on the mock.

Confirms the shipped outbox relay (``OutboxDestination.stream`` / ``to_stream``)
feeds the offset-log consumer unchanged: a staged event relays to the stream, the
commit consumer reads it with a populated position, processes it once, and the
``forze_event_id`` header the relay wrote drives exactly-once dedup on replay.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.stream import (
    OffsetReset,
    StreamMessage,
    StreamSpec,
)
from forze.base.serialization import PydanticModelCodec
from forze.testing import context_from_modules
from forze_kits.integrations.consumer import CommitStreamGroupConsumer
from forze_kits.integrations.outbox import OutboxRelay
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockCommitStreamGroupAdminAdapter, MockStreamAdapter


class _Event(BaseModel):
    n: int


_CODEC = PydanticModelCodec(_Event)
_CHANNEL = "audit"


@pytest.mark.asyncio
async def test_relayed_event_is_consumed_once_via_inbox() -> None:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state, strict_tx=True))

    outbox_spec = OutboxSpec(
        name="events",
        codec=_CODEC,
        destination=OutboxDestination.stream(route=_CHANNEL, channel=_CHANNEL),
    )
    stream_spec = StreamSpec(name=_CHANNEL, codec=_CODEC)

    # Stage + flush + relay the event onto the stream (shipped path, unchanged).
    await ctx.outbox.command(outbox_spec).stage("project.created", _Event(n=7))
    await ctx.outbox.command(outbox_spec).flush()
    relayed = await OutboxRelay(
        outbox_spec=outbox_spec, reclaim_stale_after=None
    ).to_stream(ctx, stream_spec)
    assert relayed.published == 1

    admin = MockCommitStreamGroupAdminAdapter(
        stream=MockStreamAdapter(state=state, namespace=_CHANNEL, codec=_CODEC),
        state=state,
    )
    await admin.ensure_group("g", [_CHANNEL], start=OffsetReset.EARLIEST)

    seen: list[int] = []

    async def handler(msg: StreamMessage[_Event]) -> None:
        seen.append(msg.payload.n)

    consumer = CommitStreamGroupConsumer(
        topics=[_CHANNEL],
        group="g",
        consumer="c",
        stream_spec=stream_spec,
        handler=handler,
        inbox_spec=InboxSpec(name="inbox"),
        tx_route="default",
    )

    first = await consumer.run(ctx, timeout=timedelta(milliseconds=100))
    assert (first.processed, seen) == (1, [7])

    # Replay the offsets: the forze_event_id header dedups the redelivery.
    await admin.reset_offsets("g", _CHANNEL, to=OffsetReset.EARLIEST)
    second = await consumer.run(ctx, timeout=timedelta(milliseconds=100))
    assert (second.processed, second.duplicates, seen) == (0, 1, [7])

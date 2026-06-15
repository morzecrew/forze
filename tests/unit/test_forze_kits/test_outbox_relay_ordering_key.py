"""Relay partitions on the staged ordering key; event id stays in headers.

The relay publishes ``key=claim.ordering_key or str(claim.event_id)`` — the
ordering key occupies the transport partition slot on capable backends while
``forze_event_id`` keeps riding the headers for consumer-side dedup.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.outbox import (
    OutboxDestination,
    OutboxSpec,
    OutboxStatus,
)
from forze.application.contracts.pubsub import PubSubSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.stream import StreamSpec
from forze.base.primitives import utcnow
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.inbox import process_with_inbox
from forze_kits.integrations.outbox import (
    OutboxRelay,
)
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.outbox_adapter import MockOutboxRow
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _EventPayload(BaseModel):
    n: int


def _codec() -> PydanticModelCodec[_EventPayload]:
    return PydanticModelCodec(_EventPayload)


def _row(
    route: str,
    *,
    event_type: str = "thing.happened",
    n: int = 1,
    ordering_key: str | None = None,
) -> MockOutboxRow:
    return MockOutboxRow(
        id=uuid4(),
        outbox_route=route,
        event_id=uuid4(),
        event_type=event_type,
        payload={"n": n},
        status=OutboxStatus.PENDING,
        tenant_id=None,
        execution_id=None,
        correlation_id=None,
        causation_id=None,
        occurred_at=utcnow(),
        created_at=utcnow(),
        ordering_key=ordering_key,
    )


# ----------------------- #


@pytest.mark.asyncio
async def test_queue_relay_publishes_ordering_key_with_event_id_header() -> None:
    codec = _codec()
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
    )
    queue_spec = QueueSpec(name="jobs", codec=codec)

    ctx = context_from_modules(MockDepsModule())
    state = ctx.deps.provide(MockStateDepKey)
    keyed = _row("events", ordering_key="order-42")
    state.outbox_rows["events"] = [keyed]

    result = await OutboxRelay(
        outbox_spec=outbox_spec, reclaim_stale_after=None
    ).to_queue(ctx, queue_spec)

    assert result.published == 1
    message = state.queues["jobs"]["jobs"][0].message
    # key carries the partition key, NOT the event id...
    assert message.key == "order-42"
    # ...while the event id stays available for dedup via the header.
    assert message.headers[HEADER_EVENT_ID] == str(keyed.event_id)


@pytest.mark.asyncio
async def test_queue_relay_key_falls_back_to_event_id_without_ordering_key() -> None:
    codec = _codec()
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
    )
    queue_spec = QueueSpec(name="jobs", codec=codec)

    ctx = context_from_modules(MockDepsModule())
    state = ctx.deps.provide(MockStateDepKey)
    unkeyed = _row("events", ordering_key=None)
    state.outbox_rows["events"] = [unkeyed]

    await OutboxRelay(
        outbox_spec=outbox_spec, reclaim_stale_after=None
    ).to_queue(ctx, queue_spec)

    message = state.queues["jobs"]["jobs"][0].message
    # Pre-ordering-key behavior is preserved exactly.
    assert message.key == str(unkeyed.event_id)
    assert message.headers[HEADER_EVENT_ID] == str(unkeyed.event_id)


@pytest.mark.asyncio
async def test_stream_and_pubsub_relay_publish_ordering_key() -> None:
    codec = _codec()
    ctx = context_from_modules(MockDepsModule())
    state = ctx.deps.provide(MockStateDepKey)

    stream_row = _row("audit-events", ordering_key="agg-s")
    pubsub_row = _row("fanout-events", ordering_key="agg-p")
    state.outbox_rows["audit-events"] = [stream_row]
    state.outbox_rows["fanout-events"] = [pubsub_row]

    await OutboxRelay(
        outbox_spec=OutboxSpec(
            name="audit-events",
            codec=codec,
            destination=OutboxDestination.stream(route="audit", channel="audit"),
        ),
        reclaim_stale_after=None,
    ).to_stream(ctx, StreamSpec(name="audit", codec=codec))
    await OutboxRelay(
        outbox_spec=OutboxSpec(
            name="fanout-events",
            codec=codec,
            destination=OutboxDestination.pubsub(route="fanout", channel="fanout"),
        ),
        reclaim_stale_after=None,
    ).to_pubsub(ctx, PubSubSpec(name="fanout", codec=codec))

    [stream_message] = state.streams["audit"]["audit"]
    assert stream_message.key == "agg-s"
    assert stream_message.headers[HEADER_EVENT_ID] == str(stream_row.event_id)

    [pubsub_message] = state.pubsub_logs["fanout"]["fanout"]
    assert pubsub_message.key == "agg-p"
    assert pubsub_message.headers[HEADER_EVENT_ID] == str(pubsub_row.event_id)


# ----------------------- #
# End-to-end showcase: stage -> relay -> consume in order, dedup by event id.


@pytest.mark.asyncio
async def test_same_ordering_key_events_relay_in_order_and_both_process() -> None:
    """Two events of one aggregate share the key, keep order, and never dedupe
    each other — while a redelivery of the same event is still skipped."""

    codec = _codec()
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
    )
    queue_spec = QueueSpec(name="jobs", codec=codec)
    inbox_spec = InboxSpec(name="jobs-inbox")

    ctx = context_from_modules(MockDepsModule())
    state = ctx.deps.provide(MockStateDepKey)

    outbox = ctx.outbox.command(outbox_spec)
    await outbox.stage("order.created", _EventPayload(n=1), ordering_key="order-1")
    await outbox.stage("order.shipped", _EventPayload(n=2), ordering_key="order-1")
    assert await outbox.flush() == 2

    result = await OutboxRelay(
        outbox_spec=outbox_spec, reclaim_stale_after=None
    ).to_queue(ctx, queue_spec)
    assert result.published == 2

    messages = [entry.message for entry in state.queues["jobs"]["jobs"]]
    # Same-key events arrive in staged (created_at) order on the happy path.
    assert [m.type for m in messages] == ["order.created", "order.shipped"]
    assert {m.key for m in messages} == {"order-1"}
    # Distinct events, distinct event-id headers.
    event_ids = [m.headers[HEADER_EVENT_ID] for m in messages]
    assert len(set(event_ids)) == 2

    processed: list[int] = []

    async def handler(message) -> None:  # noqa: ANN001
        processed.append(message.payload.n)

    for message in messages:
        assert (
            await process_with_inbox(
                ctx,
                message,
                inbox_spec=inbox_spec,
                handler=handler,
                tx_route="mock",
            )
            is True
        )

    # Both events processed despite the shared key (dedup is per event id).
    assert processed == [1, 2]

    # A broker redelivery of the first event (same event id header) is skipped.
    assert (
        await process_with_inbox(
            ctx,
            messages[0],
            inbox_spec=inbox_spec,
            handler=handler,
            tx_route="mock",
        )
        is False
    )
    assert processed == [1, 2]

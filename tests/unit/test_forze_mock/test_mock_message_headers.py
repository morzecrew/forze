"""Mock adapters: header round-trip and queue ``delivery_count`` surfacing."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pydantic import BaseModel

from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze.base.serialization import PydanticModelCodec
from forze_mock.adapters import (
    MockPubSubAdapter,
    MockQueueAdapter,
    MockState,
    MockStreamAdapter,
)

# ----------------------- #

_T0 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)


class _Msg(BaseModel):
    body: str


def _queue(st: MockState) -> MockQueueAdapter[_Msg]:
    return MockQueueAdapter(
        state=st,
        namespace="q",
        codec=PydanticModelCodec(model_type=_Msg),
        visibility_timeout=timedelta(seconds=30),
    )


# ----------------------- #


async def test_queue_headers_round_trip_verbatim() -> None:
    q = _queue(MockState())
    headers = {"forze_correlation_id": "abc", "trace": "t-1"}

    await q.enqueue("jobs", _Msg(body="x"), headers=headers)
    [message] = await q.receive("jobs", limit=1)

    assert dict(message.headers) == headers


async def test_queue_enqueue_many_headers_apply_to_batch() -> None:
    q = _queue(MockState())

    await q.enqueue_many(
        "jobs",
        [_Msg(body="a"), _Msg(body="b")],
        headers={"trace": "t-1"},
    )
    batch = await q.receive("jobs", limit=2)

    assert [dict(m.headers) for m in batch] == [{"trace": "t-1"}] * 2


async def test_queue_delivery_count_increments_across_redeliveries() -> None:
    st = MockState()
    q = _queue(st)
    clock = FrozenTimeSource(instant=_T0)

    with bind_time_source(clock):
        mid = await q.enqueue("jobs", _Msg(body="x"))

        [first] = await q.receive("jobs", limit=1)
        assert first.id == mid
        assert first.delivery_count == 1

        # Visibility lapses -> redelivered with the same id, count goes up.
        clock.instant = _T0 + timedelta(seconds=31)
        [second] = await q.receive("jobs", limit=1)
        assert second.id == mid
        assert second.delivery_count == 2

        # nack(requeue=True) -> third delivery.
        assert await q.nack("jobs", [mid], requeue=True) == 1
        [third] = await q.receive("jobs", limit=1)
        assert third.delivery_count == 3


# ....................... #


async def test_stream_headers_round_trip() -> None:
    st = MockState()
    stream: MockStreamAdapter[_Msg] = MockStreamAdapter(
        state=st,
        namespace="s",
        codec=PydanticModelCodec(model_type=_Msg),
    )

    await stream.append("audit", _Msg(body="x"), headers={"trace": "t-1"})
    [message] = await stream.read({"audit": "0"})

    assert dict(message.headers) == {"trace": "t-1"}


async def test_pubsub_headers_round_trip() -> None:
    st = MockState()
    pubsub: MockPubSubAdapter[_Msg] = MockPubSubAdapter(
        state=st,
        namespace="p",
        codec=PydanticModelCodec(model_type=_Msg),
    )

    await pubsub.publish("events", _Msg(body="x"), headers={"trace": "t-1"})

    log = st.pubsub_logs["p"]["events"]
    assert [dict(m.headers) for m in log] == [{"trace": "t-1"}]

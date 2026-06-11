"""Mock queue parity: consume idle-timeout, visibility-based redelivery, nack DLQ.

Redelivery deadlines are checked against the TimeSource-aware ``utcnow()``,
so tests freeze and advance time with ``bind_time_source`` + ``FrozenTimeSource``
(no real waiting). Consume tests use the real clock with small sleeps because
the idle window is tracked on the monotonic clock by design.
"""

import asyncio
from datetime import UTC, datetime, timedelta

from pydantic import BaseModel

from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze.base.serialization import PydanticModelCodec
from forze_mock.adapters import MockQueueAdapter, MockState

# ----------------------- #

_T0 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)


class _Msg(BaseModel):
    body: str


def _adapter(
    st: MockState,
    *,
    namespace: str = "q",
    visibility_timeout: timedelta = timedelta(seconds=30),
) -> MockQueueAdapter[_Msg]:
    return MockQueueAdapter(
        state=st,
        namespace=namespace,
        codec=PydanticModelCodec(model_type=_Msg),
        visibility_timeout=visibility_timeout,
    )


# ----------------------- #
# consume() idle-timeout contract


async def test_consume_none_timeout_survives_idle_gap_and_yields_late_message() -> None:
    q = _adapter(MockState())
    gen = q.consume("jobs", timeout=None)

    async def _late_producer() -> None:
        # Longer than several poll intervals: the consumer must keep waiting.
        await asyncio.sleep(0.1)
        await q.enqueue("jobs", _Msg(body="late"))

    producer = asyncio.create_task(_late_producer())
    try:
        msg = await asyncio.wait_for(anext(gen), timeout=1.0)
        assert msg.payload.body == "late"
    finally:
        await producer
        await gen.aclose()


async def test_consume_finite_idle_timeout_terminates_cleanly() -> None:
    q = _adapter(MockState())
    await q.enqueue("jobs", _Msg(body="only"))

    async def _drain() -> list[str]:
        out: list[str] = []
        async for msg in q.consume("jobs", timeout=timedelta(milliseconds=80)):
            out.append(msg.payload.body)
        return out

    # Terminates cleanly (no error) once idle — bounded well under the
    # wait_for guard; yields the one available message first.
    bodies = await asyncio.wait_for(_drain(), timeout=1.0)
    assert bodies == ["only"]


async def test_consume_idle_window_resets_on_each_delivery() -> None:
    q = _adapter(MockState())
    idle = timedelta(milliseconds=200)

    async def _producer() -> None:
        # Three messages, each gap (~120ms) below the idle timeout, while the
        # total span (~240ms) exceeds it: only a per-delivery reset of the
        # idle window lets the consumer see all three.
        await q.enqueue("jobs", _Msg(body="m1"))
        await asyncio.sleep(0.12)
        await q.enqueue("jobs", _Msg(body="m2"))
        await asyncio.sleep(0.12)
        await q.enqueue("jobs", _Msg(body="m3"))

    async def _drain() -> list[str]:
        out: list[str] = []
        async for msg in q.consume("jobs", timeout=idle):
            out.append(msg.payload.body)
        return out

    producer = asyncio.create_task(_producer())
    bodies = await asyncio.wait_for(_drain(), timeout=2.0)
    await producer
    assert bodies == ["m1", "m2", "m3"]


# ----------------------- #
# visibility-timeout redelivery


async def test_unacked_message_redelivered_after_visibility_lapse_with_same_id() -> (
    None
):
    st = MockState()
    q = _adapter(st, visibility_timeout=timedelta(seconds=30))
    clock = FrozenTimeSource(instant=_T0)

    with bind_time_source(clock):
        mid = await q.enqueue("jobs", _Msg(body="x"))
        first = await q.receive("jobs", limit=1)
        assert [m.id for m in first] == [mid]

        # In-flight: not receivable while the visibility window is open.
        assert await q.receive("jobs", limit=1) == []
        clock.instant = _T0 + timedelta(seconds=29)
        assert await q.receive("jobs", limit=1) == []

        # Visibility lapsed: redelivered with the SAME stable id.
        clock.instant = _T0 + timedelta(seconds=31)
        again = await q.receive("jobs", limit=1)
        assert [m.id for m in again] == [mid]

        # Delivery count observable in the pending bookkeeping.
        with st.lock:
            assert st.queue_pending["q"]["jobs"][mid].delivery_count == 2


async def test_ack_prevents_redelivery_after_visibility_lapse() -> None:
    q = _adapter(MockState(), visibility_timeout=timedelta(seconds=30))
    clock = FrozenTimeSource(instant=_T0)

    with bind_time_source(clock):
        mid = await q.enqueue("jobs", _Msg(body="x"))
        assert [m.id for m in await q.receive("jobs", limit=1)] == [mid]
        assert await q.ack("jobs", [mid]) == 1

        clock.instant = _T0 + timedelta(minutes=5)
        assert await q.receive("jobs", limit=1) == []


# ----------------------- #
# nack semantics


async def test_nack_requeue_true_is_immediately_receivable() -> None:
    q = _adapter(MockState())
    clock = FrozenTimeSource(instant=_T0)

    with bind_time_source(clock):
        mid = await q.enqueue("jobs", _Msg(body="x"))
        _ = await q.receive("jobs", limit=1)
        assert await q.nack("jobs", [mid], requeue=True) == 1

        # Same frozen instant: redelivery is immediate, same id.
        again = await q.receive("jobs", limit=1)
        assert [m.id for m in again] == [mid]


async def test_nack_requeue_false_dead_letters_and_is_not_receivable() -> None:
    q = _adapter(MockState())
    clock = FrozenTimeSource(instant=_T0)

    with bind_time_source(clock):
        mid = await q.enqueue("jobs", _Msg(body="poison"))
        _ = await q.receive("jobs", limit=1)
        assert await q.nack("jobs", [mid], requeue=False) == 1

        # Not receivable — not even after any amount of time.
        clock.instant = _T0 + timedelta(hours=1)
        assert await q.receive("jobs", limit=1) == []

        # Inspectable dead-letter list keeps the message (poison handling).
        dead = q.dead_letters("jobs")
        assert [m.id for m in dead] == [mid]
        assert dead[0].payload.body == "poison"


async def test_dead_letter_list_is_per_queue() -> None:
    q = _adapter(MockState())
    mid = await q.enqueue("jobs", _Msg(body="x"))
    _ = await q.receive("jobs", limit=1)
    await q.nack("jobs", [mid], requeue=False)

    assert [m.id for m in q.dead_letters("jobs")] == [mid]
    assert q.dead_letters("other") == []

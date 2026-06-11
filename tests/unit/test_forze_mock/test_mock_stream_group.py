"""Consumer-group semantics for the in-memory mock stream adapters.

Mirrors the Redis ``XREADGROUP``/``XACK`` contract exposed by
:class:`~forze.application.contracts.stream.StreamGroupQueryPort`: each entry
is delivered to exactly one consumer per group, pending entries are tracked
per consumer until acked, and independent groups each see the full stream.
"""

import asyncio
from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.stream.specs import StreamSpec
from forze.base.serialization import PydanticModelCodec
from forze_mock.adapters import (
    MockState,
    MockStreamAdapter,
    MockStreamGroupAdapter,
)

# ----------------------- #


class _Msg(BaseModel):
    body: str


def _adapters() -> tuple[MockStreamAdapter[_Msg], MockStreamGroupAdapter[_Msg]]:
    st = MockState()
    sa = MockStreamAdapter(
        state=st,
        namespace="s",
        codec=StreamSpec(name="s", codec=PydanticModelCodec(model_type=_Msg)).codec,
    )
    sg = MockStreamGroupAdapter(stream=sa, state=st, namespace="s")
    return sa, sg


# ....................... #


@pytest.mark.asyncio
async def test_two_consumers_split_entries_without_duplicates() -> None:
    """Alternating reads in one group: each entry goes to exactly one consumer."""

    sa, sg = _adapters()
    ids = [await sa.append("events", _Msg(body=f"m{i}")) for i in range(6)]

    seen_a: list[str] = []
    seen_b: list[str] = []

    for _ in range(3):
        seen_a.extend(m.id for m in await sg.read("g", "a", {"events": ">"}, limit=1))
        seen_b.extend(m.id for m in await sg.read("g", "b", {"events": ">"}, limit=1))

    assert not set(seen_a) & set(seen_b)
    assert sorted(seen_a + seen_b) == sorted(ids)

    # Stream fully delivered to the group: nothing new for either consumer.
    assert await sg.read("g", "a", {"events": ">"}) == []
    assert await sg.read("g", "b", {"events": ">"}) == []


@pytest.mark.asyncio
async def test_two_consumers_reading_concurrently_partition_the_stream() -> None:
    """Concurrent group reads still deliver every entry exactly once."""

    sa, sg = _adapters()
    ids = {await sa.append("events", _Msg(body=f"m{i}")) for i in range(10)}

    async def _drain(consumer: str) -> list[str]:
        got: list[str] = []
        while True:
            batch = await sg.read("g", consumer, {"events": ">"}, limit=2)
            if not batch:
                return got
            got.extend(m.id for m in batch)
            await asyncio.sleep(0)

    got_a, got_b = await asyncio.gather(_drain("a"), _drain("b"))

    assert not set(got_a) & set(got_b)
    assert set(got_a) | set(got_b) == ids
    assert len(got_a) + len(got_b) == len(ids)


@pytest.mark.asyncio
async def test_history_read_returns_own_pending_until_acked() -> None:
    """A concrete id cursor re-reads the consumer's own pending entries; ack clears them."""

    sa, sg = _adapters()
    ids = [await sa.append("events", _Msg(body=f"m{i}")) for i in range(3)]

    delivered = await sg.read("g", "a", {"events": ">"})
    assert [m.id for m in delivered] == ids

    # Own pending from the beginning of the stream.
    pending = await sg.read("g", "a", {"events": "0"})
    assert [m.id for m in pending] == ids

    # Another consumer in the same group owns no pending entries.
    assert await sg.read("g", "b", {"events": "0"}) == []

    acked = await sg.ack("g", "events", [ids[0], ids[1]])
    assert acked == 2

    remaining = await sg.read("g", "a", {"events": "0"})
    assert [m.id for m in remaining] == [ids[2]]

    # Double-ack removes nothing.
    assert await sg.ack("g", "events", [ids[0]]) == 0

    assert await sg.ack("g", "events", [ids[2]]) == 1
    assert await sg.read("g", "a", {"events": "0"}) == []


@pytest.mark.asyncio
async def test_second_group_independently_receives_all_entries() -> None:
    sa, sg = _adapters()
    ids = [await sa.append("events", _Msg(body=f"m{i}")) for i in range(4)]

    got_g1 = [m.id for m in await sg.read("g1", "a", {"events": ">"})]
    await sg.ack("g1", "events", got_g1)

    got_g2 = [m.id for m in await sg.read("g2", "z", {"events": ">"})]

    assert got_g1 == ids
    assert got_g2 == ids


@pytest.mark.asyncio
async def test_group_delivery_and_ack_do_not_affect_plain_stream_read() -> None:
    sa, sg = _adapters()
    ids = [await sa.append("events", _Msg(body=f"m{i}")) for i in range(3)]

    delivered = [m.id for m in await sg.read("g", "a", {"events": ">"})]
    await sg.ack("g", "events", delivered)

    plain = await sa.read({"events": "0"})
    assert [m.id for m in plain] == ids


@pytest.mark.asyncio
async def test_group_tail_yields_new_entries_once_per_group() -> None:
    sa, sg = _adapters()
    await sa.append("events", _Msg(body="first"))
    await sa.append("events", _Msg(body="second"))

    agen = sg.tail("g", "c", {"events": ">"}, timeout=timedelta(milliseconds=50))
    try:
        m1 = await asyncio.wait_for(agen.__anext__(), timeout=2)
        m2 = await asyncio.wait_for(agen.__anext__(), timeout=2)
    finally:
        await agen.aclose()

    assert {m1.payload.body, m2.payload.body} == {"first", "second"}

    # Tail recorded the entries pending for the consumer.
    pending = await sg.read("g", "c", {"events": "0"})
    assert {m.id for m in pending} == {m1.id, m2.id}

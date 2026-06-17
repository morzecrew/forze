"""DST P4 exit gate: the outbox→queue→inbox flow survives faults — and bugs are caught.

A seeded fault policy injects transient enqueue errors (forcing publish retries),
duplicate deliveries, and drops over the in-memory queue. Under it:

* an **idempotent** consumer (inbox dedup) applies each event exactly once despite
  retries and duplicates — at-least-once delivery + exactly-once effect both hold;
* a **non-idempotent** consumer double-applies duplicates — the bug is caught;
* a **drop**-prone broker loses events with no durable redelivery — DST detects the
  at-least-once violation.

Every run is seeded, so any failure reproduces exactly.
"""

from __future__ import annotations

import random
from contextlib import aclosing
from datetime import timedelta

from pydantic import BaseModel

from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.base.serialization import PydanticModelCodec

from forze_dst import (
    FaultyQueueCommand,
    TransportFault,
    TransportFaultPolicy,
    run_simulation,
)
from forze_mock import MockDepsModule
from forze_mock.adapters import MockQueueAdapter, MockState

from tests.support.execution_context import context_from_modules

# ----------------------- #

_QUEUE = "jobs"


class _Payload(BaseModel):
    value: str


_CODEC = PydanticModelCodec(_Payload)
_QSPEC = QueueSpec(name=_QUEUE, codec=_CODEC)
_INBOX = InboxSpec(name="events")
_IDLE = timedelta(milliseconds=500)
_N = 12


def _events() -> list[tuple[str, str]]:
    return [(f"evt-{i}", f"v{i}") for i in range(_N)]


async def _flow(
    *,
    policy: TransportFaultPolicy,
    fault_seed: int,
    dedup: bool,
) -> list[str]:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state))
    adapter = MockQueueAdapter[_Payload](state=state, namespace=_QUEUE, codec=_CODEC)
    cmd = FaultyQueueCommand(inner=adapter, policy=policy, rng=random.Random(fault_seed))

    # Relay: publish each event, retrying transient faults (at-least-once at publish).
    for event_id, value in _events():
        while True:
            try:
                await cmd.enqueue(
                    _QUEUE,
                    _Payload(value=value),
                    key=event_id,
                    headers={HEADER_EVENT_ID: event_id},
                )
                break
            except TransportFault:
                continue

    # Consume the broker, optionally deduping via the inbox before applying the effect.
    applied: list[str] = []
    async with aclosing(adapter.consume(_QUEUE, timeout=_IDLE)) as stream:
        async for message in stream:
            event_id = message.headers.get(HEADER_EVENT_ID) or message.key or message.id
            if not dedup or await ctx.inbox(_INBOX).mark_if_unseen(
                str(_INBOX.name), event_id
            ):
                applied.append(event_id)
            await adapter.ack(_QUEUE, [message.id])

    return applied


def _run(*, policy: TransportFaultPolicy, fault_seed: int = 1, dedup: bool) -> list[str]:
    return run_simulation(
        lambda: _flow(policy=policy, fault_seed=fault_seed, dedup=dedup), seed=0
    )


# ....................... #


class TestFaultInjection:
    def test_idempotent_consumer_survives_duplicates_and_retries(self) -> None:
        policy = TransportFaultPolicy(transient=0.4, duplicate=0.6)
        applied = _run(policy=policy, dedup=True)
        # at-least-once: every event delivered (transient enqueues were retried);
        # exactly-once effect: each applied once (duplicates deduped by the inbox).
        assert set(applied) == {event_id for event_id, _ in _events()}
        assert len(applied) == _N

    def test_non_idempotent_consumer_double_applies(self) -> None:
        policy = TransportFaultPolicy(transient=0.4, duplicate=0.6)
        applied = _run(policy=policy, dedup=False)
        # The bug DST catches: without dedup, duplicate deliveries apply twice.
        assert len(applied) > _N
        assert set(applied) == {event_id for event_id, _ in _events()}

    def test_drop_breaks_at_least_once(self) -> None:
        # A lossy broker with no durable redelivery loses events — DST surfaces the
        # at-least-once violation (the missing-durability assumption).
        policy = TransportFaultPolicy(drop=0.4)
        applied = _run(policy=policy, dedup=True)
        assert len(set(applied)) < _N

    def test_runs_are_reproducible(self) -> None:
        policy = TransportFaultPolicy(transient=0.3, duplicate=0.5, drop=0.1)
        first = _run(policy=policy, fault_seed=7, dedup=False)
        second = _run(policy=policy, fault_seed=7, dedup=False)
        assert first == second

    def test_invalid_probability_rejected(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            TransportFaultPolicy(duplicate=1.5)


class TestFaultWrapperMechanics:
    def test_delay_combines_and_still_delivers(self) -> None:
        # delay fault always fires; it combines with an explicit per-call delay, and
        # the message still arrives once virtual time advances past the total delay.
        async def scenario() -> list[str | None]:
            state = MockState()
            adapter = MockQueueAdapter[_Payload](
                state=state, namespace=_QUEUE, codec=_CODEC
            )
            cmd = FaultyQueueCommand(
                inner=adapter,
                policy=TransportFaultPolicy(delay=1.0, max_delay=timedelta(seconds=2)),
                rng=random.Random(3),
            )
            await cmd.enqueue(
                _QUEUE,
                _Payload(value="x"),
                key="e",
                delay=timedelta(seconds=1),
                headers={HEADER_EVENT_ID: "e"},
            )
            got: list[str | None] = []
            async with aclosing(
                adapter.consume(_QUEUE, timeout=timedelta(seconds=10))
            ) as stream:
                async for message in stream:
                    got.append(message.headers.get(HEADER_EVENT_ID))
                    await adapter.ack(_QUEUE, [message.id])
            return got

        assert run_simulation(scenario, seed=0) == ["e"]

    def test_enqueue_many_routes_each_item_through_faults(self) -> None:
        async def scenario() -> int:
            state = MockState()
            adapter = MockQueueAdapter[_Payload](
                state=state, namespace=_QUEUE, codec=_CODEC
            )
            cmd = FaultyQueueCommand(
                inner=adapter, policy=TransportFaultPolicy(), rng=random.Random(0)
            )
            ids = await cmd.enqueue_many(
                _QUEUE,
                [_Payload(value="a"), _Payload(value="b")],
                message_headers=[{HEADER_EVENT_ID: "a"}, {HEADER_EVENT_ID: "b"}],
            )
            return len(ids)

        assert run_simulation(scenario, seed=0) == 2

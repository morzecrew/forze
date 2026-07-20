"""DST P4 exit gate: the outbox→queue→inbox flow survives faults — and bugs are caught.

A seeded :class:`FaultPolicy`, applied to the queue port through the interception seam (no
hand-wrapping of the adapter), injects transient enqueue errors (forcing publish retries),
duplicate deliveries, and drops. Under it:

* an **idempotent** consumer (inbox dedup) applies each event exactly once despite retries
  and duplicates — at-least-once delivery + exactly-once effect both hold;
* a **non-idempotent** consumer double-applies duplicates — the bug is caught;
* a **drop**-prone broker loses events with no durable redelivery — DST detects the
  at-least-once violation.

Every run is seeded, so any failure reproduces exactly.
"""

from __future__ import annotations

import random
from collections.abc import AsyncIterator
from contextlib import aclosing
from datetime import timedelta

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.execution.interception import wrap_intercepted
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_dst.faults import (
    FaultPolicy,
    FaultRule,
    SimulatedCrash,
    _FaultPolicyInterceptor,
    compile_fault_policy,
)
from forze_dst.runtime import run_simulation
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


def _faulty_queue(state: MockState, policy: FaultPolicy, fault_seed: int) -> object:
    """The queue command port wrapped with *policy* via the interception seam (no adapter
    surgery): a fault interceptor seeded from *fault_seed* sits in front of every call."""

    adapter = MockQueueAdapter[_Payload](state=state, namespace=_QUEUE, codec=_CODEC)
    return wrap_intercepted(
        adapter,
        interceptors=(
            compile_fault_policy(policy, random.Random(fault_seed)),  # nosec B311
        ),
        surface="queue_command",
        route=_QUEUE,
    )


async def _flow(*, policy: FaultPolicy, fault_seed: int, dedup: bool) -> list[str]:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state))
    cmd = _faulty_queue(state, policy, fault_seed)
    adapter = MockQueueAdapter[_Payload](state=state, namespace=_QUEUE, codec=_CODEC)

    # Relay: publish each event, retrying injected errors (at-least-once at publish).
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
            except CoreException:
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


def _run(*, policy: FaultPolicy, fault_seed: int = 1, dedup: bool) -> list[str]:
    return run_simulation(
        lambda: _flow(policy=policy, fault_seed=fault_seed, dedup=dedup), seed=0
    )


# ....................... #


class TestFaultInjection:
    def test_idempotent_consumer_survives_duplicates_and_retries(self) -> None:
        policy = FaultPolicy(rules=(FaultRule(error=0.4, duplicate=0.6),))
        applied = _run(policy=policy, dedup=True)
        # at-least-once: every event delivered (errored enqueues were retried);
        # exactly-once effect: each applied once (duplicates deduped by the inbox).
        assert set(applied) == {event_id for event_id, _ in _events()}
        assert len(applied) == _N

    def test_non_idempotent_consumer_double_applies(self) -> None:
        policy = FaultPolicy(rules=(FaultRule(error=0.4, duplicate=0.6),))
        applied = _run(policy=policy, dedup=False)
        # The bug DST catches: without dedup, duplicate deliveries apply twice.
        assert len(applied) > _N
        assert set(applied) == {event_id for event_id, _ in _events()}

    def test_drop_breaks_at_least_once(self) -> None:
        # A lossy broker with no durable redelivery loses events — DST surfaces the
        # at-least-once violation (the missing-durability assumption).
        policy = FaultPolicy(rules=(FaultRule(drop=0.4),))
        applied = _run(policy=policy, dedup=True)
        assert len(set(applied)) < _N

    def test_runs_are_reproducible(self) -> None:
        policy = FaultPolicy(rules=(FaultRule(error=0.3, duplicate=0.5, drop=0.1),))
        first = _run(policy=policy, fault_seed=7, dedup=False)
        second = _run(policy=policy, fault_seed=7, dedup=False)
        assert first == second

    def test_invalid_probability_rejected(self) -> None:
        with pytest.raises(ValueError):
            FaultRule(duplicate=1.5)


class TestTransportFaultMechanics:
    def test_delay_still_delivers(self) -> None:
        # The delay fault advances virtual time before the enqueue (it never rewrites the
        # call's own ``delay`` arg); the message still arrives once time fast-forwards.
        def scenario() -> object:
            state = MockState()
            cmd = _faulty_queue(
                state,
                FaultPolicy(
                    rules=(
                        FaultRule(
                            op="enqueue", delay=1.0, max_delay=timedelta(seconds=2)
                        ),
                    )
                ),
                fault_seed=3,
            )
            adapter = MockQueueAdapter[_Payload](
                state=state, namespace=_QUEUE, codec=_CODEC
            )

            async def run() -> list[str | None]:
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

            return run()

        assert run_simulation(scenario, seed=0) == ["e"]

    def test_enqueue_many_through_the_seam(self) -> None:
        def scenario() -> object:
            state = MockState()
            cmd = _faulty_queue(state, FaultPolicy(), fault_seed=0)

            async def run() -> int:
                ids = await cmd.enqueue_many(
                    _QUEUE,
                    [_Payload(value="a"), _Payload(value="b")],
                    message_headers=[{HEADER_EVENT_ID: "a"}, {HEADER_EVENT_ID: "b"}],
                )
                return len(ids)

            return run()

        assert run_simulation(scenario, seed=0) == 2


# ....................... #


class _ScriptedRng:
    """A deterministic stand-in for ``random.Random`` that returns scripted ``random()`` values,
    so a mid-stream fault fires at an exact item regardless of platform RNG."""

    def __init__(self, values: list[float]) -> None:
        self._it = iter(values)

    def random(self) -> float:
        return next(self._it)

    def uniform(self, a: float, b: float) -> float:  # pragma: no cover - unused here
        return a

    def getrandbits(self, n: int) -> int:  # pragma: no cover - unused here
        return 0


@attrs.define(slots=True)
class _StreamPort:
    async def stream(self, n: int) -> AsyncIterator[int]:
        for i in range(n):
            yield i


class TestStreamFaults:
    """``FaultRule(stream_faults=True)`` injects a raise-fault *mid-stream*; off, a stream
    behaves exactly as before (fail before opening, then clean iteration)."""

    async def _drain(self, interceptor: object) -> list[int]:
        port = wrap_intercepted(
            _StreamPort(), interceptors=(interceptor,), surface="f", route=None
        )
        return [i async for i in port.stream(5)]

    async def test_mid_stream_fault_when_opted_in(self) -> None:
        # rule has only error>0, so each roll is one ``random()`` draw. Pre-open: no fire
        # (0.9 >= 0.5); after item 0: fire (0.1 < 0.5).
        interceptor = _FaultPolicyInterceptor(
            rules=(FaultRule(error=0.5, stream_faults=True),),
            rng=_ScriptedRng([0.9, 0.1]),  # type: ignore[arg-type]
        )
        got: list[int] = []
        with pytest.raises(CoreException):
            port = wrap_intercepted(
                _StreamPort(), interceptors=(interceptor,), surface="f", route=None
            )
            async for i in port.stream(5):
                got.append(i)

        assert got == [0]  # one item delivered, then the mid-stream fault

    async def test_no_mid_stream_fault_when_off(self) -> None:
        # Same pre-open draw (no fire); with stream_faults off, iteration is clean — no
        # per-item draws — so the whole stream delivers.
        interceptor = _FaultPolicyInterceptor(
            rules=(FaultRule(error=0.5, stream_faults=False),),
            rng=_ScriptedRng([0.9]),  # type: ignore[arg-type]
        )
        assert await self._drain(interceptor) == [0, 1, 2, 3, 4]

    @pytest.mark.parametrize(
        ("rule", "expected"),
        [
            (FaultRule(crash=0.5, stream_faults=True), SimulatedCrash),
            (FaultRule(timeout=0.5, stream_faults=True), CoreException),
        ],
    )
    async def test_mid_stream_crash_and_timeout_when_opted_in(
        self, rule: FaultRule, expected: type[BaseException]
    ) -> None:
        # A single-kind rule means each roll is one draw: pre-open no-fire (0.9), then the
        # mid-stream fault fires after item 0 (0.1). Covers the crash and timeout branches of
        # ``_mid_stream_fault`` alongside the error case above.
        interceptor = _FaultPolicyInterceptor(
            rules=(rule,),
            rng=_ScriptedRng([0.9, 0.1]),  # type: ignore[arg-type]
        )
        got: list[int] = []
        with pytest.raises(expected):
            port = wrap_intercepted(
                _StreamPort(), interceptors=(interceptor,), surface="f", route=None
            )
            async for i in port.stream(5):
                got.append(i)

        assert got == [0]  # one item delivered, then the mid-stream fault

    @pytest.mark.parametrize(
        ("rule", "expected"),
        [
            (FaultRule(crash=0.5), SimulatedCrash),
            (FaultRule(error=0.5), CoreException),
            (FaultRule(timeout=0.5), CoreException),
        ],
    )
    async def test_pre_open_stream_fault_fails_before_any_item(
        self, rule: FaultRule, expected: type[BaseException]
    ) -> None:
        # The pre-open rolls (identical to ``around``) fire before the stream opens, so no
        # item is delivered — mirroring a stream that dies on acquisition.
        interceptor = _FaultPolicyInterceptor(
            rules=(rule,),
            rng=_ScriptedRng([0.1]),  # type: ignore[arg-type]
        )
        got: list[int] = []
        with pytest.raises(expected):
            port = wrap_intercepted(
                _StreamPort(), interceptors=(interceptor,), surface="f", route=None
            )
            async for i in port.stream(5):
                got.append(i)

        assert got == []  # failed before opening — no item delivered

    async def test_pre_open_stream_delay_then_delivers(self) -> None:
        # A pre-open delay advances virtual time (one ``random`` draw fires it, one ``uniform``
        # draw sizes it) and then the whole stream delivers cleanly.
        interceptor = _FaultPolicyInterceptor(
            rules=(FaultRule(delay=0.5),),
            rng=_ScriptedRng([0.1]),  # type: ignore[arg-type]
        )
        assert await self._drain(interceptor) == [0, 1, 2, 3, 4]

    async def test_stream_faults_on_but_no_roll_fires_delivers_all(self) -> None:
        # ``stream_faults`` on, but every roll misses: the per-item fault check runs and returns
        # without raising, so the full stream still delivers (the no-fault mid-stream path).
        interceptor = _FaultPolicyInterceptor(
            rules=(FaultRule(error=0.5, stream_faults=True),),
            rng=_ScriptedRng([0.9] * 6),  # type: ignore[arg-type]  # pre-open + 5 per-item
        )
        assert await self._drain(interceptor) == [0, 1, 2, 3, 4]

    async def test_stream_with_no_matching_rule_passes_through(self) -> None:
        # No rule matches the call, so ``around_stream`` is a transparent pass-through (the
        # ``rule is None`` branch) that still closes the inner stream via ``aclosing``.
        interceptor = _FaultPolicyInterceptor(
            rules=(),
            rng=_ScriptedRng([]),  # type: ignore[arg-type]  # no rolls on the pass-through
        )
        assert await self._drain(interceptor) == [0, 1, 2, 3, 4]


@attrs.define(slots=True)
class _CallPort:
    async def call(self, x: int) -> int:
        return x * 2


class TestAroundRaiseFaults:
    """The non-stream ``around`` raise-faults (crash / timeout) short-circuit the call."""

    @pytest.mark.parametrize(
        ("rule", "expected"),
        [
            (FaultRule(crash=0.5), SimulatedCrash),
            (FaultRule(timeout=0.5), CoreException),
        ],
    )
    async def test_around_crash_and_timeout_short_circuit(
        self, rule: FaultRule, expected: type[BaseException]
    ) -> None:
        interceptor = _FaultPolicyInterceptor(
            rules=(rule,),
            rng=_ScriptedRng([0.1]),  # type: ignore[arg-type]
        )
        port = wrap_intercepted(
            _CallPort(), interceptors=(interceptor,), surface="f", route=None
        )
        with pytest.raises(expected):
            await port.call(3)


def test_crash_policy_rejects_out_of_range_probability() -> None:
    from forze_dst.faults import CrashPolicy

    with pytest.raises(ValueError, match="probability"):
        CrashPolicy(probability=1.5)

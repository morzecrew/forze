"""The background relay runs a pass when it hears its route's wake, not only on its poll."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    DEFAULT_OUTBOX_WAKE_TOPIC,
    OutboxRelayResult,
    OutboxSpec,
    OutboxWake,
    OutboxWakeSignal,
)
from forze.application.contracts.pubsub import PubSubCommandDepKey
from forze.application.contracts.queue import QueueSpec
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import (
    OutboxRelay,
    RelayBinding,
    outbox_relay_background_lifecycle_step,
)
from forze_kits.integrations.outbox.lifecycle import _OutboxRelayBackgroundStartup
from forze_mock import MockDepsModule, MockRouteConfig, MockState
from forze_mock.adapters.pubsub import MockPubSubAdapter

# ----------------------- #


class _Payload(BaseModel):
    note: str


CODEC = PydanticModelCodec(_Payload)
WAKE = OutboxWake(route="outbox-wake")
SPEC = OutboxSpec(name="events", codec=CODEC, wake=WAKE)
QUEUE = QueueSpec(name="jobs", codec=CODEC)
POLL = timedelta(seconds=10)


def _queued(state: MockState) -> int:
    return len(state.queues.get("jobs", {}).get("jobs", []))


async def _until(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return

        await asyncio.sleep(0.01)

    raise AssertionError(f"not reached within {timeout}s")


async def _commit_one(ctx: ExecutionContext, spec: OutboxSpec[_Payload]) -> None:
    async with ctx.tx_ctx.scope("mock"):
        outbox = ctx.outbox.command(spec)
        await outbox.stage("thing.done", _Payload(note="n"))
        await outbox.flush()


async def _wake(ctx: ExecutionContext, route: str) -> None:
    port = ctx.deps.resolve_configurable(ctx, PubSubCommandDepKey, WAKE.spec, route=WAKE.route)
    await port.publish(DEFAULT_OUTBOX_WAKE_TOPIC, OutboxWakeSignal(route=route))


@asynccontextmanager
async def _running(
    state: MockState, spec: OutboxSpec[_Payload] = SPEC, **step: Any
) -> AsyncIterator[tuple[ExecutionContext, _OutboxRelayBackgroundStartup]]:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule(state=state)).freeze())
    lifecycle = outbox_relay_background_lifecycle_step(
        outbox_spec=spec, queue_spec=QUEUE, **{"interval": POLL, **step}
    )
    startup = lifecycle.startup
    assert isinstance(startup, _OutboxRelayBackgroundStartup)

    async with runtime.scope():
        ctx = runtime.get_context()
        await lifecycle.startup(ctx)
        # The first pass runs at startup, before anything is staged; let it park on its
        # poll and the wake listener subscribe.
        await asyncio.sleep(0.1)

        try:
            yield ctx, startup

        finally:
            await lifecycle.shutdown(ctx)


# ....................... #


async def test_a_committed_row_is_relayed_long_before_the_poll() -> None:
    state = MockState()

    async with _running(state) as (ctx, startup):
        await _commit_one(ctx, SPEC)
        await _until(lambda: _queued(state) == 1)

        listener = startup.listener
        assert listener is not None

    assert startup.listener is None
    assert listener.done()


async def test_without_a_wake_the_row_waits_for_the_poll() -> None:
    state = MockState()
    plain = OutboxSpec(name="events", codec=CODEC)

    async with _running(state, plain) as (ctx, startup):
        await _commit_one(ctx, plain)
        await asyncio.sleep(0.5)

        assert _queued(state) == 0
        assert startup.listener is None


async def test_a_wake_for_another_route_runs_no_pass() -> None:
    state = MockState()
    passes: list[None] = []

    async def _count(*_: Any, **__: Any) -> OutboxRelayResult:
        passes.append(None)
        return OutboxRelayResult()

    with patch.object(OutboxRelay, "to_queue", _count):
        async with _running(state) as (ctx, _):
            before = len(passes)
            await _wake(ctx, "someone-else")
            await asyncio.sleep(0.3)
            assert len(passes) == before

            await _wake(ctx, "events")
            await _until(lambda: len(passes) == before + 1)


async def test_wake_driven_passes_reclaim_stale_claims_once_per_poll() -> None:
    state = MockState()
    reclaims: list[timedelta | None] = []

    async def _record(self: OutboxRelay, *_: Any, **__: Any) -> OutboxRelayResult:
        reclaims.append(self.reclaim_stale_after)
        return OutboxRelayResult()

    with patch.object(OutboxRelay, "to_queue", _record):
        async with _running(state, reclaim_stale_after=timedelta(minutes=5)) as (ctx, _):
            for _ in range(3):
                passes = len(reclaims)
                await _wake(ctx, "events")
                await _until(lambda passes=passes: len(reclaims) > passes)

    assert reclaims == [timedelta(minutes=5), None, None, None]


async def test_a_reclaim_that_failed_is_tried_again_on_the_next_pass() -> None:
    state = MockState()
    reclaims: list[timedelta | None] = []

    async def _blip_once(self: OutboxRelay, *_: Any, **__: Any) -> OutboxRelayResult:
        reclaims.append(self.reclaim_stale_after)

        if len(reclaims) == 1:
            raise ConnectionError("database blip")

        return OutboxRelayResult()

    every = {"reclaim_stale_after": timedelta(minutes=5), "reclaim_every": timedelta(minutes=1)}

    with patch.object(OutboxRelay, "to_queue", _blip_once):
        async with _running(state, **every) as (ctx, _):
            passes = len(reclaims)
            await _wake(ctx, "events")
            await _until(lambda: len(reclaims) > passes)

    # The startup pass's reclaim failed and its next batch ran without one; the wake's
    # pass reclaims rather than waiting out the minute.
    assert reclaims == [timedelta(minutes=5), None, timedelta(minutes=5)]


async def test_a_tenant_aware_wake_route_fails_the_relay_startup() -> None:
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(
            MockDepsModule(routes={"outbox-wake": MockRouteConfig(tenant_aware=True)})
        ).freeze()
    )
    step = outbox_relay_background_lifecycle_step(outbox_spec=SPEC, queue_spec=QUEUE, interval=POLL)

    async with runtime.scope():
        with pytest.raises(CoreException, match="tenant-global"):
            await step.startup(runtime.get_context())


async def test_a_broken_wake_subscription_leaves_the_relay_polling_and_retrying() -> None:
    state = MockState()
    attempts: list[None] = []

    async def _refused(*_: Any, **__: Any) -> AsyncIterator[Any]:
        attempts.append(None)
        raise ConnectionError("broker down")
        yield  # pragma: no cover - makes this an async generator

    with patch.object(MockPubSubAdapter, "subscribe", _refused):
        async with _running(state, interval=timedelta(milliseconds=200)) as (ctx, startup):
            await _commit_one(ctx, SPEC)
            await _until(lambda: _queued(state) == 1)

            # Each failure is retried a poll later, not given up on.
            await _until(lambda: len(attempts) >= 3)
            assert startup.listener is not None
            assert not startup.listener.done()


# ....................... #


def _startup(**overrides: Any) -> _OutboxRelayBackgroundStartup:
    step = outbox_relay_background_lifecycle_step(
        outbox_spec=SPEC, queue_spec=QUEUE, **{"interval": POLL, "jitter": 0.2, **overrides}
    )
    assert isinstance(step.startup, _OutboxRelayBackgroundStartup)
    return step.startup


def test_every_poll_tick_still_reclaims_by_default() -> None:
    startup = _startup()

    assert startup._reclaim_due(1000.0)  # nothing reclaimed yet

    startup.last_reclaim_at = 1000.0
    assert not startup._reclaim_due(1001.0)
    # The shortest a jittered poll tick can be after the last one.
    assert startup._reclaim_due(1000.0 + POLL.total_seconds() * 0.8)


def test_reclaim_every_sets_the_gap_between_reclaims() -> None:
    startup = _startup(reclaim_every=timedelta(minutes=1))
    startup.last_reclaim_at = 1000.0

    assert not startup._reclaim_due(1059.0)
    assert startup._reclaim_due(1060.0)


def test_no_reclaim_is_due_when_reclaim_is_off() -> None:
    assert not _startup(reclaim_stale_after=None)._reclaim_due(1000.0)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"reclaim_every": timedelta(0)}, "Reclaim every must be positive"),
        (
            {"reclaim_every": timedelta(minutes=1), "reclaim_stale_after": None},
            "set both or neither",
        ),
    ],
)
def test_a_reclaim_cadence_that_cannot_hold_is_refused(
    overrides: dict[str, Any], message: str
) -> None:
    with pytest.raises(CoreException, match=message):
        _startup(**overrides)


def test_a_relay_binding_passes_its_reclaim_cadence_on() -> None:
    step = RelayBinding(queue_spec=QUEUE, reclaim_every=timedelta(minutes=1)).as_lifecycle_step(
        SPEC
    )

    assert isinstance(step.startup, _OutboxRelayBackgroundStartup)
    assert step.startup.reclaim_every == timedelta(minutes=1)

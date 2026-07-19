"""The pubsub-backed signal source — broadcast live lane behind the source seam.

# covers: forze_socketio.gateway.PubSubSignalSource (delivery, tenant headers, stop,
#         handler-failure isolation, config fail-fast, encrypted refusal, validation),
#         forze_kits.integrations.realtime (realtime_pubsub_spec,
#         build_realtime_pubsub_publisher, read-only refusal)

At-most-once by the port contract: no ack, no reclaim, no poison ceiling — the test
battery therefore focuses on what this lane does promise (live delivery with the
tenant riding the headers, one bad signal never wedging the channel) and on the
supervision-facing contract (stop at a read boundary, config errors terminal).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.pubsub import PubSubSpec
from forze.application.contracts.realtime import Audience, RealtimeEvent, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import HlcTimestamp
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.realtime import (
    build_realtime_pubsub_publisher,
    realtime_pubsub_spec,
)
from forze_mock import MockDepsModule
from forze_socketio import PubSubSignalSource

# ----------------------- #


class _View(BaseModel):
    n: int


_EVENT = RealtimeEvent(name="e", payload_type=_View)


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


def _source(**overrides: Any) -> PubSubSignalSource:
    return PubSubSignalSource(
        pubsub_spec=realtime_pubsub_spec(),
        poll_interval=timedelta(milliseconds=10),
        resubscribe_backoff=timedelta(milliseconds=10),
        **overrides,
    )


class _Recorder:
    def __init__(self) -> None:
        self.received: list[tuple[RealtimeSignal, UUID | None, str | None, HlcTimestamp]] = []

    async def __call__(
        self,
        signal: RealtimeSignal,
        tenant: UUID | None,
        dedup_id: str | None,
        hlc: HlcTimestamp,
    ) -> None:
        self.received.append((signal, tenant, dedup_id, hlc))


async def _wait_for(condition: Any, *, timeout: float = 5.0) -> None:
    waited = 0.0

    while not condition() and waited < timeout:
        await asyncio.sleep(0.01)
        waited += 0.01

    assert condition()


# ----------------------- #


class TestDelivery:
    async def test_published_signals_reach_the_handler_with_tenant_and_hlc(self) -> None:
        runtime = _runtime()
        tenant = uuid4()
        recorder = _Recorder()
        source = _source()
        stop = asyncio.Event()

        async with runtime.scope():
            ctx = runtime.get_context()
            task = asyncio.create_task(source.run(ctx, recorder, stop=stop))
            await asyncio.sleep(0.05)  # the subscription must be live before publishing

            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                pub = build_realtime_pubsub_publisher(ctx, pubsub_spec=realtime_pubsub_spec())
                await pub.publish(Audience.topic("t"), _EVENT, _View(n=1))

            pub_untenanted = build_realtime_pubsub_publisher(
                ctx, pubsub_spec=realtime_pubsub_spec()
            )
            await pub_untenanted.publish(Audience.principal("p"), _EVENT, _View(n=2))

            await _wait_for(lambda: len(recorder.received) == 2)
            stop.set()
            await asyncio.wait_for(task, timeout=5)

        first, second = recorder.received
        assert first[0].payload == {"n": 1}
        assert first[1] == tenant  # the ambient tenant rode the message headers
        assert first[2] is None  # ephemeral lane: no durable id to dedup on
        assert second[0].payload == {"n": 2}
        assert second[1] is None
        assert all(isinstance(r[3], HlcTimestamp) for r in recorder.received)

    async def test_stop_before_any_message_exits_cleanly(self) -> None:
        runtime = _runtime()
        source = _source()
        stop = asyncio.Event()

        async with runtime.scope():
            ctx = runtime.get_context()
            task = asyncio.create_task(source.run(ctx, _Recorder(), stop=stop))
            await asyncio.sleep(0.05)  # idle inside the raced subscription read

            stop.set()
            await asyncio.wait_for(task, timeout=5)


class TestFailurePolicy:
    async def test_one_failing_bridge_does_not_wedge_the_channel(self) -> None:
        runtime = _runtime()
        received: list[int] = []
        failures = {"n": 0}
        stop = asyncio.Event()

        async def flaky(signal: RealtimeSignal, *args: Any) -> None:
            if signal.payload["n"] == 1:
                failures["n"] += 1
                raise RuntimeError("bad signal")

            received.append(signal.payload["n"])

        source = _source()

        async with runtime.scope():
            ctx = runtime.get_context()
            task = asyncio.create_task(source.run(ctx, flaky, stop=stop))
            await asyncio.sleep(0.05)

            pub = build_realtime_pubsub_publisher(ctx, pubsub_spec=realtime_pubsub_spec())
            await pub.publish(Audience.topic("t"), _EVENT, _View(n=1))  # bridge fails
            await pub.publish(Audience.topic("t"), _EVENT, _View(n=2))  # still delivered

            await _wait_for(lambda: received == [2])
            stop.set()
            await asyncio.wait_for(task, timeout=5)

        assert failures["n"] == 1  # failed once, dropped (at-most-once), moved on

    async def test_configuration_error_from_the_bridge_is_terminal(self) -> None:
        runtime = _runtime()

        async def miswired(*args: Any) -> None:
            raise exc.configuration("mailbox route is not wired")

        source = _source()

        async with runtime.scope():
            ctx = runtime.get_context()
            run = asyncio.create_task(source.run(ctx, miswired))
            await asyncio.sleep(0.05)

            pub = build_realtime_pubsub_publisher(ctx, pubsub_spec=realtime_pubsub_spec())
            await pub.publish(Audience.topic("t"), _EVENT, _View(n=1))

            with pytest.raises(CoreException):
                await asyncio.wait_for(run, timeout=5)


class TestWiring:
    async def test_encrypted_channel_refused_at_run(self) -> None:
        sealed = PubSubSpec(
            name="realtime",
            codec=PydanticModelCodec(model_type=RealtimeSignal),
            encryption="end_to_end",
        )
        source = PubSubSignalSource(pubsub_spec=sealed)

        async def handler(*args: Any) -> None:  # pragma: no cover - never reached
            return

        with pytest.raises(CoreException) as caught:
            await source.run(None, handler)  # type: ignore[arg-type]

        assert caught.value.code == "realtime_stream_encryption_unsupported"

    def test_invalid_settings_are_refused(self) -> None:
        with pytest.raises(CoreException):
            PubSubSignalSource(pubsub_spec=realtime_pubsub_spec(), poll_interval=timedelta(0))

        with pytest.raises(CoreException):
            PubSubSignalSource(
                pubsub_spec=realtime_pubsub_spec(), resubscribe_backoff=timedelta(0)
            )

    async def test_publisher_build_refused_in_read_only_operation(self) -> None:
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()

            with ctx.inv_ctx.bind_read_only():
                with pytest.raises(CoreException) as caught:
                    build_realtime_pubsub_publisher(ctx, pubsub_spec=realtime_pubsub_spec())

        assert caught.value.kind.value == "precondition"

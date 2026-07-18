"""Gateway edge branches — error taxonomy on the bridge, loop retry, header binding.

# covers: forze_socketio.gateway (_bind_tenant enabled, mailbox-store re-raise, post-commit
#         config fail-fast, presence-skip stat, loop-level retry, config fail-fast in loop),
#         forze_socketio.gateway_lifecycle (duplicate startup)
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.realtime import Audience, RealtimeEvent, RealtimeSignal
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import HlcTimestamp, utcnow
from forze_kits.integrations.realtime import (
    build_realtime_publisher,
    realtime_inbox_spec,
    realtime_stream_spec,
)
from forze_mock import MockDepsModule
from forze_socketio import (
    GatewayDedup,
    RealtimeGateway,
    RealtimeGatewayStats,
    StreamGroupSignalSource,
    realtime_gateway_lifecycle_step,
)
from forze_socketio.gateway import _bind_tenant  # pyright: ignore[reportPrivateUsage]

# ----------------------- #


class _StubSio:
    def __init__(self) -> None:
        self.emits: list[Any] = []
        self.raise_config = False

    async def emit(self, event: str, data: Any = None, **_: Any) -> None:
        if self.raise_config:
            raise exc.configuration("emitter is miswired")

        self.emits.append(data)


class _FakeMailbox:
    def __init__(self, *, raise_core: CoreException | None = None) -> None:
        self.raise_core = raise_core
        self.stored = 0

    async def store(self, **_: Any) -> None:
        if self.raise_core is not None:
            raise self.raise_core

        self.stored += 1


class _EmptyPresence:
    async def joined(self, room: str, sid: str) -> None: ...

    async def left(self, room: str, sid: str) -> None: ...

    async def count(self, room: str) -> int:
        return 0  # nobody home — the live emit is skippable


def _hlc() -> HlcTimestamp:
    return HlcTimestamp(physical_ms=int(utcnow().timestamp() * 1000), logical=0)


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


def _gateway(sio: _StubSio, **overrides: Any) -> RealtimeGateway:
    return RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=StreamGroupSignalSource(stream_spec=realtime_stream_spec()),
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
        **overrides,
    )


_SIGNAL = RealtimeSignal.of(Audience.principal("u1"), "e", {"n": 1})


class _View(BaseModel):
    text: str


_EVENT = RealtimeEvent(name="e", payload_type=_View)


# ----------------------- #


async def test_bind_tenant_binds_the_header_tenant_when_opted_in() -> None:
    runtime = _runtime()
    tenant = uuid4()

    async with runtime.scope():
        ctx = runtime.get_context()

        with _bind_tenant(ctx, tenant, enabled=True):
            bound = ctx.inv_ctx.get_tenant()
            assert bound is not None and bound.tenant_id == tenant

        with _bind_tenant(ctx, tenant, enabled=False):
            assert ctx.inv_ctx.get_tenant() is None  # opt-out binds nothing


async def test_mailbox_store_core_errors_other_than_tenant_required_propagate() -> None:
    runtime = _runtime()
    sio = _StubSio()
    gw = _gateway(sio)
    mailbox = _FakeMailbox(raise_core=exc.infrastructure("mailbox backend down"))

    async with runtime.scope():
        ctx = runtime.get_context()

        with pytest.raises(CoreException) as caught:
            await gw._handle(ctx, mailbox, _SIGNAL, None, str(uuid4()), _hlc())  # pyright: ignore[reportPrivateUsage]

    # not rewrapped — only the opaque tenant_required gets the actionable wrapper
    assert caught.value.code != "realtime_mailbox_tenant_unbound"
    assert sio.emits == []  # the transaction rolled back; nothing was emitted


async def test_post_commit_configuration_emit_error_fails_fast() -> None:
    runtime = _runtime()
    sio = _StubSio()
    sio.raise_config = True  # a wiring error must not be swallowed as best-effort
    gw = _gateway(sio)
    mailbox = _FakeMailbox()

    async with runtime.scope():
        ctx = runtime.get_context()

        with pytest.raises(CoreException):
            await gw._handle(ctx, mailbox, _SIGNAL, None, str(uuid4()), _hlc())  # pyright: ignore[reportPrivateUsage]

    assert mailbox.stored == 1  # the durable obligation was already met


async def test_presence_skip_counts_on_stats() -> None:
    runtime = _runtime()
    sio = _StubSio()
    stats = RealtimeGatewayStats()
    gw = _gateway(sio, presence=_EmptyPresence(), stats=stats)
    mailbox = _FakeMailbox()

    async with runtime.scope():
        ctx = runtime.get_context()
        await gw._handle(ctx, mailbox, _SIGNAL, None, str(uuid4()), _hlc())  # pyright: ignore[reportPrivateUsage]

    assert mailbox.stored == 1  # stored for reconnect...
    assert sio.emits == []  # ...but the empty room's live emit was skipped
    assert stats.presence_skipped == 1
    assert stats.mailboxed == 1


# ----------------------- #


async def test_consume_loop_retries_transient_read_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from forze_mock.adapters.stream import MockAckStreamGroupAdapter

    original = MockAckStreamGroupAdapter.read
    failures = {"n": 0}

    async def _flaky_read(self: Any, *args: Any, **kwargs: Any) -> Any:
        if failures["n"] < 2:
            failures["n"] += 1
            raise exc.infrastructure("broker blip")

        return await original(self, *args, **kwargs)

    monkeypatch.setattr(MockAckStreamGroupAdapter, "read", _flaky_read)

    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=StreamGroupSignalSource(
            stream_spec=spec, poll_interval=timedelta(milliseconds=10)
        ),
    )

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        pub = build_realtime_publisher(ctx, stream_spec=spec)
        await pub.publish(Audience.topic("t"), _EVENT, _View(text="x"))

        run = asyncio.create_task(gw.run(ctx))
        waited = 0.0
        while not sio.emits and waited < 5.0:
            await asyncio.sleep(0.01)
            waited += 0.01
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run

    assert failures["n"] == 2  # two transient failures absorbed...
    assert len(sio.emits) == 1  # ...and delivery still happened


async def test_consume_loop_fails_fast_on_configuration_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from forze_mock.adapters.stream import MockAckStreamGroupAdapter

    async def _miswired(self: Any, *args: Any, **kwargs: Any) -> Any:
        raise exc.configuration("group route is not wired")

    monkeypatch.setattr(MockAckStreamGroupAdapter, "read", _miswired)

    gw = RealtimeGateway(
        sio=_StubSio(),  # pyright: ignore[reportArgumentType]
        source=StreamGroupSignalSource(
            stream_spec=realtime_stream_spec(), poll_interval=timedelta(milliseconds=10)
        ),
    )

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()

        with pytest.raises(CoreException):
            await asyncio.wait_for(gw.run(ctx), timeout=5)


# ----------------------- #


async def test_duplicate_gateway_startup_is_ignored() -> None:
    gw = _gateway(_StubSio())
    step = realtime_gateway_lifecycle_step(gw)

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)
        first = step.startup.task  # type: ignore[attr-defined]

        await step.startup(ctx)  # must not orphan the running task
        assert step.startup.task is first  # type: ignore[attr-defined]
        assert step.startup.loop_name == "realtime_gateway"  # type: ignore[attr-defined]

        await step.shutdown(ctx)


async def test_handler_configuration_error_fails_the_bridge_fast() -> None:
    """A config error raised by the bridge itself propagates out of the consume loop."""

    spec = realtime_stream_spec()
    source = StreamGroupSignalSource(stream_spec=spec, poll_interval=timedelta(milliseconds=10))

    async def _miswired_handler(*args: Any) -> None:
        raise exc.configuration("tenant-aware mailbox with no bound tenant")

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        pub = build_realtime_publisher(ctx, stream_spec=spec)
        await pub.publish(Audience.topic("t"), _EVENT, _View(text="x"))

        with pytest.raises(CoreException):
            await asyncio.wait_for(source.run(ctx, _miswired_handler), timeout=5)


async def test_consume_loop_retries_plain_exceptions_too(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from forze_mock.adapters.stream import MockAckStreamGroupAdapter

    original = MockAckStreamGroupAdapter.read
    failures = {"n": 0}

    async def _flaky_read(self: Any, *args: Any, **kwargs: Any) -> Any:
        if failures["n"] < 1:
            failures["n"] += 1
            raise RuntimeError("not even a CoreException")

        return await original(self, *args, **kwargs)

    monkeypatch.setattr(MockAckStreamGroupAdapter, "read", _flaky_read)

    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=StreamGroupSignalSource(
            stream_spec=spec, poll_interval=timedelta(milliseconds=10)
        ),
    )

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        pub = build_realtime_publisher(ctx, stream_spec=spec)
        await pub.publish(Audience.topic("t"), _EVENT, _View(text="x"))

        run = asyncio.create_task(gw.run(ctx))
        waited = 0.0
        while not sio.emits and waited < 5.0:
            await asyncio.sleep(0.01)
            waited += 0.01
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run

    assert failures["n"] == 1 and len(sio.emits) == 1

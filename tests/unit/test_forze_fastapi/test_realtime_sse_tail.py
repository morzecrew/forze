"""The per-node SSE live-tail loop — fast-forward, hub publish, supervision, refusals.

# covers: forze_fastapi.realtime.lifecycle (_tail_to_hub fast-forward + tenant headers,
#         realtime_sse_tail_lifecycle_step wiring validation, startup/shutdown, dupes)
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, cast
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.realtime import Audience, RealtimeEvent, RealtimeSignal
from forze.application.contracts.stream import StreamSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_fastapi.realtime import RealtimeSseHub, realtime_sse_tail_lifecycle_step
from forze_fastapi.realtime.lifecycle import _tail_to_hub  # pyright: ignore[reportPrivateUsage]
from forze_kits.integrations.realtime import build_realtime_publisher, realtime_stream_spec
from forze_mock import MockDepsModule

# ----------------------- #


class _View(BaseModel):
    n: int


_EVENT = RealtimeEvent(name="e", payload_type=_View)


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


async def _drain(hub_sub: Any, *, timeout: float = 5.0) -> RealtimeSignal:
    signal, _event_id = await asyncio.wait_for(hub_sub.queue.get(), timeout=timeout)

    return signal


# ----------------------- #


class TestTailToHub:
    async def test_fast_forwards_the_backlog_then_publishes_new_signals(self) -> None:
        spec = realtime_stream_spec()
        hub = RealtimeSseHub()
        sub = hub.subscribe(principal="nobody", tenant=None, topics=frozenset({"t"}))
        stop = asyncio.Event()

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            pub = build_realtime_publisher(ctx, stream_spec=spec)

            # pre-startup backlog: the live leg starts at *now*, so these must be skipped
            await pub.publish(Audience.topic("t"), _EVENT, _View(n=1))
            await pub.publish(Audience.topic("t"), _EVENT, _View(n=2))

            task = asyncio.create_task(
                _tail_to_hub(
                    ctx,
                    hub=hub,
                    stream_spec=spec,
                    batch=16,
                    poll_interval=timedelta(milliseconds=10),
                    stop=stop,
                )
            )
            await asyncio.sleep(0.2)  # fast-forward completes against the in-memory store
            assert sub.queue.empty()

            await pub.publish(Audience.topic("t"), _EVENT, _View(n=3))
            live = await _drain(sub)
            assert live.payload == {"n": 3}

            stop.set()
            await asyncio.wait_for(task, timeout=5)

    async def test_tenant_header_scopes_the_fanout(self) -> None:
        spec = realtime_stream_spec()
        tenant = uuid4()
        hub = RealtimeSseHub()
        matching = hub.subscribe(principal="nobody", tenant=tenant, topics=frozenset({"t"}))
        other = hub.subscribe(principal="nobody", tenant=None, topics=frozenset({"t"}))
        stop = asyncio.Event()

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()

            task = asyncio.create_task(
                _tail_to_hub(
                    ctx,
                    hub=hub,
                    stream_spec=spec,
                    batch=16,
                    poll_interval=timedelta(milliseconds=10),
                    stop=stop,
                )
            )
            await asyncio.sleep(0.05)

            # published under a bound tenant: the header rides the stream row
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                pub = build_realtime_publisher(ctx, stream_spec=spec)
                await pub.publish(Audience.topic("t"), _EVENT, _View(n=1))

            live = await _drain(matching)
            assert live.payload == {"n": 1}
            assert other.queue.empty()  # untenanted subscription never sees a tenanted signal

            stop.set()
            await asyncio.wait_for(task, timeout=5)

    async def test_encrypted_stream_is_refused(self) -> None:
        sealed = StreamSpec(
            name="realtime",
            codec=PydanticModelCodec(model_type=RealtimeSignal),
            encryption="end_to_end",
        )

        with pytest.raises(CoreException) as caught:
            await _tail_to_hub(
                cast(ExecutionContext, None),
                hub=RealtimeSseHub(),
                stream_spec=sealed,
                batch=16,
                poll_interval=timedelta(milliseconds=10),
                stop=asyncio.Event(),
            )

        assert caught.value.code == "realtime_stream_encryption_unsupported"


# ----------------------- #


class TestLifecycleStep:
    async def test_startup_supervises_and_shutdown_stops(self) -> None:
        spec = realtime_stream_spec()
        hub = RealtimeSseHub()
        step = realtime_sse_tail_lifecycle_step(
            hub, stream_spec=spec, poll_interval=timedelta(milliseconds=10)
        )

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)
            first = step.startup.task  # type: ignore[attr-defined]
            assert first is not None and not first.done()
            assert step.startup.loop_name == "realtime_sse_tail"  # type: ignore[attr-defined]

            await step.startup(ctx)  # duplicate startup must not orphan the running task
            assert step.startup.task is first  # type: ignore[attr-defined]

            await step.shutdown(ctx)
            assert first.done()

    def test_invalid_settings_are_refused(self) -> None:
        spec = realtime_stream_spec()
        hub = RealtimeSseHub()

        with pytest.raises(CoreException):
            realtime_sse_tail_lifecycle_step(hub, stream_spec=spec, batch=0)

        with pytest.raises(CoreException):
            realtime_sse_tail_lifecycle_step(hub, stream_spec=spec, poll_interval=timedelta(0))

        with pytest.raises(CoreException):
            realtime_sse_tail_lifecycle_step(hub, stream_spec=spec, restart_backoff=timedelta(0))

        with pytest.raises(CoreException):
            realtime_sse_tail_lifecycle_step(hub, stream_spec=spec, max_consecutive_crashes=0)


# ----------------------- #


class _ScriptedPort:
    """Serves scripted read pages and records every (cursor, limit) request."""

    def __init__(self, pages: list[list[Any]]) -> None:
        self.pages = list(pages)
        self.reads: list[tuple[dict[str, str], int | None]] = []

    async def read(
        self, mapping: dict[str, str], *, limit: int | None = None, timeout: Any = None
    ) -> list[Any]:
        del timeout
        self.reads.append((dict(mapping), limit))

        return self.pages.pop(0) if self.pages else []


def _port_ctx(port: _ScriptedPort) -> Any:
    class _Deps:
        def resolve_configurable(self, ctx: Any, key: Any, spec: Any, *, route: Any) -> Any:
            del ctx, key, spec, route
            return port

    class _Ctx:
        deps = _Deps()

    return _Ctx()


def _msg(msg_id: str, n: int) -> Any:
    from forze.application.contracts.stream import StreamMessage

    return StreamMessage(
        stream="realtime",
        id=msg_id,
        payload=RealtimeSignal.of(Audience.topic("t"), "e", {"n": n}),
        headers={},
    )


class TestFastForwardBoundAndReadiness:
    async def test_short_read_ends_the_fast_forward_instead_of_chasing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import forze_fastapi.realtime.lifecycle as lifecycle_module

        monkeypatch.setattr(lifecycle_module, "_FAST_FORWARD_PAGE", 2)

        # page 1: full (keep skipping) · page 2: short (the tail — stop here, do NOT
        # read-until-empty) · page 3: what a chasing loop would also have swallowed
        port = _ScriptedPort(
            [
                [_msg("1-0", 1), _msg("2-0", 2)],
                [_msg("3-0", 3)],
                [_msg("4-0", 4)],
            ]
        )
        hub = RealtimeSseHub()
        hub.ready.clear()
        sub = hub.subscribe(principal="nobody", tenant=None, topics=frozenset({"t"}))
        stop = asyncio.Event()

        went_live = asyncio.Event()

        def _on_live() -> None:
            assert not hub.ready.is_set()  # fires exactly at the ff/live boundary
            went_live.set()
            hub.ready.set()

        task = asyncio.create_task(
            _tail_to_hub(
                _port_ctx(port),
                hub=hub,
                stream_spec=realtime_stream_spec(),
                batch=16,
                poll_interval=timedelta(milliseconds=10),
                stop=stop,
                on_live=_on_live,
            )
        )

        try:
            await asyncio.wait_for(went_live.wait(), timeout=5)

            signal, _ = await asyncio.wait_for(sub.queue.get(), timeout=5)
            assert signal.payload == {"n": 4}  # published live, not eaten as backlog

            # exactly two fast-forward reads (full page, then the short page), then live
            assert [limit for _, limit in port.reads[:2]] == [2, 2]
            assert port.reads[2][1] == 16

        finally:
            stop.set()
            await asyncio.wait_for(task, timeout=5)

    async def test_hub_starts_ready_and_step_restores_readiness_after_a_dead_tail(
        self,
    ) -> None:
        hub = RealtimeSseHub()
        assert hub.ready.is_set()  # manual/test hubs are never gated

        # an encrypted spec is a terminal config error: the supervised run exits and
        # the step's finally must restore readiness — a dead tail degrades to
        # catch-up quality, never a permanently gated connect
        sealed = StreamSpec(
            name="realtime",
            codec=PydanticModelCodec(model_type=RealtimeSignal),
            encryption="end_to_end",
        )
        step = realtime_sse_tail_lifecycle_step(
            hub, stream_spec=sealed, poll_interval=timedelta(milliseconds=10)
        )

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            task = step.startup.task  # type: ignore[attr-defined]
            assert task is not None
            await asyncio.wait_for(task, timeout=5)  # terminal: supervision exits

            assert hub.ready.is_set()
            await step.shutdown(ctx)

"""The per-node SSE live-tail loop, supervised like every other background loop.

One loop per node reads the realtime stream with the plain (non-group)
:class:`~forze.application.contracts.stream.StreamQueryPort` — broadcast semantics
with zero consumer-group lifecycle: every node sees every signal, which is exactly
what fanning out to node-local SSE connections needs. The loop fast-forwards past
the existing backlog at startup (the live leg starts at *now*; the mailbox covers
durables, and ephemerals before startup are contractually gone), then publishes
each new signal into the node's :class:`~forze_fastapi.realtime.RealtimeSseHub`.

Supervision matches the gateway: restart on crash with jittered backoff, terminal
on configuration errors, graceful stop at a read boundary, registered in
``ctx.drainables`` so the runtime drains it before lifecycle teardown.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import asyncio
from datetime import timedelta
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.envelope import HEADER_EVENT_ID, HEADER_TENANT_ID
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.realtime import RealtimeSignal
from forze.application.contracts.stream import StreamQueryDepKey, StreamSpec
from forze.application.execution import ExecutionContext
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
    run_supervised,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from .hub import RealtimeSseHub

# ----------------------- #

__all__ = [
    "realtime_sse_tail_lifecycle_step",
]

_IDLE_FLOOR = 0.05
"""Seconds: a small idle pause floor so a non-blocking backend can't hot-loop."""

_FAST_FORWARD_PAGE = 1000
"""Entries consumed per read while skipping the pre-startup backlog."""


def _tenant_from_headers(headers: object) -> UUID | None:
    """The (untrusted-at-this-layer) tenant a signal was published under, if any."""

    raw = headers.get(HEADER_TENANT_ID) if hasattr(headers, "get") else None  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]

    if not isinstance(raw, str) or not raw:
        return None

    try:
        return UUID(raw)

    except ValueError:
        return None


# ....................... #


def _refuse_encrypted_stream(spec: StreamSpec[RealtimeSignal]) -> None:
    """Fail closed: an encrypted realtime stream would forward ciphertext to browsers.

    Whole-payload stream encryption is consumer-decrypted everywhere in the framework
    and this loop has no decrypt seam — the same posture as the Socket.IO gateway.
    """

    if spec.encrypts:
        raise exc.configuration(
            f"Realtime stream {spec.name!r} declares encryption {spec.encryption!r}, "
            "but the SSE live tail has no decrypt seam — it would forward ciphertext "
            "to clients. Keep the realtime stream route plaintext (encryption='none').",
            code="realtime_stream_encryption_unsupported",
        )


# ....................... #


async def _tail_to_hub(
    ctx: ExecutionContext,
    *,
    hub: RealtimeSseHub,
    stream_spec: StreamSpec[RealtimeSignal],
    batch: int,
    poll_interval: timedelta,
    stop: asyncio.Event,
) -> None:
    """Fast-forward to the stream's tail, then publish every new signal into the hub."""

    _refuse_encrypted_stream(stream_spec)

    port = ctx.deps.resolve_configurable(
        ctx, StreamQueryDepKey, stream_spec, route=stream_spec.name
    )
    stream = str(stream_spec.name)
    cursor = {stream: "0"}

    # Start at *now*: page through the retained backlog without publishing. Backend-
    # neutral (no reliance on a "$" cursor) and bounded by the stream's retention cap.
    while not stop.is_set():
        backlog = await port.read(dict(cursor), limit=_FAST_FORWARD_PAGE, timeout=None)

        if not backlog:
            break

        cursor[stream] = backlog[-1].id

    while not stop.is_set():
        messages = await port.read(dict(cursor), limit=batch, timeout=poll_interval)

        for message in messages:
            cursor[stream] = message.id
            hub.publish(
                message.payload,
                _tenant_from_headers(message.headers),
                event_id=message.headers.get(HEADER_EVENT_ID),
            )

        if not messages:
            # the read timeout already paces blocking backends; this is a small
            # floor so a non-blocking backend cannot hot-loop.
            await asyncio.sleep(min(_IDLE_FLOOR, poll_interval.total_seconds()))


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _SseTailStartup(LifecycleHook):
    """Spawn the supervised per-node tail loop as a background task."""

    hub: RealtimeSseHub
    stream_spec: StreamSpec[RealtimeSignal]
    batch: int
    poll_interval: timedelta
    restart_backoff: timedelta
    max_consecutive_crashes: int | None

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(lambda: BackgroundLoopControl(name="realtime_sse_tail")),
        init=False,
    )
    """Stop signal and bounded teardown, shared with every other background loop."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # Validate synchronously — inside the detached task a raise would only surface
        # as a dead loop instead of failing the wiring at construction.
        if self.batch <= 0:
            raise exc.configuration("SSE tail batch must be positive")

        if self.poll_interval.total_seconds() <= 0:
            raise exc.configuration("SSE tail poll interval must be positive")

        if self.restart_backoff.total_seconds() <= 0:
            raise exc.configuration("Restart backoff must be positive")

        if self.max_consecutive_crashes is not None and self.max_consecutive_crashes <= 0:
            raise exc.configuration("Crash ceiling must be positive")

    # ....................... #

    @property
    def task(self) -> asyncio.Task[None] | None:
        """The running loop, if any."""

        return self.control.task

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop``."""

        return self.control.loop_name

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """Stop the tail at its next read boundary. Idempotent."""

        return await self.control.stop(deadline=deadline)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if self.control.running:
            return  # the runtime invokes startup once per scope; ignore a direct double call

        stop = self.control.arm()

        async def _run_once() -> None:
            await _tail_to_hub(
                ctx,
                hub=self.hub,
                stream_spec=self.stream_spec,
                batch=self.batch,
                poll_interval=self.poll_interval,
                stop=stop,
            )

        self.control.task = asyncio.create_task(
            run_supervised(
                _run_once,
                stop=stop,
                name=self.control.loop_name,
                restart_backoff=self.restart_backoff,
                max_consecutive_crashes=self.max_consecutive_crashes,
            ),
            name=self.control.loop_name,
        )
        ctx.drainables.register(self)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _SseTailShutdown(LifecycleHook):
    """Stop the tail loop — the fallback for a hand-driven lifecycle; idempotent."""

    startup: _SseTailStartup

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)


# ----------------------- #


def realtime_sse_tail_lifecycle_step(
    hub: RealtimeSseHub,
    *,
    stream_spec: StreamSpec[RealtimeSignal],
    batch: int = 64,
    poll_interval: timedelta = timedelta(seconds=1),
    restart_backoff: timedelta = timedelta(seconds=5),
    max_consecutive_crashes: int | None = None,
    step_id: StrKey = "realtime_sse_tail",
) -> LifecycleStep:
    """Build the supervised lifecycle step that tails the realtime stream into *hub*.

    Register it alongside :func:`~forze_fastapi.realtime.attach_realtime_sse_route`
    (sharing the same hub instance) on every node serving SSE connections. The plain
    stream read is broadcast — no consumer group, so no group lifecycle to manage —
    and at-most-once by design: the mailbox carries the durable guarantee.
    """

    startup = _SseTailStartup(
        hub=hub,
        stream_spec=stream_spec,
        batch=batch,
        poll_interval=poll_interval,
        restart_backoff=restart_backoff,
        max_consecutive_crashes=max_consecutive_crashes,
    )

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=_SseTailShutdown(startup=startup),
        requires_long_running=True,
    )

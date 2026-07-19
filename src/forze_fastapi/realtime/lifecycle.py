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
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.envelope import HEADER_EVENT_ID, HEADER_TENANT_ID
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.realtime import RealtimeShard, RealtimeSignal
from forze.application.contracts.stream import StreamQueryDepKey, StreamSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
    periodic_lifecycle_step,
    run_supervised,
)
from forze.application.integrations.realtime import RealtimePresence
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from .hub import RealtimeSseHub

# ----------------------- #

__all__ = [
    "realtime_sse_tail_lifecycle_step",
    "realtime_sse_sharded_tail_lifecycle_step",
    "realtime_sse_presence_heartbeat_lifecycle_step",
    "refresh_sse_presence",
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


def _header_tenant(message: Any) -> UUID | None:
    """The default tenant strategy: the (untrusted) ``forze_tenant_id`` header."""

    return _tenant_from_headers(message.headers)


async def _tail_to_hub(
    ctx: ExecutionContext,
    *,
    hub: RealtimeSseHub,
    stream_spec: StreamSpec[RealtimeSignal],
    batch: int,
    poll_interval: timedelta,
    stop: asyncio.Event,
    tenant_for: Callable[[Any], UUID | None] | None = None,
    on_live: Callable[[], None] | None = None,
) -> None:
    """Fast-forward to the stream's tail, then publish every new signal into the hub.

    *tenant_for* resolves the tenant a signal is published under: ``None`` (the
    tenant-global stream) reads the untrusted ``forze_tenant_id`` header; the sharded
    loop passes the **bound** shard tenant instead — the stream's identity, trusted.
    *on_live* fires once the fast-forward completes and live publishing begins — the
    runners flip ``hub.ready`` through it (the sharded runner only after every tenant).
    """

    _refuse_encrypted_stream(stream_spec)

    port = ctx.deps.resolve_configurable(
        ctx, StreamQueryDepKey, stream_spec, route=stream_spec.name
    )
    stream = str(stream_spec.name)
    cursor = {stream: "0"}
    tenant_of = tenant_for if tenant_for is not None else _header_tenant

    # Start at *now*: page through the retained backlog without publishing. Backend-
    # neutral (no reliance on a "$" cursor) and bounded by the stream's retention cap.
    # A short read means we are within one page of the tail — stop there instead of
    # reading until empty, which on an active stream would keep chasing concurrent
    # appends and silently classify them (and their live delivery) as backlog.
    while not stop.is_set():
        backlog = await port.read(dict(cursor), limit=_FAST_FORWARD_PAGE, timeout=None)

        if backlog:
            cursor[stream] = backlog[-1].id

        if len(backlog) < _FAST_FORWARD_PAGE:
            break

    if on_live is not None:
        on_live()

    while not stop.is_set():
        messages = await port.read(dict(cursor), limit=batch, timeout=poll_interval)

        for message in messages:
            cursor[stream] = message.id
            hub.publish(
                message.payload,
                tenant_of(message),
                event_id=message.headers.get(HEADER_EVENT_ID),
            )

        if not messages:
            # the read timeout already paces blocking backends; this is a small
            # floor so a non-blocking backend cannot hot-loop.
            await asyncio.sleep(min(_IDLE_FLOOR, poll_interval.total_seconds()))


# ....................... #


async def _sharded_tail_to_hub(
    ctx: ExecutionContext,
    *,
    hub: RealtimeSseHub,
    shard: RealtimeShard,
    batch: int,
    poll_interval: timedelta,
    restart_backoff: timedelta,
    max_consecutive_crashes: int | None,
    stop: asyncio.Event,
) -> None:
    """One tail loop per assigned tenant, each bound to it — the namespace-tier twin.

    The tenant-global loop trusts a header; here the realtime stream route is wired
    ``tenant_aware``, so each loop binds its shard tenant, resolves the port to that
    tenant's key/partition, and publishes under the **stream's** identity — no header
    trust. Each tenant loop is individually supervised (``run_supervised`` absorbs a
    terminal error by logging it), so one tenant's fault degrades that tenant's live
    fan-out only — the siblings keep tailing.
    """

    _refuse_encrypted_stream(shard.stream_spec)

    tenants = list(shard.tenants)

    if not tenants:
        # Nothing assigned: idle until stopped. Returning early would look like a
        # crash to the outer supervision and restart-loop for no reason.
        hub.ready.set()  # no tenant to fast-forward — nothing gates the connections
        await stop.wait()
        return

    # Readiness counts down across the shard: the hub is live once every tenant's
    # fast-forward has completed (a per-tenant supervised restart later re-fires the
    # callback past zero — harmless, readiness only ever latches on).
    pending = {"n": len(tenants)}

    def _one_live() -> None:
        pending["n"] -= 1

        if pending["n"] <= 0:
            hub.ready.set()

    def _tenant_runner(tenant: UUID) -> Callable[[], Awaitable[None]]:
        def _shard_tenant(_message: Any) -> UUID | None:
            return tenant  # the stream's identity, not a per-message header

        async def _run() -> None:
            # Bound for the whole loop, in this task's own copied context (sibling
            # bindings never race): the port resolves to the tenant's key/partition
            # and every published signal carries the trusted shard tenant.
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                await _tail_to_hub(
                    ctx,
                    hub=hub,
                    stream_spec=shard.stream_spec,
                    batch=batch,
                    poll_interval=poll_interval,
                    stop=stop,
                    tenant_for=_shard_tenant,
                    on_live=_one_live,
                )

        return _run

    async with asyncio.TaskGroup() as tasks:
        for tenant in tenants:
            tasks.create_task(
                run_supervised(
                    _tenant_runner(tenant),
                    stop=stop,
                    name=f"realtime_sse_tail:{tenant}",
                    restart_backoff=restart_backoff,
                    max_consecutive_crashes=max_consecutive_crashes,
                ),
                name=f"realtime_sse_tail:{tenant}",
            )


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _SseTailStartup(LifecycleHook):
    """Spawn a supervised per-node loop (plain or sharded tail) as a background task."""

    runner: Callable[[ExecutionContext, asyncio.Event], Awaitable[None]]
    name: str
    restart_backoff: timedelta
    max_consecutive_crashes: int | None

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(lambda self: BackgroundLoopControl(name=self.name), takes_self=True),
        init=False,
    )
    """Stop signal and bounded teardown, shared with every other background loop."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # Validate synchronously — inside the detached task a raise would only surface
        # as a dead loop instead of failing the wiring at construction.
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
            await self.runner(ctx, stop)

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

    _validate_tail_settings(batch=batch, poll_interval=poll_interval)

    async def _run(ctx: ExecutionContext, stop: asyncio.Event) -> None:
        # Gate connections while the fast-forward runs (see RealtimeSseHub.ready);
        # always restore readiness on exit so a dead tail degrades to catch-up mode
        # instead of holding every connect at the gate.
        hub.ready.clear()

        try:
            await _tail_to_hub(
                ctx,
                hub=hub,
                stream_spec=stream_spec,
                batch=batch,
                poll_interval=poll_interval,
                stop=stop,
                on_live=hub.ready.set,
            )

        finally:
            hub.ready.set()

    startup = _SseTailStartup(
        runner=_run,
        name="realtime_sse_tail",
        restart_backoff=restart_backoff,
        max_consecutive_crashes=max_consecutive_crashes,
    )

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=_SseTailShutdown(startup=startup),
        requires_long_running=True,
    )


# ....................... #


def _validate_tail_settings(*, batch: int, poll_interval: timedelta) -> None:
    # Validated at the builder, synchronously — inside the detached task a raise would
    # only surface as a dead loop instead of failing the wiring at construction.
    if batch <= 0:
        raise exc.configuration("SSE tail batch must be positive")

    if poll_interval.total_seconds() <= 0:
        raise exc.configuration("SSE tail poll interval must be positive")


# ....................... #


def realtime_sse_sharded_tail_lifecycle_step(
    hub: RealtimeSseHub,
    *,
    shard: RealtimeShard,
    batch: int = 64,
    poll_interval: timedelta = timedelta(seconds=1),
    restart_backoff: timedelta = timedelta(seconds=5),
    max_consecutive_crashes: int | None = None,
    step_id: StrKey = "realtime_sse_sharded_tail",
) -> LifecycleStep:
    """The namespace-tier twin of :func:`realtime_sse_tail_lifecycle_step`.

    For a realtime stream route wired ``tenant_aware``: one supervised tail loop per
    tenant in *shard*, each bound to its tenant, so signals fan out under the
    **stream's** trusted identity instead of an untrusted header — the SSE analog of
    ``TenantShardedSignalSource``. Hand the same :class:`RealtimeShard` to the
    publish-side per-tenant steps so the components cannot drift; assign disjoint
    shards across nodes' *serving* processes only if each tenant's SSE clients are
    routed to the node holding that tenant — otherwise give every SSE-serving node
    the full tenant set (broadcast reads don't contend, unlike consumer groups).
    """

    _validate_tail_settings(batch=batch, poll_interval=poll_interval)

    async def _run(ctx: ExecutionContext, stop: asyncio.Event) -> None:
        # Gate connections until every tenant's fast-forward completes; always
        # restore readiness on exit (a dead shard degrades to catch-up, never a
        # permanently gated connect).
        hub.ready.clear()

        try:
            await _sharded_tail_to_hub(
                ctx,
                hub=hub,
                shard=shard,
                batch=batch,
                poll_interval=poll_interval,
                restart_backoff=restart_backoff,
                max_consecutive_crashes=max_consecutive_crashes,
                stop=stop,
            )

        finally:
            hub.ready.set()

    startup = _SseTailStartup(
        runner=_run,
        name="realtime_sse_sharded_tail",
        restart_backoff=restart_backoff,
        max_consecutive_crashes=max_consecutive_crashes,
    )

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=_SseTailShutdown(startup=startup),
        requires_long_running=True,
    )


# ----------------------- #


async def refresh_sse_presence(hub: RealtimeSseHub, presence: RealtimePresence) -> int:
    """Re-assert presence for every live SSE subscription on this node; return how many.

    A TTL-backed presence store expires entries so a crashed node's rows don't leak —
    which means open streams must re-assert (heartbeat) or they'd wrongly expire too.
    The hub's subscription set *is* this node's SSE connection registry, so no extra
    bookkeeping exists to drift from it.
    """

    refreshed = 0

    for subscription in hub.subscriptions:
        for room in subscription.rooms():
            await presence.joined(room, subscription.key)

        refreshed += 1

    return refreshed


# ....................... #


def realtime_sse_presence_heartbeat_lifecycle_step(
    hub: RealtimeSseHub,
    presence: RealtimePresence,
    *,
    interval: timedelta = timedelta(seconds=30),
    step_id: StrKey = "realtime_sse_presence_heartbeat",
) -> LifecycleStep:
    """Periodically re-assert presence for this node's open SSE streams.

    The SSE twin of the Socket.IO presence heartbeat: required by a TTL-backed store
    (use an *interval* comfortably shorter than the store's TTL), harmless with the
    in-memory tracker. Share the same *hub* the route and the tail step use, and the
    same *presence* store the Socket.IO side reports into.
    """

    async def _tick() -> None:
        await refresh_sse_presence(hub, presence)

    return periodic_lifecycle_step(
        tick=_tick, interval=interval, name="realtime_sse_presence_heartbeat", step_id=step_id
    )

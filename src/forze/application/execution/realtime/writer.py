"""The transport-neutral mailbox writer — offline delivery without a Socket.IO server.

The SSE and raw-WebSocket routes replay the offline mailbox on connect and ack against it,
but they never write it: their live leg is a per-node broadcast tail. Something has to
consume the realtime stream through a consumer group and store each durable,
principal-addressed signal, and until now only the Socket.IO gateway did. A deployment that
serves no Socket.IO runs :class:`RealtimeMailboxWriter` instead: the gateway's store half and
nothing else, through the same :class:`~.delivery.RealtimeDelivery` the gateway uses, so the
two cannot drift.
"""

import asyncio
from collections.abc import Callable
from datetime import timedelta
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.realtime import RealtimeEventCatalog, RealtimeSignal
from forze.application.execution import ExecutionContext
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
    run_supervised,
)
from forze.application.integrations.realtime import RealtimeMailbox
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import HlcTimestamp, StrKey

from .delivery import GatewayDedup, RealtimeDelivery, claim_dedup_inbox
from .observability import RealtimeGatewayStats
from .sources import PubSubSignalSource, RealtimeSignalSource, require_tenant_global_source

# ----------------------- #


def _mailbox_tenant_unbound(event_id: str | None) -> CoreException:
    """The per-signal error for a tenant-aware mailbox with no tenant to scope by.

    Deliberately **not** a configuration error, for the reason the gateway's twin gives: a
    missing tenant is a per-message data condition, and a configuration verdict would stop
    the consume loop for every tenant. The signal stays pending and the poison ceiling
    bounds it.
    """

    return exc.precondition(
        "Realtime mailbox writer cannot store into a tenant-aware mailbox: no tenant is "
        "bound for this signal. The writer has no ambient tenant of its own, so on the "
        "tenant-global stream the only tenant source is the forze_tenant_id header. Either "
        "set RealtimeMailboxWriter.bind_tenant_from_headers=True to bind it (the header "
        "must be present on every signal, and is untrusted/forgeable — enable only where "
        "every stream producer is trusted to assert tenancy), consume per-tenant streams "
        "with TenantShardedSignalSource, or wire a tenant-global mailbox route "
        "(tenant_aware=False). The signal stays pending until the delivery ceiling drops it.",
        code="realtime_mailbox_tenant_unbound",
        details={"event_id": event_id},
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RealtimeMailboxWriter:
    """Store durable principal signals from the realtime stream into the offline mailbox.

    For each signal the *source* hands it: admit it against the catalog, apply the tenancy
    rules, and — for a durable signal addressed to a principal that the catalog lets go
    offline — mark it seen and store it in one transaction. Nothing is emitted: live
    delivery stays with the SSE and WebSocket hubs, and ephemeral, topic and opted-out
    signals are left unmarked.

    Delivery is the gateway's: at-least-once off the consumer group, exactly one mailbox row
    per signal through the dedup mark, a mark that never stands without its row. Run it on
    as many nodes as you like with one consumer group — a signal goes to one of them, a
    crashed node's entries are reclaimed, and a redelivery finds the mark.

    Ordering is the gateway's too. A row is stored at the signal's carried HLC, and replay
    serves the rows above a device's cursor, but rows are not stored in HLC order: a
    reclaimed entry, another node, or an out-of-order append can store a row below a cursor
    the device has already acked past, and that row is never replayed. One writer per
    stream or tenant shard narrows this window to reclaims and out-of-order appends; it
    does not close it.

    Never run it beside a ``RealtimeGateway`` that shares its dedup inbox, with or without
    a mailbox: whichever marks a signal first owns it, so the gateway skips its live emit or
    the writer never stores the row. In one process the second to start is refused
    (``realtime_dedup_inbox_shared``); across processes it is the deployment's to avoid.
    """

    source: RealtimeSignalSource
    """Where signals come from: a ``StreamGroupSignalSource`` over the realtime stream, or
    ``TenantShardedSignalSource`` for per-tenant streams. A ``PubSubSignalSource`` is
    refused — pub/sub keeps nothing for a writer that was not subscribed."""

    dedup: GatewayDedup
    """The inbox the dedup mark goes to, and the transaction route it commits on."""

    mailbox_factory: Callable[[ExecutionContext], RealtimeMailbox]
    """Builds the mailbox once at ``run(ctx)`` start, with its ports resolved — the same
    factory the SSE and WebSocket routes replay from, and the place its retention is
    declared (see ``RealtimeGateway.mailbox_factory``)."""

    event_catalog: RealtimeEventCatalog | None = None
    """Closes the admitted surface and carries the per-event ``offline_delivery`` opt-out;
    without one every durable principal signal is stored as published."""

    bind_tenant_from_headers: bool = False
    """Opt-in: bind each signal's ``forze_tenant_id`` header so a tenant-aware mailbox
    scopes by it. The header is not authenticated: on a tenant-global stream, whoever can
    append to the stream chooses the tenant whose mailbox a signal is stored in. Enable it
    only where every producer is trusted to assert tenancy, the gateway's posture; otherwise
    use ``TenantShardedSignalSource``, which takes the tenant from the stream itself."""

    require_tenant: bool | None = None
    """Drop, loudly, a signal that resolves no tenant. ``None`` follows
    :attr:`bind_tenant_from_headers`, as on the gateway."""

    stats: RealtimeGatewayStats | None = None
    """Delivery counters (stores, dedup skips, admission rejects, untenanted drops). Share
    the same instance with the source and hand it to ``instrument_realtime_gateway``."""

    _delivery: RealtimeDelivery = attrs.field(
        init=False,
        repr=False,
        eq=False,
        default=attrs.Factory(
            lambda self: RealtimeDelivery(
                tenant_unbound=_mailbox_tenant_unbound,
                dedup=self.dedup,
                event_catalog=self.event_catalog,
                bind_tenant_from_headers=self.bind_tenant_from_headers,
                require_tenant=self.require_tenant,
                stats=self.stats,
            ),
            takes_self=True,
        ),
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if isinstance(self.source, PubSubSignalSource):
            raise exc.configuration(
                "RealtimeMailboxWriter cannot consume a PubSubSignalSource: pub/sub is "
                "at-most-once with no redelivery, so a durable signal published while no "
                "writer is subscribed would never reach the mailbox. Consume the realtime "
                "stream with StreamGroupSignalSource (or TenantShardedSignalSource).",
                code="realtime_mailbox_writer_source_unsupported",
            )

    # ....................... #

    async def run(self, ctx: ExecutionContext, *, stop: asyncio.Event | None = None) -> None:
        """Consume signals and store each durable one, until *stop* (forever when ``None``).

        A stop request is honored at the source's unit boundary (an in-flight batch
        finishes and acks first).

        :raises CoreException: ``configuration`` (``realtime_dedup_inbox_shared``) when a
            gateway already runs on the same dedup inbox in this process.
        """

        # resolve the mailbox's ports once, against the run ctx (worker-resolved-once)
        mailbox = self.mailbox_factory(ctx)

        async def handle(
            signal: RealtimeSignal,
            tenant: UUID | None,
            dedup_id: str | None,
            hlc: HlcTimestamp,
        ) -> None:
            await self._handle(ctx, mailbox, signal, tenant, dedup_id, hlc)

        with claim_dedup_inbox(self.dedup, "RealtimeMailboxWriter"):
            await self.source.run(ctx, handle, stop=stop)

    # ....................... #

    async def _handle(
        self,
        ctx: ExecutionContext,
        mailbox: RealtimeMailbox,
        signal: RealtimeSignal,
        tenant: UUID | None,
        dedup_id: str | None,
        hlc: HlcTimestamp,
    ) -> None:
        await self._delivery.deliver(ctx, mailbox, signal, tenant, dedup_id, hlc)


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _MailboxWriterStartup(LifecycleHook):
    """Spawn the writer's supervised ``run`` loop as a background task."""

    writer: RealtimeMailboxWriter
    restart_backoff: timedelta
    max_consecutive_crashes: int | None

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(lambda: BackgroundLoopControl(name="realtime_mailbox_writer")),
        init=False,
    )
    """Stop signal and bounded teardown, shared with every other background loop."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # Validated here, synchronously — inside the detached task a raise would only
        # surface as a dead loop instead of failing the wiring at construction.
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
        """Stop the writer at its next batch boundary. Idempotent."""

        return await self.control.stop(deadline=deadline)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if self.control.running:
            return  # the runtime invokes startup once per scope; ignore a direct double call

        require_tenant_global_source(self.writer.source, ctx, what="Realtime mailbox writer")

        stop = self.control.arm()

        async def _run_once() -> None:
            await self.writer.run(ctx, stop=stop)

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
class _MailboxWriterShutdown(LifecycleHook):
    """Stop the writer loop — the fallback for a hand-driven lifecycle; idempotent."""

    startup: _MailboxWriterStartup

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)


# ....................... #


def realtime_mailbox_writer_lifecycle_step(
    writer: RealtimeMailboxWriter,
    *,
    restart_backoff: timedelta = timedelta(seconds=5),
    max_consecutive_crashes: int | None = None,
    step_id: StrKey = "realtime_mailbox_writer",
) -> LifecycleStep:
    """Build the supervised lifecycle step that runs *writer* for the process lifetime.

    Register it where no mailboxing Socket.IO gateway runs, beside the consumer group's
    ensure step and the outbox relay. Supervision matches the gateway's: restart on crash
    with jittered backoff, terminal on a configuration error, stop at a batch boundary,
    drained before lifecycle teardown.

    :param writer: The writer to run.
    :param restart_backoff: Base backoff between restarts after a crash (jittered ×[1.0, 1.5)).
    :param max_consecutive_crashes: Optional terminal ceiling on short-lived runs; ``None``
        restarts forever (every crash is still logged loudly).
    :param step_id: The step's id.
    :raises CoreException: ``configuration`` — *restart_backoff* or
        *max_consecutive_crashes* is not positive; at startup, ``tenant_global_required``
        for a tenant-global source over a ``tenant_aware`` stream route.
    """

    startup = _MailboxWriterStartup(
        writer=writer,
        restart_backoff=restart_backoff,
        max_consecutive_crashes=max_consecutive_crashes,
    )

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=_MailboxWriterShutdown(startup=startup),
        requires_long_running=True,
    )

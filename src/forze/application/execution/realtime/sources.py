"""Realtime signal sources — read the realtime stream, hand each signal to a handler once.

Transport-neutral: a source decodes signals from a messaging substrate (a stream consumer
group, a pubsub channel, per-tenant streams) and invokes a per-signal handler, acking per the
durable/ephemeral policy. The Socket.IO gateway runs one to emit to rooms, and
:class:`~forze.application.execution.realtime.RealtimeMailboxWriter` runs one to fill the
offline mailbox for the SSE and WebSocket routes. Neither carries its own copy of the consume
loop: redelivery, the poison ceiling and the drain ladder are part of the delivery guarantee.

It sits in the execution layer because it needs the
:class:`~forze.application.execution.ExecutionContext`, the drain gate and supervision.
"""

import asyncio
import os
import socket
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from datetime import timedelta
from typing import Any, Protocol, cast, final, runtime_checkable
from uuid import UUID

import attrs

from forze.application._logger import logger as _default_logger
from forze.application.contracts.envelope import (
    HEADER_EVENT_ID,
    HEADER_HLC,
    HEADER_TENANT_ID,
    HEADER_TRACEPARENT,
)
from forze.application.contracts.pubsub import PubSubQueryDepKey, PubSubSpec
from forze.application.contracts.realtime import (
    DEFAULT_REALTIME_GROUP,
    RealtimeShard,
    RealtimeSignal,
)
from forze.application.contracts.stream import AckStreamGroupQueryDepKey, StreamSpec
from forze.application.contracts.tenancy import TenantIdentity, require_tenant_global
from forze.application.execution import ExecutionContext, is_draining_refusal
from forze.application.execution.background import run_supervised
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.logging import Logger
from forze.base.primitives import HlcTimestamp, JsonDict, utcnow

from .observability import RealtimeGatewayStats

# ----------------------- #

_IDLE_FLOOR = 0.05
"""Seconds: a small idle pause floor so a non-blocking backend can't hot-loop."""

_POISON_SCAN_FLOOR = 256
"""Minimum pending-entries scanned per reclaim tick for poison delivery counts."""

SignalHandler = Callable[[RealtimeSignal, UUID | None, str | None, HlcTimestamp], Awaitable[None]]
"""A per-signal bridge: a decoded signal, its tenant, a dedup id, and its HLC position.

The dedup id is the durable ``forze_event_id`` (``None`` for ephemeral signals); the
HLC is the carried ``forze_hlc`` (or a wall-clock fallback) used for mailbox ordering.
"""

# ....................... #


def _logger_or_default(logger: Logger | None) -> Logger:
    """A source's own logger, or the application logger when it names none."""

    return logger if logger is not None else _default_logger


# ....................... #


def _default_consumer() -> str:
    """A consumer name unique to this process (host + pid).

    Redis consumer-group semantics key the pending-entries list by consumer name, so two
    gateway instances sharing a name would each claim/reclaim the *other's* in-flight entries
    (double-processing, broken idle recovery). One stable, distinct name per process avoids it.
    """

    return f"{socket.gethostname()}-{os.getpid()}"


# ....................... #


def _tenant_from_headers(headers: object) -> UUID | None:
    """Extract the tenant id from the headers."""

    if not hasattr(headers, "get"):
        return None

    # dirty cast to supress pyright
    headers = cast(JsonDict, headers)
    raw = headers.get(HEADER_TENANT_ID)

    if not raw:
        return None

    try:
        return UUID(str(raw))  # str(): a JSON-decoded number reaches UUID as a non-str
    except (ValueError, TypeError, AttributeError):
        # the header is untrusted input — a malformed value is dropped, not raised (raising
        # would fail the bridge and reclaim-loop the message forever)
        return None


# ....................... #


def _hlc_from_headers(headers: object) -> HlcTimestamp:
    """The carried HLC (``forze_hlc``), or a wall-clock fallback when absent.

    The durable relay forwards the outbox HLC on HLC-ordering backends; when no
    HLC is carried, a ``(now_ms, 0)`` stamp keeps mailbox ordering wall-clock-close.
    """

    raw = (  # pyright: ignore[reportUnknownVariableType]
        headers.get(  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
            HEADER_HLC
        )
        if hasattr(headers, "get")
        else None
    )

    if raw:
        # untrusted header — a malformed value falls back to a wall-clock stamp rather than
        # raising (which would fail the bridge and reclaim-loop the message). str(): a
        # JSON-decoded number reaches parse() as a non-str. CoreException: parse() raises
        # exc.validation (not ValueError) on a malformed string, which must fall back too.
        with suppress(ValueError, TypeError, AttributeError, CoreException):
            return HlcTimestamp.parse(
                str(raw)  # pyright: ignore[reportUnknownArgumentType]
            )

    return HlcTimestamp(physical_ms=int(utcnow().timestamp() * 1000), logical=0)


def _refuse_encrypted_realtime_stream(
    spec: StreamSpec[RealtimeSignal] | PubSubSpec[RealtimeSignal],
) -> None:
    """Fail closed on an encryption tier the gateway cannot honor.

    Whole-payload messaging encryption is consumer-decrypted everywhere in the framework —
    and the gateway has no decrypt seam, so an encrypted realtime route would emit
    ciphertext envelopes to browsers and call it delivery. Refusing at run start (the
    resolve seam every source passes through) beats defining a decrypt path nobody has
    asked for; the realtime channel is plaintext by design, isolation comes from room
    membership and tenancy tiers.
    """

    if spec.encrypts:
        raise exc.configuration(
            f"Realtime route {spec.name!r} declares encryption "
            f"{spec.encryption!r}, but the gateway has no decrypt seam — it would "
            "emit ciphertext to clients. Keep the realtime route plaintext "
            "(encryption='none'); confidentiality on the wire is the transport's "
            "TLS, isolation is room membership / tenancy tier.",
            code="realtime_stream_encryption_unsupported",
        )


# ....................... #


@contextmanager
def _trace_context(headers: object, *, stream: str) -> Iterator[None]:
    """Bridge under the producer's W3C trace context, when the signal carries one.

    Completes the outbox→relay→stream propagation chain: the relay forwards
    ``traceparent`` onto the stream row, and this attaches it around the bridge so the
    dedup mark, the mailbox store, and the emit all land in the producing operation's
    distributed trace — plus one explicit CONSUMER span for the bridge itself, so the
    emit leg is visible even without port spans. Naturally gated: a signal with no
    traceparent (untraced producer, tracing off) pays nothing but a dict lookup.
    """

    raw = (  # pyright: ignore[reportUnknownVariableType]
        headers.get(  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
            HEADER_TRACEPARENT
        )
        if hasattr(headers, "get")
        else None
    )

    if not isinstance(raw, str) or not raw:
        yield
        return

    from opentelemetry import context as otel_context
    from opentelemetry import trace

    from forze.application.execution.tracing.propagation import context_from_traceparent

    token = otel_context.attach(context_from_traceparent(raw))

    try:
        with trace.get_tracer("forze").start_as_current_span(
            "realtime.gateway.bridge",
            kind=trace.SpanKind.CONSUMER,
            attributes={"forze.realtime.stream": stream},
        ):
            yield
    finally:
        otel_context.detach(token)


# ----------------------- #


@runtime_checkable
class RealtimeSignalSource(Protocol):
    """A source of realtime signals — decode from *some* substrate, deliver once.

    ``run`` reads signals and invokes *handler* for each, acknowledging only after
    the handler returns, so each signal is delivered to exactly one gateway.
    """

    def run(
        self,
        ctx: ExecutionContext,
        handler: SignalHandler,
        *,
        stop: asyncio.Event | None = None,
    ) -> Awaitable[None]:
        """Consume signals until *stop* is set (forever when ``None``)."""

        ...  # pragma: no cover


# ....................... #


async def _process_messages(
    *,
    group: Any,
    group_name: str,
    stream: str,
    messages: list[Any],
    handler: SignalHandler,
    tenant_for: Callable[[Any], UUID | None],
    delivery_counts: Mapping[str, int] | None = None,
    max_deliveries: int | None = None,
    stats: RealtimeGatewayStats | None = None,
    logger: Logger,
) -> None:
    """Bridge each message to *handler*, acking per the durable/ephemeral policy.

    *tenant_for* resolves the tenant handed to the handler — from the message header
    (tenant-global stream) or the bound shard tenant (tenant-aware stream).
    A durable signal (carries an event id) is acked only on success, so a transient
    failure stays pending and is recovered (at-least-once); an ephemeral signal is
    acked regardless, so one bad signal can never wedge the live stream (at-most-once).

    The at-least-once lane needs a ceiling: a durable signal whose bridge fails on
    *every* delivery would otherwise be reclaimed and retried forever. On the reclaim
    pass the caller supplies each entry's *delivery count*; a durable failure at or past
    *max_deliveries* is acked and **dropped** — loudly — instead of redelivered. Dropping
    is the honest bound: a mailboxed principal signal was already stored on its first
    successful mark (the client recovers it on reconnect), and for the rest an unbounded
    silent retry loop is strictly worse than a bounded, observable loss.
    """

    for message in messages:
        durable = HEADER_EVENT_ID in message.headers
        dedup_id = message.headers.get(HEADER_EVENT_ID)
        ack = True
        poisoned = False
        deliveries: int | None = None

        try:
            with _trace_context(message.headers, stream=stream):
                await handler(
                    message.payload,
                    tenant_for(message),
                    dedup_id,
                    _hlc_from_headers(message.headers),
                )

        except Exception as error:
            # Draining refusal (the runtime is quiescing): a shutdown artifact, never a
            # delivery. Nothing was emitted, so neither ack lane applies — acking the
            # ephemeral lane would *lose* an undelivered signal, and counting the
            # durable lane could push a healthy signal to the poison ceiling across
            # deploys. Re-raise: the loop stops (the gate is one-way — the rest of the
            # batch would only re-refuse), everything unacked redelivers to the next
            # process. Same ladder as the kits queue/commit-stream runners.
            if is_draining_refusal(error):
                raise

            # A deterministic wiring error (a misrouted spec, a missing dep) never succeeds
            # on retry — re-raise to fail fast instead of leaving the durable message
            # pending and reclaim-looping it forever. The message stays unacked, so it
            # redelivers once the operator fixes the wiring and restarts. Per-message data
            # conditions (an untenanted signal against a tenant-aware mailbox) are NOT
            # configuration errors: they park below and the poison ceiling bounds them.
            if isinstance(error, CoreException) and error.kind is ExceptionKind.CONFIGURATION:
                raise

            deliveries = (delivery_counts or {}).get(message.id)
            poisoned = (
                durable
                and max_deliveries is not None
                and deliveries is not None
                and deliveries >= max_deliveries
            )

            if not poisoned and stats is not None:
                stats.bridge_failed += 1

            logger.critical_exception(
                "Realtime bridge failed", stream=stream, message_id=message.id
            )

            ack = poisoned or not durable

        if ack:
            await group.ack(group=group_name, stream=stream, ids=[message.id])

            if poisoned:
                # The drop is only a fact once the ack lands — an ack that failed would
                # leave the entry pending (redelivered), and a "dropped" log or counter
                # written before it would have been a lie.
                if stats is not None:
                    stats.poisoned += 1

                logger.critical(
                    "Realtime signal poisoned: dropped after repeated delivery failures",
                    stream=stream,
                    message_id=message.id,
                    event_id=dedup_id,
                    deliveries=deliveries,
                )


# ....................... #


async def _consume_group_stream(
    *,
    group: Any,
    stream: str,
    group_name: str,
    consumer: str,
    batch: int,
    poll_interval: timedelta,
    reclaim_idle: timedelta | None,
    handler: SignalHandler,
    tenant_for: Callable[[Any], UUID | None],
    stop: asyncio.Event | None = None,
    max_deliveries: int | None = None,
    stats: RealtimeGatewayStats | None = None,
    logger: Logger,
) -> None:
    """Consume one stream's consumer group: read, reclaim, bridge, ack — until *stop*.

    The loop body shared by every source (tenant-global and per-tenant): the sources
    differ only in how the *group* port is resolved and how *tenant_for* derives the
    tenant. A transient broker error is logged and retried, never fatal. The batch is
    the unit boundary: a stop request is honored between cycles, so an in-flight batch
    finishes (and acks) before the loop returns — never mid-signal.
    """

    mapping = {stream: ">"}

    while True:
        if stop is not None and stop.is_set():
            return

        try:
            fresh = await group.read(
                group_name, consumer, mapping, limit=batch, timeout=poll_interval
            )
            await _process_messages(
                group=group,
                group_name=group_name,
                stream=stream,
                messages=fresh,
                handler=handler,
                tenant_for=tenant_for,
                stats=stats,
                logger=logger,
            )

            reclaimed: list[Any] = []
            if reclaim_idle is not None:
                reclaimed = await group.claim(
                    group_name, consumer, stream, idle=reclaim_idle, limit=batch
                )

                delivery_counts: dict[str, int] = {}
                if reclaimed and max_deliveries is not None:
                    # The claim itself counted as a delivery, so the snapshot taken here
                    # includes the attempt about to run — a poison entry is dropped on
                    # the attempt that reaches the ceiling, not one redelivery later.
                    # Bounded scan: the PEL can be large, and this runs every reclaim
                    # tick. Reclaim takes the oldest idle entries, which cluster at the
                    # front of the id-ordered PEL, so a scan a few times the batch deep
                    # covers them; an entry past the bound simply is not counted this
                    # round (the safe direction — it survives and is re-examined as it
                    # ages toward the front, never dropped on an unknown count).
                    delivery_counts = {
                        entry.id: entry.delivery_count
                        for entry in await group.pending(
                            group_name, stream, limit=max(4 * batch, _POISON_SCAN_FLOOR)
                        )
                        if entry.delivery_count is not None
                    }

                await _process_messages(
                    group=group,
                    group_name=group_name,
                    stream=stream,
                    messages=reclaimed,
                    handler=handler,
                    tenant_for=tenant_for,
                    delivery_counts=delivery_counts,
                    max_deliveries=max_deliveries,
                    stats=stats,
                    logger=logger,
                )

            if not fresh and not reclaimed:
                # the read timeout already paces blocking backends; this is a small
                # floor so a non-blocking backend cannot hot-loop.
                await asyncio.sleep(min(_IDLE_FLOOR, poll_interval.total_seconds()))

        except asyncio.CancelledError:
            raise

        except CoreException as error:
            if is_draining_refusal(error):
                # The runtime is quiescing: everything unacked (the refused message and
                # the batch tail) stays pending for the next process. Return cleanly —
                # continuing would re-refuse against the one-way gate; supervision's
                # backoff paces any restart until the stop signal lands.
                logger.info(
                    "Realtime gateway draining: stopping consume for %s; "
                    "unacked signals will be redelivered",
                    stream,
                )
                return

            if error.kind is ExceptionKind.CONFIGURATION:
                raise  # a wiring error won't fix itself by retrying — let the task exit (logged)

            logger.critical_exception("Realtime gateway loop error", stream=stream)
            await asyncio.sleep(poll_interval.total_seconds())

        except Exception:
            logger.critical_exception("Realtime gateway loop error", stream=stream)
            await asyncio.sleep(poll_interval.total_seconds())


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StreamGroupSignalSource(RealtimeSignalSource):
    """A signal source backed by a stream **consumer group** (exactly-once delivery).

    Each signal goes to exactly one consumer in *group*, so multiple gateway
    instances share the load without double-emitting. A handler that raises is
    logged and the message is still acknowledged (ephemeral is at-most-once; the
    durable path dedupes downstream), so one bad signal cannot wedge the stream.
    """

    stream_spec: StreamSpec[RealtimeSignal]
    """The realtime stream to consume (same spec the publisher appends to)."""

    group: str = DEFAULT_REALTIME_GROUP
    """Consumer group name shared by all gateway instances."""

    consumer: str = attrs.field(factory=_default_consumer)
    """This instance's consumer name within the group — defaults **unique per process**
    (host + pid) so multiple instances don't clobber each other's pending-entries list."""

    batch: int = 64
    """Maximum signals to read per poll."""

    poll_interval: timedelta = timedelta(seconds=1)
    """Block timeout for one group read."""

    reclaim_idle: timedelta | None = timedelta(seconds=60)
    """Reclaim entries stranded (delivered, unacked) at least this long.

    Recovers durable signals whose consumer died after read but before ack (the
    ``">"`` cursor never redelivers them): each tick claims stale pending entries
    and reprocesses them — deduped, so a recovered durable signal still emits at
    most once. ``None`` disables recovery (e.g. a single ephemeral-only node).
    """

    max_deliveries: int | None = 5
    """Ceiling on deliveries of one durable signal before it is dropped as poison.

    A durable signal whose bridge fails on every delivery would otherwise be reclaimed
    and retried forever; at the ceiling it is acked and dropped, with a critical log.
    A mailboxed principal signal is never affected (its store commits with the first
    successful mark, so the client recovers it on reconnect) — this bounds the
    at-least-once lane (topic durable, ``offline_delivery=False``), where an unbounded
    silent retry loop is strictly worse than a bounded, observable loss. ``None``
    restores unbounded redelivery.
    """

    stats: RealtimeGatewayStats | None = None
    """Delivery counters (bridge failures, poison drops). Share the **same** instance
    with :attr:`RealtimeGateway.stats` and hand it to ``instrument_realtime_gateway``."""

    logger: Logger | None = None
    """Where delivery failures are logged; ``None`` logs under the application logger.
    ``RealtimeGateway`` fills it with ``socketio.errors`` when left unset."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_deliveries is not None and self.max_deliveries <= 0:
            # a non-positive ceiling would drop every reclaimed durable signal as
            # poison on its first recovery — fail the wiring, not the deliveries
            raise exc.configuration("max_deliveries must be positive when set (None = unbounded)")

    # ....................... #

    async def run(
        self,
        ctx: ExecutionContext,
        handler: SignalHandler,
        *,
        stop: asyncio.Event | None = None,
    ) -> None:
        _refuse_encrypted_realtime_stream(self.stream_spec)

        group = ctx.deps.resolve_configurable(
            ctx,
            AckStreamGroupQueryDepKey,
            self.stream_spec,
            route=self.stream_spec.name,
        )
        await _consume_group_stream(
            group=group,
            stream=str(self.stream_spec.name),
            group_name=self.group,
            consumer=self.consumer,
            batch=self.batch,
            poll_interval=self.poll_interval,
            reclaim_idle=self.reclaim_idle,
            handler=handler,
            # tenant-global: the tenant rides each message's (untrusted) header.
            tenant_for=lambda message: _tenant_from_headers(message.headers),
            stop=stop,
            max_deliveries=self.max_deliveries,
            stats=self.stats,
            logger=_logger_or_default(self.logger),
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PubSubSignalSource(RealtimeSignalSource):
    """A signal source backed by a pubsub channel (broadcast, at-most-once).

    The alternative live lane behind the same seam: native broadcast fan-out with no
    consumer-group lifecycle, for deployments that prefer pubsub over the stream for
    ephemerals. Every subscribed gateway node receives **every** signal, so on a
    multi-node fleet a purely ephemeral signal is emitted once *per node* — durable
    signals (a producer that sets the ``forze_event_id`` header) still collapse to one
    emit through the shared dedup mark. Nothing is retained or redelivered: a signal
    published while no node is subscribed is gone (durables still reach offline
    recipients via the outbox → stream → mailbox path, which this source does not
    replace). Publish with ``build_realtime_pubsub_publisher`` over the same
    ``realtime_pubsub_spec``.

    Composable beyond the gateway: any consumer of the source seam (e.g. an SSE hub
    bridged as ``async def handler(signal, tenant, event_id, hlc): hub.publish(...)``)
    can run it — the app composes the two packages.
    """

    pubsub_spec: PubSubSpec[RealtimeSignal]
    """The realtime pubsub channel to subscribe (same spec the publisher uses)."""

    poll_interval: timedelta = timedelta(seconds=1)
    """Subscription read pacing passed to the backend's ``subscribe`` timeout."""

    resubscribe_backoff: timedelta = timedelta(seconds=1)
    """Pause before re-subscribing after a transport error ends the subscription."""

    stats: RealtimeGatewayStats | None = None
    """Delivery counters (bridge failures). Share with :attr:`RealtimeGateway.stats`."""

    logger: Logger | None = None
    """Where delivery failures are logged; ``None`` logs under the application logger.
    ``RealtimeGateway`` fills it with ``socketio.errors`` when left unset."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.poll_interval.total_seconds() <= 0:
            raise exc.configuration("poll_interval must be positive")

        if self.resubscribe_backoff.total_seconds() <= 0:
            raise exc.configuration("resubscribe_backoff must be positive")

    # ....................... #

    async def run(
        self,
        ctx: ExecutionContext,
        handler: SignalHandler,
        *,
        stop: asyncio.Event | None = None,
    ) -> None:
        _refuse_encrypted_realtime_stream(self.pubsub_spec)

        log = _logger_or_default(self.logger)
        port = ctx.deps.resolve_configurable(
            ctx, PubSubQueryDepKey, self.pubsub_spec, route=self.pubsub_spec.name
        )
        channel = str(self.pubsub_spec.name)

        while stop is None or not stop.is_set():
            try:
                await self._consume(port, channel, handler, stop=stop)

                if stop is not None and stop.is_set():
                    return

                # the subscription generator ended (backend read timeout) — resubscribe
                continue

            except asyncio.CancelledError:
                raise

            except CoreException as error:
                if is_draining_refusal(error):
                    # Quiescing: pub/sub is fire-and-forget, so there is nothing to
                    # redeliver — but resubscribing would only re-refuse against the
                    # one-way gate. Stop cleanly instead of critical-logging a churn.
                    log.info("Realtime pubsub source draining: stopping consume for %s", channel)
                    return

                if error.kind is ExceptionKind.CONFIGURATION:
                    raise  # a wiring error won't fix itself by retrying

                log.critical_exception("Realtime pubsub source error", channel=channel)

            except Exception:
                log.critical_exception("Realtime pubsub source error", channel=channel)

            await asyncio.sleep(self.resubscribe_backoff.total_seconds())

    # ....................... #

    async def _consume(
        self,
        port: Any,
        channel: str,
        handler: SignalHandler,
        *,
        stop: asyncio.Event | None,
    ) -> None:
        """Drain one subscription until it ends or *stop* is set.

        The subscribe generator can idle indefinitely between yields, so the stop
        signal is raced against ``anext`` instead of being checked between messages —
        cancelling the pending read at stop is fine here (at-most-once by contract).
        """

        log = _logger_or_default(self.logger)
        messages = port.subscribe([channel], timeout=self.poll_interval)
        stop_waiter = asyncio.ensure_future(stop.wait()) if stop is not None else None
        pending: asyncio.Task[Any] | None = None

        try:
            while True:
                if pending is None:
                    pending = asyncio.ensure_future(anext(messages))

                waiters = {pending} if stop_waiter is None else {pending, stop_waiter}
                done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)

                if stop_waiter is not None and stop_waiter in done:
                    return

                try:
                    message = pending.result()

                except StopAsyncIteration:
                    return  # subscription ended — the run loop resubscribes

                pending = None

                try:
                    with _trace_context(message.headers, stream=channel):
                        await handler(
                            message.payload,
                            _tenant_from_headers(message.headers),
                            message.headers.get(HEADER_EVENT_ID),
                            _hlc_from_headers(message.headers),
                        )

                except CoreException as error:
                    if is_draining_refusal(error):
                        # Shutdown artifact, not a bridge failure: re-raise so the run
                        # loop stops cleanly instead of counting/logging a churn
                        # against the one-way gate (pub/sub is fire-and-forget, so
                        # there is nothing to redeliver either way).
                        raise

                    if error.kind is ExceptionKind.CONFIGURATION:
                        raise

                    # at-most-once: one bad signal must not wedge the live channel
                    if self.stats is not None:
                        self.stats.bridge_failed += 1
                    log.critical_exception("Realtime pubsub bridge failed", channel=channel)

                except Exception:
                    if self.stats is not None:
                        self.stats.bridge_failed += 1
                    log.critical_exception("Realtime pubsub bridge failed", channel=channel)

        finally:
            if pending is not None:
                # Await the cancellation out: aclose() on a generator whose anext is
                # still running raises RuntimeError ("already running") — the close
                # must only start once the read has actually unwound. A read that
                # instead finished with a transport error is swallowed here too (we
                # are exiting; the run loop's taxonomy already handled the real path).
                pending.cancel()

                with suppress(Exception, asyncio.CancelledError):
                    await pending

            if stop_waiter is not None:
                stop_waiter.cancel()

            await messages.aclose()


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TenantShardedSignalSource(RealtimeSignalSource):
    """Consume a **per-tenant** realtime stream for each tenant in this gateway's
    assigned shard, binding the tenant from the **stream identity**.

    The tenant-global :class:`StreamGroupSignalSource` reads one shared stream and takes
    the tenant from the (untrusted) ``forze_tenant_id`` header. This is the **tenant-aware**
    alternative: the realtime stream route is wired ``tenant_aware`` (so the adapter scopes
    each tenant to its own key/partition), and this source runs one consume loop per assigned
    tenant, each **bound** to that tenant. The tenant a signal belongs to is therefore the
    stream it was read from — set by the publisher's ambient tenant at write time — so a
    tenant-aware mailbox and the room scope by a **trusted** tenant, with no header trust
    (``RealtimeGateway.bind_tenant_from_headers`` is irrelevant in this mode). Pair it with
    :func:`~forze_kits.integrations.realtime.realtime_tenant_group_ensure_lifecycle_step`.

    Per-tenant loops run as sibling tasks; each binds its tenant in its own task-copied
    context (``asyncio`` snapshots ContextVars at task creation), so the bindings never race.
    Each tenant loop is **individually supervised** (restart on crash with jittered backoff),
    so one tenant's broker fault or configuration error degrades that tenant's delivery only —
    the siblings keep consuming. Assign **disjoint** tenant shards across gateway instances;
    rebalancing a running fleet is out of scope — repartition by restart.
    """

    shard: RealtimeShard
    """The per-instance tenant shard — stream, tenants, group — shared with the group-ensure
    and relay steps so they cannot drift (one instance owns a shard end to end). The stream is
    wired ``tenant_aware``, so it resolves to a per-tenant key/partition under the bound tenant;
    the tenants are the shard's fixed snapshot (same set every component sees)."""

    consumer: str = attrs.field(factory=_default_consumer)
    """This instance's consumer name within the group — defaults **unique per process**
    (host + pid) so multiple instances don't clobber each other's pending-entries list."""

    batch: int = 64
    """Maximum signals to read per poll, per tenant."""

    poll_interval: timedelta = timedelta(seconds=1)
    """Block timeout for one group read."""

    reclaim_idle: timedelta | None = timedelta(seconds=60)
    """Reclaim entries stranded (delivered, unacked) at least this long; ``None`` disables."""

    max_deliveries: int | None = 5
    """Ceiling on deliveries of one durable signal before it is dropped as poison
    (see :attr:`StreamGroupSignalSource.max_deliveries`); ``None`` = unbounded."""

    stats: RealtimeGatewayStats | None = None
    """Delivery counters, shared across every tenant loop (and with the gateway)."""

    logger: Logger | None = None
    """Where delivery failures are logged; ``None`` logs under the application logger.
    ``RealtimeGateway`` fills it with ``socketio.errors`` when left unset."""

    restart_backoff: timedelta = timedelta(seconds=5)
    """Base backoff between per-tenant loop restarts (jittered ×[1.0, 1.5)).

    Each tenant's loop is supervised on its own, so a crash in one tenant's consume loop
    restarts that loop only; a configuration error stops that tenant's supervision
    (wiring does not fix itself) while the sibling tenants keep consuming.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # Validated here, synchronously — run_supervised re-checks inside the per-tenant
        # tasks, where a raise would surface as N dead loops instead of a wiring failure.
        if self.restart_backoff.total_seconds() <= 0:
            raise exc.configuration("Restart backoff must be positive")

        if self.max_deliveries is not None and self.max_deliveries <= 0:
            raise exc.configuration("max_deliveries must be positive when set (None = unbounded)")

    # ....................... #

    async def run(
        self,
        ctx: ExecutionContext,
        handler: SignalHandler,
        *,
        stop: asyncio.Event | None = None,
    ) -> None:
        _refuse_encrypted_realtime_stream(self.shard.stream_spec)

        tenants = list(self.shard.tenants)
        # Per-tenant supervision wants a real event to honor; an unstoppable local one
        # preserves the "consume forever" contract for callers that pass none.
        stop_event = stop if stop is not None else asyncio.Event()

        if not tenants:
            # Nothing assigned: idle until stopped or cancelled. Returning early would
            # look like a crash to supervision, which restarts an unexpectedly-ended run.
            await stop_event.wait()
            return

        def _tenant_runner(tenant: UUID) -> Callable[[], Awaitable[None]]:
            async def _run() -> None:
                await self._run_tenant(ctx, tenant, handler, stop_event)

            return _run

        async with asyncio.TaskGroup() as tasks:
            for tenant in tenants:
                tasks.create_task(
                    run_supervised(
                        _tenant_runner(tenant),
                        stop=stop_event,
                        name=f"realtime_gateway:{tenant}",
                        restart_backoff=self.restart_backoff,
                    ),
                    name=f"realtime_gateway_t:{tenant}",
                )

    # ....................... #

    async def _run_tenant(
        self,
        ctx: ExecutionContext,
        tenant: UUID,
        handler: SignalHandler,
        stop: asyncio.Event,
    ) -> None:
        # Bind the shard tenant for the whole loop so the per-tenant group port resolves
        # to this tenant's key/partition and every handler call scopes under it. The
        # tenant is the stream's identity (trusted), not a per-message header.
        stream_spec = self.shard.stream_spec

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            group = ctx.deps.resolve_configurable(
                ctx,
                AckStreamGroupQueryDepKey,
                stream_spec,
                route=stream_spec.name,
            )
            await _consume_group_stream(
                group=group,
                stream=str(stream_spec.name),
                group_name=self.shard.group,
                consumer=self.consumer,
                batch=self.batch,
                poll_interval=self.poll_interval,
                reclaim_idle=self.reclaim_idle,
                handler=handler,
                tenant_for=lambda _message: tenant,
                stop=stop,
                max_deliveries=self.max_deliveries,
                stats=self.stats,
                logger=_logger_or_default(self.logger),
            )


# ....................... #


def require_tenant_global_source(source: object, ctx: ExecutionContext, *, what: str) -> None:
    """Refuse a tenant-global source whose stream route is wired per tenant.

    The stream-group and pubsub sources read with no tenant bound, so over a
    ``tenant_aware`` route every read would be refused; checking the resolved port at
    startup fails the wiring instead. The sharded source binds each tenant it is assigned,
    and a source of the app's own is the app's to check.

    :param source: The source a consumer is about to run.
    :param ctx: The context its ports resolve against.
    :param what: Names the consumer in the refusal.
    :raises CoreException: ``configuration`` (``tenant_global_required``) for a
        ``tenant_aware`` route.
    """

    port: object

    if isinstance(source, StreamGroupSignalSource):
        port = ctx.deps.resolve_configurable(
            ctx, AckStreamGroupQueryDepKey, source.stream_spec, route=source.stream_spec.name
        )

    elif isinstance(source, PubSubSignalSource):
        port = ctx.deps.resolve_configurable(
            ctx, PubSubQueryDepKey, source.pubsub_spec, route=source.pubsub_spec.name
        )

    else:
        return

    require_tenant_global(port, what=what)

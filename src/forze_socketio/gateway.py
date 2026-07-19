"""The realtime egress gateway — consume realtime signals, bridge to connections.

The egress twin of the inbound :class:`ForzeSocketIOAdapter`. It is **not** a
contract — it is an edge adapter that consumes :class:`RealtimeSignal` s from a
messaging substrate and emits them to the live Socket.IO connections it owns.

Three separate seams:

- **source** — where signals come from (a stream consumer group here); swappable.
- **bridge** — :meth:`RealtimeGateway._emit`: ``signal → room → sio.emit``. The
  Socket.IO Redis manager fans the emit to whichever node holds the room.
- **supervision** — the lifecycle step (see :mod:`forze_socketio.gateway_lifecycle`) runs
  the gateway under the shared core runner
  (:func:`~forze.application.execution.background.run_supervised`): restart on crash with
  jittered backoff, graceful stop at a batch boundary, registered in ``ctx.drainables``.

Room membership (auto-join, topic subscription) is a transport-edge concern too;
the helpers here build the same tenant-scoped room names the gateway emits to, so
publish and membership always agree.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

import asyncio
import os
import socket
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager, nullcontext, suppress
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    cast,
    final,
    runtime_checkable,
)
from uuid import UUID

import attrs
from pydantic import ValidationError
from socketio.async_server import AsyncServer

from forze.application.contracts.envelope import (
    HEADER_EVENT_ID,
    HEADER_HLC,
    HEADER_TENANT_ID,
    HEADER_TRACEPARENT,
)
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.pubsub import PubSubQueryDepKey, PubSubSpec
from forze.application.contracts.realtime import (
    DEFAULT_REALTIME_GROUP,
    Audience,
    AudienceKind,
    RealtimeEventCatalog,
    RealtimeShard,
    RealtimeSignal,
)
from forze.application.contracts.stream import AckStreamGroupQueryDepKey, StreamSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.execution.background import run_supervised
from forze.application.integrations.realtime import (
    room_for as room_for,  # re-export: established home
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.logging import Logger
from forze.base.primitives import HlcTimestamp, JsonDict, StrKey, utcnow

from ._logging import ForzeSocketIOLogger
from .mailbox import RealtimeMailbox
from .observability import RealtimeGatewayStats

if TYPE_CHECKING:
    from .connection import RealtimePresence

# ----------------------- #

_logger = Logger(ForzeSocketIOLogger.ERRORS)

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


def _default_consumer() -> str:
    """A consumer name unique to this process (host + pid).

    Redis consumer-group semantics key the pending-entries list by consumer name, so two
    gateway instances sharing a name would each claim/reclaim the *other's* in-flight entries
    (double-processing, broken idle recovery). One stable, distinct name per process avoids it.
    """

    return f"{socket.gethostname()}-{os.getpid()}"


# ....................... #


# room_for lives in the transport-neutral kernel (imported above, re-exported here):
# the gateway emits to it, the membership helpers join it, and SSE presence reports
# under it — one naming scheme, so publish, membership, and presence always agree.

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


# ....................... #


def _bind_tenant(
    ctx: ExecutionContext,
    tenant: UUID | None,
    *,
    enabled: bool,
) -> AbstractContextManager[None]:
    """Bind the per-signal header *tenant* so a tenant-aware mailbox scopes ambiently.

    **Opt-in** (``enabled``): the ``forze_tenant_id`` header is **untrusted** input —
    within a deployment the relay writes it, but any producer with broker access could
    forge it, so binding it is only safe on brokers where every producer is trusted to
    assert tenancy (the same posture as the inbox consumer's ``bind_tenant_from_headers``).
    Disabled, or untenanted, binds nothing — the mailbox runs under the ambient tenant.
    """

    if not enabled or tenant is None:
        return nullcontext()

    return ctx.inv_ctx.bind_identity(
        authn=ctx.inv_ctx.get_authn(), tenant=TenantIdentity(tenant_id=tenant)
    )


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
            # A deterministic wiring error (e.g. a tenant-aware mailbox with no bound tenant)
            # never succeeds on retry — re-raise to fail fast instead of leaving the durable
            # message pending and reclaim-looping it forever. The message stays unacked, so it
            # redelivers once the operator fixes the wiring and restarts.
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

            _logger.critical_exception(
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

                _logger.critical(
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
                )

            if not fresh and not reclaimed:
                # the read timeout already paces blocking backends; this is a small
                # floor so a non-blocking backend cannot hot-loop.
                await asyncio.sleep(min(_IDLE_FLOOR, poll_interval.total_seconds()))

        except asyncio.CancelledError:
            raise

        except CoreException as error:
            if error.kind is ExceptionKind.CONFIGURATION:
                raise  # a wiring error won't fix itself by retrying — let the task exit (logged)

            _logger.critical_exception("Realtime gateway loop error", stream=stream)
            await asyncio.sleep(poll_interval.total_seconds())

        except Exception:
            _logger.critical_exception("Realtime gateway loop error", stream=stream)
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
                if error.kind is ExceptionKind.CONFIGURATION:
                    raise  # a wiring error won't fix itself by retrying

                _logger.critical_exception("Realtime pubsub source error", channel=channel)

            except Exception:
                _logger.critical_exception("Realtime pubsub source error", channel=channel)

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
                    if error.kind is ExceptionKind.CONFIGURATION:
                        raise

                    # at-most-once: one bad signal must not wedge the live channel
                    if self.stats is not None:
                        self.stats.bridge_failed += 1
                    _logger.critical_exception("Realtime pubsub bridge failed", channel=channel)

                except Exception:
                    if self.stats is not None:
                        self.stats.bridge_failed += 1
                    _logger.critical_exception("Realtime pubsub bridge failed", channel=channel)

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
    the tenant from the (untrusted) ``forze_tenant_id`` header. This is the **namespace-tier**
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
            )


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GatewayDedup:
    """Inbox-based delivery dedup for durable signals at the gateway.

    A mailboxed (recoverable) signal is exactly-once — the mark and the mailbox store commit
    together, then the live emit is best-effort (recovery via reconnect-replay). A signal with
    no replay-safe store (topic, no mailbox, ``offline_delivery=False``) is at-least-once: the
    emit must succeed for the mark to stand, so a failed emit is reclaimed and re-delivered,
    and the client dedups any redelivery by the envelope id."""

    inbox_spec: InboxSpec
    """The inbox route that records already-emitted durable signals."""

    tx_route: StrKey
    """Transaction route the dedup mark commits on."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RealtimeGateway:
    """Bridge realtime signals from a *source* to live Socket.IO connections."""

    sio: AsyncServer
    """Socket.IO async server used for delivery and room membership."""

    source: RealtimeSignalSource
    """Where signals come from (substrate-agnostic)."""

    namespace: str = "/"
    """Namespace this gateway emits on and manages rooms within."""

    dedup: GatewayDedup | None = None
    """When set, durable signals (those with a dedup id) are deduplicated at the gateway —
    exactly-once for mailboxed signals, at-least-once otherwise (see :class:`GatewayDedup`)."""

    mailbox_factory: Callable[[ExecutionContext], RealtimeMailbox] | None = None
    """Builds the mailbox once at ``run(ctx)`` start, with its ports resolved (e.g.
    ``build_realtime_mailbox``). When set (with ``dedup``), a durable **principal**
    signal is stored for offline replay before it is emitted, so a recipient offline at
    emit time receives it on reconnect. Topic and ephemeral signals are never
    mailboxed. A factory (not a built object) so deps materialize against the run ctx."""

    event_catalog: RealtimeEventCatalog | None = None
    """Optional catalog consulted for the per-event ``offline_delivery`` opt-out; when
    absent, every durable principal signal is mailboxed (always-store default)."""

    presence: "RealtimePresence | None" = None
    """When set with a mailbox, the live emit is skipped for a mailboxed signal whose
    principal room is empty (saves a cross-node fan-out; the reconnect drain delivers
    it). Never skips a signal that is not recoverable from the mailbox."""

    bind_tenant_from_headers: bool = False
    """Opt-in: bind each signal's ``forze_tenant_id`` header so a tenant-aware mailbox
    scopes by it. Off by default because the header is untrusted/forgeable — enable only
    on brokers where every producer is trusted to assert tenancy (see :func:`_bind_tenant`)."""

    emit_timeout: timedelta | None = timedelta(seconds=5)
    """Bound on a single ``sio.emit`` — **bounded by default**; ``None`` opts into
    waiting indefinitely.

    Transport-level flow control (a slow consumer) is engine.io's; this only stops
    one stuck delivery (a dead backplane, a wedged manager) from freezing the whole
    consume loop, which an unbounded default silently allowed. On timeout the emit
    raises, so the source's per-signal error policy applies — an ephemeral signal is
    acked (at-most-once) and a durable one is left pending to be redelivered.
    """

    stats: RealtimeGatewayStats | None = None
    """Delivery counters for the live path (emit outcomes, dedup/presence skips,
    admission rejects, mailbox stores). Share the **same** instance with the source's
    ``stats`` and hand it to ``instrument_realtime_gateway`` at assembly."""

    # ....................... #

    async def run(self, ctx: ExecutionContext, *, stop: asyncio.Event | None = None) -> None:
        """Consume signals and emit each to its room, until *stop* (forever when ``None``).

        A stop request is honored at the source's unit boundary (an in-flight batch
        finishes and acks first), so a graceful shutdown never abandons a signal mid-bridge.
        """

        # resolve the mailbox's ports once, against the run ctx (worker-resolved-once)
        mailbox = self.mailbox_factory(ctx) if self.mailbox_factory is not None else None

        async def handle(
            signal: RealtimeSignal,
            tenant: UUID | None,
            dedup_id: str | None,
            hlc: HlcTimestamp,
        ) -> None:
            await self._handle(ctx, mailbox, signal, tenant, dedup_id, hlc)

        await self.source.run(ctx, handle, stop=stop)

    # ....................... #

    async def _handle(
        self,
        ctx: ExecutionContext,
        mailbox: RealtimeMailbox | None,
        signal: RealtimeSignal,
        tenant: UUID | None,
        dedup_id: str | None,
        hlc: HlcTimestamp,
    ) -> None:
        admitted = self._admit(signal)

        if admitted is None:
            # undeclared event, disallowed audience, or wrong payload shape — drop it (the
            # caller then acks, so it never reaches a client and never reclaim-loops).
            return

        signal = admitted  # emit/store the catalog-normalized payload, not the raw one

        if self.dedup is None or dedup_id is None:
            # ephemeral, or durable with no dedup configured — emit directly
            await self._emit(signal, tenant, event_id=dedup_id)
            return

        # durable: mark (+ store) inside one transaction. Where the live emit sits relative to
        # the commit depends on whether the signal is **replay-safe**, so the dedup mark never
        # becomes final until delivery is guaranteed:
        #
        #  - mailboxed (recoverable): the store commits with the mark, so the recipient gets
        #    the signal on reconnect even if the live emit fails. Emit best-effort AFTER the
        #    commit — a commit failure then can't double-emit (exactly-once, mailbox recovery).
        #  - not mailboxed (topic, no mailbox, offline opt-out): nothing persists it, so the
        #    emit must succeed for the mark to stand. Emit INSIDE the transaction — a failed
        #    emit rolls the mark back, the entry stays pending, and reclaim re-delivers instead
        #    of dropping the frame. A commit failure after a successful emit redelivers too; the
        #    client dedups by the envelope id (at-least-once).
        #
        # The header tenant is bound only when opted-in (it is untrusted, see _bind_tenant).
        with _bind_tenant(ctx, tenant, enabled=self.bind_tenant_from_headers):
            async with ctx.tx_ctx.scope(self.dedup.tx_route):
                inbox = ctx.inbox(self.dedup.inbox_spec)

                if not await inbox.mark_if_unseen(str(self.dedup.inbox_spec.name), dedup_id):
                    if self.stats is not None:
                        self.stats.dedup_skipped += 1
                    return

                store = mailbox if (mailbox is not None and self._should_mailbox(signal)) else None

                if store is not None:
                    try:
                        await store.store(
                            principal=signal.audience.name,
                            event_id=dedup_id,
                            hlc=hlc,
                            signal=signal,
                        )

                        if self.stats is not None:
                            self.stats.mailboxed += 1
                    except CoreException as error:
                        # A tenant-aware mailbox fails closed with an opaque
                        # ``tenant_required`` when nothing is bound; the gateway is the
                        # only place that knows *why* nothing is bound, so rewrap it.
                        if error.code == "tenant_required":
                            raise self._mailbox_tenant_unbound() from error
                        raise
                else:
                    # not replay-safe: emit inside the tx so a failed emit rolls the mark back
                    await self._emit_live(signal, tenant, event_id=dedup_id, recoverable=False)

            if store is not None:
                # replay-safe: mark + store committed, so the durable obligation is met and the
                # recipient gets the signal on reconnect. The live emit is best-effort — a
                # failure (Socket.IO/presence outage, timeout) must not propagate, or the caller
                # would leave the message pending forever (it never re-emits live once the mark
                # is committed, and reclaim may be disabled). Swallow it; the mailbox recovers.
                try:
                    await self._emit_live(signal, tenant, event_id=dedup_id, recoverable=True)
                except Exception as error:
                    if (
                        isinstance(error, CoreException)
                        and error.kind is ExceptionKind.CONFIGURATION
                    ):
                        raise  # a wiring error must fail fast, not be swallowed as best-effort

                    _logger.critical_exception(
                        "Realtime live emit failed after commit (recoverable via mailbox)",
                        event_id=dedup_id,
                    )

    # ....................... #

    def _mailbox_tenant_unbound(self) -> CoreException:
        """Actionable error when a tenant-aware mailbox has no tenant to scope by.

        The gateway is a **cross-tenant** consumer with no ambient tenant of its own
        (the realtime stream is tenant-global). So a tenant-aware mailbox's
        only possible tenant is the stream's ``forze_tenant_id`` header — bound only
        when :attr:`bind_tenant_from_headers` is enabled *and* the header is present.
        Otherwise the adapter raises a bare ``tenant_required``; this names the wiring
        contract instead. (Per-tenant *trusted* mailbox scoping without header trust is
        the tenant-aware-gateway follow-up.)
        """

        return exc.configuration(
            "Realtime gateway cannot store into a tenant-aware mailbox: no tenant is "
            "bound. The gateway has no ambient tenant of its own, so the only tenant "
            "source is the stream's forze_tenant_id header. Either set "
            "RealtimeGateway.bind_tenant_from_headers=True to bind it (the header must "
            "be present on every signal, and is untrusted/forgeable — enable only where "
            "every stream producer is trusted to assert tenancy), or wire a "
            "tenant-global mailbox route (tenant_aware=False).",
            code="realtime_mailbox_tenant_unbound",
        )

    # ....................... #

    def _admit(self, signal: RealtimeSignal) -> RealtimeSignal | None:
        """Admit *signal* on the gateway's declared surface, **normalized** — or ``None``.

        With a catalog set the emitted surface is **closed**: a signal whose event is
        undeclared, whose audience kind the event forbids, or whose payload does not match
        the declared :class:`RealtimeEvent` is rejected (logged + dropped) rather than
        emitted — so a raw ``RealtimeSignal.of(...)`` producer or a malformed stream row
        can't bypass the contract. An admitted signal is returned with its payload replaced
        by the **parsed model's JSON form** (defaults, aliases, and coercions applied — the
        same ``model_dump(mode="json")`` :meth:`RealtimeSignal.for_event` produces), so the
        client receives the declared shape, not the raw payload. No catalog means an open
        surface: the signal passes through unchanged.
        """

        if self.event_catalog is None:
            return signal

        event = self.event_catalog.get(signal.event)

        if event is None:
            _logger.critical(
                "Realtime signal rejected: event not in catalog",
                realtime_event=signal.event,
            )
            self._count_rejection()
            return None

        if not event.accepts(signal.audience):
            _logger.critical(
                "Realtime signal rejected: audience kind not allowed for event",
                realtime_event=signal.event,
                audience_kind=signal.audience.kind.value,
            )
            self._count_rejection()
            return None

        try:
            normalized = event.parse(signal.payload).model_dump(mode="json")
        except ValidationError:
            _logger.critical_exception(
                "Realtime signal rejected: payload does not match catalog",
                realtime_event=signal.event,
            )
            self._count_rejection()
            return None

        return signal.model_copy(update={"payload": normalized})

    # ....................... #

    def _count_rejection(self) -> None:
        """Count an admission rejection on the delivery stats, when wired."""

        if self.stats is not None:
            self.stats.admission_rejected += 1

    # ....................... #

    def _should_mailbox(self, signal: RealtimeSignal) -> bool:
        """Whether this durable signal is stored for offline replay (audience + opt-out)."""

        if signal.audience.kind is not AudienceKind.PRINCIPAL:
            return False

        if self.event_catalog is None:
            return True  # always-store default

        event = self.event_catalog.get(signal.event)

        return event is None or event.offline_delivery

    # ....................... #

    async def _emit_live(
        self,
        signal: RealtimeSignal,
        tenant: UUID | None,
        *,
        event_id: str | None,
        recoverable: bool,
    ) -> None:
        if (
            recoverable
            and self.presence is not None
            and await self.presence.count(room_for(signal.audience, tenant)) == 0
        ):
            if self.stats is not None:
                self.stats.presence_skipped += 1
            return

        await self._emit(signal, tenant, event_id=event_id)

    # ....................... #

    async def _emit(
        self,
        signal: RealtimeSignal,
        tenant: UUID | None,
        *,
        event_id: str | None = None,
    ) -> None:
        # Uniform delivery envelope: every frame is ``{id, data}`` — the id
        # is the durable event id (``None`` for ephemeral) so the client dedups
        # live-vs-replayed and acks by it.
        emit = self.sio.emit(
            signal.event,
            data={"id": event_id, "data": signal.payload},
            room=room_for(signal.audience, tenant),
            namespace=self.namespace,
        )

        try:
            if self.emit_timeout is None:
                await emit
            else:
                await asyncio.wait_for(emit, timeout=self.emit_timeout.total_seconds())

        except Exception:
            # Exception, not BaseException: a shutdown's CancelledError is not an emit
            # failure and must not inflate the counter on its way out.
            if self.stats is not None:
                self.stats.emit_failed += 1
            raise

        if self.stats is not None:
            self.stats.emitted += 1

    # ....................... #

    async def join_principal(
        self,
        sid: str,
        principal_id: UUID | str,
        tenant: UUID | None,
    ) -> None:
        """Join *sid* to its tenant-scoped principal room (auto-join on connect)."""

        await self.sio.enter_room(
            sid,
            room_for(Audience.principal(str(principal_id)), tenant),
            namespace=self.namespace,
        )

    # ....................... #

    async def join_topic(self, sid: str, topic: str, tenant: UUID | None) -> None:
        """Subscribe *sid* to a tenant-scoped topic room (app-driven)."""

        await self.sio.enter_room(
            sid,
            room_for(Audience.topic(topic), tenant),
            namespace=self.namespace,
        )

    # ....................... #

    async def leave_topic(self, sid: str, topic: str, tenant: UUID | None) -> None:
        """Unsubscribe *sid* from a tenant-scoped topic room."""

        await self.sio.leave_room(
            sid,
            room_for(Audience.topic(topic), tenant),
            namespace=self.namespace,
        )

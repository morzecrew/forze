"""Gateway delivery counters — the live-emit half of the realtime plane's observability.

The offline mailbox already exports its counters (``instrument_realtime_mailbox``); this is
the same pattern for the **live path**, which was previously blind: hand the gateway (and its
source) one :class:`RealtimeGatewayStats`, call :func:`instrument_realtime_gateway` once at
assembly, and every delivery outcome becomes an observable counter. OpenTelemetry is imported
lazily, so an uninstrumented app pays nothing.

Connections have their own instruments (:func:`instrument_realtime_connections`): how many
are open per namespace, how many connects were refused and why, and how the drain's closes
ended.

Depth/lag is deliberately **not** a counter here: backlog is a property of the consumer
group, not of this process — poll
:meth:`~forze.application.contracts.stream.AckStreamGroupAdminPort.depth` (the quiesce sweep
does) or export it from a scheduler that can await the port.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

from collections.abc import Callable, Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Literal, final

import attrs

from forze.base.primitives import utcnow

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter, Observation

    from forze.application.execution.background import SpreadOutcome

# ----------------------- #

GATEWAY_EMITTED_COUNTER = "forze.realtime.gateway.emitted"
GATEWAY_EMIT_FAILED_COUNTER = "forze.realtime.gateway.emit_failed"
GATEWAY_PRESENCE_SKIPPED_COUNTER = "forze.realtime.gateway.presence_skipped"
GATEWAY_DEDUP_SKIPPED_COUNTER = "forze.realtime.gateway.dedup_skipped"
GATEWAY_ADMISSION_REJECTED_COUNTER = "forze.realtime.gateway.admission_rejected"
GATEWAY_UNTENANTED_DROPPED_COUNTER = "forze.realtime.gateway.untenanted_dropped"
GATEWAY_MAILBOXED_COUNTER = "forze.realtime.gateway.mailboxed"
GATEWAY_BRIDGE_FAILED_COUNTER = "forze.realtime.gateway.bridge_failed"
GATEWAY_POISONED_COUNTER = "forze.realtime.gateway.poisoned"

BACKPLANE_SECONDS_SINCE_OK_GAUGE = "forze.realtime.backplane.seconds_since_ok"
BACKPLANE_CONSECUTIVE_FAILURES_GAUGE = "forze.realtime.backplane.consecutive_failures"

REALTIME_CONNECTIONS_COUNTER = "forze.realtime.connections"
REALTIME_CONNECT_REFUSED_COUNTER = "forze.realtime.connect_refused"
REALTIME_DRAIN_CLOSED_COUNTER = "forze.realtime.drain_closed"

ConnectRefusal = Literal["protocol", "auth", "draining", "admission", "internal"]
"""Why a connect was refused: an unsupported protocol, failed authentication, the replica
draining, the app's admission hook, or an unexpected error."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class BackplaneHealth:
    """Freshness of the Socket.IO fan-out path, fed by the backplane heartbeat step.

    A dead ``AsyncRedisManager`` listener silently stops every cross-node emit — nothing
    in python-socketio surfaces it. The heartbeat step pushes a probe frame through the
    manager on an interval and records the outcome here; the gauges make "the backplane
    has not accepted a frame in N seconds" alarmable.
    """

    last_ok_at: datetime | None = None
    """Instant of the last probe the manager accepted; ``None`` until the first."""

    consecutive_failures: int = 0
    """Failed probes since the last success."""

    # ....................... #

    def ok(self) -> None:
        """Record a successful probe."""

        self.last_ok_at = utcnow()
        self.consecutive_failures = 0

    # ....................... #

    def failed(self) -> None:
        """Record a failed probe."""

        self.consecutive_failures += 1

    # ....................... #

    @property
    def seconds_since_ok(self) -> float:
        """Seconds since the last accepted probe; ``-1.0`` when none ever succeeded.

        ``-1`` (not a huge number) so a dashboard can tell "never worked — wiring"
        from "worked and stopped — outage" at a glance.
        """

        if self.last_ok_at is None:
            return -1.0

        return max(0.0, (utcnow() - self.last_ok_at).total_seconds())


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class RealtimeGatewayStats:
    """Mutable delivery counters for one gateway (share one instance gateway ↔ source).

    Deliberately **one** object for both wiring points: the gateway counts emit outcomes,
    its signal source counts bridge failures and poison drops — splitting them would mean
    two instruments that can drift. Plain ints mutated on the event loop (no lock needed);
    the instrument's callbacks read them at scrape time.
    """

    emitted: int = 0
    """Frames delivered to Socket.IO (``sio.emit`` returned)."""

    emit_failed: int = 0
    """``sio.emit`` calls that raised — including ``emit_timeout`` expiries."""

    presence_skipped: int = 0
    """Live emits skipped because the principal room was empty (mailbox recovers)."""

    dedup_skipped: int = 0
    """Durable signals skipped as already-seen (relay retries, reclaim duplicates)."""

    admission_rejected: int = 0
    """Signals dropped at the catalog gate (undeclared event, audience, payload shape)."""

    untenanted_dropped: int = 0
    """Signals dropped for resolving no tenant on a ``require_tenant`` gateway."""

    mailboxed: int = 0
    """Durable principal signals stored for offline replay."""

    bridge_failed: int = 0
    """Bridge attempts that failed and were left for redelivery (or acked, if ephemeral)."""

    poisoned: int = 0
    """Durable signals dropped at the delivery ceiling — every one is a bounded loss."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class RealtimeConnectionStats:
    """This node's connection counts, for :func:`instrument_realtime_connections`.

    Hand one instance to ``attach_realtime_connection`` for every namespace and to
    ``connection_drain_lifecycle_step``. Plain ints mutated on the event loop; the
    instruments read them at scrape time.
    """

    connections: dict[str, int] = attrs.field(factory=dict)
    """Accepted connections open on this node, by namespace."""

    connect_refused: dict[tuple[str, ConnectRefusal], int] = attrs.field(factory=dict)
    """Refused connects, by namespace and reason."""

    drain_closed: dict[str, int] = attrs.field(factory=dict)
    """Closes the drain started, by outcome: ``closed``, ``failed`` or ``timed_out``."""

    # ....................... #

    def connected(self, namespace: str) -> None:
        """Count an accepted connection."""

        self.connections[namespace] = self.connections.get(namespace, 0) + 1

    # ....................... #

    def disconnected(self, namespace: str) -> None:
        """Uncount a connection that was counted."""

        self.connections[namespace] = self.connections.get(namespace, 0) - 1

    # ....................... #

    def refused(self, namespace: str, reason: ConnectRefusal) -> None:
        """Count a refused connect."""

        key = (namespace, reason)
        self.connect_refused[key] = self.connect_refused.get(key, 0) + 1

    # ....................... #

    def drained(self, outcome: "SpreadOutcome") -> None:
        """Count how the drain's closes ended."""

        for name, count in (
            ("closed", outcome.closed),
            ("failed", outcome.failed),
            ("timed_out", outcome.timed_out),
        ):
            self.drain_closed[name] = self.drain_closed.get(name, 0) + count


# ----------------------- #


def instrument_realtime_gateway(
    stats: RealtimeGatewayStats,
    *,
    channel: str = "realtime",
    meter: "Meter | None" = None,
) -> None:
    """Export the gateway's delivery counters as OTel observable counters.

    Emits via the global OTel meter unless *meter* is supplied; labelled
    ``forze.realtime.channel``. Call once at assembly time, with the same *stats*
    instance handed to the gateway and its source.

    The two to alarm on: ``forze.realtime.gateway.poisoned`` (every increment is a
    dropped durable delivery) and ``forze.realtime.gateway.emit_failed`` climbing while
    ``emitted`` is flat (the Socket.IO side — or its Redis backplane — stopped taking
    frames).
    """

    from opentelemetry import metrics

    meter = meter or metrics.get_meter("forze")
    attributes = {"forze.realtime.channel": channel}

    def _counter(name: str, pick: Callable[[], int], description: str) -> None:
        def callback(_options: "CallbackOptions") -> "Iterable[Observation]":
            return [metrics.Observation(pick(), attributes)]

        meter.create_observable_counter(
            name, callbacks=[callback], unit="1", description=description
        )

    _counter(GATEWAY_EMITTED_COUNTER, lambda: stats.emitted, "Frames delivered to Socket.IO.")
    _counter(
        GATEWAY_EMIT_FAILED_COUNTER,
        lambda: stats.emit_failed,
        "sio.emit calls that raised (including emit-timeout expiries).",
    )
    _counter(
        GATEWAY_PRESENCE_SKIPPED_COUNTER,
        lambda: stats.presence_skipped,
        "Live emits skipped for an empty principal room (recoverable via mailbox).",
    )
    _counter(
        GATEWAY_DEDUP_SKIPPED_COUNTER,
        lambda: stats.dedup_skipped,
        "Durable signals skipped as already seen.",
    )
    _counter(
        GATEWAY_ADMISSION_REJECTED_COUNTER,
        lambda: stats.admission_rejected,
        "Signals rejected at the catalog admission gate.",
    )
    _counter(
        GATEWAY_UNTENANTED_DROPPED_COUNTER,
        lambda: stats.untenanted_dropped,
        "Signals dropped for resolving no tenant on a require_tenant gateway.",
    )
    _counter(
        GATEWAY_MAILBOXED_COUNTER,
        lambda: stats.mailboxed,
        "Durable principal signals stored for offline replay.",
    )
    _counter(
        GATEWAY_BRIDGE_FAILED_COUNTER,
        lambda: stats.bridge_failed,
        "Bridge attempts that failed (redelivered if durable, dropped if ephemeral).",
    )
    _counter(
        GATEWAY_POISONED_COUNTER,
        lambda: stats.poisoned,
        "Durable signals dropped at the delivery ceiling (bounded loss).",
    )


# ----------------------- #


def instrument_realtime_backplane(
    health: BackplaneHealth,
    *,
    channel: str = "realtime",
    meter: "Meter | None" = None,
) -> None:
    """Export the backplane heartbeat's freshness as OTel observable gauges.

    Pair with ``realtime_backplane_heartbeat_lifecycle_step`` (which feeds *health*).
    Alarm on ``forze.realtime.backplane.seconds_since_ok`` exceeding a few heartbeat
    intervals — that is cross-node emit silently down; ``-1`` means no probe has ever
    succeeded (wiring, not outage).
    """

    from opentelemetry import metrics

    meter = meter or metrics.get_meter("forze")
    attributes = {"forze.realtime.channel": channel}

    def _gauge(name: str, pick: Callable[[], float], description: str, unit: str) -> None:
        def callback(_options: "CallbackOptions") -> "Iterable[Observation]":
            return [metrics.Observation(pick(), attributes)]

        meter.create_observable_gauge(
            name, callbacks=[callback], unit=unit, description=description
        )

    _gauge(
        BACKPLANE_SECONDS_SINCE_OK_GAUGE,
        lambda: health.seconds_since_ok,
        "Seconds since the Socket.IO backplane last accepted a probe (-1: never).",
        "s",
    )
    _gauge(
        BACKPLANE_CONSECUTIVE_FAILURES_GAUGE,
        lambda: float(health.consecutive_failures),
        "Failed backplane probes since the last success.",
        "1",
    )


# ----------------------- #


def instrument_realtime_connections(
    stats: RealtimeConnectionStats,
    *,
    channel: str = "realtime",
    meter: "Meter | None" = None,
) -> None:
    """Export the node's connection counts as OTel observable instruments.

    - ``forze.realtime.connections`` — an up-down counter of open connections, labelled
      ``forze.realtime.namespace``.
    - ``forze.realtime.connect_refused`` — refused connects, labelled
      ``forze.realtime.namespace`` and ``forze.realtime.reason`` (``protocol``, ``auth``,
      ``draining``, ``admission``, ``internal``).
    - ``forze.realtime.drain_closed`` — closes the drain started, labelled
      ``forze.realtime.outcome`` (``closed``, ``failed``, ``timed_out``).

    Every series also carries ``forze.realtime.channel``. Emits via the global OTel meter
    unless *meter* is supplied. Call once at assembly time, with the *stats* handed to
    ``attach_realtime_connection`` and the drain step.
    """

    from opentelemetry import metrics

    meter = meter or metrics.get_meter("forze")

    def connections(_options: "CallbackOptions") -> "Iterable[Observation]":
        return [
            metrics.Observation(
                count, {"forze.realtime.channel": channel, "forze.realtime.namespace": namespace}
            )
            for namespace, count in stats.connections.items()
        ]

    def refused(_options: "CallbackOptions") -> "Iterable[Observation]":
        return [
            metrics.Observation(
                count,
                {
                    "forze.realtime.channel": channel,
                    "forze.realtime.namespace": namespace,
                    "forze.realtime.reason": reason,
                },
            )
            for (namespace, reason), count in stats.connect_refused.items()
        ]

    def drain_closed(_options: "CallbackOptions") -> "Iterable[Observation]":
        return [
            metrics.Observation(
                count, {"forze.realtime.channel": channel, "forze.realtime.outcome": outcome}
            )
            for outcome, count in stats.drain_closed.items()
        ]

    meter.create_observable_up_down_counter(
        REALTIME_CONNECTIONS_COUNTER,
        callbacks=[connections],
        unit="1",
        description="Open realtime connections on this node.",
    )
    meter.create_observable_counter(
        REALTIME_CONNECT_REFUSED_COUNTER,
        callbacks=[refused],
        unit="1",
        description="Refused realtime connects, by reason.",
    )
    meter.create_observable_counter(
        REALTIME_DRAIN_CLOSED_COUNTER,
        callbacks=[drain_closed],
        unit="1",
        description="Connections the drain closed, by outcome.",
    )

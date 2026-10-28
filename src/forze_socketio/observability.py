"""Gateway delivery counters — the live-emit half of the realtime plane's observability.

The offline mailbox already exports its counters (``instrument_realtime_mailbox``); this is
the same pattern for the **live path**, which was previously blind: hand the gateway (and its
source) one :class:`RealtimeGatewayStats`, call :func:`instrument_realtime_gateway` once at
assembly, and every delivery outcome becomes an observable counter. OpenTelemetry is imported
lazily, so an uninstrumented app pays nothing. Both live in
:mod:`forze.application.execution.realtime`, shared with the transport-neutral mailbox
writer, and are re-exported here.

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

import threading
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Literal, final

import attrs

from forze.application.execution.realtime import (
    RealtimeGatewayStats as RealtimeGatewayStats,  # re-export: established home
)
from forze.application.execution.realtime import (
    instrument_realtime_gateway as instrument_realtime_gateway,  # re-export: established home
)
from forze.base.primitives import utcnow

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter, Observation

    from forze.application.execution.background import SpreadOutcome

# ----------------------- #

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
class RealtimeConnectionStats:
    """This node's connection counts, for :func:`instrument_realtime_connections`.

    Hand one instance to ``attach_realtime_connection`` for every namespace and to
    ``connection_drain_lifecycle_step``. Counted on the event loop and read at scrape time,
    which may be another thread — the Prometheus route collects in a worker — so the counts
    change only under :attr:`lock`, and :meth:`snapshot` reads them under it too.
    """

    lock: threading.Lock = attrs.field(factory=threading.Lock, init=False, repr=False, eq=False)
    """Guards the three dictionaries against a scrape iterating them mid-change."""

    connections: dict[str, int] = attrs.field(factory=dict)
    """Accepted connections open on this node, by namespace."""

    connect_refused: dict[tuple[str, ConnectRefusal], int] = attrs.field(factory=dict)
    """Refused connects, by namespace and reason."""

    drain_closed: dict[str, int] = attrs.field(factory=dict)
    """Closes the drain started, by outcome: ``closed``, ``failed`` or ``timed_out``."""

    # ....................... #

    def connected(self, namespace: str) -> None:
        """Count an accepted connection."""

        with self.lock:
            self.connections[namespace] = self.connections.get(namespace, 0) + 1

    # ....................... #

    def disconnected(self, namespace: str) -> None:
        """Uncount a connection that was counted."""

        with self.lock:
            self.connections[namespace] = self.connections.get(namespace, 0) - 1

    # ....................... #

    def refused(self, namespace: str, reason: ConnectRefusal) -> None:
        """Count a refused connect."""

        key = (namespace, reason)

        with self.lock:
            self.connect_refused[key] = self.connect_refused.get(key, 0) + 1

    # ....................... #

    def drained(self, outcome: "SpreadOutcome") -> None:
        """Count how the drain's closes ended."""

        with self.lock:
            for name, count in (
                ("closed", outcome.closed),
                ("failed", outcome.failed),
                ("timed_out", outcome.timed_out),
            ):
                self.drain_closed[name] = self.drain_closed.get(name, 0) + count

    # ....................... #

    def snapshot(
        self,
    ) -> tuple[dict[str, int], dict[tuple[str, ConnectRefusal], int], dict[str, int]]:
        """Copies of the connections, refused connects and drain closes, taken under
        :attr:`lock`, safe to read from any thread."""

        with self.lock:
            return dict(self.connections), dict(self.connect_refused), dict(self.drain_closed)


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
            for namespace, count in stats.snapshot()[0].items()
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
            for (namespace, reason), count in stats.snapshot()[1].items()
        ]

    def drain_closed(_options: "CallbackOptions") -> "Iterable[Observation]":
        return [
            metrics.Observation(
                count, {"forze.realtime.channel": channel, "forze.realtime.outcome": outcome}
            )
            for outcome, count in stats.snapshot()[2].items()
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

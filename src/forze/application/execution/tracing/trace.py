"""Runtime tracing data model: events, violations, and the per-task trace buffer."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar, final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TracingEvent:
    """One observed runtime operation in order of execution."""

    seq: int
    """Monotonic sequence number within the trace."""

    at: float = 0.0
    """Monotonic clock reading (via the time seam) when recorded — virtual time under
    simulation, real ``monotonic`` in production. ``0.0`` when unstamped."""

    domain: str
    """Contract family (for example ``tx``, ``document``, ``search``)."""

    op: str
    """Operation name (for example ``enter``, ``exit``, ``get``, ``find_page``)."""

    surface: str | None = None
    """Dependency surface name (typically :class:`~forze.application.contracts.base.DepKey.name`)."""

    route: str | None = None
    """Spec route or transaction route name."""

    phase: str | None = None
    """Port phase when applicable (for example ``query`` or ``command``)."""

    tx_depth: int = 0
    """Active transaction nesting depth when the event was recorded."""

    tx_route: str | None = None
    """Transaction route name when inside or entering a scope."""

    tx_id: int | None = None
    """Run-global id of the root transaction this event belongs to, when one is active. Stamped
    only under simulation (a per-run counter is bound; ``None`` in production), it gives the oracle
    a sound way to group port calls into the transaction that issued them — which operation spans
    cannot, since concurrent transactions interleave. An ``int``, so the trace stays PII-free."""

    key: str | None = None
    """Entity / correlation key the call targets (e.g. a document primary key), when one is
    cheaply available. Recorded id-only (UUID / int) — never free-form values — so the trace
    stays free of PII without a redaction pass."""

    outcome: str | None = None
    """Terminal outcome of an operation event; ``None`` for non-terminal or non-operation
    events. For an operation boundary: ``ok`` (completed), ``failed`` (raised a declared
    domain failure — a :class:`~forze.base.exceptions.CoreException`, an expected outcome), or
    ``error`` (raised an unhandled exception — a bug). The ``failed`` / ``error`` split makes
    the trace the single source of truth for the domain-failure-vs-bug distinction."""

    error: str | None = None
    """Exception type name when ``outcome`` is ``failed`` or ``error``."""

    corr: int | None = None
    """Correlation id linking an operation's terminal (``complete``/``error``) back to its
    ``invoke`` — the invoke's own ``seq``. ``None`` on the invoke itself and on non-operation
    events. Lets a consumer pair a terminal to the exact invoke it belongs to (rather than
    per-op FIFO), so concurrent calls of the same operation are attributed precisely."""

    nested: bool = False
    """Whether this operation ``invoke`` ran *inside* another operation (a cascade — a saga or
    event handler invoking a sub-operation), so it has no top-level driver. ``False`` on
    non-operation events and on top-level invocations."""

    payload: Mapping[str, Any] | None = None
    """A redaction-applied structured view of the call's value argument (the write payload),
    captured **only** when value capture is enabled (off in production → always ``None``, so the
    trace stays id-only and PII-free). Fields the spec declares sensitive are masked to
    ``"<redacted>"``. Lets value-level invariants (wrong-value, lost-update-by-value) assert on
    what was written, not just which key."""

    result: Mapping[str, Any] | None = None
    """A redaction-applied structured view of a read's returned value, captured on the call's
    *return* event under value capture (``None`` otherwise). Lets ``read_your_writes`` assert on
    what a read actually observed."""

    result_native: Mapping[str, Any] | None = None
    """A redaction-applied **native-typed** view of a *write*'s returned row (``mode="python"``: UUID
    / IP / Decimal / datetime kept as objects), captured alongside :attr:`result` only on command
    return events under value capture. The isolation oracle matches a captured scan predicate against
    this — the same native representation the backend's in-memory scan matches — so its predicate
    evaluation agrees with the live scan instead of comparing a JSON string to a native value. Not
    serialized to the portable timeline/bundle (those use :attr:`result`)."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TracingViolation:
    """A validator-reported rule violation from a :class:`RuntimeTrace`."""

    profile: str
    """Validator-defined rule set name (for example ``reads_before_writes_in_tx``)."""

    message: str
    """Human-readable explanation."""

    at_seq: int
    """Sequence number of the offending event."""


# ....................... #


@final
@attrs.define(slots=True)
class RuntimeTrace:
    """Append-only sequence of observed runtime events for one async task."""

    MAX_EVENTS: ClassVar[int] = 10_000
    """Maximum stored events before truncation."""

    events: list[TracingEvent] = attrs.field(factory=list)
    """Recorded events in execution order."""

    _next_seq: int = attrs.field(default=0, init=False)
    _truncated: bool = attrs.field(default=False, init=False)

    # ....................... #

    def record(self, event: TracingEvent) -> None:
        """Append an event (caller supplies ``seq`` via :meth:`next_event`)."""

        if self._truncated:
            return

        if len(self.events) >= self.MAX_EVENTS:
            self._truncated = True
            self.events.append(
                TracingEvent(
                    seq=self._next_seq,
                    domain="tracing",
                    op="truncated",
                    surface=None,
                    route=None,
                    phase=None,
                    tx_depth=0,
                    tx_route=None,
                )
            )
            self._next_seq += 1
            return

        self.events.append(event)

    # ....................... #

    def next_event(
        self,
        *,
        domain: str,
        op: str,
        surface: str | None = None,
        route: str | None = None,
        phase: str | None = None,
        tx_depth: int = 0,
        tx_route: str | None = None,
        tx_id: int | None = None,
        at: float = 0.0,
        key: str | None = None,
        outcome: str | None = None,
        error: str | None = None,
        corr: int | None = None,
        nested: bool = False,
        payload: Mapping[str, Any] | None = None,
        result: Mapping[str, Any] | None = None,
        result_native: Mapping[str, Any] | None = None,
    ) -> TracingEvent:
        """Build and record an event with the next sequence number."""

        event = TracingEvent(
            seq=self._next_seq,
            at=at,
            domain=domain,
            op=op,
            surface=surface,
            route=route,
            phase=phase,
            tx_depth=tx_depth,
            tx_route=tx_route,
            tx_id=tx_id,
            key=key,
            outcome=outcome,
            error=error,
            corr=corr,
            nested=nested,
            payload=payload,
            result=result,
            result_native=result_native,
        )
        self._next_seq += 1
        self.record(event)
        return event

    # ....................... #

    def format_lines(self) -> str:
        """Return human-readable lines for logging."""

        lines: list[str] = []

        for event in self.events:
            parts = [f"{event.seq:04d}", event.domain, event.op]

            if event.surface is not None:
                parts.append(f"surface={event.surface}")

            if event.route is not None:
                parts.append(f"route={event.route}")

            if event.phase is not None:
                parts.append(f"phase={event.phase}")

            if event.tx_route is not None:
                parts.append(f"tx={event.tx_route}")

            if event.key is not None:
                parts.append(f"key={event.key}")

            if event.outcome is not None:
                parts.append(f"outcome={event.outcome}")

            if event.error is not None:
                parts.append(f"error={event.error}")

            parts.append(f"depth={event.tx_depth}")
            lines.append(" ".join(parts))

        return "\n".join(lines)

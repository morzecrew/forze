"""Runtime tracing event and violation records."""

from __future__ import annotations

from typing import final

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

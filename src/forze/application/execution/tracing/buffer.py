"""Runtime trace buffer."""

from __future__ import annotations

from typing import final

import attrs

from .events import TracingEvent

# ----------------------- #


@final
@attrs.define(slots=True)
class RuntimeTrace:
    """Append-only sequence of observed runtime events for one async task."""

    events: list[TracingEvent] = attrs.field(factory=list)
    """Recorded events in execution order."""

    _next_seq: int = attrs.field(default=0, init=False)

    # ....................... #

    def record(self, event: TracingEvent) -> None:
        """Append an event (caller supplies ``seq`` via :meth:`next_event`)."""

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
    ) -> TracingEvent:
        """Build and record an event with the next sequence number."""

        event = TracingEvent(
            seq=self._next_seq,
            domain=domain,
            op=op,
            surface=surface,
            route=route,
            phase=phase,
            tx_depth=tx_depth,
            tx_route=tx_route,
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

            parts.append(f"depth={event.tx_depth}")
            lines.append(" ".join(parts))

        return "\n".join(lines)

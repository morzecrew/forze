"""Request-scoped, per-route buffers for outbox staging."""

from __future__ import annotations

from contextvars import ContextVar

import attrs

from forze.base.primitives import ContextualBuffer

from .value_objects import StagedOutboxEntry

# ----------------------- #


@attrs.define(slots=True, kw_only=True)
class OutboxStagingContext:
    """Per-request outbox staging state, keyed by outbox route.

    Each outbox route (``OutboxSpec.name``) gets its own
    :class:`~forze.base.primitives.ContextualBuffer` and flushed flag, so one
    route's flush never pops or blocks another route's staged rows. Both the
    buffer contents and the flushed flag are additionally scoped per async
    task via :class:`~contextvars.ContextVar`.

    The per-route buffers create per-instance ``ContextVar``s. This is intentional and
    safe because an ``OutboxStagingContext`` is a per-runtime-scope singleton (one per
    execution context), never created per request; see
    :class:`~forze.base.primitives.ContextualBuffer` for the rationale.
    """

    _buffers: dict[str, ContextualBuffer[StagedOutboxEntry]] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
        eq=False,
    )
    """Lazily created per-route buffers (route -> contextual buffer)."""

    _flushed_vars: dict[str, ContextVar[bool]] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
        eq=False,
    )
    """Lazily created per-route, per-task flush flags."""

    # ....................... #

    def buffer_for(self, route: str) -> ContextualBuffer[StagedOutboxEntry]:
        """Return the staging buffer for *route*, creating it lazily."""

        buffer = self._buffers.get(route)

        if buffer is None:
            # ``setdefault`` keeps creation race-free across threads; in asyncio
            # there is no await between the miss and the insert.
            buffer = self._buffers.setdefault(
                route,
                ContextualBuffer[StagedOutboxEntry](),
            )

        return buffer

    # ....................... #

    def flushed_for(self, route: str) -> bool:
        """Whether *route* was flushed in the current task."""

        var = self._flushed_vars.get(route)

        return var.get() if var is not None else False

    # ....................... #

    def set_flushed(self, route: str, value: bool) -> None:
        """Set the per-task flushed flag for *route*."""

        var = self._flushed_vars.get(route)

        if var is None:
            var = self._flushed_vars.setdefault(
                route,
                ContextVar(f"outbox_flushed::{route}", default=False),
            )

        var.set(value)

    # ....................... #

    def peek(self, route: str | None = None) -> list[StagedOutboxEntry]:
        """Staged entries for *route*, or across all routes when omitted."""

        if route is not None:
            return self.buffer_for(route).peek()

        entries: list[StagedOutboxEntry] = []

        for buffer in self._buffers.values():
            entries.extend(buffer.peek())

        return entries

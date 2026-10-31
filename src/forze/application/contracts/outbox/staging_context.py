"""Request-scoped, per-route buffers for outbox staging."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
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
    task via :class:`~contextvars.ContextVar`, and follow the transaction scopes
    the task opens (see :meth:`transaction_scope`).

    The per-route buffers create per-instance ``ContextVar``s. This is intentional and
    safe because an ``OutboxStagingContext`` is a per-runtime-scope singleton (one per
    execution context), never created per request; see
    :class:`~forze.base.primitives.ContextualBuffer` for the rationale.

    **Invariant:** ``route`` is a static ``OutboxSpec.name`` — a finite, registry-bounded
    set — so ``_buffers`` / ``_flushed_vars`` and the ``ContextVar``s they create stay
    bounded. A ``ContextVar`` is never reclaimed by the contextvars machinery, so a
    per-tenant or otherwise dynamic ``route`` would leak one per distinct value. Any future
    dynamic-route feature must key these by a bounded id (not the raw route) or cap them.
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

    @contextmanager
    def transaction_scope(self, *, root: bool) -> Iterator[None]:
        """Scope staging to one transaction scope of the current task.

        A *root* scope is a new unit of work: it reopens every route's flushed flag, so one
        task can run one transaction after another and each can stage and flush. Events still
        staged when a transaction commits without a flush stay staged, and the task's next
        flush writes them. A nested scope (a savepoint) leaves the flags alone — a route its
        root already flushed stays closed, as rows staged after that flush would never be
        written.

        A scope that exits with an exception rolled back, so it restores the buffers and flags
        it found on entry: the events it staged are discarded, and those its flush wrote in
        the rolled-back transaction are staged again for the transaction still running.

        :param root: Whether the scope is a root transaction rather than a savepoint.
        """

        # Copied up front: the dicts may grow while the scope runs (a first stage on a route).
        # Each list is kept beside its contents: tasks started from this one share the list
        # object, so the restore changes it in place for every holder to see.
        entered: dict[str, tuple[list[StagedOutboxEntry], list[StagedOutboxEntry]]] = {}

        for route, buffer in list(self._buffers.items()):
            held = buffer.peek()
            entered[route] = (held, list(held))

        flushed = {route: var.get() for route, var in list(self._flushed_vars.items())}

        if root:
            for var in list(self._flushed_vars.values()):
                var.set(False)

        try:
            yield

        except BaseException:
            for route, buffer in list(self._buffers.items()):
                held, contents = entered.get(route, ([], []))

                # The task's current list too: a flush in the scope replaced the one it held.
                for entries in (held, buffer.peek()):
                    entries[:] = contents

            for route, var in list(self._flushed_vars.items()):
                var.set(flushed.get(route, False))

            raise

    # ....................... #

    def peek(self, route: str | None = None) -> list[StagedOutboxEntry]:
        """Staged entries for *route*, or across all routes when omitted."""

        if route is not None:
            return self.buffer_for(route).peek()

        entries: list[StagedOutboxEntry] = []

        for buffer in self._buffers.values():
            entries.extend(buffer.peek())

        return entries

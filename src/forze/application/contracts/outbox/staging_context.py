"""Request-scoped buffer for outbox staging."""

from __future__ import annotations

from contextvars import ContextVar

import attrs

from forze.base.primitives import ContextualBuffer

from .value_objects import StagedOutboxEntry

# ----------------------- #


@attrs.define(slots=True, kw_only=True)
class OutboxStagingContext:
    """Per-request outbox staging state."""

    buffer: ContextualBuffer[StagedOutboxEntry] = attrs.field(
        factory=ContextualBuffer[StagedOutboxEntry],
    )
    """Buffered entries awaiting flush via :class:`~forze.application.contracts.outbox.OutboxCommandPort`."""

    _flushed: ContextVar[bool] = attrs.field(
        factory=lambda: ContextVar("outbox_flushed", default=False),
        init=False,
        repr=False,
        eq=False,
    )
    """Per-task flush flag, scoped like :attr:`buffer` (fresh per async task)."""

    # ....................... #

    @property
    def flushed(self) -> bool:
        """Whether the buffer was flushed for the current task."""

        return self._flushed.get()

    # ....................... #

    @flushed.setter
    def flushed(self, value: bool) -> None:
        self._flushed.set(value)

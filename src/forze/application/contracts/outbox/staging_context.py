"""Request-scoped buffer for outbox staging."""

from __future__ import annotations

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

    flushed: bool = False
    """Whether the buffer was flushed for this request."""

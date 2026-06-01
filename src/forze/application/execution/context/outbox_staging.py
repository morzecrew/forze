"""Request-scoped buffer for outbox staging."""

from __future__ import annotations

import attrs

from forze.application.contracts.outbox import StagedOutboxEntry
from forze.base.primitives import ContextualBuffer

# ----------------------- #


@attrs.define(slots=True, kw_only=True)
class OutboxStagingContext:
    """Per-request outbox staging state."""

    buffer: ContextualBuffer[StagedOutboxEntry] = attrs.field(
        factory=ContextualBuffer[StagedOutboxEntry],
    )
    """Buffered entries awaiting :meth:`~forze.application.contracts.outbox.OutboxCommandPort.flush`."""

    flushed: bool = False
    """Whether the buffer was flushed for this request."""

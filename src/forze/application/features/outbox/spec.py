"""Pre-built :class:`~forze.application.contracts.document.DocumentSpec` for outbox events."""

from forze.application.contracts.document import DocumentSpec

from .model import (
    CreateOutboxEventCmd,
    OutboxEvent,
    ReadOutboxEvent,
    UpdateOutboxEventCmd,
)

# ----------------------- #

OutboxSpec = DocumentSpec[
    ReadOutboxEvent,
    OutboxEvent,
    CreateOutboxEventCmd,
    UpdateOutboxEventCmd,
]
"""Document specification wired for outbox event models."""

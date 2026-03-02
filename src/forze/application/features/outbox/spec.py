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

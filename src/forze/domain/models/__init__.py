"""Domain models exported by the ``forze.domain.models`` package."""

from ..validation import invariant
from .aggregate import AggregateRoot
from .base import BaseDTO, CoreModel
from .document import (
    CreateDocumentCmd,
    Document,
    DocumentHistory,
    ReadDocument,
    ReplaceOnUpdate,
    replace_on_update_fields,
)
from .emitters import event_emitter
from .events import DomainEvent

# ----------------------- #


__all__ = [
    "AggregateRoot",
    "CoreModel",
    "BaseDTO",
    "Document",
    "CreateDocumentCmd",
    "ReadDocument",
    "DocumentHistory",
    "ReplaceOnUpdate",
    "replace_on_update_fields",
    "DomainEvent",
    "event_emitter",
    "invariant",
]

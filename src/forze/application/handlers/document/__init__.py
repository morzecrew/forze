"""Document handlers."""

from .create import CreateDocument
from .delete import DeleteDocument, KillDocument, RestoreDocument
from .get import GetDocument
from .list_ import (
    AggregatedListDocuments,
    RawCursorListDocuments,
    RawListDocuments,
    TypedCursorListDocuments,
    TypedListDocuments,
)
from .update import UpdateDocument

# ----------------------- #

__all__ = [
    "CreateDocument",
    "DeleteDocument",
    "KillDocument",
    "RestoreDocument",
    "GetDocument",
    "UpdateDocument",
    "TypedListDocuments",
    "RawListDocuments",
    "TypedCursorListDocuments",
    "RawCursorListDocuments",
    "AggregatedListDocuments",
]

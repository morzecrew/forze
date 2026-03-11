"""Document CRUD and search usecases.

Provides create, read, update, delete, restore, kill, and search operations
backed by :class:`forze.application.contracts.document.DocumentPort` and
:class:`forze.application.contracts.document.DocumentReadPort`.
"""

from .create import CreateDocument
from .delete import DeleteDocument, KillDocument, RestoreDocument, SoftDeleteArgs
from .get import GetDocument
from .list_ import (
    RawListDocuments,
    RawListDocumentsArgs,
    TypedListDocuments,
    TypedListDocumentsArgs,
)
from .update import UpdateArgs, UpdateDocument

# ----------------------- #

__all__ = [
    "CreateDocument",
    "DeleteDocument",
    "KillDocument",
    "RestoreDocument",
    "GetDocument",
    "UpdateDocument",
    "UpdateArgs",
    "SoftDeleteArgs",
    "TypedListDocuments",
    "RawListDocuments",
    "TypedListDocumentsArgs",
    "RawListDocumentsArgs",
]

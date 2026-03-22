"""Document CRUD and search usecases.

Provides create, read, update, delete, restore, kill, and search operations
backed by :class:`forze.application.contracts.document.DocumentWritePort` and
:class:`forze.application.contracts.document.DocumentReadPort`.
"""

from .create import CreateDocument
from .delete import DeleteDocument, KillDocument, RestoreDocument
from .get import GetDocument
from .list_ import RawListDocuments, TypedListDocuments
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
]

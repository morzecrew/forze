"""Document operation identifiers for usecase registration and resolution."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class DocumentOperation(StrEnum):
    """Logical operation identifiers for document usecases.

    Used as keys in :class:`UsecaseRegistry` and when resolving usecases from
    :class:`DocumentUsecasesFacade`. Values are dot-prefixed for namespacing.
    """

    GET = "document.get"
    """Fetch a single document by primary key."""

    SEARCH = "document.search"
    """Search with typed paginated results."""

    RAW_SEARCH = "document.raw_search"
    """Search with field-projected raw results."""

    CREATE = "document.create"
    """Create a new document."""

    UPDATE = "document.update"
    """Update an existing document."""

    KILL = "document.kill"
    """Permanently delete a document (hard delete)."""

    DELETE = "document.delete"
    """Soft-delete a document."""

    RESTORE = "document.restore"
    """Restore a soft-deleted document."""

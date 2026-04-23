"""Document operation identifiers for usecase registration and resolution."""

from enum import StrEnum
from typing import final

# ----------------------- #
#! Review "DocumentInternalOperation" where can be: import, upsert, ensure


@final
class DocumentOperation(StrEnum):
    """Logical operation identifiers for document usecases."""

    GET = "document.get"
    """Fetch a single document by primary key."""

    GET_BY_NUMBER_ID = "document.get_by_number_id"
    """Fetch a single document by number ID."""

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

    LIST = "document.list"
    """List documents."""

    RAW_LIST = "document.raw_list"
    """List documents with raw results."""

    LIST_CURSOR = "document.list_cursor"
    """List documents with cursor-based pagination."""

    RAW_LIST_CURSOR = "document.raw_list_cursor"
    """List documents with cursor-based pagination and raw results."""

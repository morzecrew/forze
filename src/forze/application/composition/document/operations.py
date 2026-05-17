"""Document operation kernel suffixes for usecase registration and resolution."""

from enum import StrEnum
from typing import final

# ----------------------- #
#! Review "DocumentInternalOperation" where can be: import, upsert, ensure


@final
class DocumentKernelOp(StrEnum):
    """Kernel segments (suffix only) for document usecase operation keys."""

    GET = "get"
    """Fetch a single document by primary key."""

    GET_BY_NUMBER_ID = "get_by_number_id"
    """Fetch a single document by number ID."""

    CREATE = "create"
    """Create a new document."""

    UPDATE = "update"
    """Update an existing document."""

    KILL = "kill"
    """Permanently delete a document (hard delete)."""

    DELETE = "delete"
    """Soft-delete a document."""

    RESTORE = "restore"
    """Restore a soft-deleted document."""

    LIST = "list"
    """List documents."""

    RAW_LIST = "raw_list"
    """List documents with raw results."""

    LIST_CURSOR = "list_cursor"
    """List documents with cursor-based pagination."""

    RAW_LIST_CURSOR = "raw_list_cursor"
    """List documents with cursor-based pagination and raw results."""

    AGG_LIST = "agg_list"
    """List documents with aggregates."""

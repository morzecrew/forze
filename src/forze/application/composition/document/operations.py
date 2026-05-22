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

    CREATE = "create"
    """Create a new document."""

    UPDATE = "update"
    """Update an existing document."""

    KILL = "kill"
    """Permanently delete a document (hard delete)."""

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

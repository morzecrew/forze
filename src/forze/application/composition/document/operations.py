from enum import StrEnum
from typing import final

# ----------------------- #


@final
class DocumentOperation(StrEnum):
    """Logical operation identifiers for document usecases."""

    GET = "document.get"
    SEARCH = "document.search"
    RAW_SEARCH = "document.raw_search"
    CREATE = "document.create"
    UPDATE = "document.update"
    KILL = "document.kill"
    DELETE = "document.delete"
    RESTORE = "document.restore"

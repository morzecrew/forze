from typing import NotRequired, TypedDict

# ----------------------- #


class MongoDocumentConfig(TypedDict):
    """Mapping from document name to its MongoDB-specific mapping."""

    read: tuple[str, str]
    """Read collection (database, collection / view)"""

    write: NotRequired[tuple[str, str]]
    """Write collection (database, collection), optional."""

    history: NotRequired[tuple[str, str]]
    """History collection (database, collection), optional."""

    tenant_aware: NotRequired[bool]
    """Whether the document is tenant-aware."""


# ....................... #

MongoDocumentConfigs = dict[str, MongoDocumentConfig]

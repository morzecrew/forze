from typing import NotRequired, TypedDict, final

# ----------------------- #


class _BaseMongoConfig(TypedDict):
    """Base configuration for a Mongo resource."""

    tenant_aware: NotRequired[bool]
    """Whether the resource is tenant-aware."""


# ....................... #


class MongoReadOnlyDocumentConfig(_BaseMongoConfig):
    """Configuration for a Mongo read-only document."""

    read: tuple[str, str]
    """Read collection (database, collection / view)"""

    batch_size: NotRequired[int]
    """Chunk size for bulk writes and for internal chunked offset reads when pagination
    omits ``limit``. Optional; defaults to 200."""


# ....................... #


@final
class MongoDocumentConfig(MongoReadOnlyDocumentConfig):
    """Mapping from document name to its MongoDB-specific mapping."""

    write: tuple[str, str]
    """Write collection (database, collection), optional."""

    history: NotRequired[tuple[str, str]]
    """History collection (database, collection), optional."""

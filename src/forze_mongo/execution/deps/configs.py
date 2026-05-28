from typing import Any, Literal, Mapping, NotRequired, TypedDict, final

from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import exc

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


# ....................... #


class MongoSearchConfig(_BaseMongoConfig):
    """Physical Mongo mapping for one :class:`~forze.application.contracts.search.SearchSpec` route."""

    read: tuple[str, str]
    """Read collection (database, collection) for filters and row shape."""

    engine: Literal["text", "atlas", "vector"]
    """Search engine: native text index, Atlas Search, or vector KNN."""

    field_map: NotRequired[Mapping[str, str]]
    """Maps :class:`SearchSpec` field names to BSON paths when they differ."""

    index_name: NotRequired[str]
    """Physical index name; meaning depends on :attr:`engine`.

    - ``atlas``: Atlas Search index for ``$search`` (required).
    - ``vector``: Atlas Vector Search index for ``$vectorSearch`` (required).
    - ``text``: unused by queries (Mongo ``$text`` uses the collection text index); optional for ops/docs.
    """

    default_language: NotRequired[str]
    """Default text index language when creating a text index (``text`` engine)."""

    vector_path: NotRequired[str]
    """Document field holding the embedding array (``vector`` engine)."""

    embeddings_name: NotRequired[str]
    """:class:`~forze.application.contracts.embeddings.EmbeddingsSpec` name for query embedding."""

    embedding_dimensions: NotRequired[int]
    """Expected query embedding size (``vector`` engine)."""


# ....................... #


def validate_mongo_search_conf(
    cfg: MongoSearchConfig,
    spec: SearchSpec[Any] | None = None,
) -> None:
    """Validate a Mongo search configuration (and optionally its logical spec)."""

    eng = cfg["engine"]
    field_map = cfg.get("field_map") or {}

    if spec is not None:
        for key in field_map:
            if key not in spec.fields:
                raise exc.internal(
                    f"field_map key {key!r} is not in SearchSpec.fields for {spec.name!r}."
                )

    match eng:
        case "atlas":
            if not cfg.get("index_name"):
                raise exc.internal("index_name is required for atlas engine.")

        case "vector":
            if not cfg.get("vector_path"):
                raise exc.internal("vector_path is required for vector engine.")

            if not cfg.get("index_name"):
                raise exc.internal("index_name is required for vector engine.")

            if not cfg.get("embeddings_name"):
                raise exc.internal("embeddings_name is required for vector engine.")

            if cfg.get("embedding_dimensions") is None:
                raise exc.internal(
                    "embedding_dimensions is required for vector engine."
                )

        case "text":
            pass

        case _:  # pyright: ignore[reportUnnecessaryComparison]
            raise exc.internal(f"Unsupported Mongo search engine: {eng!r}.")

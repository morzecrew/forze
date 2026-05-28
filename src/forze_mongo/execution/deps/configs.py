"""Mongo dependency integration configs (frozen attrs)."""

from typing import TYPE_CHECKING, Any, Literal, Mapping

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc
if TYPE_CHECKING:
    from forze.application.contracts.search import SearchSpec

# ----------------------- #

MongoSearchEngine = Literal["text", "atlas", "vector"]

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoReadOnlyDocumentConfig(TenantAwareIntegrationConfig):
    """Configuration for a Mongo read-only document."""

    read: tuple[str, str]
    """Read collection (database, collection / view)."""

    batch_size: int = 200
    """Chunk size for bulk writes and internal chunked offset reads."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoDocumentConfig(MongoReadOnlyDocumentConfig):
    """Configuration for a Mongo read-write document."""

    write: tuple[str, str]
    """Write collection (database, collection)."""

    history: tuple[str, str] | None = None
    """History collection (database, collection), optional."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoSearchConfig(TenantAwareIntegrationConfig):
    """Physical Mongo mapping for one :class:`~forze.application.contracts.search.SearchSpec` route."""

    read: tuple[str, str]
    """Read collection (database, collection) for filters and row shape."""

    engine: MongoSearchEngine
    """Search engine: native text index, Atlas Search, or vector KNN."""

    field_map: Mapping[str, str] | None = None
    """Maps :class:`SearchSpec` field names to BSON paths when they differ."""

    index_name: str | None = None
    """Physical index name (required for ``atlas`` and ``vector`` engines)."""

    default_language: str | None = None
    """Default text index language when creating a text index (``text`` engine)."""

    vector_path: str | None = None
    """Document field holding the embedding array (``vector`` engine)."""

    embeddings_name: str | None = None
    """Embeddings spec name for query embedding (``vector`` engine)."""

    embedding_dimensions: int | None = None
    """Expected query embedding size (``vector`` engine)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        match self.engine:
            case "atlas":
                if not self.index_name:
                    raise exc.internal("index_name is required for atlas engine.")

            case "vector":
                if not self.vector_path:
                    raise exc.internal("vector_path is required for vector engine.")

                if not self.index_name:
                    raise exc.internal("index_name is required for vector engine.")

                if not self.embeddings_name:
                    raise exc.internal("embeddings_name is required for vector engine.")

                if self.embedding_dimensions is None:
                    raise exc.internal(
                        "embedding_dimensions is required for vector engine."
                    )

            case "text":
                pass

            case _:  # pyright: ignore[reportUnnecessaryComparison]
                raise exc.internal(f"Unsupported Mongo search engine: {self.engine!r}.")

    # ....................... #

    def validate_against_spec(self, spec: "SearchSpec[Any]") -> None:
        """Validate field_map keys against the logical search spec."""

        if self.field_map is None:
            return

        for key in self.field_map:
            if key not in spec.fields:
                raise exc.internal(
                    f"field_map key {key!r} is not in SearchSpec.fields for {spec.name!r}."
                )

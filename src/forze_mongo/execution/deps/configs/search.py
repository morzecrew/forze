"""Mongo search execution configs."""

from typing import TYPE_CHECKING, Any, Literal, Mapping

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    RelationSpec,
    coerce_named_resource_spec,
    coerce_relation_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc

if TYPE_CHECKING:
    from forze.application.contracts.search import SearchSpec

# ----------------------- #

MongoSearchEngine = Literal["text", "atlas", "vector"]

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoSearchConfig(TenantAwareIntegrationConfig):  # type: ignore[no-untyped-def]
    """Physical Mongo mapping for one :class:`~forze.application.contracts.search.SearchSpec` route."""

    read: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Read collection (database, collection) for filters and row shape."""

    engine: MongoSearchEngine
    """Search engine: native text index, Atlas Search, or vector KNN."""

    field_map: Mapping[str, str] | None = None
    """Maps :class:`SearchSpec` field names to BSON paths when they differ."""

    index_name: NamedResourceSpec | None = attrs.field(  # type: ignore[var-annotated]
        default=None,
        converter=lambda v: coerce_named_resource_spec(v) if v is not None else None,
    )
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

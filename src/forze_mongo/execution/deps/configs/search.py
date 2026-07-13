"""Mongo search execution configs."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal

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
"""Engine discriminator string (the resolved kind of :attr:`MongoSearchConfig.engine`)."""


def _named_resource_spec(value: object) -> NamedResourceSpec:
    return coerce_named_resource_spec(value)  # type: ignore[arg-type]


# ----------------------- #
# Engine variants (the public construction surface for ``engine=``)


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoTextEngine:
    """Native Mongo text-index engine."""

    default_language: str | None = None
    """Default text index language when creating a text index."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoAtlasEngine:
    """Atlas Search engine."""

    index_name: NamedResourceSpec = attrs.field(converter=_named_resource_spec)
    """Physical Atlas Search index name."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.index_name:
            raise exc.configuration("index_name is required for atlas engine.")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoVectorEngine:
    """Atlas vector KNN engine."""

    index_name: NamedResourceSpec = attrs.field(converter=_named_resource_spec)
    """Physical vector index name."""

    vector_path: str
    """Document field holding the embedding array."""

    embeddings_name: str
    """Embeddings spec name for query embedding."""

    dimensions: int
    """Expected query embedding size."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.index_name:
            raise exc.configuration("index_name is required for vector engine.")

        if not self.vector_path:
            raise exc.configuration("vector_path is required for vector engine.")

        if not self.embeddings_name:
            raise exc.configuration("embeddings_name is required for vector engine.")

        if self.dimensions < 1:
            raise exc.configuration("embedding_dimensions must be at least 1.")


# ....................... #

MongoSearchEngineSpec = MongoTextEngine | MongoAtlasEngine | MongoVectorEngine
"""Tagged union of engine variants. Construct one and pass it as ``engine=``."""


def _coerce_mongo_engine_spec(
    value: "MongoSearchEngineSpec | MongoSearchEngine",
) -> MongoSearchEngineSpec:
    """Normalize the ``engine=`` argument to an engine value object.

    Accepts an engine value object directly, or the bare string ``"text"`` as a
    shorthand for a default-configured :class:`MongoTextEngine`. ``"atlas"`` and
    ``"vector"`` have no valid defaults (they require fields), so the bare string is
    rejected with a pointer to the value object to use instead.
    """

    if isinstance(value, (MongoTextEngine, MongoAtlasEngine, MongoVectorEngine)):
        return value

    if value == "text":
        return MongoTextEngine()

    if value == "atlas":
        raise exc.configuration(
            "engine='atlas' requires an index; pass engine=MongoAtlasEngine(index_name=...).",
        )

    if value == "vector":
        raise exc.configuration(
            "engine='vector' requires fields; pass engine=MongoVectorEngine("
            "index_name=..., vector_path=..., embeddings_name=..., dimensions=...).",
        )

    raise exc.configuration(f"Unknown Mongo search engine: {value!r}")


# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoSearchConfig(TenantAwareIntegrationConfig):
    """Physical Mongo mapping for one :class:`~forze.application.contracts.search.SearchSpec` route."""

    read: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Read collection (database, collection) for filters and row shape."""

    engine_spec: MongoSearchEngineSpec = attrs.field(
        alias="engine",
        converter=_coerce_mongo_engine_spec,  # type: ignore[misc]
    )
    """Engine variant: :class:`MongoTextEngine`, :class:`MongoAtlasEngine`, or
    :class:`MongoVectorEngine` (construct with ``engine=``; ``"text"`` is accepted as a
    shorthand for ``MongoTextEngine()``).

    Read :attr:`engine` for the resolved discriminator string."""

    field_map: Mapping[str, str] | None = None
    """Maps :class:`SearchSpec` field names to BSON paths when they differ."""

    # ....................... #
    # Flat read shims: internal factories/module/warnings read engine knobs by their
    # historical flat names; each dispatches into the active engine variant.

    @property
    def engine(self) -> MongoSearchEngine:
        """Resolved engine discriminator string (``text`` / ``atlas`` / ``vector``)."""

        match self.engine_spec:
            case MongoTextEngine():
                return "text"

            case MongoAtlasEngine():
                return "atlas"

            case MongoVectorEngine():
                return "vector"

    @property
    def index_name(self) -> NamedResourceSpec | None:
        if isinstance(self.engine_spec, (MongoAtlasEngine, MongoVectorEngine)):
            return self.engine_spec.index_name

        return None

    @property
    def default_language(self) -> str | None:
        return (
            self.engine_spec.default_language
            if isinstance(self.engine_spec, MongoTextEngine)
            else None
        )

    @property
    def vector_path(self) -> str | None:
        return (
            self.engine_spec.vector_path
            if isinstance(self.engine_spec, MongoVectorEngine)
            else None
        )

    @property
    def embeddings_name(self) -> str | None:
        return (
            self.engine_spec.embeddings_name
            if isinstance(self.engine_spec, MongoVectorEngine)
            else None
        )

    @property
    def embedding_dimensions(self) -> int | None:
        return (
            self.engine_spec.dimensions if isinstance(self.engine_spec, MongoVectorEngine) else None
        )

    # ....................... #

    def validate_against_spec(self, spec: "SearchSpec[Any]") -> None:
        """Validate field_map keys against the logical search spec."""

        if self.field_map is None:
            return

        for key in self.field_map:
            if key not in spec.fields:
                raise exc.configuration(
                    f"field_map key {key!r} is not in SearchSpec.fields for {spec.name!r}."
                )

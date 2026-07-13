"""Postgres single-index search execution configs and validation."""

from collections.abc import Mapping, Sequence
from functools import reduce
from typing import TYPE_CHECKING, Any, Literal, get_args

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze_postgres.kernel.relation import RelationSpec, coerce_relation_spec

from ....adapters import FtsGroupLetter

if TYPE_CHECKING:
    from forze.application.contracts.search import SearchSpec

# ----------------------- #

VectorEngineDistance = Literal["l2", "cosine", "inner_product"]

PgroongaScoreVersion = Literal["v1", "v2"]
"""``v1``: ``pgroonga_score(heap_alias)``. ``v2``: ``pgroonga_score(tableoid, ctid)`` (default)."""

PgroongaPlan = Literal["filter_first", "index_first", "auto"]
"""PGroonga ranked search SQL shape."""

SearchEngine = Literal["pgroonga", "fts", "vector"]
"""Engine discriminator string (the resolved kind of :attr:`PostgresSearchConfig.engine`)."""

# ....................... #

_DEFAULT_PGROONGA_CANDIDATE_LIMIT = 5000
_DEFAULT_PGROONGA_AUTO_INDEX_FIRST_MIN_ROWS = 100_000
_DEFAULT_PGROONGA_AUTO_FILTER_FIRST_MAX_ROWS = 50_000
_DEFAULT_PGROONGA_INDEX_FIRST_FILTER_MARGIN = 3.0

# ....................... #


def _optional_relation_spec(value: object) -> RelationSpec | None:
    return None if value is None else coerce_relation_spec(value)


# ----------------------- #
# Engine variants (the public construction surface for ``engine=``)


@attrs.define(slots=True, kw_only=True, frozen=True)
class PgroongaAuto:
    """Auto-plan tuning for the PGroonga engine (used when :attr:`PgroongaEngine.plan` is ``auto``)."""

    index_first_min_rows: int = _DEFAULT_PGROONGA_AUTO_INDEX_FIRST_MIN_ROWS
    """When filters are empty, use ``index_first`` if the read relation estimate is at least this many rows."""

    use_exact_count: bool = False
    """Run ``COUNT(*)`` on the filtered projection to pick the plan (extra round trip)."""

    with_filters: bool = True
    """When filters are index-first eligible, use planner estimates to pick the plan."""

    filter_first_max_rows: int = _DEFAULT_PGROONGA_AUTO_FILTER_FIRST_MAX_ROWS
    """With :attr:`with_filters`, prefer ``filter_first`` when the filtered estimate is at most this many rows."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.index_first_min_rows < 1:
            raise exc.configuration("pgroonga auto index_first_min_rows must be at least 1.")

        if self.filter_first_max_rows < 1:
            raise exc.configuration("pgroonga auto filter_first_max_rows must be at least 1.")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PgroongaEngine:
    """PGroonga full-text search engine."""

    score_version: PgroongaScoreVersion = "v2"
    """PGroonga score overload."""

    plan: PgroongaPlan = "filter_first"
    """Ranked search plan (``filter_first``, ``index_first``, ``auto``)."""

    index_first_filter_margin: float = _DEFAULT_PGROONGA_INDEX_FIRST_FILTER_MARGIN
    """Multiply the heap top-K cap when index-first applies projection post-filters."""

    auto: PgroongaAuto = attrs.field(factory=PgroongaAuto)
    """Auto-plan tuning; only consulted when :attr:`plan` is ``auto``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.score_version not in ("v1", "v2"):
            raise exc.configuration("pgroonga_score_version must be 'v1' or 'v2'.")

        if self.plan not in ("filter_first", "index_first", "auto"):
            raise exc.configuration(
                "pgroonga_plan must be 'filter_first', 'index_first', or 'auto'.",
            )

        if self.index_first_filter_margin < 1.0:
            raise exc.configuration("pgroonga index_first_filter_margin must be at least 1.0.")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FtsEngine:
    """Postgres native FTS (``tsvector``) engine."""

    groups: dict[FtsGroupLetter, Sequence[str]]
    """FTS weight groups; every search field must appear in exactly one group."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.groups:
            raise exc.configuration("FTS groups are required for FTS engine.")

        invalid = set(self.groups) - set(get_args(FtsGroupLetter))

        if invalid:
            raise exc.configuration(
                f"FTS group letters must be one of A, B, C, D; got {sorted(invalid)!r}. "
                "Other letters are silently dropped by the rank weights.",
            )

        all_fields = reduce(lambda a, g: a + g, map(list, self.groups.values()))

        if len(all_fields) != len(set(all_fields)):
            raise exc.configuration("FTS groups cannot contain duplicate fields.")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class VectorEngine:
    """pgvector KNN engine."""

    column: str
    """Heap ``vector`` column."""

    embeddings_name: StrKey
    """Embeddings spec name."""

    dimensions: int
    """Query embedding size."""

    distance: VectorEngineDistance = "l2"
    """pgvector distance operator family."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.column:
            raise exc.configuration("vector_column is required for vector engine.")

        if not self.embeddings_name:
            raise exc.configuration("embeddings_name is required for vector engine.")

        if self.dimensions < 1:
            raise exc.configuration("embedding_dimensions must be at least 1.")


# ....................... #

SearchEngineSpec = PgroongaEngine | FtsEngine | VectorEngine
"""Tagged union of engine variants. Construct one and pass it as ``engine=``."""

# ....................... #


def _coerce_engine_spec(value: "SearchEngineSpec | SearchEngine") -> SearchEngineSpec:
    """Normalize the ``engine=`` argument to an engine value object.

    Accepts an engine value object directly, or the bare string ``"pgroonga"`` as a
    shorthand for a default-configured :class:`PgroongaEngine`. ``"fts"`` and ``"vector"``
    have no valid defaults (they require fields), so the bare string is rejected with a
    pointer to the value object to use instead.
    """

    if isinstance(value, (PgroongaEngine, FtsEngine, VectorEngine)):
        return value

    if value == "pgroonga":
        return PgroongaEngine()

    if value == "fts":
        raise exc.configuration(
            "engine='fts' requires groups; pass engine=FtsEngine(groups=...).",
        )

    if value == "vector":
        raise exc.configuration(
            "engine='vector' requires fields; pass "
            "engine=VectorEngine(column=..., embeddings_name=..., dimensions=...).",
        )

    raise exc.configuration(f"Unknown search engine: {value!r}")


# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresSearchConfig(TenantAwareIntegrationConfig):
    """Configuration for a Postgres search route."""

    index: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Index relation (schema, index name) or resolver."""

    read: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Read relation for filters and row shape or resolver."""

    read_validation: Literal["strict", "trusted"] = "strict"
    """Row decode mode for search hits (``trusted`` skips Pydantic validation)."""

    engine_spec: SearchEngineSpec = attrs.field(
        alias="engine",
        converter=_coerce_engine_spec,  # type: ignore[misc]
    )
    """Engine variant: :class:`PgroongaEngine`, :class:`FtsEngine`, or :class:`VectorEngine`
    (construct with ``engine=``; ``"pgroonga"`` is accepted as a shorthand for ``PgroongaEngine()``).

    Read :attr:`engine` for the resolved discriminator string."""

    heap: RelationSpec | None = attrs.field(
        default=None,
        converter=_optional_relation_spec,
    )
    """Heap relation; defaults to :attr:`read` when omitted."""

    candidate_limit: int | None = _DEFAULT_PGROONGA_CANDIDATE_LIMIT
    """Max heap rows scored per query; the shared ranked-heap cap for every engine.
    ``None`` disables the cap."""

    field_map: Mapping[str, str] | None = None
    """Maps spec field names to physical heap columns."""

    join_pairs: Sequence[tuple[str, str]] | None = None
    """Join pairs (projection column, index heap column)."""

    nested_field_hints: Mapping[str, Any] | None = None
    """Per-path type hints for filters/sorts on the read projection."""

    # ....................... #

    @property
    def heap_relation(self) -> RelationSpec:
        """Heap qualified name used for index joins."""

        return self.heap if self.heap is not None else self.read

    # ....................... #
    # Flat read shims: internal factories/adapters/lifecycle read the engine knobs by
    # their historical flat names; each dispatches into the active engine variant and
    # returns the prior default for non-active variants (only ``candidate_limit`` is
    # genuinely shared and read across variants).

    @property
    def engine(self) -> SearchEngine:
        """Resolved engine discriminator string (``pgroonga`` / ``fts`` / ``vector``)."""

        match self.engine_spec:
            case PgroongaEngine():
                return "pgroonga"

            case FtsEngine():
                return "fts"

            case VectorEngine():
                return "vector"

    @property
    def fts_groups(self) -> dict[FtsGroupLetter, Sequence[str]] | None:
        return self.engine_spec.groups if isinstance(self.engine_spec, FtsEngine) else None

    @property
    def vector_column(self) -> str | None:
        return self.engine_spec.column if isinstance(self.engine_spec, VectorEngine) else None

    @property
    def vector_distance(self) -> VectorEngineDistance:
        return self.engine_spec.distance if isinstance(self.engine_spec, VectorEngine) else "l2"

    @property
    def embeddings_name(self) -> StrKey | None:
        return (
            self.engine_spec.embeddings_name if isinstance(self.engine_spec, VectorEngine) else None
        )

    @property
    def embedding_dimensions(self) -> int | None:
        return self.engine_spec.dimensions if isinstance(self.engine_spec, VectorEngine) else None

    @property
    def pgroonga_score_version(self) -> PgroongaScoreVersion:
        return (
            self.engine_spec.score_version if isinstance(self.engine_spec, PgroongaEngine) else "v2"
        )

    @property
    def pgroonga_plan(self) -> PgroongaPlan:
        return (
            self.engine_spec.plan
            if isinstance(self.engine_spec, PgroongaEngine)
            else "filter_first"
        )

    @property
    def pgroonga_candidate_limit(self) -> int | None:
        return self.candidate_limit

    @property
    def pgroonga_auto_index_first_min_rows(self) -> int:
        if isinstance(self.engine_spec, PgroongaEngine):
            return self.engine_spec.auto.index_first_min_rows

        return _DEFAULT_PGROONGA_AUTO_INDEX_FIRST_MIN_ROWS

    @property
    def pgroonga_auto_use_exact_count(self) -> bool:
        return (
            self.engine_spec.auto.use_exact_count
            if isinstance(self.engine_spec, PgroongaEngine)
            else False
        )

    @property
    def pgroonga_auto_with_filters(self) -> bool:
        return (
            self.engine_spec.auto.with_filters
            if isinstance(self.engine_spec, PgroongaEngine)
            else True
        )

    @property
    def pgroonga_auto_filter_first_max_rows(self) -> int:
        if isinstance(self.engine_spec, PgroongaEngine):
            return self.engine_spec.auto.filter_first_max_rows

        return _DEFAULT_PGROONGA_AUTO_FILTER_FIRST_MAX_ROWS

    @property
    def pgroonga_index_first_filter_margin(self) -> float:
        if isinstance(self.engine_spec, PgroongaEngine):
            return self.engine_spec.index_first_filter_margin

        return _DEFAULT_PGROONGA_INDEX_FIRST_FILTER_MARGIN

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.read_validation not in ("strict", "trusted"):
            raise exc.configuration(
                f"read_validation must be 'strict' or 'trusted', got {self.read_validation!r}",
            )

        if self.candidate_limit is not None and self.candidate_limit < 1:
            raise exc.configuration("candidate_limit must be at least 1.")


# ....................... #


def validate_fts_groups_for_search_spec(
    spec: "SearchSpec[Any]",
    fts_groups: dict[FtsGroupLetter, Sequence[str]],
) -> None:
    """Ensure ``fts_groups`` covers every field in ``spec`` (shared by search + hub)."""

    if not fts_groups:
        raise exc.configuration("FTS groups are required for FTS engine.")

    grouped_fields = reduce(lambda a, g: a + g, map(list, fts_groups.values()))

    if any(f not in grouped_fields for f in spec.fields):
        raise exc.configuration("All search fields must be included in FTS groups.")

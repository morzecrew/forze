"""Postgres single-index search execution configs and validation."""

from functools import reduce
from typing import TYPE_CHECKING, Any, Literal, Mapping, Sequence

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

_DEFAULT_PGROONGA_CANDIDATE_LIMIT = 5000
_DEFAULT_PGROONGA_AUTO_INDEX_FIRST_MIN_ROWS = 100_000

# ....................... #


def _optional_relation_spec(value: object) -> RelationSpec | None:
    if value is None:
        return None

    return coerce_relation_spec(value)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresSearchConfig(TenantAwareIntegrationConfig):
    """Configuration for a Postgres search route."""

    index: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Index relation (schema, index name) or resolver."""

    read: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Read relation for filters and row shape or resolver."""

    engine: SearchEngine
    """Search engine: PGroonga, FTS, or pgvector KNN."""

    heap: RelationSpec | None = attrs.field(
        default=None,
        converter=_optional_relation_spec,
    )
    """Heap relation; defaults to :attr:`read` when omitted."""

    fts_groups: dict[FtsGroupLetter, Sequence[str]] | None = None
    """FTS weight groups (required when :attr:`engine` is ``fts``)."""

    vector_column: str | None = None
    """Heap ``vector`` column (required for ``vector`` engine)."""

    vector_distance: VectorEngineDistance = "l2"
    """pgvector distance operator family."""

    embeddings_name: StrKey | None = None
    """Embeddings spec name (required for ``vector`` engine)."""

    embedding_dimensions: int | None = None
    """Query embedding size (required for ``vector`` engine)."""

    field_map: Mapping[str, str] | None = None
    """Maps spec field names to physical heap columns."""

    join_pairs: Sequence[tuple[str, str]] | None = None
    """Join pairs (projection column, index heap column)."""

    nested_field_hints: Mapping[str, Any] | None = None
    """Per-path type hints for filters/sorts on the read projection."""

    pgroonga_score_version: PgroongaScoreVersion = "v2"
    """PGroonga score overload when :attr:`engine` is ``pgroonga``."""

    pgroonga_plan: PgroongaPlan = "filter_first"
    """PGroonga ranked search plan (``filter_first``, ``index_first``, ``auto``)."""

    pgroonga_candidate_limit: int | None = _DEFAULT_PGROONGA_CANDIDATE_LIMIT
    """Max heap rows scored per query; ``None`` disables the cap."""

    pgroonga_auto_index_first_min_rows: int = _DEFAULT_PGROONGA_AUTO_INDEX_FIRST_MIN_ROWS
    """When :attr:`pgroonga_plan` is ``auto`` and filters are empty, use ``index_first`` if the read relation estimate is at least this many rows."""

    pgroonga_auto_use_exact_count: bool = False
    """When :attr:`pgroonga_plan` is ``auto``, run ``COUNT(*)`` on the filtered projection to pick the plan (extra round trip)."""

    # ....................... #

    @property
    def heap_relation(self) -> RelationSpec:
        """Heap qualified name used for index joins."""

        return self.heap if self.heap is not None else self.read

    # ....................... #

    def __attrs_post_init__(self) -> None:
        match self.engine:
            case "vector":
                if not self.vector_column:
                    raise exc.internal("vector_column is required for vector engine.")

                if self.embedding_dimensions is None:
                    raise exc.internal(
                        "embedding_dimensions is required for vector engine."
                    )

                if not self.embeddings_name:
                    raise exc.internal("embeddings_name is required for vector engine.")

            case "fts":
                if not self.fts_groups:
                    raise exc.internal("FTS groups are required for FTS engine.")

                all_fields = reduce(
                    lambda a, g: a + g, map(list, self.fts_groups.values())
                )

                if len(all_fields) != len(set(all_fields)):
                    raise exc.internal("FTS groups cannot contain duplicate fields.")

            case "pgroonga":
                if self.pgroonga_score_version not in ("v1", "v2"):
                    raise exc.internal("pgroonga_score_version must be 'v1' or 'v2'.")

                if self.pgroonga_plan not in ("filter_first", "index_first", "auto"):
                    raise exc.internal(
                        "pgroonga_plan must be 'filter_first', 'index_first', or 'auto'."
                    )

                if (
                    self.pgroonga_candidate_limit is not None
                    and self.pgroonga_candidate_limit < 1
                ):
                    raise exc.internal("pgroonga_candidate_limit must be at least 1.")

                if self.pgroonga_auto_index_first_min_rows < 1:
                    raise exc.internal(
                        "pgroonga_auto_index_first_min_rows must be at least 1."
                    )


# ....................... #


def validate_fts_groups_for_search_spec(
    spec: "SearchSpec[Any]",
    fts_groups: dict[FtsGroupLetter, Sequence[str]],
) -> None:
    """Ensure ``fts_groups`` covers every field in ``spec`` (shared by search + hub)."""

    if not fts_groups:
        raise exc.internal("FTS groups are required for FTS engine.")

    grouped_fields = reduce(lambda a, g: a + g, map(list, fts_groups.values()))

    if any(f not in grouped_fields for f in spec.fields):
        raise exc.internal("All search fields must be included in FTS groups.")

from functools import reduce
from typing import (
    Any,
    Literal,
    Mapping,
    NotRequired,
    Sequence,
    TypedDict,
    Union,
    cast,
    final,
)

from forze.application.contracts.search import SearchSpec
from forze.base.errors import CoreError
from forze.domain.constants import ID_FIELD

from ...adapters import FtsGroupLetter
from ...kernel.gateways import PostgresBookkeepingStrategy
from ...kernel.hub_fk_columns import normalize_hub_fk_columns

# ----------------------- #


class _BasePostgresConfig(TypedDict, total=False):
    """Base configuration for a Postgres resource."""

    tenant_aware: bool
    """Whether the resource is tenant-aware."""


# ....................... #


class PostgresReadOnlyDocumentConfig(_BasePostgresConfig):
    """Configuration for a Postgres read-only document."""

    read: tuple[str, str]
    """Read relation (schema, table / view / materialized view)"""

    nested_field_hints: NotRequired[Mapping[str, Any]]
    """Optional Python types (``type`` objects) for dot-separated filter/sort paths when
    the read model alone does not resolve a leaf type (e.g. ``dict`` / ``Any``)."""

    batch_size: NotRequired[int]
    """Chunk size for bulk writes and for internal chunked offset reads when pagination
    omits ``limit``. Optional; defaults to 200."""


# ....................... #


@final
class PostgresDocumentConfig(PostgresReadOnlyDocumentConfig):
    """Configuration for a Postgres document."""

    write: tuple[str, str]
    """Write relation (schema, table)."""

    bookkeeping_strategy: PostgresBookkeepingStrategy
    """Bookkeeping strategy."""

    history: NotRequired[tuple[str, str]]
    """History relation (schema, table), optional."""


# ....................... #


VectorEngineDistance = Literal["l2", "cosine", "inner_product"]

PgroongaScoreVersion = Literal["v1", "v2"]
"""``v1``: ``pgroonga_score(heap_alias)``. ``v2``: ``pgroonga_score(tableoid, ctid)`` (default)."""


# ....................... #


class PostgresSearchConfig(_BasePostgresConfig):
    """Configuration for a Postgres search."""

    index: tuple[str, str]
    """Index relation (schema, index name) - to resolve the search definition."""

    read: tuple[str, str]
    """Read relation (schema, table / view / materialized view) for filters and row shape."""

    heap: NotRequired[tuple[str, str]]
    """Heap relation (schema, table) where index is built on. If not provided, ``read`` is used."""

    engine: Literal["pgroonga", "fts", "vector"]
    """Search engine to use (full-text or pgvector KNN on a ``vector`` column)."""

    fts_groups: NotRequired[dict[FtsGroupLetter, Sequence[str]]]
    """Mapping of FTS weight letters to field names (required only for FTS engines)."""

    vector_column: NotRequired[str]
    """Heap column with type ``vector`` (required for ``vector`` engine)."""

    vector_distance: NotRequired[VectorEngineDistance]
    """``pgvector`` operator family; default is ``l2`` (Euclidean)."""

    embeddings_name: NotRequired[str]
    """:class:`EmbeddingsSpec` ``name`` to resolve the query embedder (``vector`` engine)."""

    embedding_dimensions: NotRequired[int]
    """Expected query embedding size; must match the ``vector`` column (``vector`` engine)."""

    field_map: NotRequired[Mapping[str, str]]
    """Maps :class:`SearchSpec` field names to physical heap columns when they differ."""

    join_pairs: NotRequired[Sequence[tuple[str, str]]]
    """Join pairs (projection column, index heap column)."""

    nested_field_hints: NotRequired[Mapping[str, Any]]
    """Same semantics as :attr:`PostgresReadOnlyDocumentConfig.nested_field_hints`."""

    pgroonga_score_version: NotRequired[PgroongaScoreVersion]
    """
    Which ``pgroonga_score`` overload to use when :attr:`engine` is ``pgroonga``.

    ``v2`` (default when omitted): ``pgroonga_score(tableoid, ctid)`` — faster (PGroonga 2.0.4+).
    ``v1``: ``pgroonga_score(heap row alias)`` — legacy single-argument form; use if the heap is a
    view or another case where ``tableoid``/``ctid`` are not available on the scan.
    """


# ....................... #


@final
class PostgresHubSearchMemberConfig(PostgresSearchConfig):
    """Configuration for a Postgres hub search member."""

    hub_fk: str | Sequence[str]
    """Hub foreign key column(s); multiple columns OR-link the hub row to the same heap."""

    heap_pk: NotRequired[str]
    """Heap primary key column (default ``id``)."""

    same_heap_as_hub: NotRequired[bool]
    """
    When ``True``, this leg’s heap is the same relation as the hub: skip the
    leg’s heap self-join and evaluate the leg match directly on the hub CTE
    (``hf``). Requires ``hub_fk`` to be a single column equal to
    :attr:`heap_pk`, the leg ``read``/``heap`` to match the hub pair, and (for
    ``pgroonga``) :attr:`pgroonga_score_version` ``v2`` and no :attr:`field_map`.
    Not supported for ``fts`` engine.
    """


# ....................... #


@final
class PostgresHubSearchConfig(_BasePostgresConfig):
    """Postgres configuration for :class:`PostgresHubSearchAdapter`."""

    hub: tuple[str, str]
    """Hub relation (schema, table / view / materialized view) for filters and row shape."""

    members: Mapping[str, PostgresHubSearchMemberConfig]
    """Mapping of member spec names to their Postgres-specific configurations."""

    combine_strategy: NotRequired[Literal["or", "and"]]
    """How leg text matches combine (default ``or``)."""

    merge_strategy: NotRequired[Literal["max", "sum"]]
    """How leg scores merge for ordering (default ``max``, i.e. greatest score wins)."""

    nested_field_hints: NotRequired[Mapping[str, Any]]
    """Per-path type hints for filters/sorts on the hub read projection."""


# ....................... #

PostgresFederatedMemberConfig = Union[PostgresSearchConfig, PostgresHubSearchConfig]
"""Per-leg Postgres wiring: single-index search or embedded :class:`PostgresHubSearchConfig`."""


@final
class PostgresFederatedSearchConfig(_BasePostgresConfig):
    """Postgres configuration for :class:`PostgresFederatedSearchAdapter`."""

    members: Mapping[str, PostgresFederatedMemberConfig]
    """Mapping of federated member names to single-index configs or embedded hub configs."""

    rrf_k: NotRequired[int]
    """RRF smoothing constant (default 60)."""

    rrf_per_leg_limit: NotRequired[int]
    """Max hits fetched per member for merging (default 5000)."""


# ....................... #


def is_postgres_federated_embedded_hub_config(obj: Mapping[str, Any]) -> bool:
    """Return True if ``obj`` is shaped like :class:`PostgresHubSearchConfig` (embedded hub leg)."""

    return "hub" in obj and "members" in obj


# ....................... #


def validate_postgres_federated_search_conf(cfg: PostgresFederatedSearchConfig) -> None:
    """Validate a Postgres federated search configuration."""

    if len(cfg["members"]) < 2:
        raise CoreError("Federated search requires at least two member configurations.")

    for name, leg in cfg["members"].items():
        if is_postgres_federated_embedded_hub_config(leg):
            validate_postgres_hub_search_conf(cast(PostgresHubSearchConfig, leg))
            continue

        if "index" not in leg or ("heap" not in leg and "read" not in leg):
            raise CoreError(
                f"Federated search member {name!r} must include 'index' and 'heap' or 'read', "
                "or be an embedded hub with 'hub' and 'members'.",
            )

        eng = leg.get("engine", "pgroonga")

        if eng == "fts" and not leg.get("fts_groups"):
            raise CoreError(
                f"Federated search member {name!r} with engine 'fts' requires fts_groups.",
            )

        if eng == "vector":
            if not leg.get("vector_column"):
                raise CoreError(
                    f"Federated search member {name!r} with engine 'vector' requires vector_column.",
                )
            if leg.get("embedding_dimensions") is None:
                raise CoreError(
                    f"Federated search member {name!r} with engine 'vector' requires embedding_dimensions.",
                )
            if not leg.get("embeddings_name"):
                raise CoreError(
                    f"Federated search member {name!r} with engine 'vector' requires embeddings_name.",
                )


# ....................... #


def _validate_same_heap_as_hub(
    leg: Mapping[str, Any],
    leg_index: int,
    cfg: PostgresHubSearchConfig,
    eng: str,
) -> None:
    if eng == "fts":
        raise CoreError(
            f"Hub search leg {leg_index} cannot use same_heap_as_hub with engine 'fts'.",
        )

    if leg.get("field_map"):
        raise CoreError(
            f"Hub search leg {leg_index} cannot use same_heap_as_hub together with 'field_map'.",
        )

    hub_pair = cfg["hub"]
    heap_read = leg.get("heap", leg.get("read"))

    if not heap_read:
        raise CoreError(
            f"Hub search leg {leg_index} with same_heap_as_hub must include 'heap' or 'read'.",
        )

    if tuple(hub_pair) != tuple(heap_read):
        raise CoreError(
            f"Hub search leg {leg_index} with same_heap_as_hub must use the same "
            "qualified relation as the hub in 'read' or 'heap'.",
        )

    hpk = str(leg.get("heap_pk", ID_FIELD))
    fk_cols = normalize_hub_fk_columns(leg["hub_fk"])

    if len(fk_cols) != 1 or fk_cols[0] != hpk:
        raise CoreError(
            f"Hub search leg {leg_index} with same_heap_as_hub requires 'hub_fk' "
            "to be a single column name equal to 'heap_pk' (default 'id').",
        )

    if eng == "pgroonga" and leg.get("pgroonga_score_version", "v2") != "v2":
        raise CoreError(
            f"Hub search leg {leg_index} with same_heap_as_hub and engine 'pgroonga' "
            "requires 'pgroonga_score_version' 'v2'.",
        )


# ....................... #


def validate_postgres_hub_search_conf(cfg: PostgresHubSearchConfig) -> None:
    """Validate a Postgres hub search configuration."""

    legs = list(cfg["members"].values())

    if not legs:
        raise CoreError("Hub search requires at least one leg configuration.")

    fk_seen: set[str] = set()

    for i, leg in enumerate(legs):
        if "index" not in leg or ("heap" not in leg and "read" not in leg):
            raise CoreError(
                f"Hub search leg {i} must include 'index' and 'heap' or 'read'."
            )

        if "hub_fk" not in leg:
            raise CoreError(f"Hub search leg {i} must include 'hub_fk'.")

        for col in normalize_hub_fk_columns(leg["hub_fk"]):
            if col in fk_seen:
                raise CoreError(
                    "Each hub_fk column may belong to at most one leg "
                    "(duplicate column across legs).",
                )
            fk_seen.add(col)

        eng = leg.get("engine", "pgroonga")

        if eng == "fts" and not leg.get("fts_groups"):
            raise CoreError(
                f"Hub search leg {i} with engine 'fts' requires fts_groups."
            )

        if eng == "vector":
            if not leg.get("vector_column"):
                raise CoreError(
                    f"Hub search leg {i} with engine 'vector' requires vector_column."
                )
            if leg.get("embedding_dimensions") is None:
                raise CoreError(
                    f"Hub search leg {i} with engine 'vector' requires embedding_dimensions."
                )
            if not leg.get("embeddings_name"):
                raise CoreError(
                    f"Hub search leg {i} with engine 'vector' requires embeddings_name."
                )

        if leg.get("same_heap_as_hub"):
            _validate_same_heap_as_hub(leg, i, cfg, eng)

        if eng == "pgroonga":
            pv = leg.get("pgroonga_score_version", "v2")
            if pv not in ("v1", "v2"):
                raise CoreError("pgroonga_score_version must be 'v1' or 'v2'.")


# ....................... #


def validate_fts_groups_for_search_spec(
    spec: SearchSpec[Any],
    fts_groups: dict[FtsGroupLetter, Sequence[str]],
) -> None:
    """Ensure ``fts_groups`` covers every field in ``spec`` (shared by search + hub)."""

    if not fts_groups:
        raise CoreError("FTS groups are required for FTS engine.")

    grouped_fields = reduce(lambda a, g: a + g, map(list, fts_groups.values()))

    if any(f not in grouped_fields for f in spec.fields):
        raise CoreError("All search fields must be included in FTS groups.")


# ....................... #


#! TODO: move to deps class
def validate_pg_search_conf(cfg: PostgresSearchConfig) -> None:
    """Validate a Postgres search configuration."""

    eng = cfg["engine"]

    match eng:
        case "vector":
            if not cfg.get("vector_column"):
                raise CoreError("vector_column is required for vector engine.")

            if cfg.get("embedding_dimensions") is None:
                raise CoreError("embedding_dimensions is required for vector engine.")

            if not cfg.get("embeddings_name"):
                raise CoreError("embeddings_name is required for vector engine.")

        case "fts":
            fts_groups = cfg.get("fts_groups")

            if not fts_groups:
                raise CoreError("FTS groups are required for FTS engine.")

            all_fields = reduce(lambda a, g: a + g, map(list, fts_groups.values()))

            if len(all_fields) != len(set(all_fields)):
                raise CoreError("FTS groups cannot contain duplicate fields.")

        case "pgroonga":
            v = cfg.get("pgroonga_score_version", "v2")
            if v not in ("v1", "v2"):
                raise CoreError("pgroonga_score_version must be 'v1' or 'v2'.")

from functools import reduce
from typing import Any, Literal, Mapping, NotRequired, Sequence, TypedDict, final

from forze.application.contracts.search import SearchSpec
from forze.base.errors import CoreError

from ...adapters import FtsGroupLetter
from ...kernel.gateways import PostgresBookkeepingStrategy

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

    batch_size: NotRequired[int]
    """Batch size for writing, optional. Defaults to 200."""


# ....................... #


class PostgresSearchConfig(_BasePostgresConfig):
    """Configuration for a Postgres search."""

    index: tuple[str, str]
    """Index relation (schema, index name) - to resolve the search definition."""

    read: tuple[str, str]
    """Read relation (schema, table / view / materialized view) for filters and row shape."""

    heap: NotRequired[tuple[str, str]]
    """Heap relation (schema, table) where index is built on. If not provided, ``read`` is used."""

    engine: Literal["pgroonga", "fts"]
    """Search engine to use for the index."""

    fts_groups: NotRequired[dict[FtsGroupLetter, Sequence[str]]]
    """Mapping of FTS weight letters to field names (required only for FTS engines)."""

    field_map: NotRequired[Mapping[str, str]]
    """Maps :class:`SearchSpec` field names to physical heap columns when they differ."""

    join_pairs: NotRequired[Sequence[tuple[str, str]]]
    """Join pairs (projection column, index heap column)."""


# ....................... #


class PostgresHubSearchMemberConfig(PostgresSearchConfig):
    """Configuration for a Postgres hub search member."""

    hub_fk: str
    """Hub foreign key column."""

    heap_pk: NotRequired[str]
    """Heap primary key column (default ``id``)."""


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


# ....................... #


def validate_postgres_hub_search_conf(cfg: PostgresHubSearchConfig) -> None:
    """Validate a Postgres hub search configuration."""

    legs = list(cfg["members"].values())

    if len(legs) < 2:
        raise CoreError("Hub search requires at least two leg configurations.")

    fk_seen: list[str] = []

    for i, leg in enumerate(legs):
        if "index" not in leg or ("heap" not in leg and "read" not in leg):
            raise CoreError(
                f"Hub search leg {i} must include 'index' and 'heap' or 'read'."
            )

        if "hub_fk" not in leg:
            raise CoreError(f"Hub search leg {i} must include 'hub_fk'.")

        fk = leg["hub_fk"]

        if fk in fk_seen:
            raise CoreError("hub_fk_column must be unique for each leg.")

        fk_seen.append(fk)

        eng = leg.get("engine", "pgroonga")

        if eng == "fts" and not leg.get("fts_groups"):
            raise CoreError(
                f"Hub search leg {i} with engine 'fts' requires fts_groups."
            )


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

    if cfg["engine"] == "pgroonga":
        return

    fts_groups = cfg.get("fts_groups")

    if not fts_groups:
        raise CoreError("FTS groups are required for FTS engine.")

    all_fields = reduce(lambda a, g: a + g, map(list, fts_groups.values()))

    if len(all_fields) != len(set(all_fields)):
        raise CoreError("FTS groups cannot contain duplicate fields.")

from functools import reduce
from typing import Literal, NotRequired, Sequence, TypedDict, final

from forze.base.errors import CoreError

from ...adapters import FtsGroupLetter
from ...kernel.gateways import PostgresBookkeepingStrategy

# ----------------------- #


class _BasePostgresConfig(TypedDict):
    """Base configuration for a Postgres resource."""

    tenant_aware: NotRequired[bool]
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


@final
class PostgresSearchConfig(_BasePostgresConfig):
    """Configuration for a Postgres search."""

    index: tuple[str, str]
    """Schema-qualified **index** name (used to resolve the search definition; not the ``FROM`` table)."""

    source: tuple[str, str]
    """Schema-qualified **heap** relation (table or view) that rows are selected from."""

    engine: Literal["pgroonga", "fts"]
    """Search engine to use for the index."""

    fts_groups: NotRequired[dict[FtsGroupLetter, Sequence[str]]]
    """Mapping of FTS weight letters to field names (required only for FTS engines)."""


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

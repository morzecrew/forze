from functools import reduce
from typing import Literal, NotRequired, Sequence, TypedDict

from forze.base.errors import CoreError

from ...adapters import FtsGroupLetter

# ----------------------- #


class PostgresDocumentConfig(TypedDict):
    """Configuration for a Postgres document."""

    read: tuple[str, str]
    """Read relation (schema, table / view / materialized view)"""

    write: NotRequired[tuple[str, str]]
    """Write relation (schema, table), optional."""

    history: NotRequired[tuple[str, str]]
    """History relation (schema, table), optional."""

    tenant_aware: NotRequired[bool]
    """Whether the document is tenant-aware."""


# ....................... #


class PostgresSearchConfig(TypedDict):
    """Configuration for a Postgres search."""

    index: tuple[str, str]
    """Index relation (schema, index)"""

    source: tuple[str, str]
    """Source relation (schema, table)"""

    engine: Literal["pgroonga", "fts"]
    """Search engine to use for the index."""

    tenant_aware: NotRequired[bool]
    """Whether the document is tenant-aware."""

    fts_groups: NotRequired[dict[FtsGroupLetter, Sequence[str]]]
    """Mapping of FTS weight letters to field names (required only for FTS engines)."""


# ....................... #

PostgresDocumentConfigs = dict[str, PostgresDocumentConfig]
PostgresSearchConfigs = dict[str, PostgresSearchConfig]

# ....................... #


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

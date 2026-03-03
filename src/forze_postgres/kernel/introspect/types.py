from typing import Literal, Optional, final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresType:
    base: str
    is_array: bool
    not_null: bool


# ....................... #

PostgresColumnTypes = dict[str, PostgresType]
PostgresColumnCache = dict[tuple[Optional[str], str], PostgresColumnTypes]

# ....................... #

PostgresIndexEngine = Literal["pgroonga", "fts", "unknown"]
PostgresIndexCache = dict[tuple[Optional[str], str, str], PostgresIndexEngine]

# ....................... #

PostgresRelationKind = Literal[
    "table", "view", "materialized_view", "partitioned_table", "other"
]
PostgresRelationCache = dict[tuple[Optional[str], str], PostgresRelationKind]

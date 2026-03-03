from __future__ import annotations

from typing import Literal, Optional, final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresType:
    base: str
    is_array: bool
    not_null: bool


PostgresColumnTypes = dict[str, PostgresType]
PostgresColumnCache = dict[tuple[str, str], PostgresColumnTypes]

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresIndexInfo:
    schema: str
    name: str
    amname: str
    engine: PostgresIndexEngine
    indexdef: str
    expr: Optional[str] = None
    columns: tuple[str, ...] = ()
    has_tsvector_col: bool = False


PostgresIndexEngine = Literal["pgroonga", "fts", "unknown"]
PostgresIndexCache = dict[tuple[str, str], PostgresIndexInfo]
PostgresIndexDefCache = dict[tuple[str, str], str]

# ....................... #

PostgresRelationKind = Literal[
    "table", "view", "materialized_view", "partitioned_table", "other"
]
PostgresRelationCache = dict[tuple[str, str], PostgresRelationKind]

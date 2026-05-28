"""Shared value objects for ranked pipeline search adapters."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import TYPE_CHECKING, Any

import attrs

if TYPE_CHECKING:
    from psycopg import sql

    from ._pipeline_sql import PipelineAliases

# ----------------------- #


@attrs.define(frozen=True, slots=True, kw_only=True)
class RankedPipelineSql:
    """SQL fragments shared by offset and cursor ranked search on one engine."""

    with_clause: "sql.Composable"
    from_outer: "sql.Composable"
    params_body: list[Any]
    count_params: list[Any] | None
    """When set, used for ``COUNT(*)`` only (e.g. FTS empty-query uses filter params)."""

    pipeline: "PipelineAliases"
    rank_column: str
    projection_alias: str

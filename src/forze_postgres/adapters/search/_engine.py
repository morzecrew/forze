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

    count_with_clause: "sql.Composable | None" = None
    """When set with :attr:`count_from_outer`, exact ``COUNT(*)`` uses uncapped ranked SQL."""

    count_from_outer: "sql.Composable | None" = None
    """``FROM`` fragment for :attr:`count_with_clause` (often same as :attr:`from_outer`)."""

    pipeline: "PipelineAliases"
    rank_column: str
    projection_alias: str
    resolved_plan: str | None = None
    """Resolved PGroonga plan name when applicable."""
    candidate_limit: int | None = None
    """Effective ranked-row cap when applicable."""

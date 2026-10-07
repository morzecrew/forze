"""One aggregate ``SELECT`` over whatever row source a reader passes."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any

import attrs
from psycopg import sql

from forze.application.contracts.querying import (
    AggregatesExpression,
    ParsedAggregates,
    QueryFilterExpressionParser,
)

from .render import PsycopgQueryRenderer

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class AggregateStatement:
    """A grouped ``SELECT`` and its parameters, with ``$having`` applied, unordered."""

    parsed: ParsedAggregates
    """The parsed aggregate, whose aliases an ``ORDER BY`` may name."""

    stmt: sql.Composable
    """``SELECT … FROM … [GROUP BY …]``, wrapped in a ``$having`` filter when one is given."""

    params: list[Any]
    """Parameters in statement order: the select list's, the source's, then ``$having``'s."""


# ....................... #


def compose_aggregate_statement(
    renderer: PsycopgQueryRenderer,
    aggregates: AggregatesExpression,
    *,
    filter_parser: QueryFilterExpressionParser | None,
    source: sql.Composable,
    source_params: list[Any],
) -> AggregateStatement:
    """Group and measure the rows *source* (a ``FROM … [WHERE …]`` fragment) yields.

    ``$having`` filters the aggregated rows, so it wraps the group query and filters on its
    output aliases, rendered against their types
    (:meth:`~PsycopgQueryRenderer.aggregate_outputs`) so an operator the output cannot
    take is refused here rather than by the server.
    """

    parsed, select_clause, group_clause, aggregate_params = renderer.render_aggregates(
        aggregates,
        filter_parser=filter_parser,
    )
    stmt: sql.Composable = sql.SQL("SELECT {cols} {source}").format(
        cols=select_clause,
        source=source,
    )
    params = [*aggregate_params, *source_params]

    if group_clause is not None:
        stmt += sql.SQL(" GROUP BY {group}").format(group=group_clause)

    if parsed.having is not None:
        types, columns = renderer.aggregate_outputs(parsed, table_alias="_agg")
        having_renderer = PsycopgQueryRenderer(
            types=types,
            table_alias="_agg",
            column_exprs=columns,
        )
        having_sql, having_params = having_renderer.render(parsed.having)
        stmt = sql.SQL("SELECT * FROM ({inner}) AS _agg WHERE {having}").format(
            inner=stmt,
            having=having_sql,
        )
        params.extend(having_params)

    return AggregateStatement(parsed=parsed, stmt=stmt, params=params)

"""Shared run of a search aggregate: order, count and page the groups of the matched rows."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any

from psycopg import sql

from forze.application.contracts.base import CountlessPage, Page, page_from_limit_offset
from forze.application.contracts.querying import (
    AggregatesExpression,
    PaginationExpression,
    QuerySortExpression,
    with_group_tiebreakers,
)
from forze.application.integrations.document._limits import page_limit, page_offset
from forze.base.primitives import JsonDict

from ...kernel.client import PostgresClientPort
from ...kernel.sql.query import AggregateStatement, PsycopgQueryRenderer

# ----------------------- #


async def execute_search_aggregate(
    client: PostgresClientPort,
    *,
    head: sql.Composable,
    head_params: list[Any],
    aggregate: AggregateStatement,
    aggregates: AggregatesExpression,
    pagination: PaginationExpression | None,
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    return_count: bool,
) -> CountlessPage[JsonDict] | Page[JsonDict]:
    """Run *aggregate* under the ``WITH`` clause *head* that defines its matched rows.

    :param head_params: The parameters *head* binds, ahead of the aggregate's own.
    :param return_count: Also count the groups, after ``$having``.
    :returns: A page of groups; without a ``limit``, every group.
    """

    params = [*head_params, *aggregate.params]
    window: dict[str, Any] = dict(pagination or {})
    limit, offset = page_limit(window), page_offset(window)
    # Rendered before anything runs, so a sort naming no output alias costs no query.
    # The group keys close the order, so pages of groups neither repeat nor skip one.
    order = PsycopgQueryRenderer.render_aggregate_order_by(
        aggregate.parsed,
        with_group_tiebreakers(aggregates, sorts),
    )
    total: int | None = None

    if return_count:
        count_stmt = sql.SQL("{head} SELECT COUNT(*) FROM ({inner}) AS _groups").format(
            head=head,
            inner=aggregate.stmt,
        )
        total = int(await client.fetch_value(count_stmt, params, default=0))

    stmt = sql.SQL("{head} {inner}").format(head=head, inner=aggregate.stmt)

    if order is not None:
        stmt += sql.SQL(" ORDER BY {order}").format(order=order)

    page_params = list(params)

    # Without a limit every group comes back, as the document port's aggregate drains them.
    if limit is not None:
        stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
        page_params.append(limit)

    if offset:
        stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
        page_params.append(offset)

    rows = await client.fetch_all(stmt, page_params, row_factory="dict")
    hits = [dict(row) for row in rows]

    if total is None:
        return page_from_limit_offset(hits, window)

    return page_from_limit_offset(hits, window, total=total)

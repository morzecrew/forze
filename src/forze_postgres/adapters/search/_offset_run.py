"""Shared offset pagination + snapshot execution for ranked Postgres search."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import TYPE_CHECKING, Any, Sequence, TypeVar

import attrs
from psycopg import sql
from pydantic import BaseModel

if TYPE_CHECKING:
    from ._highlights import HighlightSelect

from forze.application.contracts.base import page_from_limit_offset
from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    HubSearchSpec,
    SearchOptions,
    SearchResultSnapshotOptions,
    SearchSpec,
    facet_size_of,
    normalize_search_queries,
    resolve_facet_fields,
)
from forze.application.integrations.search import (
    SearchResultSnapshot,
    SnapshotWindow,
    build_snapshot_pool_streaming,
)
from forze.application.integrations.search.offset_executor import (
    OffsetFetchWindow,
    OffsetRowsResult,
    execute_simple_offset_search_with_snapshot,
    materialize_offset_page,
    offset_from_dict,
)
from forze.base.serialization import materialize_mapping_rows

from ...kernel.gateways import PostgresGateway
from ._facets import fetch_pg_facets
from ._highlights import extract_and_strip_highlights
from ._search_count import effective_search_count

# ----------------------- #

M = TypeVar("M", bound=BaseModel)

# ....................... #


@attrs.define(frozen=True, slots=True, kw_only=True)
class RankedOffsetPlan:
    """SQL fragments for one ranked offset search (count + data)."""

    with_clause: sql.Composable
    """``WITH`` clause."""

    from_outer: sql.Composable
    """``FROM …`` fragment appended after ``SELECT cols``."""

    order_sql: sql.Composable
    """ORDER BY clause."""

    params: list[Any]
    """Parameters for the ``SELECT`` statement."""

    count_params: list[Any] | None = None
    """When set, used for ``COUNT(*)`` only (e.g. FTS empty-query uses filter params only)."""

    count_with_clause: sql.Composable | None = None
    """Uncapped ranked ``WITH`` for exact totals when data pipeline uses a candidate cap."""

    count_from_outer: sql.Composable | None = None

    approximate_total: int | None = None
    """When set, used for ``search_count=approximate`` instead of ``COUNT(*)``."""

    count_relation: str = "combo"
    """Hub - relation name for ``COUNT(*)`` (defaults to full ``combo``)."""

    data_relation: str = "combo"
    """Hub - relation name for the ranked data ``SELECT`` (e.g. ``combo_top``)."""

    select_table_alias: str
    """Table alias passed to :meth:`~PostgresGateway.return_clause`."""

    highlight: "HighlightSelect | None" = None
    """Synthetic highlight columns to splice into the data SELECT (RFC 0006)."""

    from_outer_param_count: int = 0
    """Trailing :attr:`params` that belong to :attr:`from_outer` (highlight params splice
    before these)."""


# ....................... #


@attrs.define(slots=True)
class _PostgresSimpleOffsetHooks:
    gw: PostgresGateway[Any]
    plan: RankedOffsetPlan
    return_type: type[BaseModel] | None
    return_fields: Sequence[str] | None
    return_count: bool
    count_policy: str
    pagination_dict: dict[str, Any]
    facet_fields: tuple[str, ...] = ()
    facet_size: int = 0

    async def fetch_count(self) -> int | None:
        if not self.return_count or self.count_policy == "none":
            return None

        if self.count_policy == "approximate" and self.plan.approximate_total is not None:
            return int(self.plan.approximate_total)

        use_uncapped_count = (
            self.count_policy == "exact"
            and self.plan.count_with_clause is not None
            and self.plan.count_from_outer is not None
        )

        if use_uncapped_count:
            count_with = self.plan.count_with_clause
            count_from = self.plan.count_from_outer
            count_params = (
                self.plan.count_params
                if self.plan.count_params is not None
                else self.plan.params
            )

        else:
            count_with = self.plan.with_clause
            count_from = self.plan.from_outer
            count_params = (
                self.plan.count_params
                if self.plan.count_params is not None
                else self.plan.params
            )

        count_stmt = sql.SQL(
            """
            {with_clause}
            SELECT COUNT(*) {from_outer}
            """
        ).format(with_clause=count_with, from_outer=count_from)

        return int(
            await self.gw.client.fetch_value(count_stmt, count_params, default=0),
        )

    async def fetch_rows(
        self,
        window: OffsetFetchWindow,
        *,
        want_snap: bool,
    ) -> OffsetRowsResult:
        cols = self.gw.return_clause(
            self.return_type,
            self.return_fields,
            table_alias=self.plan.select_table_alias,
        )

        hl = self.plan.highlight
        hl_cols = hl.select_fragment() if hl is not None else sql.SQL("")

        data_stmt = sql.SQL(
            """
            {with_clause}
            SELECT {cols}{hl_cols} {from_outer}
            ORDER BY {order}
            """
        ).format(
            with_clause=self.plan.with_clause,
            cols=cols,
            hl_cols=hl_cols,
            from_outer=self.plan.from_outer,
            order=self.plan.order_sql,
        )

        # Highlight column placeholders sit in the SELECT list, between the WITH-clause
        # params and any from_outer params (index-first puts the projection filter there).
        body = list(self.plan.params)
        if hl is not None:
            split = len(body) - self.plan.from_outer_param_count
            params = [*body[:split], *hl.params, *body[split:]]
        else:
            params = body

        if window.fetch_limit is not None:
            data_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(int(window.fetch_limit))

        if want_snap:
            data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(int(window.fetch_offset))

        elif self.pagination_dict.get("offset") is not None:
            data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(offset_from_dict(self.pagination_dict))

        rows = [
            dict(row)
            for row in await self.gw.client.fetch_all(
                data_stmt, params, row_factory="dict"
            )
        ]

        highlights = (
            extract_and_strip_highlights(rows, hl) if hl is not None else None
        )
        facets = await self._fetch_facets()

        return OffsetRowsResult(rows=rows, facets=facets, highlights=highlights)

    async def _fetch_facets(self) -> Any:
        """Companion ``GROUP BY`` over the uncapped matched set (mirrors :meth:`fetch_count`)."""

        if not self.facet_fields:
            return None

        if (
            self.plan.count_with_clause is not None
            and self.plan.count_from_outer is not None
        ):
            facet_with: sql.Composable = self.plan.count_with_clause
            facet_body: sql.Composable = self.plan.count_from_outer
        else:
            facet_with = self.plan.with_clause
            facet_body = self.plan.from_outer

        facet_params = (
            self.plan.count_params
            if self.plan.count_params is not None
            else self.plan.params
        )

        return await fetch_pg_facets(
            self.gw.client,
            with_clause=facet_with,
            body=facet_body,
            params=facet_params,
            table_alias=self.plan.select_table_alias,
            fields=self.facet_fields,
            size=self.facet_size,
        )


# ....................... #


async def execute_simple_ranked_offset_search(
    gw: PostgresGateway[M],
    *,
    plan: RankedOffsetPlan,
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    spec: SearchSpec[Any],
    variant: str,
    fingerprint_extras: dict[str, object] | None,
    pagination: PaginationExpression | None,
    snapshot: SearchResultSnapshotOptions | None,
    return_count: bool,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    model_type: type[M],
    result_snapshot: SearchResultSnapshot | None,
    options: SearchOptions | None = None,
    trust_source: bool = False,
) -> Any:
    """Run count (optional), data fetch, snapshot materialization for simple search adapters."""

    count_policy = effective_search_count(options)
    pagination_dict: dict[str, Any] = dict(pagination or {})
    facet_fields = resolve_facet_fields(spec, options)

    return await execute_simple_offset_search_with_snapshot(
        query=query,
        filters=filters,
        sorts=sorts,
        spec=spec,
        variant=variant,
        fingerprint_extras=fingerprint_extras,
        pagination=pagination,
        snapshot=snapshot,
        return_count=return_count,
        snapshot_return_count=return_count and count_policy != "none",
        page_return_count=return_count and count_policy != "none",
        return_type=return_type,
        return_fields=return_fields,
        model_type=model_type,
        codec=spec.resolved_read_codec,
        result_snapshot=result_snapshot,
        hooks=_PostgresSimpleOffsetHooks(
            gw=gw,
            plan=plan,
            return_type=return_type,
            return_fields=return_fields,
            return_count=return_count,
            count_policy=count_policy,
            pagination_dict=pagination_dict,
            facet_fields=facet_fields,
            facet_size=facet_size_of(options),
        ),
        trust_source=trust_source,
    )


# ....................... #


async def execute_hub_ranked_offset_search(
    gw: PostgresGateway[M],
    *,
    plan: RankedOffsetPlan,
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    hub_spec: HubSearchSpec[Any],
    members_weighted: list[tuple[str, float]],
    score_merge: str,
    combine: str,
    per_leg_limit: int,
    pagination: PaginationExpression | None,
    snapshot: SearchResultSnapshotOptions | None,
    return_count: bool,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    model_type: type[M],
    result_snapshot: SearchResultSnapshot | None,
    combo_alias: str = "comb",
    options: SearchOptions | None = None,
    execution: str | None = None,
    combo_limit: int | None = None,
    trust_source: bool = False,
) -> Any:
    """Ranked offset search for :class:`~forze_postgres.adapters.search.hub.PostgresHubSearchAdapter`."""

    rs_spec = hub_spec.snapshot
    count_policy = effective_search_count(options)
    snapshot_return_count = return_count and count_policy != "none"
    fp_fingerprint = SearchResultSnapshot.hub_search_fingerprint(
        query,
        filters,
        sorts,
        spec_name=hub_spec.name,
        members_weighted=members_weighted,
        score_merge=score_merge,
        combine=combine,
        per_leg_limit=per_leg_limit,
        execution=execution,
        combo_limit=combo_limit,
        search_count=count_policy,
    )

    if result_snapshot is not None and rs_spec is not None:
        read_page = await result_snapshot.read_hub_result_snapshot(
            rs_spec=rs_spec,
            snap_opt=snapshot,
            fp_computed=fp_fingerprint,
            model_type=model_type,
            pagination=dict(pagination or {}),
            return_type=return_type,
            return_fields=return_fields,
            return_count=snapshot_return_count,
        )

        if read_page is not None:
            return read_page

    count_stmt = sql.SQL(
        """
            {with_clause}
            SELECT COUNT(*) FROM {combo} {ca}
            """
    ).format(
        with_clause=plan.with_clause,
        combo=sql.Identifier(plan.count_relation),
        ca=sql.Identifier(combo_alias),
    )

    total = 0
    pagination_dict: dict[str, Any] = dict(pagination or {})

    if return_count and count_policy != "none":
        if count_policy == "approximate" and plan.approximate_total is not None:
            total = int(plan.approximate_total)

        elif count_policy == "exact" and hasattr(gw, "_hub_sql_combo_count"):
            hub_count = getattr(gw, "_hub_sql_combo_count")
            total = int(
                await hub_count(
                    query_terms=tuple(normalize_search_queries(query)),
                    filters=filters,
                    leg_options=options,
                    member_weights_list=[float(w) for _name, w in members_weighted],
                    per_leg_limit=per_leg_limit,
                    sorts=sorts if sorts else hub_spec.default_sort,
                )
            )

        else:
            total = int(await gw.client.fetch_value(count_stmt, plan.params, default=0))

        if return_count and total == 0:
            return page_from_limit_offset(  # pyright: ignore[reportUnknownVariableType]
                [],
                pagination or {},
                total=0,
            )

    cols = gw.return_clause(
        return_type,
        return_fields,
        table_alias=plan.select_table_alias,
    )

    data_stmt = sql.SQL(
        """
            {with_clause}
            SELECT {cols} FROM {combo} {ca}
            ORDER BY {order}
            """
    ).format(
        with_clause=plan.with_clause,
        cols=cols,
        combo=sql.Identifier(plan.data_relation),
        ca=sql.Identifier(combo_alias),
        order=plan.order_sql,
    )

    read_codec = hub_spec.resolved_read_codec
    page_offset = offset_from_dict(pagination_dict)

    want_sn = (
        result_snapshot is not None
        and rs_spec is not None
        and result_snapshot.should_write_result_snapshot(snapshot, rs_spec)
    )

    if want_sn and result_snapshot is not None and rs_spec is not None:
        # Stream the merged combo result window-by-window into the snapshot store so peak
        # memory is one chunk, never the whole (up to ``max_ids``) decoded pool at once.
        page_limit = SearchResultSnapshot.snapshot_pagination(
            True, 0, pagination_dict
        )[2]
        base_params = list(plan.params)

        async def fetch_window(
            window_offset: int, window_limit: int
        ) -> SnapshotWindow:
            stmt = data_stmt + sql.SQL(" LIMIT {} OFFSET {}").format(
                sql.Placeholder(), sql.Placeholder()
            )
            window_rows = await gw.client.fetch_all(
                stmt,
                [*base_params, int(window_limit), int(window_offset)],
                row_factory="dict",
            )

            return SnapshotWindow(rows=[dict(row) for row in window_rows])

        stream = await build_snapshot_pool_streaming(
            result_snapshot=result_snapshot,
            rs_spec=rs_spec,
            snap_opt=snapshot,
            fp_computed=fp_fingerprint,
            codec=read_codec,
            prepare_rows=None,
            fetch_window=fetch_window,
            page_offset=page_offset,
            page_limit=page_limit,
            trust_source=trust_source,
        )
        page = materialize_mapping_rows(
            codec=read_codec,
            model_type=model_type,
            page_rows=stream.page_rows,
            pool=None,
            u=page_offset,
            page_limit=page_limit,
            return_type=return_type,
            return_fields=return_fields,
            trust_source=trust_source,
        )

        return page_from_limit_offset(
            page,
            pagination_dict,
            total=(
                total if (return_count and count_policy != "none") else None
            ),
            snapshot=stream.handle,
        )

    params = list(plan.params)
    user_limit = pagination_dict.get("limit")

    if user_limit is not None:
        data_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
        params.append(int(user_limit))

    if pagination_dict.get("offset") is not None:
        data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
        params.append(offset_from_dict(pagination_dict))

    rows = await gw.client.fetch_all(data_stmt, params, row_factory="dict")

    return await materialize_offset_page(
        rows=list(rows),
        pagination_dict=pagination_dict,
        return_count=return_count,
        total=total if count_policy != "none" else None,
        return_type=return_type,
        return_fields=return_fields,
        model_type=model_type,
        codec=read_codec,
        trust_source=trust_source,
    )

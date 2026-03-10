"""Full-text search gateway using Postgres built-in ``tsvector``/``tsquery``."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Optional, Sequence, TypeVar, overload

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.search import (
    SearchIndexSpecInternal,
    SearchOptions,
)
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate

from ..base import PostgresQualifiedName
from .base import PostgresSearchGateway
from .utils import fts_rank_weights_array

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


class PostgresFTSSearchGateway[M: BaseModel](PostgresSearchGateway[M]):
    """Search gateway that builds ``tsvector @@ tsquery`` queries with ``ts_rank_cd`` ordering."""

    async def _resolve_tsvector_expr(
        self,
        index: str,
        spec: SearchIndexSpecInternal,
    ) -> sql.Composable:
        # 1) explicit hints win
        hints = spec.hints or {}

        if "tsvector_expr" in hints:
            # raw SQL snippet (trusted config)
            return sql.SQL(
                str(hints["tsvector_expr"])  # pyright: ignore[reportArgumentType]
            )

        if "tsvector_col" in hints:
            return sql.Identifier(str(hints["tsvector_col"]))

        # 2) try parsing pg_get_indexdef(...)
        q = PostgresQualifiedName.from_string(index)
        index_info = await self.introspector.get_index_info(
            index=q.name,
            schema=q.schema,
        )

        if index_info.engine != "fts":
            raise CoreError(
                f"Index {q.string()} has unsupported engine: {index_info.engine} (required: fts)"
            )

        if not index_info.expr:
            raise CoreError(
                "Unable to infer tsvector expression from index definition. "
                "Provide IndexSpec.hints['tsvector_expr'] or ['tsvector_col']."
            )

        # NOTE: expr is raw SQL fragment from Postgres catalog; we still treat it as config.
        return sql.SQL(index_info.expr)  # pyright: ignore[reportArgumentType]

    # ....................... #

    def _tsquery_expr(
        self,
        query: str,
        spec: SearchIndexSpecInternal,
        *,
        options: Optional[SearchOptions] = None,
    ) -> tuple[sql.Composable, list[Any]]:
        q = query.strip()
        options = options or {}
        lang: Optional[str] = options.get("language")
        params: list[Any] = []

        match spec.mode:
            case "phrase":
                fn = "phraseto_tsquery"

            case "exact":
                fn = "plainto_tsquery"

            case _:  # fulltext, prefix
                fn = "websearch_to_tsquery"

        if lang:
            params.extend([lang, q])

            return (
                sql.SQL(f"{fn}({{}}::regconfig, {{}}::text)").format(
                    sql.Placeholder(), sql.Placeholder()
                ),
                params,
            )

        params.append(q)

        return sql.SQL(f"{fn}({{}}::text)").format(sql.Placeholder()), params

    # ....................... #

    async def _build_search_parts(
        self,
        query: str,
        filters: Optional[QueryFilterExpression],  # type: ignore[valid-type]
        *,
        options: Optional[SearchOptions] = None,
    ) -> tuple[tuple[sql.Composable, list[Any]], tuple[sql.Composable, list[Any]]]:
        idx_name, idx_spec = self._pick_index(options)
        rank_weights = fts_rank_weights_array(idx_spec)

        tsv = await self._resolve_tsvector_expr(idx_name, idx_spec)
        filter_cond, filter_params = await self.where_clause(filters)

        # empty query: only filters, no rank
        if not query.strip():
            return (filter_cond, filter_params), (sql.SQL("0.0"), [])

        tsq, tsq_params = self._tsquery_expr(query, idx_spec, options=options)

        search_cond = sql.SQL("({tsv}) @@ ({tsq})").format(tsv=tsv, tsq=tsq)

        where_sql = sql.SQL(" AND ").join([search_cond, filter_cond])
        where_params = [*tsq_params, *filter_params]

        # order by rank
        rank_expr = sql.SQL("ts_rank_cd({weights}::float4[], ({tsv}), ({tsq}))").format(
            weights=sql.Placeholder(),
            tsv=tsv,
            tsq=tsq,
        )
        rank_params = [rank_weights, *tsq_params]

        return (where_sql, where_params), (rank_expr, rank_params)

    # ....................... #
    # API

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[SearchOptions] = ...,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> tuple[list[M], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[SearchOptions] = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> tuple[list[T], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[SearchOptions] = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]: ...

    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = None,  # type: ignore[valid-type]
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        *,
        options: Optional[SearchOptions] = None,
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[M] | list[T] | list[JsonDict], int]:
        (
            (where_sql, where_params),
            (rank_sql, rank_params),
        ) = await self._build_search_parts(query, filters, options=options)

        count_stmt = sql.SQL("SELECT COUNT(*) FROM {table} WHERE {where}").format(
            table=self.qname.ident(),
            where=where_sql,
        )

        total = int(await self.client.fetch_value(count_stmt, where_params, default=0))

        if total == 0:
            return [], total

        # order: rank desc + optional secondary sorts
        order_parts: list[sql.Composable] = [sql.SQL("{} DESC").format(rank_sql)]

        if sorts:
            for field, order in sorts.items():
                order_parts.append(
                    sql.SQL("{} {}").format(
                        sql.Identifier(field), sql.SQL(order.upper())
                    )
                )
        order_sql = sql.SQL(", ").join(order_parts)

        # rows
        stmt = sql.SQL(
            "SELECT {cols} FROM {table} WHERE {where} ORDER BY {order}"
        ).format(
            cols=self.return_clause(return_model, return_fields),
            table=self.qname.ident(),
            where=where_sql,
            order=order_sql,
        )

        params = [*where_params, *rank_params]

        if limit is not None:
            stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(limit)

        if offset is not None:
            stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(offset)

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")

        if return_model is not None:
            return [pydantic_validate(return_model, r) for r in rows], total

        if return_fields is not None:
            return [{k: r.get(k, None) for k in return_fields} for r in rows], total

        return [pydantic_validate(self.model, r) for r in rows], total

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Never, Optional, Sequence, TypeVar, overload

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.search import SearchIndexSpec
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate

from ..spec import PostgresQualifiedName
from .base import PostgresSearchGateway
from .utils import extract_index_expr_from_indexdef

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


class PostgresFTSSearchGateway[M: BaseModel](PostgresSearchGateway[M]):
    async def _fetch_indexdef(self, index: str) -> str:
        q = PostgresQualifiedName.from_string(index)
        schema = q.schema or self.spec.schema or "public"

        stmt = sql.SQL(
            """
            SELECT pg_get_indexdef(c.oid) AS indexdef
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = {schema}
              AND c.relname = {idx}
            LIMIT 1
            """
        ).format(schema=sql.Placeholder(), idx=sql.Placeholder())

        row = await self.client.fetch_one(stmt, [schema, q.name], row_factory="dict")
        if row is None or not row.get("indexdef"):
            raise CoreError(f"Cannot load indexdef for index: {index}")

        return str(row["indexdef"])

    # ....................... #

    async def _resolve_tsvector_expr(
        self, index: str, spec: SearchIndexSpec
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
        indexdef = await self._fetch_indexdef(index)
        expr = extract_index_expr_from_indexdef(indexdef)

        if not expr:
            raise CoreError(
                "Unable to infer tsvector expression from index definition. "
                "Provide IndexSpec.hints['tsvector_expr'] or ['tsvector_col']."
            )

        # NOTE: expr is raw SQL fragment from Postgres catalog; we still treat it as config.
        return sql.SQL(expr)  # pyright: ignore[reportArgumentType]

    # ....................... #

    def _tsquery_expr(
        self,
        query: str,
        spec: SearchIndexSpec,
        *,
        options: Optional[JsonDict] = None,
    ) -> tuple[sql.Composable, list[Any]]:
        q = query.strip()

        if not q:
            return sql.SQL("TRUE"), []

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

    async def _fts_where(
        self,
        query: str,
        index: str,
        spec: SearchIndexSpec,
        *,
        options: Optional[JsonDict] = None,
    ) -> tuple[sql.Composable, list[Any]]:
        tsv = await self._resolve_tsvector_expr(index, spec)
        tsq, tsp = self._tsquery_expr(query, spec, options=options)

        cond = sql.SQL("({tsv}) @@ ({tsq})").format(tsv=tsv, tsq=tsq)

        return cond, tsp

    # ....................... #

    def _fts_rank_expr(self, spec: SearchIndexSpec) -> sql.Composable:
        # simple default: ts_rank_cd(tsv, tsq)
        # For weights: if you want, you can translate FieldSpec.weight into A/B/C/D weights later via hints.
        return sql.SQL("ts_rank_cd")

    # ....................... #

    async def _where(
        self,
        query: str,
        filters: Optional[QueryFilterExpression],
        *,
        options: Optional[JsonDict],
    ) -> tuple[tuple[sql.Composable, list[Any]], tuple[sql.Composable, list[Any]]]:
        idx_key, idx = self._pick_index(options)

        sw, sp = await self._fts_where(query, idx_key, idx, options=options)
        fw, fp = await self.where_clause(filters)

        # also return rank order expr, because fts ordering depends on same tsquery
        # We'll rebuild tsquery again inside ORDER BY to keep it simple+deterministic.
        tsv = await self._resolve_tsvector_expr(idx_key, idx)
        tsq, tsq_params = self._tsquery_expr(query, idx, options=options)

        rank = sql.SQL("ts_rank_cd(({tsv}), ({tsq}))").format(tsv=tsv, tsq=tsq)
        # IMPORTANT: params must match SELECT order usage; easiest is to reuse same params
        # for WHERE and ORDER BY (duplicated). We'll do: params = where_params + order_params

        where_sql = sql.SQL(" AND ").join([sw, fw])
        where_params = [*sp, *fp]
        order_sql = rank
        order_params = tsq_params  # same as for ORDER BY

        return (where_sql, where_params), (order_sql, order_params)

    # ....................... #
    # API

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[JsonDict] = ...,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> tuple[list[M], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[JsonDict] = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> tuple[list[T], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[JsonDict] = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[JsonDict] = ...,
        return_model: type[T] = ...,
        return_fields: Sequence[str] = ...,
    ) -> Never: ...

    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        *,
        options: Optional[JsonDict] = None,
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[M] | list[T] | list[JsonDict], int]:
        (where_sql, where_params), (rank_sql, rank_params) = await self._where(
            query, filters, options=options
        )

        count_stmt = sql.SQL("SELECT COUNT(*) FROM {table} WHERE {where}").format(
            table=self.spec.ident(),
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
            table=self.spec.ident(),
            where=where_sql,
            order=order_sql,
        )

        params = [*where_params, *rank_params]  # ORDER BY needs its tsquery params

        if limit is not None:
            stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(int(limit))

        if offset is not None:
            stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(int(offset))

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")

        if return_model is not None:
            return [pydantic_validate(return_model, r) for r in rows], total

        if return_fields is not None:
            return [{k: r.get(k, None) for k in return_fields} for r in rows], total

        return [pydantic_validate(self.model, r) for r in rows], total

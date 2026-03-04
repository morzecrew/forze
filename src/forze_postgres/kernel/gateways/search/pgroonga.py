from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Never, Optional, Sequence, TypeVar, overload

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.search import SearchIndexSpecInternal, SearchOptions
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate

from ..base import PostgresQualifiedName
from .base import PostgresSearchGateway

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


class PostgresPGroongaSearchGateway[M: BaseModel](PostgresSearchGateway[M]):
    def _effective_field_weights(
        self,
        spec: SearchIndexSpecInternal,
        options: Optional[SearchOptions] = None,
    ) -> list[tuple[str, float]]:
        options = options or {}
        ov_weights = options.get("weights", {})

        ov_groups = ov_weights.get("groups", {})
        ov_fields = ov_weights.get("fields", {})

        res: list[tuple[str, float]] = []

        for f in spec.fields:
            p = f.path_safe

            if f.group:
                group = spec.groups.get(f.group)
                base_group_weight = group.weight if group else 1.0
            else:
                base_group_weight = 1.0

            if f.group and f.group in ov_groups:
                base_group_weight = ov_groups[f.group]

            base_field_weight = f.weight if f.weight is not None else 1.0

            if p in ov_fields:
                base_field_weight = ov_fields[p]

            effective = base_group_weight * base_field_weight
            res.append((p, effective))

        return res

    # ....................... #

    async def _pgroonga_where(
        self,
        query: str,
        index: str,
        spec: SearchIndexSpecInternal,
        *,
        options: Optional[SearchOptions] = None,
    ) -> tuple[sql.Composable, list[Any]]:
        options = options or {}
        q = query.strip()

        if not q:
            return sql.SQL("TRUE"), []

        eff = self._effective_field_weights(spec, options)
        fields = [p for p, _ in eff]
        weights = [int(w * 100) for _, w in eff]

        fuzzy = options.get("fuzzy", {})
        use_fuzzy = fuzzy.get("enabled", False)
        ratio = fuzzy.get("max_distance_ratio")

        if ratio is None and spec.fuzzy is not None:
            ratio = spec.fuzzy.max_distance_ratio

        if ratio is None:
            ratio = 0.34  # default value for fuzzy search

        params: list[Any] = [q, index]

        q_ph = sql.Placeholder()
        idx_ph = sql.Placeholder()
        r_ph = sql.Placeholder()
        w_ph = sql.Placeholder()

        q = PostgresQualifiedName.from_string(index)
        index_info = await self.introspector.get_index_info(
            index=q.name, schema=q.schema
        )

        if index_info.engine != "pgroonga":
            raise CoreError(
                f"Index {q.string()} has unsupported engine: {index_info.engine} (required: pgroonga)"
            )

        # check expression for array or single field
        if index_info.expr is None or ("ARRAY" not in index_info.expr.upper()):
            text_expr = sql.SQL("coalesce({}::text, '')").format(
                sql.Identifier(fields[0])
            )

            if use_fuzzy:
                params.append(float(ratio))
                cond = sql.SQL(
                    "pgroonga_condition({}::text, index_name => {}::text, fuzzy_max_distance_ratio => {}::float4)"
                ).format(q_ph, idx_ph, r_ph)

            else:
                cond = sql.SQL(
                    "pgroonga_condition({}::text, index_name => {}::text)"
                ).format(q_ph, idx_ph)

            return sql.SQL("{} &@~ {}").format(text_expr, cond), params

        # if array in index expression - we need to build array expression
        array_expr = sql.SQL("(ARRAY[{}])").format(
            sql.SQL(", ").join(
                sql.SQL("coalesce({}::text, '')").format(sql.Identifier(f))
                for f in fields
            )
        )

        params.append(weights)

        if use_fuzzy:
            params.append(float(ratio))
            cond = sql.SQL(
                "pgroonga_condition({}::text, index_name => {}::text, weights => {}::int[], fuzzy_max_distance_ratio => {}::float4)"
            ).format(q_ph, idx_ph, w_ph, r_ph)

        else:
            cond = sql.SQL(
                "pgroonga_condition({}::text, index_name => {}::text, weights => {}::int[])"
            ).format(q_ph, idx_ph, w_ph)

        return sql.SQL("{} &@~ {}").format(array_expr, cond), params

    # ....................... #

    def _pgroonga_order(
        self,
        sorts: Optional[QuerySortExpression] = None,
    ) -> sql.Composable:
        parts: list[sql.Composable] = [sql.SQL("pgroonga_score(tableoid, ctid) DESC")]

        if sorts:
            for field, order in sorts.items():
                parts.append(
                    sql.SQL("{} {}").format(
                        sql.Identifier(field), sql.SQL(order.upper())
                    )
                )

        return sql.SQL(", ").join(parts)

    # ....................... #

    async def _where_clause(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = None,
        *,
        options: Optional[SearchOptions] = None,
    ) -> tuple[sql.Composable, list[Any]]:
        index, spec = self._pick_index(options)
        sw, sp = await self._pgroonga_where(query, index, spec, options=options)
        fw, fp = await self.where_clause(filters)

        where_parts = sql.SQL(" AND ").join([sw, fw])
        params = [*sp, *fp]

        return where_parts, params

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
        options: Optional[SearchOptions] = ...,
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
        options: Optional[SearchOptions] = ...,
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
        options: Optional[SearchOptions] = ...,
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
        options: Optional[SearchOptions] = ...,
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
        options: Optional[SearchOptions] = None,
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[M] | list[T] | list[JsonDict], int]:
        where, params = await self._where_clause(query, filters, options=options)
        order = self._pgroonga_order(sorts)

        # total
        count_stmt = sql.SQL("SELECT COUNT(*) FROM {table} WHERE {where}").format(
            table=self.qname.ident(),
            where=where,
        )
        total = int(await self.client.fetch_value(count_stmt, params, default=0))

        # Exit early if no results
        if total == 0:
            return [], total

        # rows
        stmt = sql.SQL(
            "SELECT {cols} FROM {table} WHERE {where} ORDER BY {order}"
        ).format(
            cols=self.return_clause(return_model, return_fields),
            table=self.qname.ident(),
            where=where,
            order=order,
        )

        if limit is not None:
            stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params = [*params, int(limit)]

        if offset is not None:
            stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params = [*params, int(offset)]

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")

        if return_model is not None:
            return [pydantic_validate(return_model, r) for r in rows], total

        if return_fields is not None:
            return [{k: r.get(k, None) for k in return_fields} for r in rows], total

        return [pydantic_validate(self.model, r) for r in rows], total

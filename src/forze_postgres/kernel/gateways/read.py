"""Read-only gateway for fetching documents from a Postgres relation."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Never, Sequence, TypeVar, final, overload
from uuid import UUID

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.query import (
    AggregatesExpression,
    CursorPaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.base.errors import CoreError, NotFoundError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate, pydantic_validate_many
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.query import PsycopgQueryRenderer
from forze_postgres.kernel.query.nested import sort_key_expr
from forze_postgres.pagination import (
    build_order_by_sql,
    build_seek_condition,
    decode_keyset_v1,
    normalize_sorts_with_id,
)

from .base import PostgresGateway

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


@final
class PostgresReadGateway[M: BaseModel](PostgresGateway[M]):
    """Read-only gateway providing single/batch lookups, filtered queries, and counting."""

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> M: ...

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> T: ...

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> JsonDict: ...

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never: ...

    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> M | T | JsonDict:
        where_sql = sql.SQL("{pk} = {ph}").format(
            pk=self.ident_pk(),
            ph=sql.Placeholder(),
        )
        where_params = [pk]

        where_sql, where_params = self._add_tenant_where(where_sql, where_params)  # type: ignore[assignment]

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=self.return_clause(return_model, return_fields),
            table=self.source_qname.ident(),
            where=where_sql,
        )

        if for_update:
            self.client.require_transaction()
            stmt += sql.SQL(" FOR UPDATE")

        row = await self.client.fetch_one(stmt, where_params, row_factory="dict")

        if row is None:
            raise NotFoundError(f"Record not found: {pk}")

        if return_model is not None:
            return pydantic_validate(return_model, row)

        if return_fields is not None:
            return {k: row.get(k, None) for k in return_fields}

        return pydantic_validate(self.model_type, row)

    # ....................... #

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> list[M]: ...

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_model: type[T],
        return_fields: None = ...,
    ) -> list[T]: ...

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> list[JsonDict]: ...

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never: ...

    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> list[M] | list[T] | list[JsonDict]:
        if not pks:
            return []

        where_sql = sql.SQL("{pk} = ANY({ph})").format(
            pk=self.ident_pk(),
            ph=sql.Placeholder(),
        )
        where_params = [list(pks)]

        where_sql, where_params = self._add_tenant_where(where_sql, where_params)  # type: ignore[assignment]

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=self.return_clause(return_model, return_fields),
            table=self.source_qname.ident(),
            where=where_sql,
        )

        rows = await self.client.fetch_all(stmt, where_params, row_factory="dict")

        m = {row[ID_FIELD]: row for row in rows}
        ordered = [m[pk] for pk in pks if pk in m]
        missing = [x for x in pks if x not in m]

        if missing:
            raise NotFoundError(f"Some records not found: {missing}")

        if return_model is not None:
            return pydantic_validate_many(return_model, ordered)

        if return_fields is not None:
            return [{k: row.get(k, None) for k in return_fields} for row in ordered]

        return pydantic_validate_many(self.model_type, ordered)

    # ....................... #

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> M | None: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> T | None: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> JsonDict | None: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never: ...

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = False,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> M | T | JsonDict | None:
        # tenant id supplied by where clause
        where, params = await self.where_clause(filters)

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where} LIMIT 1").format(
            cols=self.return_clause(return_model, return_fields),
            table=self.source_qname.ident(),
            where=where,
        )

        if for_update:
            self.client.require_transaction()
            stmt += sql.SQL(" FOR UPDATE")

        row = await self.client.fetch_one(stmt, params, row_factory="dict")

        if row is None:
            return None

        if return_model is not None:
            return pydantic_validate(return_model, row)

        if return_fields is not None:
            return {k: row.get(k, None) for k in return_fields}

        return pydantic_validate(self.model_type, row)

    # ....................... #

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> list[JsonDict]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_model: type[T],
        return_fields: None = ...,
    ) -> list[T]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> list[M]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> list[T]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> list[JsonDict]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never: ...

    async def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        limit: int | None = None,
        offset: int | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        aggregates: AggregatesExpression | None = None,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> list[M] | list[T] | list[JsonDict]:
        if aggregates is not None:
            return await self.find_many_aggregates(
                filters=filters,
                limit=limit,
                offset=offset,
                sorts=sorts,
                aggregates=aggregates,
                return_model=return_model,
                return_fields=return_fields,
            )

        # tenant id supplied by where clause
        where, params = await self.where_clause(filters)
        sort_clause = await self.order_by_clause(sorts)

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=self.return_clause(return_model, return_fields),
            table=self.source_qname.ident(),
            where=where,
        )

        if sort_clause is not None:
            stmt += sql.SQL(" ORDER BY {sort}").format(sort=sort_clause)

        if limit is not None:
            stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(limit)

        if offset is not None:
            stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(offset)

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")

        if return_model is not None:
            return pydantic_validate_many(return_model, rows)

        if return_fields is not None:
            return [{k: row.get(k, None) for k in return_fields} for row in rows]

        return pydantic_validate_many(self.model_type, rows)

    # ....................... #

    async def find_many_aggregates(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        limit: int | None = None,
        offset: int | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        aggregates: AggregatesExpression,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> list[T] | list[JsonDict]:
        """Find aggregate rows."""

        if return_fields is not None:
            raise CoreError("Aggregates cannot be combined with return_fields")

        where, params = await self.where_clause(filters)
        types = await self.column_types()
        renderer = PsycopgQueryRenderer(
            types=types,
            model_type=self.model_type,
            nested_field_hints=self.nested_field_hints,
        )
        parsed, select_clause, group_clause, aggregate_params = (
            renderer.render_aggregates(
                aggregates,
            )
        )
        params = list(aggregate_params) + list(params)
        sort_clause = renderer.render_aggregate_order_by(parsed, sorts)

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=select_clause,
            table=self.source_qname.ident(),
            where=where,
        )

        if group_clause is not None:
            stmt += sql.SQL(" GROUP BY {group}").format(group=group_clause)

        if sort_clause is not None:
            stmt += sql.SQL(" ORDER BY {sort}").format(sort=sort_clause)

        if limit is not None:
            stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(limit)

        if offset is not None:
            stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(offset)

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")

        if return_model is not None:
            return pydantic_validate_many(return_model, rows)

        return list(rows)

    # ....................... #

    async def count_aggregates(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        aggregates: AggregatesExpression,
    ) -> int:
        """Count aggregate result groups."""

        where, params = await self.where_clause(filters)
        types = await self.column_types()
        renderer = PsycopgQueryRenderer(
            types=types,
            model_type=self.model_type,
            nested_field_hints=self.nested_field_hints,
        )
        _parsed, select_clause, group_clause, aggregate_params = (
            renderer.render_aggregates(
                aggregates,
            )
        )
        params = list(aggregate_params) + list(params)

        inner = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=select_clause,
            table=self.source_qname.ident(),
            where=where,
        )

        if group_clause is not None:
            inner += sql.SQL(" GROUP BY {group}").format(group=group_clause)

        stmt = sql.SQL("SELECT COUNT(*) FROM ({inner}) AS agg").format(inner=inner)
        res = await self.client.fetch_value(stmt, params, default=0)

        return int(res)

    # ....................... #

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> list[M]: ...

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: type[T],
        return_fields: None = ...,
    ) -> list[T]: ...

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> list[JsonDict]: ...

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never: ...

    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> list[M] | list[T] | list[JsonDict]:
        c = dict(cursor or {})

        if c.get("after") and c.get("before"):
            raise CoreError(
                "Cursor pagination: pass at most one of 'after' or 'before'"
            )

        limit_raw = c.get("limit")

        lim: int = 10 if limit_raw is None else int(limit_raw)  # type: ignore[call-overload]

        if lim < 1:
            raise CoreError("Cursor pagination 'limit' must be positive")

        use_before = c.get("before") is not None
        use_after = c.get("after") is not None

        normalized = normalize_sorts_with_id(sorts)
        sort_keys = [k for k, _ in normalized]
        directions = [d for _, d in normalized]

        where_base, params = await self.where_clause(filters)
        types = await self.column_types()
        alias = self.filter_table_alias
        exprs = [
            sort_key_expr(
                field=k,
                column_types=types,
                model_type=self.model_type,
                nested_field_hints=self.nested_field_hints,
                table_alias=alias,
            )
            for k, _ in normalized
        ]

        seek_params: list[Any] = []
        where_fin = where_base

        if use_after or use_before:
            token = str(c["after" if use_after else "before"])
            tk, td, tv = decode_keyset_v1(token)

            if tk != sort_keys or len(td) != len(directions):
                raise CoreError("Cursor does not match current sort keys")

            for i in range(len(directions)):
                if (td[i] or "").lower() != (directions[i] or "").lower():
                    raise CoreError("Cursor does not match current sort order")

            seek_sql, seek_params = build_seek_condition(
                exprs,
                directions,
                tv,
                "before" if use_before else "after",
            )
            where_fin = sql.SQL("({} AND ({}))").format(where_base, seek_sql)
            params = list(params) + seek_params  # type: ignore[operator]

        order_fwd = build_order_by_sql(exprs, directions, flip=False)
        order_bwd = build_order_by_sql(exprs, directions, flip=True)

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=self.return_clause(return_model, return_fields),
            table=self.source_qname.ident(),
            where=where_fin,
        )
        o_sql = order_fwd if not use_before else order_bwd
        stmt = sql.SQL("{} ORDER BY {}").format(stmt, o_sql)  # type: ignore[assignment]
        plim = int(lim) + 1
        stmt = sql.SQL("{} LIMIT {}").format(stmt, sql.Placeholder())  # type: ignore[assignment]
        params = list(params)  # type: ignore[assignment]
        params.append(plim)

        raw_rows = list(await self.client.fetch_all(stmt, params, row_factory="dict"))

        if use_before:
            raw_rows = list(reversed(raw_rows))

        # At most *lim* + 1 rows; caller slices and derives ``has_more``.
        if return_model is not None:
            return pydantic_validate_many(return_model, raw_rows)

        if return_fields is not None:
            return [{k: r.get(k, None) for k in return_fields} for r in raw_rows]

        return pydantic_validate_many(self.model_type, raw_rows)

    # ....................... #

    async def count(self, filters: QueryFilterExpression | None = None) -> int:  # type: ignore[valid-type]
        # tenant id supplied by where clause
        where, params = await self.where_clause(filters)

        stmt = sql.SQL("SELECT COUNT(*) FROM {table} WHERE {where}").format(
            table=self.source_qname.ident(),
            where=where,
        )

        res = await self.client.fetch_value(stmt, params, default=0)

        return int(res)

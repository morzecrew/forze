"""Read-only gateway for fetching documents from a Postgres relation."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Never, Sequence, TypeVar, final, overload
from uuid import UUID

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.base.errors import NotFoundError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate, pydantic_validate_many
from forze.domain.constants import ID_FIELD

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
            table=self.qname.ident(),
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
            table=self.qname.ident(),
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
            table=self.qname.ident(),
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
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> list[M] | list[T] | list[JsonDict]:
        # tenant id supplied by where clause
        where, params = await self.where_clause(filters)
        sort_clause = self.order_by_clause(sorts)

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {where}").format(
            cols=self.return_clause(return_model, return_fields),
            table=self.qname.ident(),
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

    async def count(self, filters: QueryFilterExpression | None = None) -> int:  # type: ignore[valid-type]
        # tenant id supplied by where clause
        where, params = await self.where_clause(filters)

        stmt = sql.SQL("SELECT COUNT(*) FROM {table} WHERE {where}").format(
            table=self.qname.ident(),
            where=where,
        )

        res = await self.client.fetch_value(stmt, params, default=0)

        return int(res)

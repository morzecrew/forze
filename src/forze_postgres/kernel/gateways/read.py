from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Never, Optional, Sequence, TypeVar, final, overload
from uuid import UUID

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.base.errors import NotFoundError, ValidationError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate
from forze.domain.constants import ID_FIELD

from .base import PostgresGateway

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


@final
class PostgresReadGateway[M: BaseModel](PostgresGateway[M]):
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
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> M | T | JsonDict:
        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {pk} = {ph}").format(
            cols=self.return_clause(return_model, return_fields),
            table=self.qname.ident(),
            pk=self.ident_pk(),
            ph=sql.Placeholder(),
        )

        if for_update:
            self.client.require_transaction()
            stmt += sql.SQL(" FOR UPDATE")

        row = await self.client.fetch_one(stmt, (pk,), row_factory="dict")

        if row is None:
            raise NotFoundError(f"Запись не найдена: {pk}")

        if return_model is not None:
            return pydantic_validate(return_model, row)

        if return_fields is not None:
            return {k: row.get(k, None) for k in return_fields}

        return pydantic_validate(self.model, row)

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
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> list[M] | list[T] | list[JsonDict]:
        if not pks:
            return []

        stmt = sql.SQL("SELECT {cols} FROM {table} WHERE {pk} = ANY({ph})").format(
            cols=self.return_clause(return_model, return_fields),
            table=self.qname.ident(),
            pk=self.ident_pk(),
            ph=sql.Placeholder(),
        )

        params: list[Any] = [list(pks)]

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")

        m = {row[ID_FIELD]: row for row in rows}
        ordered = [m[pk] for pk in pks if pk in m]
        missing = [x for x in pks if x not in m]

        if missing:
            raise NotFoundError(f"Некоторые записи не найдены: {missing}")

        if return_model is not None:
            return [pydantic_validate(return_model, row) for row in ordered]

        if return_fields is not None:
            return [{k: row.get(k, None) for k in return_fields} for row in ordered]

        return [pydantic_validate(self.model, row) for row in ordered]

    # ....................... #

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,
        *,
        for_update: bool = ...,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> Optional[M]: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,
        *,
        for_update: bool = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> Optional[T]: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,
        *,
        for_update: bool = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> Optional[JsonDict]: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,
        *,
        for_update: bool = ...,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never: ...

    async def find(
        self,
        filters: QueryFilterExpression,
        *,
        for_update: bool = False,
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Optional[M | T | JsonDict]:
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

        return pydantic_validate(self.model, row)

    # ....................... #

    @overload
    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> list[M]: ...

    @overload
    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        return_model: type[T],
        return_fields: None = ...,
    ) -> list[T]: ...

    @overload
    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> list[JsonDict]: ...

    @overload
    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never: ...

    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        *,
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> list[M] | list[T] | list[JsonDict]:
        if not filters and limit is None:
            raise ValidationError("Фильтры или лимит должны быть предоставлены")

        where, params = await self.where_clause(filters)
        sort = self.sort_clause(sorts)

        stmt = sql.SQL(
            "SELECT {cols} FROM {table} WHERE {where} ORDER BY {sort}"
        ).format(
            cols=self.return_clause(return_model, return_fields),
            table=self.qname.ident(),
            where=where,
            sort=sort,
        )

        if limit is not None:
            stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(limit)

        if offset is not None:
            stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(offset)

        rows = await self.client.fetch_all(stmt, params, row_factory="dict")

        if return_model is not None:
            return [pydantic_validate(return_model, row) for row in rows]

        if return_fields is not None:
            return [{k: row.get(k, None) for k in return_fields} for row in rows]

        return [pydantic_validate(self.model, row) for row in rows]

    # ....................... #

    async def count(self, filters: Optional[QueryFilterExpression] = None) -> int:
        where, params = await self.where_clause(filters)

        stmt = sql.SQL("SELECT COUNT(*) FROM {table} WHERE {where}").format(
            table=self.qname.ident(),
            where=where,
        )

        res = await self.client.fetch_value(stmt, params, default=0)

        return int(res)

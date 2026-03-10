from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from typing import Never, Optional, Sequence, TypeVar, final, overload
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.base.errors import NotFoundError, ValidationError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate
from forze.domain.constants import ID_FIELD

from .base import MongoGateway

# ----------------------- #

T = TypeVar("T", bound=BaseModel)


@final
class MongoReadGateway[M: BaseModel](MongoGateway[M]):
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
        if for_update:
            self.client.require_transaction()

        raw = await self.client.find_one(
            self.coll(),
            {"_id": self._storage_pk(pk)},
            projection=self._projection(return_fields),
        )

        if raw is None:
            raise NotFoundError(f"Record not found: {pk}")

        data = self._from_storage_doc(raw)

        if return_model is not None:
            return pydantic_validate(return_model, data)

        if return_fields is not None:
            return self._return_subset(data, return_fields)

        return pydantic_validate(self.model, data)

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

        ids = [self._storage_pk(pk) for pk in pks]
        rows = await self.client.find_many(
            self.coll(),
            {"_id": {"$in": ids}},
            projection=self._projection(return_fields),
        )

        by_pk: dict[str, JsonDict] = {}
        for row in rows:
            normalized = self._from_storage_doc(row)
            by_pk[str(normalized[ID_FIELD])] = normalized

        missing = [pk for pk in pks if self._storage_pk(pk) not in by_pk]
        if missing:
            raise NotFoundError(f"Some records not found: {missing}")

        ordered = [by_pk[self._storage_pk(pk)] for pk in pks]

        if return_model is not None:
            return [pydantic_validate(return_model, row) for row in ordered]

        if return_fields is not None:
            return [self._return_subset(row, return_fields) for row in ordered]

        return [pydantic_validate(self.model, row) for row in ordered]

    # ....................... #

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> Optional[M]: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> Optional[T]: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> Optional[JsonDict]: ...

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
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Optional[M | T | JsonDict]:
        if for_update:
            self.client.require_transaction()

        query = self._render_filters(filters)
        raw = await self.client.find_one(
            self.coll(),
            query,
            projection=self._projection(return_fields),
        )

        if raw is None:
            return None

        data = self._from_storage_doc(raw)

        if return_model is not None:
            return pydantic_validate(return_model, data)

        if return_fields is not None:
            return self._return_subset(data, return_fields)

        return pydantic_validate(self.model, data)

    # ....................... #

    @overload
    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
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
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
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
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
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
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never: ...

    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = None,  # type: ignore[valid-type]
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        *,
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> list[M] | list[T] | list[JsonDict]:
        if not filters and limit is None:
            raise ValidationError("Filters or limit must be provided")

        query = self._render_filters(filters)
        rows = await self.client.find_many(
            self.coll(),
            query,
            projection=self._projection(return_fields),
            sort=self._sorts(sorts),
            limit=limit,
            skip=offset,
        )
        normalized = [self._from_storage_doc(row) for row in rows]

        if return_model is not None:
            return [pydantic_validate(return_model, row) for row in normalized]

        if return_fields is not None:
            return [self._return_subset(row, return_fields) for row in normalized]

        return [pydantic_validate(self.model, row) for row in normalized]

    # ....................... #

    async def count(self, filters: Optional[QueryFilterExpression] = None) -> int:  # type: ignore[valid-type]
        query = self._render_filters(filters)
        return await self.client.count(self.coll(), query)

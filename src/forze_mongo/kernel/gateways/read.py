"""Mongo gateway for read operations (get, find, count)."""

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
    """Read-only Mongo gateway for single-document and multi-document queries.

    Supports fetching by primary key, filter-based lookups, paginated listing,
    and counting. Results can be projected to a subset of fields or mapped to
    an alternative model type.
    """

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> M:
        """Fetch a document by primary key as the gateway model."""
        ...

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> T:
        """Fetch a document by primary key validated against *return_model*."""
        ...

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> JsonDict:
        """Fetch a document by primary key projected to *return_fields*."""
        ...

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never:
        """Invalid combination; specifying both *return_model* and *return_fields* is unsupported."""
        ...

    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> M | T | JsonDict:
        """Fetch a single document by primary key.

        When *for_update* is ``True``, an active transaction is required.

        :param pk: Document primary key.
        :param for_update: Require a transaction context for pessimistic reads.
        :param return_model: Optional alternative Pydantic model for validation.
        :param return_fields: Optional field subset to project.
        :raises NotFoundError: If no document matches the primary key.
        """

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
    ) -> list[M]:
        """Fetch multiple documents as the gateway model."""
        ...

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_model: type[T],
        return_fields: None = ...,
    ) -> list[T]:
        """Fetch multiple documents validated against *return_model*."""
        ...

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> list[JsonDict]:
        """Fetch multiple documents projected to *return_fields*."""
        ...

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never:
        """Invalid combination; specifying both *return_model* and *return_fields* is unsupported."""
        ...

    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> list[M] | list[T] | list[JsonDict]:
        """Fetch multiple documents by primary key, preserving input order.

        :param pks: Primary keys to fetch.
        :param return_model: Optional alternative Pydantic model.
        :param return_fields: Optional field subset to project.
        :raises NotFoundError: If any primary key is missing.
        """

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
    ) -> Optional[M]:
        """Find one document matching filters as the gateway model."""
        ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> Optional[T]:
        """Find one document matching filters validated against *return_model*."""
        ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> Optional[JsonDict]:
        """Find one document matching filters projected to *return_fields*."""
        ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never:
        """Invalid combination; specifying both *return_model* and *return_fields* is unsupported."""
        ...

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = False,
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Optional[M | T | JsonDict]:
        """Find a single document matching the given filter expression.

        Returns ``None`` when no document matches.

        :param filters: Query filter expression.
        :param for_update: Require a transaction context for pessimistic reads.
        :param return_model: Optional alternative Pydantic model.
        :param return_fields: Optional field subset to project.
        """

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
    ) -> list[M]:
        """Find documents as the gateway model."""
        ...

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
    ) -> list[T]:
        """Find documents validated against *return_model*."""
        ...

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
    ) -> list[JsonDict]:
        """Find documents projected to *return_fields*."""
        ...

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
    ) -> Never:
        """Invalid combination; specifying both *return_model* and *return_fields* is unsupported."""
        ...

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
        """Find multiple documents with optional filters, sorting, and pagination.

        At least one of *filters* or *limit* must be provided to prevent
        unbounded queries.

        :param filters: Optional filter expression.
        :param limit: Maximum number of results.
        :param offset: Number of results to skip.
        :param sorts: Sort expression; defaults to descending by ID.
        :param return_model: Optional alternative Pydantic model.
        :param return_fields: Optional field subset to project.
        :raises ValidationError: If neither *filters* nor *limit* is provided.
        """

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
        """Count documents matching the given filters.

        :param filters: Optional filter expression; ``None`` counts all
            documents in the collection.
        """

        query = self._render_filters(filters)
        return await self.client.count(self.coll(), query)

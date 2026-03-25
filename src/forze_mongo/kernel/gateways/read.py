"""Mongo gateway for read operations (get, find, count)."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from typing import Never, Sequence, TypeVar, final, overload
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.base.errors import NotFoundError, ValidationError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate, pydantic_validate_many
from forze.domain.constants import ID_FIELD

from .base import MongoGateway

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


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
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
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

        filters = {"_id": self._storage_pk(pk)}
        filters = self._add_tenant_filter(filters)

        raw = await self.client.find_one(
            self.coll(),
            filters,
            projection=self.render_projection(return_fields),
        )

        if raw is None:
            raise NotFoundError(f"Record not found: {pk}")

        data = self._from_storage_doc(raw)

        if return_model is not None:
            return pydantic_validate(return_model, data)

        if return_fields is not None:
            return self.return_subset(data, return_fields)

        return pydantic_validate(self.model_type, data)

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
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
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
        filters = {"_id": {"$in": ids}}
        filters = self._add_tenant_filter(filters)

        rows = await self.client.find_many(
            self.coll(),
            filters,
            projection=self.render_projection(return_fields),
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
            return pydantic_validate_many(return_model, ordered)

        if return_fields is not None:
            return [self.return_subset(row, return_fields) for row in ordered]

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
    ) -> M | None:
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
    ) -> T | None:
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
    ) -> JsonDict | None:
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
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> M | T | JsonDict | None:
        """Find a single document matching the given filter expression.

        Returns ``None`` when no document matches.

        :param filters: Query filter expression.
        :param for_update: Require a transaction context for pessimistic reads.
        :param return_model: Optional alternative Pydantic model.
        :param return_fields: Optional field subset to project.
        """

        if for_update:
            self.client.require_transaction()

        query = self.render_filters(filters)

        raw = await self.client.find_one(
            self.coll(),
            query,
            projection=self.render_projection(return_fields),
        )

        if raw is None:
            return None

        data = self._from_storage_doc(raw)

        if return_model is not None:
            return pydantic_validate(return_model, data)

        if return_fields is not None:
            return self.return_subset(data, return_fields)

        return pydantic_validate(self.model_type, data)

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
    ) -> list[M]:
        """Find documents as the gateway model."""
        ...

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
    ) -> list[T]:
        """Find documents validated against *return_model*."""
        ...

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
    ) -> list[JsonDict]:
        """Find documents projected to *return_fields*."""
        ...

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
    ) -> Never:
        """Invalid combination; specifying both *return_model* and *return_fields* is unsupported."""
        ...

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

        query = self.render_filters(filters)
        rows = await self.client.find_many(
            self.coll(),
            query,
            projection=self.render_projection(return_fields),
            sort=self.render_sorts(sorts),
            limit=limit,
            skip=offset,
        )
        normalized = [self._from_storage_doc(row) for row in rows]

        if return_model is not None:
            return pydantic_validate_many(return_model, normalized)

        if return_fields is not None:
            return [self.return_subset(row, return_fields) for row in normalized]

        return pydantic_validate_many(self.model_type, normalized)

    # ....................... #

    async def count(self, filters: QueryFilterExpression | None = None) -> int:  # type: ignore[valid-type]
        """Count documents matching the given filters.

        :param filters: Optional filter expression; ``None`` counts all
            documents in the collection.
        """

        query = self.render_filters(filters)

        return await self.client.count(self.coll(), query)

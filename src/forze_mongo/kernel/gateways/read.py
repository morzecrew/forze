"""Mongo gateway for read operations (get, find, count)."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from typing import Any, Never, Sequence, TypeVar, cast, final, overload
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.query import (
    AggregatesExpression,
    CursorPaginationExpression,
    ParsedAggregates,
    QueryFilterExpression,
    QuerySortExpression,
    decode_keyset_v1,
    normalize_sorts_with_id,
)
from forze.base.errors import CoreError, NotFoundError, ValidationError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate, pydantic_validate_many
from forze.domain.constants import ID_FIELD

from .base import MongoGateway

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


def _empty_global_aggregate_row(parsed: ParsedAggregates) -> JsonDict:
    return {
        computed.alias: 0 if computed.function == "$count" else None
        for computed in parsed.computed_fields
    }


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
        aggregates: AggregatesExpression,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> list[JsonDict]:
        """Find aggregate rows as JSON mappings."""
        ...

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
    ) -> list[T]:
        """Find aggregate rows validated against *return_model*."""
        ...

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
        aggregates: None = ...,
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
        aggregates: None = ...,
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
        aggregates: None = ...,
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
        aggregates: AggregatesExpression | None = None,
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

        match = self.render_filters(filters)
        parsed, pipeline = self.renderer.render_aggregates(
            aggregates,
            match=match or None,
            sorts=sorts,
            limit=limit,
            skip=offset,
        )
        rows = await self.client.aggregate(self.coll(), pipeline, limit=limit)

        if (
            not rows
            and not parsed.fields
            and (offset is None or offset == 0)
            and (limit is None or limit > 0)
        ):
            rows = [_empty_global_aggregate_row(parsed)]

        if return_model is not None:
            return pydantic_validate_many(return_model, rows)

        return rows

    # ....................... #

    async def count_aggregates(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        aggregates: AggregatesExpression,
    ) -> int:
        """Count aggregate result groups."""

        match = self.render_filters(filters)
        parsed, pipeline = self.renderer.render_aggregates(
            aggregates,
            match=match or None,
        )
        pipeline.append({"$count": "count"})
        rows = await self.client.aggregate(self.coll(), pipeline, limit=1)

        if not rows and not parsed.fields:
            return 1

        if not rows:
            return 0

        return int(rows[0].get("count", 0))

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
        """Keyset on ``_id`` only (Mongo v1: sort must be the default *id* order)."""

        c = dict(cursor or {})

        if c.get("after") and c.get("before"):
            raise CoreError(
                "Cursor pagination: pass at most one of 'after' or 'before'"
            )

        limit_raw = c.get("limit")
        lim: int = 10 if limit_raw is None else int(cast(Any, limit_raw))  # type: ignore[has-type, assignment]

        if lim < 1:
            raise CoreError("Cursor pagination 'limit' must be positive")

        use_before = c.get("before") is not None
        use_after = c.get("after") is not None

        normalized = normalize_sorts_with_id(sorts)

        if [k for k, _ in normalized] != [ID_FIELD] or len(normalized) != 1:
            raise CoreError(
                "Mongo find_many_with_cursor (v1) requires sorting only by primary key: "
                "omit ``sorts`` or pass a single {id: asc|desc}.",
            )
        _id_asc = normalized[0][1] == "asc"

        base = self.render_filters(filters)
        seek: dict[str, Any] = {}

        if use_after or use_before:
            token = str(c["after" if use_after else "before"])
            tk, td, tv = decode_keyset_v1(token)
            if (
                tk != [ID_FIELD]
                or len(td) != 1
                or str(td[0]).lower()
                not in (
                    "asc",
                    "desc",
                )
            ):
                raise CoreError("Invalid cursor for current sort")

            if str(td[0]).lower() != ("asc" if _id_asc else "desc"):
                raise CoreError("Cursor does not match current sort order")

            _rid = str(tv[0]) if len(tv) == 1 else None

            if not _rid:
                raise CoreError("Invalid cursor for current sort")

            if use_after and _id_asc:
                seek = {"_id": {"$gt": _rid}}

            elif use_after and not _id_asc:
                seek = {"_id": {"$lt": _rid}}

            elif use_before and _id_asc:
                seek = {"_id": {"$lt": _rid}}

            else:
                seek = {"_id": {"$gt": _rid}}

        if seek and base:
            q: dict[str, Any] = {"$and": [base, seek]}

        elif seek:
            q = seek

        else:
            q = base

        sort_asc = _id_asc

        if use_before:
            sort_asc = not sort_asc

        mgo_sort: list[tuple[str, int]] = [("_id", 1 if sort_asc else -1)]

        rows = await self.client.find_many(
            self.coll(),
            q,
            projection=self.render_projection(return_fields),
            sort=mgo_sort,
            limit=lim + 1,
            skip=None,
        )

        if use_before:
            rows = list(reversed(rows))

        raw_normalized = [self._from_storage_doc(row) for row in rows]

        if return_model is not None:
            return pydantic_validate_many(return_model, raw_normalized)

        if return_fields is not None:
            return [self.return_subset(row, return_fields) for row in raw_normalized]

        return pydantic_validate_many(self.model_type, raw_normalized)

    # ....................... #

    async def count(self, filters: QueryFilterExpression | None = None) -> int:  # type: ignore[valid-type]
        """Count documents matching the given filters.

        :param filters: Optional filter expression; ``None`` counts all
            documents in the collection.
        """

        query = self.render_filters(filters)

        return await self.client.count(self.coll(), query)

"""Mongo gateway for read operations (get, find, count)."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from typing import (
    Any,
    AsyncGenerator,
    Literal,
    Never,
    Sequence,
    TypeVar,
    cast,
    final,
    overload,
)
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document.value_objects import (
    RowLockMode,
    row_lock_requires_transaction,
)
from forze.application.contracts.querying import (
    AggregatesExpression,
    CursorPaginationExpression,
    ParsedAggregates,
    QueryExpr,
    QueryFilterExpression,
    QuerySortExpression,
    decode_keyset_v1,
    normalize_sorts_for_keyset,
)
from forze.application.integrations.persistence import (
    ReadValidationCodecMixin,
    log_non_postgres_lock_degrade,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.domain.constants import ID_FIELD

from .base import MongoGateway

# ----------------------- #

T = TypeVar("T", bound=BaseModel)
M = TypeVar("M", bound=BaseModel)

# ....................... #


def _empty_global_aggregate_row(parsed: ParsedAggregates) -> JsonDict:
    return {
        computed.alias: 0 if computed.function == "$count" else None
        for computed in parsed.computed_fields
    }


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoReadGateway[M: BaseModel](
    ReadValidationCodecMixin[M],
    MongoGateway[M],
):
    """Read-only Mongo gateway for single-document and multi-document queries.

    Supports fetching by primary key, filter-based lookups, paginated listing,
    and counting. Results can be projected to a subset of fields or mapped to
    an alternative model type.
    """

    read_validation: Literal["strict", "trusted"] = attrs.field(
        default="strict",
        kw_only=True,
    )
    """Row decode mode for query results (``trusted`` skips validation)."""

    def _effective_find_limit(self, limit: int | None) -> int | None:
        """Apply :attr:`~forze_mongo.kernel.gateways.base.MongoGateway.find_many_implicit_limit` when *limit* is omitted."""

        if limit is not None:
            return limit

        return self.find_many_implicit_limit

    # ....................... #

    async def get(
        self,
        pk: UUID,
        *,
        for_update: RowLockMode = False,
    ) -> M:
        """Fetch a single document by primary key.

        When *for_update* is not ``False``, an active transaction is required.
        ``"nowait"`` and ``"skip_locked"`` are degraded to a transactional read.

        :param pk: Document primary key.
        :param for_update: Pessimistic read / transactional read mode.
        :raises NotFoundError: If no document matches the primary key.
        """

        if row_lock_requires_transaction(for_update):
            log_non_postgres_lock_degrade(for_update, backend="mongo")
            self.client.require_transaction()

        filters = {"_id": self._storage_pk(pk)}
        filters = self._add_tenant_filter(filters)

        raw = await self.client.find_one(
            await self.coll(),
            filters,
            projection=self.render_projection(None),
        )

        if raw is None:
            raise exc.not_found(f"Record not found: {pk}")

        data = self._from_storage_doc(raw)

        return await self._adecode_row(data)

    # ....................... #

    async def get_many(self, pks: Sequence[UUID]) -> list[M]:
        """Fetch multiple documents by primary key, preserving input order.

        :param pks: Primary keys to fetch.
        :raises NotFoundError: If any primary key is missing.
        """

        if not pks:
            return []

        ids = [self._storage_pk(pk) for pk in pks]
        filters = {"_id": {"$in": ids}}
        filters = self._add_tenant_filter(filters)

        rows = await self.client.find_many(
            await self.coll(),
            filters,
            projection=self.render_projection(None),
        )

        by_pk: dict[str, JsonDict] = {}

        for row in rows:
            normalized = self._from_storage_doc(row)
            by_pk[str(normalized[ID_FIELD])] = normalized

        missing = [pk for pk in pks if self._storage_pk(pk) not in by_pk]

        if missing:
            raise exc.not_found(f"Some records not found: {missing}")

        ordered = [by_pk[self._storage_pk(pk)] for pk in pks]

        return await self._adecode_rows(ordered)

    # ....................... #

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = ...,
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
        for_update: RowLockMode = ...,
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
        for_update: RowLockMode = ...,
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
        for_update: RowLockMode = ...,
        return_model: type[T],
        return_fields: Sequence[str],
    ) -> Never:
        """Invalid combination; specifying both *return_model* and *return_fields* is unsupported."""
        ...

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = False,
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

        if row_lock_requires_transaction(for_update):
            log_non_postgres_lock_degrade(for_update, backend="mongo")
            self.client.require_transaction()

        query = self.render_filters(filters)

        raw = await self.client.find_one(
            await self.coll(),
            query,
            projection=self.render_projection(return_fields),
        )

        if raw is None:
            return None

        data = self._from_storage_doc(raw)

        if return_model is not None:
            return await self._adecode_row(data, model=return_model)

        if return_fields is not None:
            [decrypted] = await self._adecrypt_projection_rows((data,))
            return self.return_subset(decrypted, return_fields)

        return await self._adecode_row(data)

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
        parsed: QueryExpr | None = ...,
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
        parsed: QueryExpr | None = ...,
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
        parsed: QueryExpr | None = ...,
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
        parsed: QueryExpr | None = ...,
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
        parsed: QueryExpr | None = ...,
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
        parsed: QueryExpr | None = ...,
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
        parsed: QueryExpr | None = None,
    ) -> list[M] | list[T] | list[JsonDict]:
        """Find multiple documents with optional filters, sorting, and pagination.

        When *limit* is omitted, :attr:`~forze_mongo.kernel.gateways.base.MongoGateway.find_many_implicit_limit`
        caps the result set to prevent unbounded queries.

        :param filters: Optional filter expression.
        :param limit: Maximum number of results.
        :param offset: Number of results to skip.
        :param sorts: Sort expression; defaults to descending by ID.
        :param return_model: Optional alternative Pydantic model.
        :param return_fields: Optional field subset to project.
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
                parsed=parsed,
            )

        query = self.render_filters(filters, parsed=parsed)
        null_sort = self.offset_null_sort_stages(sorts)

        if null_sort is not None:
            rows = await self._find_many_null_ordered(
                query,
                stages=null_sort[0],
                rank_fields=null_sort[1],
                limit=limit,
                offset=offset,
                return_fields=return_fields,
            )

        else:
            rows = await self.client.find_many(
                await self.coll(),
                query,
                projection=self.render_projection(return_fields),
                sort=self.render_sorts(sorts),
                limit=self._effective_find_limit(limit),
                skip=offset,
            )

        normalized = [self._from_storage_doc(row) for row in rows]

        if return_model is not None:
            return await self._adecode_rows(normalized, model=return_model)

        if return_fields is not None:
            decrypted = await self._adecrypt_projection_rows(normalized)
            return [self.return_subset(row, return_fields) for row in decrypted]

        return await self._adecode_rows(normalized)

    # ....................... #

    async def find_many_chunked(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        limit: int | None = None,
        offset: int | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        fetch_batch_size: int = 2000,
        return_model: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> AsyncGenerator[list[M] | list[T] | list[JsonDict]]:
        """Like :meth:`find_many` but stream validated row batches from the cursor.

        Each yielded list has at most ``fetch_batch_size`` rows, so peak memory is one
        batch regardless of how many documents match — the bounded-memory way to read
        past the implicit ``find_many`` cap. Same ``return_model`` / ``return_fields``
        rules as :meth:`find_many`. Computed null-ordering is not supported here.
        """

        if return_model is not None and return_fields is not None:
            raise exc.internal("return_model and return_fields cannot be combined")

        query = self.render_filters(filters)

        if self.offset_null_sort_stages(sorts) is not None:
            raise exc.precondition(
                "Chunked reads do not support computed null ordering; use find_many"
            )

        async for raw in self.client.find_many_streamed(
            await self.coll(),
            query,
            projection=self.render_projection(return_fields),
            sort=self.render_sorts(sorts),
            limit=limit,
            skip=offset,
            batch_size=fetch_batch_size,
        ):
            if not raw:
                continue

            normalized = [self._from_storage_doc(row) for row in raw]

            if return_fields is not None:
                decrypted = await self._adecrypt_projection_rows(normalized)
                yield [self.return_subset(row, return_fields) for row in decrypted]

            elif return_model is not None:
                yield await self._adecode_rows(normalized, model=return_model)

            else:
                yield await self._adecode_rows(normalized)

    # ....................... #

    async def _find_many_null_ordered(
        self,
        query: JsonDict,
        *,
        stages: list[JsonDict],
        rank_fields: list[str],
        limit: int | None,
        offset: int | None,
        return_fields: Sequence[str] | None,
    ) -> list[JsonDict]:
        """Offset read honoring a non-native null placement via an aggregation pipeline.

        Built only when :attr:`~forze_mongo.kernel.gateways.base.MongoGateway.computed_null_ordering`
        is set and a sort key overrides the canonical null order (see
        :meth:`~forze_mongo.kernel.gateways.base.MongoGateway.offset_null_sort_stages`).
        The computed rank fields are projected out so rows decode like a plain find.
        """

        eff_limit = self._effective_find_limit(limit)
        pipeline: list[JsonDict] = []

        if query:
            pipeline.append({"$match": query})

        pipeline.extend(stages)

        if offset:
            pipeline.append({"$skip": offset})

        if eff_limit is not None:
            pipeline.append({"$limit": eff_limit})

        # Inclusion projection (return_fields) drops the rank fields; otherwise exclude
        # only the ranks so the full document survives.
        projection = self.render_projection(return_fields)

        if projection is None:
            projection = {rank: 0 for rank in rank_fields}

        pipeline.append({"$project": projection})

        return await self.client.aggregate(
            await self.coll(), pipeline, limit=eff_limit
        )

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
        parsed: QueryExpr | None = None,
    ) -> list[T] | list[JsonDict]:
        """Find aggregate rows."""

        if return_fields is not None:
            raise exc.internal("Aggregates cannot be combined with return_fields")

        eff_limit = self._effective_find_limit(limit)

        match = self.render_filters(filters, parsed=parsed)
        parsed_, pipeline = self.renderer.render_aggregates(
            aggregates,
            match=match or None,
            sorts=sorts,
            limit=eff_limit,
            skip=offset,
        )
        rows = await self.client.aggregate(await self.coll(), pipeline, limit=eff_limit)

        if (
            not rows
            and not parsed_.groups
            and (offset is None or offset == 0)
            and (eff_limit is None or eff_limit > 0)
        ):
            rows = [_empty_global_aggregate_row(parsed_)]

        if return_model is not None:
            return await self._adecode_rows(rows, model=return_model)

        return rows

    # ....................... #

    async def count_aggregates(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        aggregates: AggregatesExpression,
        parsed: QueryExpr | None = None,
    ) -> int:
        """Count aggregate result groups."""

        match = self.render_filters(filters, parsed=parsed)
        parsed_, pipeline = self.renderer.render_aggregates(
            aggregates,
            match=match or None,
        )
        pipeline.append({"$count": "count"})
        rows = await self.client.aggregate(await self.coll(), pipeline, limit=1)

        if not rows and not parsed_.groups:
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
            raise exc.validation(
                "Cursor pagination: pass at most one of 'after' or 'before'"
            )

        limit_raw = c.get("limit")
        lim: int = 10 if limit_raw is None else int(cast(Any, limit_raw))  # type: ignore[has-type, assignment]

        if lim < 1:
            raise exc.validation("Cursor pagination 'limit' must be positive")

        use_before = c.get("before") is not None
        use_after = c.get("after") is not None

        normalized = normalize_sorts_for_keyset(
            sorts,
            read_fields=self.read_fields,
            model=self.model_type,
        )

        if [k for k, _, _ in normalized] != [ID_FIELD] or len(normalized) != 1:
            raise exc.precondition(
                "Mongo cursor pagination requires sorting only by primary key: "
                "omit sorts or pass a single {id: asc|desc}.",
            )
        _id_asc = normalized[0][1] == "asc"

        base = self.render_filters(filters)
        seek: dict[str, Any] = {}

        if use_after or use_before:
            token = str(c["after" if use_after else "before"])
            tk, td, _tn, tv = decode_keyset_v1(token)
            if (
                tk != [ID_FIELD]
                or len(td) != 1
                or str(td[0]).lower()
                not in (
                    "asc",
                    "desc",
                )
            ):
                raise exc.validation("Invalid cursor for current sort")

            if str(td[0]).lower() != ("asc" if _id_asc else "desc"):
                raise exc.validation("Cursor does not match current sort order")

            _rid = str(tv[0]) if len(tv) == 1 else None

            if not _rid:
                raise exc.validation("Invalid cursor for current sort")

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
            await self.coll(),
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
            return await self._adecode_rows(raw_normalized, model=return_model)

        if return_fields is not None:
            decrypted = await self._adecrypt_projection_rows(raw_normalized)
            return [self.return_subset(row, return_fields) for row in decrypted]

        return await self._adecode_rows(raw_normalized)

    # ....................... #

    async def count(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        parsed: QueryExpr | None = None,
    ) -> int:
        """Count documents matching the given filters.

        :param filters: Optional filter expression; ``None`` counts all
            documents in the collection.
        """

        query = self.render_filters(filters, parsed=parsed)

        return await self.client.count(await self.coll(), query)

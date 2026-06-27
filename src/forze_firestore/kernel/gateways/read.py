"""Firestore gateway for read operations."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from typing import Any, Literal, Never, Sequence, TypeVar, cast, final, overload
from uuid import UUID

import attrs
from google.cloud.firestore_v1.base_query import FieldFilter
from pydantic import BaseModel

from forze.application.contracts.document.types import (
    RowLockMode,
    log_non_postgres_lock_degrade,
    row_lock_requires_transaction,
)
from forze.application.contracts.querying import (
    AggregatesExpression,
    CursorPaginationExpression,
    QueryExpr,
    QueryFilterExpression,
    QuerySortExpression,
    decode_keyset_v1,
    normalize_sorts_for_keyset,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.domain.constants import ID_FIELD

from forze.application.integrations.persistence import ReadValidationCodecMixin

from .base import FirestoreGateway

# ----------------------- #

T = TypeVar("T", bound=BaseModel)
M = TypeVar("M", bound=BaseModel)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreReadGateway[M: BaseModel](
    ReadValidationCodecMixin[M],
    FirestoreGateway[M],
):
    """Read-only Firestore gateway."""

    read_validation: Literal["strict", "trusted"] = attrs.field(
        default="strict",
        kw_only=True,
    )

    def _effective_find_limit(self, limit: int | None) -> int | None:
        """Apply :attr:`~forze_firestore.kernel.gateways.base.FirestoreGateway.find_many_implicit_limit` when *limit* is omitted."""

        if limit is not None:
            return limit

        return self.find_many_implicit_limit

    # ....................... #

    async def get(self, pk: UUID, *, for_update: RowLockMode = False) -> M:
        if row_lock_requires_transaction(for_update):
            log_non_postgres_lock_degrade(for_update, backend="firestore")
            self.client.require_transaction()

        raw = await self.client.get_document(
            await self.coll(),
            self._storage_pk(pk),
        )

        if raw is None:
            raise exc.not_found(f"Record not found: {pk}")

        data = self._from_storage_doc(raw)

        return await self._adecode_row(data)

    # ....................... #

    async def get_many(self, pks: Sequence[UUID]) -> list[M]:
        if not pks:
            return []

        ids = [str(pk) for pk in pks]
        base = FieldFilter(ID_FIELD, "in", ids)
        flt = self._add_tenant_filter(base)

        rows = await self.client.query_stream(await self.coll(), filters=flt)

        by_pk: dict[str, JsonDict] = {}

        for row in rows:
            normalized = self._from_storage_doc(row)
            by_pk[str(normalized[ID_FIELD])] = normalized

        missing = [pk for pk in pks if str(pk) not in by_pk]

        if missing:
            raise exc.not_found(f"Some records not found: {missing}")

        ordered = [by_pk[str(pk)] for pk in pks]

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
        if row_lock_requires_transaction(for_update):
            log_non_postgres_lock_degrade(for_update, backend="firestore")
            self.client.require_transaction()

        flt = self.render_filters(filters)
        rows = await self.client.query_stream(await self.coll(), filters=flt, limit=1)

        if not rows:
            return None

        data = self._from_storage_doc(rows[0])

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

        if offset is not None and offset > 0:
            raise exc.precondition(
                "Firestore adapter does not support offset pagination; use cursor pagination"
            )

        flt = self.render_filters(filters, parsed=parsed)
        rows = await self.client.query_stream(
            await self.coll(),
            filters=flt,
            order_by=self.render_sorts(sorts),
            limit=self._effective_find_limit(limit),
        )
        normalized = [self._from_storage_doc(row) for row in rows]

        if return_model is not None:
            return await self._adecode_rows(normalized, model=return_model)

        if return_fields is not None:
            decrypted = await self._adecrypt_projection_rows(normalized)
            return [self.return_subset(row, return_fields) for row in decrypted]

        return await self._adecode_rows(normalized)

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
            raise exc.internal(
                "Cursor pagination: pass at most one of 'after' or 'before'"
            )

        limit_raw = c.get("limit")
        lim: int = 10 if limit_raw is None else int(cast(Any, limit_raw))  # type: ignore[has-type, assignment]

        if lim < 1:
            raise exc.internal("Cursor pagination 'limit' must be positive")

        use_before = c.get("before") is not None
        use_after = c.get("after") is not None
        normalized = normalize_sorts_for_keyset(
            sorts,
            read_fields=self.read_fields,
            model=self.model_type,
        )

        if [k for k, _, _ in normalized] != [ID_FIELD] or len(normalized) != 1:
            raise exc.internal(
                "Firestore find_many_with_cursor (v1) requires sorting only by primary key: "
                "omit ``sorts`` or pass a single {id: asc|desc}."
            )

        _id_asc = normalized[0][1] == "asc"
        start_after: str | None = None
        start_before: str | None = None

        if use_after or use_before:
            token = str(c["after" if use_after else "before"])
            tk, td, _tn, tv = decode_keyset_v1(token)

            if (
                tk != [ID_FIELD]
                or len(td) != 1
                or str(td[0]).lower() not in ("asc", "desc")
            ):
                raise exc.internal("Invalid cursor for current sort")

            if str(td[0]).lower() != ("asc" if _id_asc else "desc"):
                raise exc.internal("Cursor does not match current sort order")

            rid = str(tv[0]) if len(tv) == 1 else None

            if not rid:
                raise exc.internal("Invalid cursor for current sort")

            if use_after:
                start_after = rid
            else:
                start_before = rid

        sort_asc = _id_asc

        if use_before:
            sort_asc = not sort_asc

        order = [(ID_FIELD, "ASCENDING" if sort_asc else "DESCENDING")]
        flt = self.render_filters(filters)
        rows = await self.client.query_stream(
            await self.coll(),
            filters=flt,
            order_by=order,
            limit=lim + 1,
            start_after_id=start_after,
            start_before_id=start_before,
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
        flt = self.render_filters(filters, parsed=parsed)
        return await self.client.count_documents(await self.coll(), filters=flt)

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
        _ = filters, limit, offset, sorts, return_model, return_fields, parsed
        self.renderer.render_aggregates(aggregates)
        raise exc.internal(
            "Firestore adapter does not support aggregates in MVP"
        )  # pragma: no cover

    # ....................... #

    async def count_aggregates(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        aggregates: AggregatesExpression,
        parsed: QueryExpr | None = None,
    ) -> int:
        _ = filters, parsed
        self.renderer.render_aggregates(aggregates)
        raise exc.internal(
            "Firestore adapter does not support aggregates in MVP"
        )  # pragma: no cover

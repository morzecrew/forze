"""Postgres adapter implementing the document read/write port contracts."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import asyncio
from functools import cached_property
from typing import Any, Literal, Sequence, TypeVar, cast, final, overload
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.base import (
    CountlessPage,
    CursorPage,
    Page,
    page_from_limit_offset,
)
from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
    require_create_id,
    require_create_id_for_many,
)
from forze.application.contracts.query import (
    AggregatesExpression,
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    assemble_keyset_cursor_page,
    assert_cursor_projection_includes_sort_keys,
    normalize_sorts_with_id,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.application.coordinators import DocumentCacheCoordinator
from forze.base.errors import CoreError, InvalidOperationError
from forze.base.primitives import JsonDict
from forze.domain.constants import ID_FIELD, REV_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ..kernel.gateways import PostgresReadGateway, PostgresWriteGateway
from ._logger import logger
from .txmanager import PostgresTxScopeKey

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)
T = TypeVar("T", bound=BaseModel)

# ....................... #
#! Consider adding a method to bound or bind contextvars with 'name' as namespace or so
#! the above is related to logging


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDocumentAdapter(
    DocumentQueryPort[R],
    DocumentCommandPort[R, D, C, U],
    TxScopedPort,
):
    """Postgres-backed implementation of :class:`DocumentQueryPort` and :class:`DocumentCommandPort`."""

    spec: DocumentSpec[R, D, C, U]
    """Document specification."""

    read_gw: PostgresReadGateway[R]
    """Gateway used for all read queries."""

    write_gw: PostgresWriteGateway[D, C, U] | None = attrs.field(default=None)
    """Optional gateway for mutations; ``None`` disables write operations."""

    cache_coord: DocumentCacheCoordinator[R]
    """Unified read/write cache semantics for documents."""

    batch_size: int = 200
    """Batch size for writing."""

    # Non initable fields
    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.cache_coord.read_model_type is not self.read_gw.model_type:
            raise CoreError(
                "Document cache coordinator read model type mismatches read gateway model type."
            )

        if self.cache_coord.document_name != self.spec.name:
            raise CoreError(
                "Document cache coordinator name mismatches document specification name."
            )

        if self.write_gw is not None:
            if self.write_gw.client is not self.read_gw.client:
                raise CoreError("Write and read gateways must use the same client")

            if self.write_gw.tenant_aware != self.read_gw.tenant_aware:
                raise CoreError(
                    "Write and read gateways must have the same tenant awareness."
                )

    # ....................... #

    @cached_property
    def eff_batch_size(self) -> int:
        if self.batch_size < 10:
            logger.warning("Batch size is too small, using default value of 200")

            return 200

        if self.batch_size > 20000:
            logger.warning("Batch size is too large, using default value of 200")

            return 200

        return self.batch_size

    # ....................... #

    def _require_write(self) -> PostgresWriteGateway[D, C, U]:
        if self.write_gw is None:
            raise CoreError("Write gateway is not configured")

        return self.write_gw

    # ....................... #

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
        skip_cache: bool = ...,
    ) -> JsonDict: ...

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
        skip_cache: bool = ...,
    ) -> R: ...

    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
        return_fields: Sequence[str] | None = None,
        skip_cache: bool = False,
    ) -> R | JsonDict:
        if not self.cache_coord.id_rev_capable():
            raise InvalidOperationError(
                f"Cannot get document of type '{type(self.read_gw.model_type).__name__}' as it does not have defined id field"
            )

        logger.debug("Fetching 1 '%s' document (pk=%s)", self.spec.name, pk)

        if not self.cache_coord.read_through_eligible(
            skip_cache=skip_cache,
            return_fields=return_fields,
        ):
            return await self.read_gw.get(
                pk,
                for_update=for_update,
                return_fields=return_fields,
            )

        return await self.cache_coord.get_read_through(
            pk,
            fetch_on_cache_fault=lambda: self.read_gw.get(
                pk,
                for_update=for_update,
                return_fields=return_fields,
            ),
            fetch_on_miss_without_lock=lambda: self.read_gw.get(pk),
        )

    # ....................... #

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Sequence[str],
        skip_cache: bool = ...,
    ) -> Sequence[JsonDict]: ...

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: None = ...,
        skip_cache: bool = ...,
    ) -> Sequence[R]: ...

    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Sequence[str] | None = None,
        skip_cache: bool = False,
    ) -> Sequence[R] | Sequence[JsonDict]:
        if not pks:
            return []

        if not self.cache_coord.id_rev_capable():
            raise InvalidOperationError(
                f"Cannot get many documents of type '{type(self.read_gw.model_type).__name__}' as they do not have defined id field"
            )

        logger.debug(
            "Fetching %s '%s' document(s) (first_pk=%s)",
            len(pks),
            self.spec.name,
            pks[0],
        )

        if not self.cache_coord.read_through_eligible(
            skip_cache=skip_cache,
            return_fields=return_fields,
        ):
            return await self.read_gw.get_many(pks, return_fields=return_fields)

        return await self.cache_coord.get_many_read_through(
            pks,
            fetch_many_on_cache_fault=lambda: self.read_gw.get_many(
                pks,
                return_fields=return_fields,
            ),
            fetch_misses_many=lambda misses: self.read_gw.get_many(
                [UUID(x) for x in misses]
            ),
        )

    # ....................... #

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> JsonDict | None: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> R | None: ...

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = False,
        return_fields: Sequence[str] | None = None,
    ) -> R | JsonDict | None:
        logger.debug(
            "Finding 1 '%s' document (filter by %s, for_update=%s)",
            self.spec.name,
            list(filters.keys()),  # type: ignore[attr-defined]
            for_update,
        )

        return await self.read_gw.find(
            filters,
            for_update=for_update,
            return_fields=return_fields,
        )

    # ....................... #

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[T]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[True],
    ) -> Page[JsonDict]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: AggregatesExpression,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[True],
    ) -> Page[T]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
        return_count: Literal[False] = ...,
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[R]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
        return_count: Literal[True],
    ) -> Page[JsonDict]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[True],
    ) -> Page[R]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[T]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        aggregates: None = ...,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[True],
    ) -> Page[T]: ...

    async def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        aggregates: AggregatesExpression | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
        return_count: bool = False,
    ) -> (
        Page[R]
        | CountlessPage[R]
        | Page[T]
        | CountlessPage[T]
        | Page[JsonDict]
        | CountlessPage[JsonDict]
    ):
        if aggregates is not None and return_fields is not None:
            raise CoreError("Aggregates cannot be combined with return_fields")

        pagination = pagination or {}
        limit = pagination.get("limit")
        offset = pagination.get("offset")

        logger.debug(
            "Finding '%s' documents (filter by %s, limit=%s, offset=%s, sorts=%s, return_count=%s)",
            self.spec.name,
            list(filters.keys()) if filters else "N/A",  # type: ignore[attr-defined]
            limit if limit is not None else "N/A",
            offset if offset is not None else "N/A",
            sorts if sorts is not None else "N/A",
            return_count,
        )

        cnt = 0

        if return_count:
            cnt = (
                await self.read_gw.count_aggregates(filters, aggregates=aggregates)
                if aggregates is not None
                else await self.read_gw.count(filters)
            )
            if not cnt:
                logger.debug(
                    "No '%s' documents matching filters",
                    self.spec.name,
                )
                return page_from_limit_offset(
                    [],
                    pagination,
                    total=0,
                )
            logger.debug(
                "Found %s '%s' documents matching filters",
                cnt,
                self.spec.name,
            )

        res: list[Any]

        if aggregates is not None:
            res = await self.read_gw.find_many_aggregates(
                filters=filters,
                limit=limit,
                offset=offset,
                sorts=sorts,
                aggregates=aggregates,
                return_model=return_type,
            )
        else:
            res = await self.read_gw.find_many(  # type: ignore[misc]
                filters=filters,
                limit=limit,
                offset=offset,
                sorts=sorts,
                return_model=return_type,  # type: ignore[arg-type]
                return_fields=return_fields,  # type: ignore[arg-type]
            )

        if return_count:
            return page_from_limit_offset(  # type: ignore[return-value]
                list(res),
                pagination,
                total=cnt,
            )

        return page_from_limit_offset(list(res), pagination, total=None)  # type: ignore[return-value]

    # ....................... #

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: Sequence[str],
    ) -> CursorPage[JsonDict]: ...

    @overload
    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: None = ...,
    ) -> CursorPage[R]: ...

    async def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        return_fields: Sequence[str] | None = None,
    ) -> CursorPage[R] | CursorPage[JsonDict]:
        normalized = normalize_sorts_with_id(sorts)

        sort_keys = [k for k, _ in normalized]
        directions = [d for _, d in normalized]

        assert_cursor_projection_includes_sort_keys(
            return_fields=return_fields,
            sort_keys=sort_keys,
        )

        raw = await self.read_gw.find_many_with_cursor(  # type: ignore[call-overload, misc]
            filters,
            cursor=cursor,
            sorts=sorts,
            return_model=None,
            return_fields=return_fields,  # type: ignore[typeddict, arg-type, misc]
        )

        def _dump(o: R | JsonDict) -> JsonDict:
            if isinstance(o, dict):
                return o

            return o.model_dump(mode="json")  # type: ignore[union-attr, err]

        page_raw, has_more, next_tok, prev_tok = assemble_keyset_cursor_page(
            raw,
            cursor=cursor,
            sort_keys=sort_keys,
            directions=directions,
            dump_row=_dump,
        )

        if return_fields is not None:
            return CursorPage(
                hits=cast(list[JsonDict], page_raw),
                next_cursor=next_tok,
                prev_cursor=prev_tok,
                has_more=has_more,
            )

        out = list(page_raw)  # type: ignore[typeddict, var-annotated]

        return CursorPage(
            hits=cast(list[R], out),  # type: ignore[redundant-cast]
            next_cursor=next_tok,
            prev_cursor=prev_tok,
            has_more=has_more,
        )

    # ....................... #

    async def count(self, filters: QueryFilterExpression | None = None) -> int:  # type: ignore[valid-type]
        logger.debug(
            "Counting '%s' documents (filter by %s)",
            self.spec.name,
            list(filters.keys()) if filters else "N/A",  # type: ignore[attr-defined]
        )

        return await self.read_gw.count(filters)

    # ....................... #

    @overload
    async def create(self, dto: C, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def create(self, dto: C, *, return_new: Literal[False]) -> None: ...

    async def create(self, dto: C, *, return_new: bool = True) -> R | None:
        w = self._require_write()

        logger.debug("Creating 1 '%s' document", self.spec.name)

        domain = await w.create(dto)
        await self.cache_coord.invalidate_keys_now(domain.id)

        if not return_new:
            return None

        res = await self.read_gw.get(domain.id)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_one(res)
        )

        return res

    # ....................... #

    @overload
    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        w = self._require_write()

        if not dtos:
            logger.debug(
                "Empty list of payloads, skipping creation for '%s'",
                self.spec.name,
            )

            if not return_new:
                return None

            return []

        logger.debug("Creating %s '%s' documents", len(dtos), self.spec.name)

        domains = await w.create_many(dtos, batch_size=self.eff_batch_size)
        pks_new = [x.id for x in domains]
        await self.cache_coord.invalidate_keys_now(*pks_new)

        if not return_new:
            return None

        res = await self.read_gw.get_many(pks_new)

        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_many(res)
        )

        return res

    # ....................... #

    @overload
    async def ensure(
        self,
        dto: C,
        *,
        return_new: Literal[True] = True,
    ) -> R: ...

    @overload
    async def ensure(
        self,
        dto: C,
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def ensure(self, dto: C, *, return_new: bool = True) -> R | None:
        w = self._require_write()
        require_create_id(dto)

        logger.debug("Ensure 1 '%s' document", self.spec.name)

        domain = await w.ensure(dto)
        await self.cache_coord.invalidate_keys_now(domain.id)

        if not return_new:
            return None

        res = await self.read_gw.get(domain.id)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_one(res)
        )

        return res

    # ....................... #

    @overload
    async def ensure_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def ensure_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def ensure_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        w = self._require_write()

        if not dtos:
            if not return_new:
                return None

            return []

        require_create_id_for_many(dtos)

        logger.debug("Ensure %s '%s' documents", len(dtos), self.spec.name)

        domains = await w.ensure_many(dtos, batch_size=self.eff_batch_size)

        pks = [x.id for x in domains]
        await self.cache_coord.invalidate_keys_now(*pks)

        if not return_new:
            return None

        res = await self.read_gw.get_many(pks)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_many(res)
        )

        return res

    # ....................... #

    @overload
    async def upsert(
        self,
        create_dto: C,
        update_dto: U,
        *,
        return_new: Literal[True] = True,
    ) -> R: ...

    @overload
    async def upsert(
        self,
        create_dto: C,
        update_dto: U,
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def upsert(
        self,
        create_dto: C,
        update_dto: U,
        *,
        return_new: bool = True,
    ) -> R | None:
        w = self._require_write()
        require_create_id(create_dto)

        logger.debug("Upsert 1 '%s' document", self.spec.name)

        domain = await w.upsert(create_dto, update_dto)
        await self.cache_coord.invalidate_keys_now(domain.id)

        if not return_new:
            return None

        res = await self.read_gw.get(domain.id)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_one(res)
        )

        return res

    # ....................... #

    @overload
    async def upsert_many(
        self,
        pairs: Sequence[tuple[C, U]],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def upsert_many(
        self,
        pairs: Sequence[tuple[C, U]],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def upsert_many(
        self,
        pairs: Sequence[tuple[C, U]],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        w = self._require_write()

        if not pairs:
            if not return_new:
                return None
            return []

        require_create_id_for_many(pairs)

        logger.debug("Upsert %s '%s' document pairs", len(pairs), self.spec.name)

        domains = await w.upsert_many(pairs, batch_size=self.eff_batch_size)
        pks = [x.id for x in domains]
        await self.cache_coord.invalidate_keys_now(*pks)

        if not return_new:
            return None

        res = await self.read_gw.get_many(pks)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_many(res)
        )

        return res

    # ....................... #

    @overload
    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[False] = False,
    ) -> R: ...

    @overload
    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[True],
    ) -> tuple[R, JsonDict]: ...

    @overload
    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[False],
        return_diff: Literal[False] = False,
    ) -> None: ...

    @overload
    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[False],
        return_diff: Literal[True],
    ) -> JsonDict: ...

    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: bool = True,
        return_diff: bool = False,
    ) -> R | JsonDict | None | tuple[R, JsonDict]:
        w = self._require_write()

        logger.debug(
            "Updating 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

        (_, diff), _ = await asyncio.gather(
            w.update(pk, dto, rev=rev),
            self.cache_coord.invalidate_keys_now(pk),
        )

        if not return_new:
            if return_diff:
                return diff

            return None

        res = await self.read_gw.get(pk)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_one(res)
        )

        if return_diff:
            return res, diff

        return res

    # ....................... #

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[False] = False,
    ) -> Sequence[R]: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[True],
    ) -> Sequence[tuple[R, JsonDict]]: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[False] = False,
    ) -> None: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[True],
    ) -> Sequence[JsonDict]: ...

    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: bool = True,
        return_diff: bool = False,
    ) -> Sequence[R] | Sequence[JsonDict] | Sequence[tuple[R, JsonDict]] | None:
        w = self._require_write()

        if not updates:
            logger.debug(
                "Empty list of updates, skipping update for '%s'",
                self.spec.name,
            )

            if not return_new:
                return None

            return []

        pks = [x[0] for x in updates]
        revs = [x[1] for x in updates]
        dtos = [x[2] for x in updates]

        logger.debug(
            "Updating %s '%s' documents (first_pk=%s)",
            len(pks),
            self.spec.name,
            pks[0],
        )

        (_, diffs), _ = await asyncio.gather(
            w.update_many(pks, dtos, revs=revs, batch_size=self.eff_batch_size),
            self.cache_coord.invalidate_keys_now(*pks),
        )

        if not return_new:
            if return_diff:
                return diffs

            return None

        res = await self.read_gw.get_many(pks)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_many(res)
        )

        if return_diff:
            return list(zip(res, diffs, strict=True))

        return res

    # ....................... #

    @overload
    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[False],
    ) -> int: ...

    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: bool = True,
    ) -> Sequence[R] | int:
        w = self._require_write()

        logger.debug("update_matching (fast) on '%s'", self.spec.name)

        count, domains = await w.update_matching(filters, dto)
        pks = [d.id for d in domains]

        if pks:
            await self.cache_coord.invalidate_keys_now(*pks)

        if not return_new:
            return count

        res = await self.read_gw.get_many(pks)

        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_many(res)
        )

        return res

    # ....................... #

    @overload
    async def update_matching_strict(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[True] = True,
        chunk_size: int | None = ...,
    ) -> Sequence[R]: ...

    @overload
    async def update_matching_strict(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[False],
        chunk_size: int | None = ...,
    ) -> int: ...

    async def update_matching_strict(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: bool = True,
        chunk_size: int | None = None,
    ) -> Sequence[R] | int:
        """Apply the same partial update to every matching document using optimistic revisions.

        Loads documents in chunks (keyset by primary key), then calls :meth:`update_many`
        so each row uses its current ``rev`` like :meth:`update`.

        :param filters: Required filter expression.
        :param dto: Patch applied uniformly to each row in a chunk.
        :param chunk_size: Maximum rows per chunk; defaults to the adapter batch size when omitted.
        :param return_new: When ``True``, return all updated read models; when ``False``, return the count updated.
        """

        self._require_write()

        eff_chunk = self.eff_batch_size if chunk_size is None else chunk_size

        if eff_chunk < 1:
            raise CoreError("chunk_size must be positive")

        logger.debug(
            "update_matching_strict on '%s' (chunk=%s)",
            self.spec.name,
            eff_chunk,
        )

        n_total = 0
        out: list[R] = []
        last_id: UUID | None = None

        while True:
            chunk_filter: QueryFilterExpression = (  # type: ignore[valid-type]
                filters
                if last_id is None
                else {
                    "$and": [
                        filters,
                        {"$fields": {ID_FIELD: {"$gt": last_id}}},
                    ]
                }
            )

            page = (
                await self.find_many(
                    filters=chunk_filter,
                    pagination={"limit": eff_chunk},
                    sorts={ID_FIELD: "asc"},
                    return_count=False,
                    return_fields=[ID_FIELD, REV_FIELD],
                )
            ).hits

            if not page:
                break

            page_ids = [UUID(str(r[ID_FIELD])) for r in page]
            page_revs = [int(r[REV_FIELD]) for r in page]

            updates = list(zip(page_ids, page_revs, [dto] * len(page)))

            if return_new:
                got = await self.update_many(
                    updates,
                    return_new=True,
                )
                out.extend(got)

            else:
                await self.update_many(updates, return_new=False)

            n_total += len(page)
            last_id = page_ids[-1]

            if len(page) < eff_chunk:
                break

        if return_new:
            return out

        return n_total

    # ....................... #

    @overload
    async def touch(self, pk: UUID, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def touch(self, pk: UUID, *, return_new: Literal[False]) -> None: ...

    async def touch(self, pk: UUID, *, return_new: bool = True) -> R | None:
        w = self._require_write()

        logger.debug(
            "Touching 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

        await asyncio.gather(
            w.touch(pk),
            self.cache_coord.invalidate_keys_now(pk),
        )

        if not return_new:
            return None

        res = await self.read_gw.get(pk)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_one(res)
        )

        return res

    # ....................... #

    @overload
    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        w = self._require_write()

        if not pks:
            logger.debug(
                "Empty list of primary keys, skipping touch for '%s'",
                self.spec.name,
            )
            return []

        logger.debug(
            "Touching %s '%s' documents (first_pk=%s)",
            len(pks),
            self.spec.name,
            pks[0],
        )

        await asyncio.gather(
            w.touch_many(pks, batch_size=self.eff_batch_size),
            self.cache_coord.invalidate_keys_now(*pks),
        )

        if not return_new:
            return None

        res = await self.read_gw.get_many(pks)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_many(res)
        )

        return res

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        w = self._require_write()

        logger.debug(
            "Hard-deleting 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

        await asyncio.gather(
            w.kill(pk),
            self.cache_coord.invalidate_keys_now(pk),
        )

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        w = self._require_write()

        if not pks:
            logger.debug(
                "Empty list of primary keys, skipping hard-delete for '%s'",
                self.spec.name,
            )
            return None

        logger.debug(
            "Hard-deleting %s '%s' documents (first_pk=%s)",
            len(pks),
            self.spec.name,
            pks[0],
        )

        await asyncio.gather(
            w.kill_many(pks, batch_size=self.eff_batch_size),
            self.cache_coord.invalidate_keys_now(*pks),
        )

    # ....................... #

    @overload
    async def delete(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[True] = True,
    ) -> R: ...

    @overload
    async def delete(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def delete(self, pk: UUID, rev: int, *, return_new: bool = True) -> R | None:
        w = self._require_write()

        logger.debug(
            "Soft-deleting 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

        await asyncio.gather(
            w.delete(pk, rev=rev),
            self.cache_coord.invalidate_keys_now(pk),
        )

        if not return_new:
            return None

        res = await self.read_gw.get(pk)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_one(res)
        )

        return res

    # ....................... #

    @overload
    async def delete_many(
        self,
        deletes: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def delete_many(
        self,
        deletes: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def delete_many(
        self,
        deletes: Sequence[tuple[UUID, int]],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        w = self._require_write()

        if not deletes:
            logger.debug(
                "Empty list of deletes, skipping soft-delete for '%s'",
                self.spec.name,
            )
            return []

        pks = [x[0] for x in deletes]
        revs = [x[1] for x in deletes]

        logger.debug(
            "Soft-deleting %s '%s' documents (first_pk=%s)",
            len(pks),
            self.spec.name,
            pks[0],
        )

        await asyncio.gather(
            w.delete_many(pks, revs=revs, batch_size=self.eff_batch_size),
            self.cache_coord.invalidate_keys_now(*pks),
        )

        if not return_new:
            return None

        res = await self.read_gw.get_many(pks)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_many(res)
        )

        return res

    # ....................... #

    @overload
    async def restore(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[True] = True,
    ) -> R: ...

    @overload
    async def restore(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def restore(self, pk: UUID, rev: int, *, return_new: bool = True) -> R | None:
        w = self._require_write()

        logger.debug(
            "Restoring 1 '%s' document (pk=%s)",
            self.spec.name,
            pk,
        )

        await asyncio.gather(
            w.restore(pk, rev=rev),
            self.cache_coord.invalidate_keys_now(pk),
        )

        if not return_new:
            return None

        res = await self.read_gw.get(pk)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_one(res)
        )

        return res

    # ....................... #

    @overload
    async def restore_many(
        self,
        restores: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def restore_many(
        self,
        restores: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def restore_many(
        self,
        restores: Sequence[tuple[UUID, int]],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        w = self._require_write()

        if not restores:
            logger.debug(
                "Empty list of restores, skipping restore for '%s'",
                self.spec.name,
            )
            return []

        pks = [x[0] for x in restores]
        revs = [x[1] for x in restores]

        logger.debug(
            "Restoring %s '%s' documents (first_pk=%s)",
            len(pks),
            self.spec.name,
            pks[0],
        )

        await asyncio.gather(
            w.restore_many(pks, revs=revs, batch_size=self.eff_batch_size),
            self.cache_coord.invalidate_keys_now(*pks),
        )

        if not return_new:
            return None

        res = await self.read_gw.get_many(pks)
        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_many(res)
        )

        return res

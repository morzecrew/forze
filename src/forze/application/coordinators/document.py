"""Coordinator implementing document ports over pluggable persistence gateways."""

import asyncio
from functools import cached_property
from typing import (
    Any,
    Literal,
    Never,
    Protocol,
    Sequence,
    TypeVar,
    cast,
    overload,
)
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
from forze.base.errors import CoreError, InvalidOperationError
from forze.base.primitives import JsonDict
from forze.domain.constants import ID_FIELD, REV_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from .._logger import logger
from .cache import DocumentCacheCoordinator

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)
T = TypeVar("T", bound=BaseModel)

# ....................... #


class DocumentReadGatewayPort[M: BaseModel](Protocol):
    """Read gateway operations required by :class:`DocumentCoordinator`."""

    @property
    def model_type(self) -> type[M]: ...

    # ....................... #

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
    ) -> M | T | JsonDict: ...

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
    ) -> list[M] | list[T] | list[JsonDict]: ...

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
    ) -> M | T | JsonDict | None: ...

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
    ) -> list[M] | list[T] | list[JsonDict]: ...

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
    ) -> list[T] | list[JsonDict]: ...

    # ....................... #

    async def count_aggregates(
        self,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        *,
        aggregates: AggregatesExpression,
    ) -> int: ...

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
    ) -> list[M] | list[T] | list[JsonDict]: ...

    # ....................... #

    async def count(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
    ) -> int: ...


# ....................... #


class DocumentWriteGatewayPort[D_co: Document, C_co: CreateDocumentCmd, U_co: BaseDTO](
    Protocol
):
    """Write gateway operations required by :class:`DocumentCoordinator`."""

    async def create(self, dto: C_co) -> D_co: ...

    async def create_many(
        self,
        dtos: Sequence[C_co],
        *,
        batch_size: int,
    ) -> Sequence[D_co]: ...

    async def ensure(self, dto: C_co) -> D_co: ...

    async def ensure_many(
        self,
        dtos: Sequence[C_co],
        *,
        batch_size: int,
    ) -> Sequence[D_co]: ...

    async def upsert(self, create_dto: C_co, update_dto: U_co) -> D_co: ...

    async def upsert_many(
        self,
        pairs: Sequence[tuple[C_co, U_co]],
        *,
        batch_size: int,
    ) -> Sequence[D_co]: ...

    async def update(
        self,
        pk: UUID,
        dto: U_co,
        *,
        rev: int | None = None,
    ) -> tuple[D_co, JsonDict]: ...

    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U_co],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int,
    ) -> tuple[Sequence[D_co], Sequence[JsonDict]]: ...

    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U_co,
        *,
        batch_size: int,
    ) -> tuple[int, Sequence[D_co]]: ...

    async def touch(self, pk: UUID) -> D_co: ...

    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        batch_size: int,
    ) -> Sequence[D_co]: ...

    async def kill(self, pk: UUID) -> None: ...

    async def kill_many(self, pks: Sequence[UUID], *, batch_size: int) -> None: ...

    async def delete(self, pk: UUID, *, rev: int | None = None) -> D_co: ...

    async def delete_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 200,
    ) -> Sequence[D_co]: ...

    async def restore(self, pk: UUID, *, rev: int | None = None) -> D_co: ...

    async def restore_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int,
    ) -> Sequence[D_co]: ...


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentCoordinator(
    DocumentQueryPort[R],
    DocumentCommandPort[R, D, C, U],
    TxScopedPort,
):
    """Orchestrate :class:`~forze.application.contracts.document.DocumentQueryPort`
    / :class:`~forze.application.contracts.document.DocumentCommandPort` over gateways.
    """

    spec: DocumentSpec[R, D, C, U]
    """Document specification."""

    read_gw: DocumentReadGatewayPort[R]
    """Gateway used for all read queries."""

    write_gw: DocumentWriteGatewayPort[D, C, U] | None = attrs.field(default=None)
    """Optional gateway for mutations; ``None`` disables write operations."""

    cache_coord: DocumentCacheCoordinator[R]
    """Unified read/write cache semantics for documents."""

    batch_size: int = 200
    """Batch size for writing."""

    tx_scope: TxScopeKey
    """Transaction scope marker for callers."""

    enforce_primary_key_cursor_sort: bool = False
    """When ``True``, reject cursor queries unless sorted solely by ``id``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        """Check compatibility of cache coordinator with read gateway and specification."""

        if self.cache_coord.read_model_type is not self.read_gw.model_type:
            raise CoreError(
                "Document cache coordinator read model type mismatches read gateway model type."
            )

        if self.cache_coord.document_name != self.spec.name:
            raise CoreError(
                "Document cache coordinator name mismatches document specification name."
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

    def _require_write(self) -> DocumentWriteGatewayPort[D, C, U]:
        if self.write_gw is None:
            raise CoreError("Write gateway is not configured")

        return self.write_gw

    # ....................... #

    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
        skip_cache: bool = False,
    ) -> R:
        """Fetch a single document by primary key, using the cache when available."""

        if not self.cache_coord.id_rev_capable():
            raise InvalidOperationError(
                f"Cannot get document of type '{type(self.read_gw.model_type).__name__}' as it does not have defined id field"
            )

        if not self.cache_coord.read_through_eligible(
            skip_cache=skip_cache,
            return_fields=None,
        ):
            return await self.read_gw.get(pk, for_update=for_update)

        return await self.cache_coord.get_read_through(
            pk,
            fetch_on_cache_fault=lambda: self.read_gw.get(pk, for_update=for_update),
            fetch_on_miss_without_lock=lambda: self.read_gw.get(pk),
        )

    # ....................... #

    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        skip_cache: bool = False,
    ) -> Sequence[R]:
        """Fetch multiple documents by primary key with cache-aware batching."""

        if not pks:
            return []

        if not self.cache_coord.id_rev_capable():
            raise InvalidOperationError(
                f"Cannot get many documents of type '{type(self.read_gw.model_type).__name__}' as it does not have defined id field"
            )

        if not self.cache_coord.read_through_eligible(
            skip_cache=skip_cache,
            return_fields=None,
        ):
            return await self.read_gw.get_many(pks)

        return await self.cache_coord.get_many_read_through(
            pks,
            fetch_many_on_cache_fault=lambda: self.read_gw.get_many(pks),
            fetch_misses_many=lambda misses: self.read_gw.get_many(
                [UUID(x) for x in misses]
            ),
        )

    # ....................... #

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = False,
    ) -> R | None:
        """Find a single document matching the given filters."""

        return await self.read_gw.find(filters, for_update=for_update)

    async def project(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        fields: Sequence[str],
        *,
        for_update: bool = False,
    ) -> JsonDict | None:
        """Find one document matching filters and project ``fields``."""

        return await self.read_gw.find(
            filters,
            for_update=for_update,
            return_fields=tuple(fields),
        )

    async def select(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        return_type: type[T],
        *,
        for_update: bool = False,
    ) -> T | None:
        """Find one document matching filters as ``return_type``."""

        return await self.read_gw.find(
            filters,
            for_update=for_update,
            return_model=return_type,
        )

    # ....................... #

    @overload
    async def _offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: None,
        return_model: None,
        return_fields: None,
    ) -> CountlessPage[R]: ...

    @overload
    async def _offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: None,
        return_model: None,
        return_fields: None,
    ) -> Page[R]: ...

    @overload
    async def _offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: None,
        return_model: None,
        return_fields: Sequence[str],
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def _offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: None,
        return_model: None,
        return_fields: Sequence[str],
    ) -> Page[JsonDict]: ...

    @overload
    async def _offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: None,
        return_model: type[T],
        return_fields: None,
    ) -> CountlessPage[T]: ...

    @overload
    async def _offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: None,
        return_model: type[T],
        return_fields: None,
    ) -> Page[T]: ...

    @overload
    async def _offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: AggregatesExpression,
        return_model: None,
        return_fields: None,
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def _offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: AggregatesExpression,
        return_model: None,
        return_fields: None,
    ) -> Page[JsonDict]: ...

    @overload
    async def _offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: AggregatesExpression,
        return_model: type[T],
        return_fields: None,
    ) -> CountlessPage[T]: ...

    @overload
    async def _offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: AggregatesExpression,
        return_model: type[T],
        return_fields: None,
    ) -> Page[T]: ...

    async def _offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: bool,
        aggregates: AggregatesExpression | None,
        return_model: type[Any] | None,
        return_fields: Sequence[str] | None,
    ) -> Any:
        if aggregates is not None and return_fields is not None:
            raise CoreError("Aggregates cannot be combined with return_fields")

        pagination = pagination or {}
        cnt = 0
        if return_count:
            cnt = (
                await self.read_gw.count_aggregates(filters, aggregates=aggregates)
                if aggregates is not None
                else await self.read_gw.count(filters)
            )
            if not cnt:
                return page_from_limit_offset(  # pyright: ignore[reportUnknownVariableType]
                    [],
                    pagination,
                    total=0,
                )

        limit = pagination.get("limit")
        offset = pagination.get("offset")

        res: list[Any]

        if aggregates is not None:
            res = await self.read_gw.find_many_aggregates(
                filters=filters,
                limit=limit,
                offset=offset,
                sorts=sorts,
                aggregates=aggregates,
                return_model=return_model,
            )
        else:
            res = await self.read_gw.find_many(  # type: ignore[misc]
                filters=filters,
                limit=limit,
                offset=offset,
                sorts=sorts,
                return_model=return_model,  # type: ignore[arg-type]
                return_fields=return_fields,  # type: ignore[arg-type]
            )

        if return_count:
            return page_from_limit_offset(
                list(res),
                pagination,
                total=cnt,
            )
        return page_from_limit_offset(list(res), pagination, total=None)

    async def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[R]:
        return await self._offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=None,
            return_model=None,
            return_fields=None,
        )

    async def project_many(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[JsonDict]:
        return await self._offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=None,
            return_model=None,
            return_fields=tuple(fields),
        )

    async def select_many(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[T]:
        return await self._offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=None,
            return_model=return_type,
            return_fields=None,
        )

    async def find_page(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[R]:
        return await self._offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=None,
            return_model=None,
            return_fields=None,
        )

    async def project_page(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[JsonDict]:
        return await self._offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=None,
            return_model=None,
            return_fields=tuple(fields),
        )

    async def select_page(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[T]:
        return await self._offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=None,
            return_model=return_type,
            return_fields=None,
        )

    async def aggregate_many(
        self,
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[JsonDict]:
        return await self._offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=aggregates,
            return_model=None,
            return_fields=None,
        )

    async def aggregate_page(
        self,
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[JsonDict]:
        return await self._offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=aggregates,
            return_model=None,
            return_fields=None,
        )

    async def select_many_aggregated(
        self,
        return_type: type[T],
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[T]:
        return await self._offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=aggregates,
            return_model=return_type,
            return_fields=None,
        )

    async def select_page_aggregated(
        self,
        return_type: type[T],
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[T]:
        return await self._offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=aggregates,
            return_model=return_type,
            return_fields=None,
        )

    # ....................... #

    async def find_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CursorPage[R]:
        return await self._cursor_page(
            filters=filters,
            cursor=cursor,
            sorts=sorts,
            return_fields=None,
        )

    async def project_cursor(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CursorPage[JsonDict]:
        return await self._cursor_page(
            filters=filters,
            cursor=cursor,
            sorts=sorts,
            return_fields=tuple(fields),
        )

    @overload
    async def _cursor_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_fields: None,
    ) -> CursorPage[R]: ...

    @overload
    async def _cursor_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_fields: Sequence[str],
    ) -> CursorPage[JsonDict]: ...

    async def _cursor_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_fields: Sequence[str] | None,
    ) -> CursorPage[R] | CursorPage[JsonDict]:
        normalized = normalize_sorts_with_id(sorts)

        sort_keys = [k for k, _ in normalized]
        directions = [d for _, d in normalized]

        assert_cursor_projection_includes_sort_keys(
            return_fields=return_fields,
            sort_keys=sort_keys,
        )

        if self.enforce_primary_key_cursor_sort and (
            sort_keys != [ID_FIELD] or len(sort_keys) != 1
        ):
            raise CoreError(
                "find_cursor (strict) requires sorting only by primary key: "
                "omit ``sorts`` or pass a single {id: asc|desc}.",
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

        return CursorPage(
            hits=cast(list[R], list(page_raw)),
            next_cursor=next_tok,
            prev_cursor=prev_tok,
            has_more=has_more,
        )

    # ....................... #

    async def count(self, filters: QueryFilterExpression | None = None) -> int:  # type: ignore[valid-type]
        """Count documents matching the given filters.

        :param filters: Optional filter expression.
        """

        return await self.read_gw.count(filters)

    # ....................... #

    @overload
    async def create(self, dto: C, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def create(self, dto: C, *, return_new: Literal[False]) -> None: ...

    async def create(self, dto: C, *, return_new: bool = True) -> R | None:
        """Create a new document and populate the cache.

        :param dto: Creation payload.
        :returns: The created document as the read model.
        """

        w = self._require_write()

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
        """Bulk-create documents and populate the cache.

        :param dtos: Creation payloads.
        """

        w = self._require_write()

        if not dtos:
            if not return_new:
                return None

            return []

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
        """Update a document and refresh the cache.

        :param pk: Document primary key.
        :param dto: Update payload.
        :param rev: Expected revision for historical consistency validation.
        """

        w = self._require_write()

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
        """Bulk-update documents and refresh the cache.

        :param pks: Document primary keys.
        :param dtos: Update payloads matching *pks* by position.
        :param revs: Optional expected revisions for history validation.
        """

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

        count, domains = await w.update_matching(
            filters,
            dto,
            batch_size=self.eff_batch_size,
        )
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
                await self.project_many(
                    [ID_FIELD, REV_FIELD],
                    filters=chunk_filter,
                    pagination={"limit": eff_chunk},
                    sorts={ID_FIELD: "asc"},
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
        """Touch a document (bump revision) and refresh the cache.

        :param pk: Document primary key.
        """

        w = self._require_write()

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
        """Touch multiple documents and refresh the cache.

        :param pks: Document primary keys.
        """

        w = self._require_write()

        if not pks:
            if not return_new:
                return None

            return []

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
        """Hard-delete a document and evict it from the cache.

        :param pk: Document primary key.
        """

        w = self._require_write()

        await asyncio.gather(
            w.kill(pk),
            self.cache_coord.invalidate_keys_now(pk),
        )

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        """Hard-delete multiple documents and evict them from the cache.

        :param pks: Document primary keys.
        """

        w = self._require_write()

        if not pks:
            return

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
        """Soft-delete a document and refresh the cache.

        :param pk: Document primary key.
        :param rev: Expected revision for historical consistency validation.
        """

        w = self._require_write()

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
        """Soft-delete multiple documents and refresh the cache.

        :param pks: Document primary keys.
        :param revs: Optional expected revisions for history validation.
        """

        w = self._require_write()

        if not deletes:
            if not return_new:
                return None

            return []

        pks = [x[0] for x in deletes]
        revs = [x[1] for x in deletes]

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
        """Restore a soft-deleted document and refresh the cache.

        :param pk: Document primary key.
        :param rev: Expected revision for historical consistency validation.
        """

        w = self._require_write()

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
        """Restore multiple soft-deleted documents and refresh the cache.

        :param pks: Document primary keys.
        :param revs: Optional expected revisions for history validation.
        """

        w = self._require_write()

        if not restores:
            if not return_new:
                return None

            return []

        pks = [x[0] for x in restores]
        revs = [x[1] for x in restores]

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

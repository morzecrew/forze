"""In-memory document adapter."""

from __future__ import annotations

from typing import (
    Any,
    AsyncGenerator,
    Literal,
    Sequence,
    cast,
    final,
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
    RowLockMode,
    require_create_id,
    require_create_id_for_many,
)
from forze.application.contracts.querying import (
    AggregatesExpression,
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.integrations.document._limits import (
    DEFAULT_MAX_STREAM_PAGES,
    assert_cursor_advanced,
    check_page_limit,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow
from forze.application.contracts.codecs import default_model_codec
from forze.base.serialization import ModelCodec
from forze.domain.constants import ID_FIELD, REV_FIELD
from forze_mock.query._types import (
    C,
    D,
    R,
    T,
    U,
)
from forze_mock.query.cursors import (
    _mock_cursor_start_and_limit,  # type: ignore[reportPrivateUsage]
    _mock_cursor_tokens,  # type: ignore[reportPrivateUsage]
)
from forze_mock.query.matching import (
    _aggregate_docs,  # type: ignore[reportPrivateUsage]
    _match_filters,  # type: ignore[reportPrivateUsage]
    _project,  # type: ignore[reportPrivateUsage]
    _sort_docs,  # type: ignore[reportPrivateUsage]
)
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockDocumentAdapter(
    MockTenancyMixin,
    DocumentQueryPort[R],
    DocumentCommandPort[R, D, C, U],
):
    """In-memory document adapter with filter/sort/projection support."""

    spec: DocumentSpec[R, D, C, U]
    state: MockState
    namespace: str
    read_model: type[R]
    domain_model: type[D] | None = None

    # ....................... #

    def _store(self) -> dict[UUID, JsonDict]:
        ns = partition_namespace(self.require_tenant_if_aware(), self.namespace)
        with self.state.lock:
            return self.state.documents.setdefault(ns, {})

    def _doc_visible(self, doc: JsonDict) -> bool:
        if not self.tenant_aware:
            return True
        tenant_id = self.require_tenant_if_aware()
        doc_tid = doc.get("tenant_id")
        if doc_tid is None:
            return tenant_id is None
        return str(doc_tid) == str(tenant_id)

    # ....................... #

    def _read_codec(self) -> ModelCodec[R, Any]:
        return self.spec.resolved_codecs.read

    def _to_read(self, doc: JsonDict) -> R:
        return self._read_codec().decode_mapping(dict(doc))

    # ....................... #

    def _require_domain_model(self) -> type[D]:
        if self.domain_model is None:
            raise exc.internal("Write support requires a domain model")
        return self.domain_model

    # ....................... #

    def _domain_codec(self) -> ModelCodec[D, Any]:
        domain = self.spec.resolved_codecs.domain
        if domain is None:
            raise exc.internal("Domain codec is required when write is enabled")
        return domain

    def _create_codec(self) -> ModelCodec[D, Any]:
        create = self.spec.resolved_codecs.create
        if create is None:
            raise exc.internal("Create codec is required when write is enabled")
        return create

    def _patch_codec(self) -> ModelCodec[Any, Any]:
        codecs = self.spec.resolved_codecs
        if codecs.update is not None:
            return codecs.update
        if self.spec.write is not None:
            domain = codecs.domain
            if domain is None:
                raise exc.internal(
                    "Domain codec is required when update codec is not configured"
                )
            return domain
        return self._read_codec()

    def _to_domain(self, doc: JsonDict) -> D:
        return self._domain_codec().decode_mapping(dict(doc))

    # ....................... #

    def _ensure_exists(self, pk: UUID) -> JsonDict:
        store = self._store()

        if pk not in store or not self._doc_visible(store[pk]):
            raise exc.not_found(f"Document not found: {pk}")

        return store[pk]

    # ....................... #

    def _check_rev(self, current_rev: int, expected_rev: int | None) -> None:
        if expected_rev is None:
            return

        if expected_rev != current_rev:
            raise exc.concurrency("Revision conflict")

    # ....................... #

    def _to_read_or_projection(
        self,
        doc: JsonDict,
        return_fields: Sequence[str] | None,
    ) -> R | JsonDict:
        if return_fields is not None:
            return _project(doc, return_fields)
        return self._to_read(doc)

    # ....................... #

    async def get(
        self,
        pk: UUID,
        *,
        for_update: RowLockMode = False,
        skip_cache: bool = False,
    ) -> R:
        del for_update, skip_cache
        with self.state.lock:
            doc = dict(self._ensure_exists(pk))
        return self._to_read(doc)

    # ....................... #

    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        skip_cache: bool = False,
    ) -> Sequence[R]:
        del skip_cache

        with self.state.lock:
            store = self._store()
            missing = [pk for pk in pks if pk not in store]

            if missing:
                raise exc.not_found(f"Documents not found: {missing}")

            docs = [dict(store[pk]) for pk in pks]

        return [self._to_read(doc) for doc in docs]

    # ....................... #

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = False,
    ) -> R | None:
        del for_update

        page = await self.find_many(
            filters=filters,
            pagination={"limit": 1},
        )

        if not page.hits:
            return None

        return page.hits[0]

    async def project(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        fields: Sequence[str],
        *,
        for_update: RowLockMode = False,
    ) -> JsonDict | None:
        del for_update

        page = await self.project_many(
            tuple(fields),
            filters=filters,
            pagination={"limit": 1},
        )

        if not page.hits:
            return None

        return page.hits[0]

    async def select(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        return_type: type[T],
        *,
        for_update: RowLockMode = False,
    ) -> T | None:
        del for_update

        page = await self.select_many(
            return_type,
            filters=filters,
            pagination={"limit": 1},
        )

        if not page.hits:
            return None

        return page.hits[0]

    # ....................... #

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: None,
        return_type: None,
        return_fields: None,
    ) -> CountlessPage[R]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: None,
        return_type: None,
        return_fields: None,
    ) -> Page[R]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: None,
        return_type: None,
        return_fields: Sequence[str],
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: None,
        return_type: None,
        return_fields: Sequence[str],
    ) -> Page[JsonDict]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: None,
        return_type: type[T],
        return_fields: None,
    ) -> CountlessPage[T]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: None,
        return_type: type[T],
        return_fields: None,
    ) -> Page[T]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: AggregatesExpression,
        return_type: None,
        return_fields: None,
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: AggregatesExpression,
        return_type: None,
        return_fields: None,
    ) -> Page[JsonDict]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[False],
        aggregates: AggregatesExpression,
        return_type: type[T],
        return_fields: None,
    ) -> CountlessPage[T]: ...

    @overload
    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: Literal[True],
        aggregates: AggregatesExpression,
        return_type: type[T],
        return_fields: None,
    ) -> Page[T]: ...

    async def _mock_offset_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_count: bool,
        aggregates: AggregatesExpression | None,
        return_type: type[Any] | None,
        return_fields: Sequence[str] | None,
    ) -> Any:
        if aggregates is not None and return_fields is not None:
            raise exc.internal("Aggregates cannot be combined with return_fields")

        with self.state.lock:
            docs = [
                dict(doc) for doc in self._store().values() if self._doc_visible(doc)
            ]

        filtered = [doc for doc in docs if _match_filters(doc, filters)]
        rows: list[Any]

        if aggregates is not None:
            aggregate_rows = _aggregate_docs(filtered, aggregates)
            total = len(aggregate_rows)
            ordered_rows = _sort_docs(aggregate_rows, sorts)
            rows = (
                default_model_codec(return_type).decode_mapping_many(ordered_rows)
                if return_type is not None
                else ordered_rows
            )
        else:
            total = len(filtered)
            ordered_docs = _sort_docs(filtered, sorts)
            if return_type is not None:
                projected = [
                    self._to_read_or_projection(doc, return_fields)
                    for doc in ordered_docs
                ]
                dict_rows: list[dict[str, Any]] = []

                for row in projected:
                    if isinstance(row, BaseModel):
                        dict_rows.append(row.model_dump(mode="python"))
                    else:
                        dict_rows.append(dict(row))

                rows = default_model_codec(return_type).decode_mapping_many(dict_rows)
            else:
                rows = [
                    self._to_read_or_projection(doc, return_fields)
                    for doc in ordered_docs
                ]

        pagination = pagination or {}
        limit = pagination.get("limit")
        offset = pagination.get("offset")

        if offset:
            rows = rows[offset:]

        if limit is not None:
            rows = rows[:limit]

        if return_count:
            return page_from_limit_offset(
                cast(Any, rows),
                pagination,
                total=total,
            )
        return page_from_limit_offset(cast(Any, rows), pagination, total=None)

    async def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[R]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=None,
            return_type=None,
            return_fields=None,
        )

    async def project_many(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[JsonDict]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=None,
            return_type=None,
            return_fields=tuple(fields),
        )

    async def select_many(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[T]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=None,
            return_type=return_type,
            return_fields=None,
        )

    async def find_page(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[R]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=None,
            return_type=None,
            return_fields=None,
        )

    async def project_page(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[JsonDict]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=None,
            return_type=None,
            return_fields=tuple(fields),
        )

    async def select_page(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[T]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=None,
            return_type=return_type,
            return_fields=None,
        )

    async def aggregate_many(
        self,
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[JsonDict]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=aggregates,
            return_type=None,
            return_fields=None,
        )

    async def aggregate_page(
        self,
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[JsonDict]:
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=aggregates,
            return_type=None,
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
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=False,
            aggregates=aggregates,
            return_type=return_type,
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
        return await self._mock_offset_page(
            filters=filters,
            pagination=pagination,
            sorts=sorts,
            return_count=True,
            aggregates=aggregates,
            return_type=return_type,
            return_fields=None,
        )

    # ....................... #

    async def find_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CursorPage[R]:
        return await self._mock_cursor_page(
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
        return await self._mock_cursor_page(
            filters=filters,
            cursor=cursor,
            sorts=sorts,
            return_fields=tuple(fields),
        )

    # ....................... #

    async def select_cursor(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CursorPage[T]:
        page = await self.find_cursor(filters=filters, cursor=cursor, sorts=sorts)
        return CursorPage(
            hits=[default_model_codec(return_type).decode_mapping(hit.model_dump(mode="json")) for hit in page.hits],  # type: ignore[union-attr]
            next_cursor=page.next_cursor,
            prev_cursor=page.prev_cursor,
            has_more=page.has_more,
        )

    # ....................... #

    async def find_stream(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        sorts: QuerySortExpression | None = None,
        chunk_size: int = 500,
        max_stream_pages: int | None = DEFAULT_MAX_STREAM_PAGES,
    ) -> AsyncGenerator[Sequence[R]]:
        cursor: CursorPaginationExpression | None = {"limit": chunk_size}
        page_num = 0
        prev_cursor: str | None = None

        while True:
            check_page_limit(
                pages=page_num,
                max_pages=max_stream_pages,
                label="Mock find_stream",
            )
            page = await self.find_cursor(filters=filters, cursor=cursor, sorts=sorts)

            if not page.hits:
                break

            yield page.hits

            if not page.has_more or page.next_cursor is None:
                break

            assert_cursor_advanced(
                prev_cursor=prev_cursor,
                next_cursor=page.next_cursor,
            )
            prev_cursor = page.next_cursor
            cursor = {"limit": chunk_size, "after": page.next_cursor}
            page_num += 1

    # ....................... #

    async def project_stream(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        sorts: QuerySortExpression | None = None,
        chunk_size: int = 500,
        max_stream_pages: int | None = DEFAULT_MAX_STREAM_PAGES,
    ) -> AsyncGenerator[Sequence[JsonDict]]:
        cursor: CursorPaginationExpression | None = {"limit": chunk_size}
        page_num = 0
        prev_cursor: str | None = None

        while True:
            check_page_limit(
                pages=page_num,
                max_pages=max_stream_pages,
                label="Mock project_stream",
            )
            page = await self.project_cursor(
                fields,
                filters=filters,
                cursor=cursor,
                sorts=sorts,
            )

            if not page.hits:
                break

            yield page.hits

            if not page.has_more or page.next_cursor is None:
                break

            assert_cursor_advanced(
                prev_cursor=prev_cursor,
                next_cursor=page.next_cursor,
            )
            prev_cursor = page.next_cursor
            cursor = {"limit": chunk_size, "after": page.next_cursor}
            page_num += 1

    # ....................... #

    async def select_stream(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        sorts: QuerySortExpression | None = None,
        chunk_size: int = 500,
        max_stream_pages: int | None = DEFAULT_MAX_STREAM_PAGES,
    ) -> AsyncGenerator[Sequence[T]]:
        cursor: CursorPaginationExpression | None = {"limit": chunk_size}
        page_num = 0
        prev_cursor: str | None = None

        while True:
            check_page_limit(
                pages=page_num,
                max_pages=max_stream_pages,
                label="Mock select_stream",
            )
            page = await self.select_cursor(
                return_type,
                filters=filters,
                cursor=cursor,
                sorts=sorts,
            )

            if not page.hits:
                break

            yield page.hits

            if not page.has_more or page.next_cursor is None:
                break

            assert_cursor_advanced(
                prev_cursor=prev_cursor,
                next_cursor=page.next_cursor,
            )
            prev_cursor = page.next_cursor
            cursor = {"limit": chunk_size, "after": page.next_cursor}
            page_num += 1

    @overload
    async def _mock_cursor_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_fields: None,
    ) -> CursorPage[R]: ...

    @overload
    async def _mock_cursor_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_fields: Sequence[str],
    ) -> CursorPage[JsonDict]: ...

    async def _mock_cursor_page(
        self,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None,
        sorts: QuerySortExpression | None,
        return_fields: Sequence[str] | None,
    ) -> CursorPage[R] | CursorPage[JsonDict]:
        with self.state.lock:
            docs = [
                dict(doc) for doc in self._store().values() if self._doc_visible(doc)
            ]

        filtered = [doc for doc in docs if _match_filters(doc, filters)]
        ordered = _sort_docs(filtered, sorts)
        start, lim = _mock_cursor_start_and_limit(cursor)
        window = ordered[start : start + lim + 1]
        has_more = len(window) > lim
        page_docs = window[:lim]
        next_c, prev_c = _mock_cursor_tokens(start, len(page_docs), has_more=has_more)
        if return_fields is not None:
            out_raw = [
                self._to_read_or_projection(doc, return_fields) for doc in page_docs
            ]
            return CursorPage(
                hits=cast(list[JsonDict], out_raw),
                next_cursor=next_c,
                prev_cursor=prev_c,
                has_more=has_more,
            )
        out_typed = [self._to_read_or_projection(doc, None) for doc in page_docs]
        return CursorPage(
            hits=cast(list[R], out_typed),
            next_cursor=next_c,
            prev_cursor=prev_c,
            has_more=has_more,
        )

    # ....................... #

    async def count(self, filters: QueryFilterExpression | None = None) -> int:  # type: ignore[valid-type, return-value]
        with self.state.lock:
            docs = [
                dict(doc) for doc in self._store().values() if self._doc_visible(doc)
            ]
        return sum(1 for doc in docs if _match_filters(doc, filters))

    # ....................... #

    @overload
    async def create(self, dto: C, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def create(self, dto: C, *, return_new: Literal[False]) -> None: ...

    async def create(self, dto: C, *, return_new: bool = True) -> R | None:
        self._require_domain_model()
        domain = self._create_codec().transform(dto)
        serialized = self._domain_codec().encode_persistence_mapping(domain)

        if self.tenant_aware:
            tid = self.require_tenant_if_aware()
            if tid is not None:
                serialized = dict(serialized)
                serialized["tenant_id"] = str(tid)
        with self.state.lock:
            store = self._store()
            store[domain.id] = serialized

        if not return_new:
            return None
        return self._to_read(serialized)

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
        if not dtos:
            if not return_new:
                return None

            return []
        if return_new:
            return [await self.create(dto, return_new=True) for dto in dtos]
        for dto in dtos:
            await self.create(dto, return_new=False)
        return None

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
        require_create_id(dto)

        self._require_domain_model()
        domain = self._create_codec().transform(dto)

        with self.state.lock:
            store = self._store()
            if domain.id in store:
                raw = dict(store[domain.id])
            else:
                serialized = self._domain_codec().encode_persistence_mapping(domain)
                store[domain.id] = serialized
                raw = serialized
        if not return_new:
            return None
        return self._to_read(raw)

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
        if not dtos:
            if not return_new:
                return None
            return []

        require_create_id_for_many(dtos)

        if return_new:
            return [await self.ensure(dto, return_new=True) for dto in dtos]
        for dto in dtos:
            await self.ensure(dto, return_new=False)
        return None

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
        require_create_id(create_dto)

        self._require_domain_model()
        domain = self._create_codec().transform(create_dto)
        with self.state.lock:
            if domain.id in self._store():
                rev = self._to_domain(dict(self._store()[domain.id])).rev
            else:
                rev = None
        if rev is not None:
            return await self.update(  # type: ignore[call-overload]
                domain.id,
                rev,
                update_dto,
                return_new=return_new,
            )
        return await self.create(create_dto, return_new=return_new)  # type: ignore[call-overload]

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
        if not pairs:
            if not return_new:
                return None
            return []

        require_create_id_for_many(pairs)

        if return_new:
            return [await self.upsert(c, u, return_new=True) for c, u in pairs]

        for c, u in pairs:
            await self.upsert(c, u, return_new=False)

        return None

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
        patch = self._patch_codec().encode_persistence_mapping(
            cast(Any, dto),
            exclude={"unset": True},
        )

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)

            updated, diff = current.update(patch)
            if diff:
                updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)

            serialized = self._domain_codec().encode_persistence_mapping(updated)
            self._store()[pk] = serialized

            if diff:
                write_diff: JsonDict = {**dict(diff), REV_FIELD: updated.rev}
            else:
                write_diff = {}

        if not return_new:
            if return_diff:
                return write_diff

            return None

        read_result = self._to_read(serialized)

        if return_diff:
            return read_result, write_diff

        return read_result

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
        if not updates:
            if not return_new:
                return None

            return []

        pks = [u[0] for u in updates]
        if len(set(pks)) != len(pks):
            raise exc.internal("Primary keys must be unique")

        if return_new:
            if return_diff:
                return [
                    await self.update(pk, r, dto, return_new=True, return_diff=True)
                    for pk, r, dto in updates
                ]

            return [
                await self.update(pk, r, dto, return_new=True, return_diff=False)
                for pk, r, dto in updates
            ]

        if return_diff:
            return [
                await self.update(pk, r, dto, return_new=False, return_diff=True)
                for pk, r, dto in updates
            ]

        for pk, r, dto in updates:
            await self.update(pk, r, dto, return_new=False)

        return None

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
        if not self.spec.supports_update():
            raise exc.internal("Update command type is not supported for this model")

        patch = self._patch_codec().encode_persistence_mapping(
            cast(Any, dto),
            exclude={"unset": True},
        )

        if not patch:
            return [] if return_new else 0

        results: list[R] = []
        n = 0

        with self.state.lock:
            store = self._store()
            for pk, raw in list(store.items()):
                if not _match_filters(raw, filters):
                    continue

                current = self._to_domain(dict(raw))
                updated, diff = current.update(patch)

                if not diff:
                    continue

                updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)
                serialized = self._domain_codec().encode_persistence_mapping(updated)
                store[pk] = serialized
                n += 1

                if return_new:
                    results.append(self._to_read(serialized))

        if return_new:
            return results

        return n

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
        if not self.spec.supports_update():
            raise exc.internal("Update command type is not supported for this model")

        eff = 200 if chunk_size is None else chunk_size
        if eff < 1:
            raise exc.internal("chunk_size must be positive")

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
                        {"$values": {ID_FIELD: {"$gt": last_id}}},
                    ]
                }
            )
            page = await self.project_many(
                [ID_FIELD, REV_FIELD],
                filters=chunk_filter,
                pagination={"limit": eff},
                sorts={ID_FIELD: "asc"},
            )
            rows = page.hits
            if not rows:
                break

            updates = [(UUID(str(r[ID_FIELD])), int(r[REV_FIELD]), dto) for r in rows]

            if return_new:
                out.extend(
                    await self.update_many(updates, return_new=True),
                )
            else:
                await self.update_many(updates, return_new=False)

            n_total += len(rows)
            last_id = UUID(str(rows[-1][ID_FIELD]))

            if len(rows) < eff:
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
        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            updated, _ = current.touch()
            updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)
            serialized = self._domain_codec().encode_persistence_mapping(updated)
            self._store()[pk] = serialized

        if not return_new:
            return None
        return self._to_read(serialized)

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
        if not pks:
            if not return_new:
                return None

            return []
        if len(set(pks)) != len(pks):
            raise exc.internal("Primary keys must be unique")
        if return_new:
            return [await self.touch(pk, return_new=True) for pk in pks]
        for pk in pks:
            await self.touch(pk, return_new=False)
        return None

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        with self.state.lock:
            _ = self._ensure_exists(pk)
            del self._store()[pk]

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        if len(set(pks)) != len(pks):
            raise exc.internal("Primary keys must be unique")
        for pk in pks:
            await self.kill(pk)

    # ....................... #

    def _supports_soft_delete(self) -> bool:
        if self.domain_model is None:
            return False
        return "is_deleted" in getattr(self.domain_model, "model_fields", {})

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
        if not self._supports_soft_delete():
            raise exc.internal("Soft deletion is not supported for this model")

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)
            if cast(Any, current).is_deleted:
                serialized = self._domain_codec().encode_persistence_mapping(current)
                self._store()[pk] = serialized
            else:
                updated = current.model_copy(
                    update={
                        "is_deleted": True,
                        "last_update_at": utcnow(),
                        "rev": current.rev + 1,
                    },
                    deep=True,
                )
                serialized = self._domain_codec().encode_persistence_mapping(updated)
                self._store()[pk] = serialized

        if not return_new:
            return None
        return self._to_read(serialized)

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
        if not self._supports_soft_delete():
            raise exc.internal("Soft deletion is not supported for this model")
        if not deletes:
            if not return_new:
                return None

            return []
        if return_new:
            return [await self.delete(pk, r, return_new=True) for pk, r in deletes]
        for pk, r in deletes:
            await self.delete(pk, r, return_new=False)
        return None

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
        if not self._supports_soft_delete():
            raise exc.internal("Soft deletion is not supported for this model")
        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)
            if not cast(Any, current).is_deleted:
                serialized = self._domain_codec().encode_persistence_mapping(current)
                self._store()[pk] = serialized
            else:
                updated = current.model_copy(
                    update={
                        "is_deleted": False,
                        "last_update_at": utcnow(),
                        "rev": current.rev + 1,
                    },
                    deep=True,
                )
                serialized = self._domain_codec().encode_persistence_mapping(updated)
                self._store()[pk] = serialized

        if not return_new:
            return None
        return self._to_read(serialized)

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
        if not self._supports_soft_delete():
            raise exc.internal("Soft deletion is not supported for this model")
        if not restores:
            if not return_new:
                return None

            return []
        if return_new:
            return [await self.restore(pk, r, return_new=True) for pk, r in restores]
        for pk, r in restores:
            await self.restore(pk, r, return_new=False)
        return None

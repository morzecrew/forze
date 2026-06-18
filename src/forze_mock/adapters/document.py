"""In-memory document adapter."""

from __future__ import annotations

from collections.abc import Callable
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

from forze.application.contracts.domain import DomainEventDispatcherPort

from forze.application.contracts.base import (
    CountlessPage,
    CursorPage,
    Page,
    page_from_limit_offset,
)
from forze.application.contracts.codecs import default_model_codec
from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
    RowLockMode,
)
from forze.application.contracts.querying import (
    AggregatesExpression,
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QuerySortExpression,
    assert_cursor_projection_includes_sort_keys,
    normalize_sorts_for_keyset,
    read_fields_for_model,
    resolve_effective_sorts,
    validate_query_field_types,
)
from forze.application.integrations.document._limits import (
    DEFAULT_MAX_STREAM_PAGES,
    assert_cursor_advanced,
    check_page_limit,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec
from forze_mock.adapters._journal import JournalingStore
from forze_mock.adapters._mvcc import current_mvcc_tx
from forze_mock.query._types import (
    C,
    D,
    R,
    T,
    U,
)
from forze_mock.query.cursors import (
    _mock_keyset_window,  # type: ignore[reportPrivateUsage]
)
from forze_mock.query.matching import (
    _aggregate_docs,  # type: ignore[reportPrivateUsage]
    _match_filters,  # type: ignore[reportPrivateUsage]
    _project,  # type: ignore[reportPrivateUsage]
    _sort_docs,  # type: ignore[reportPrivateUsage]
)
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace

from ._document_command import MockDocumentCommandMixin


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockDocumentAdapter(  # pyright: ignore[reportIncompatibleVariableOverride]
    MockTenancyMixin,
    MockDocumentCommandMixin[R, D, C, U],
    DocumentQueryPort[R],
    DocumentCommandPort[R, D, C, U],
):
    """In-memory document adapter with filter/sort/projection support.

    Query/read operations live here; write (command) operations come from
    :class:`~forze_mock.adapters._document_command.MockDocumentCommandMixin`.
    """

    spec: DocumentSpec[R, D, C, U]
    state: MockState
    namespace: str
    read_model: type[R]
    domain_model: type[D] | None = None
    dispatcher_provider: Callable[[], DomainEventDispatcherPort | None] = attrs.field(
        default=lambda: None
    )

    # ....................... #

    def _store(self) -> dict[UUID, JsonDict]:
        ns = partition_namespace(self.require_tenant_if_aware(), self.namespace)
        with self.state.lock:
            store = self.state.documents.get(ns)
            if not isinstance(store, JournalingStore):
                # Make the namespace store journaling so writes are atomic under a
                # transaction (coercing a plain dict left by setup/snapshot, in place).
                store = JournalingStore(store or {})
                self.state.documents[ns] = store

            # Under a snapshot/serializable transaction, reads and writes route through the
            # MVCC overlay (buffered writes + as-of-begin snapshot reads) instead of the live
            # store; read-committed (the default) uses the write-through store directly.
            mvcc = current_mvcc_tx()
            if mvcc is not None:
                return cast("dict[UUID, JsonDict]", mvcc.view(ns, store))

            return store

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
            # Mirror the real backends: a raw projection of an encrypted/searchable
            # field is decrypted before projecting (full reads already decrypt via the
            # read codec). No-op for plain codecs. Synchronous: the mock keyring cache
            # is seeded at encrypt time / via warm(), so no async pre-pass is needed.
            decrypt = getattr(self._read_codec(), "decrypt_mapping", None)
            source = decrypt(dict(doc)) if decrypt is not None else doc
            return _project(source, return_fields)
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

    def _validate_filter_types(
        self,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    ) -> None:
        """Operator/field-type validation, mirroring the real gateways' ``compile_filters``.

        Keeps dev (mock) and prod (Postgres/Mongo) symmetric: a type-incompatible filter
        (e.g. ``$like`` on a number) raises the same clean ``query_operator_type_mismatch``
        precondition here instead of silently matching nothing.
        """

        if filters is None:
            return

        expr = QueryFilterExpressionParser.parse(filters)
        validate_query_field_types(expr, self.read_model)

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

        self._validate_filter_types(filters)

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
        # Python-mode dump keeps UUID/datetime objects intact, matching the
        # offset select path (audit: cursor vs offset value-type parity).
        return CursorPage(
            hits=[default_model_codec(return_type).decode_mapping(hit.model_dump(mode="python")) for hit in page.hits],  # type: ignore[union-attr]
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
        # Keyset pagination on the shared cursor-token machinery, mirroring
        # the real document gateways: resolve the effective sort (+ id
        # tie-breaker), seek past the token's sort values, and mint the next
        # cursor from the last returned row.
        self._validate_filter_types(filters)

        read_fields = read_fields_for_model(self.read_model)
        effective = resolve_effective_sorts(
            sorts=sorts,
            default_sort=self.spec.default_sort,
            read_fields=read_fields,
            spec_name=self.spec.name,
        )
        normalized = normalize_sorts_for_keyset(effective, read_fields=read_fields)
        sort_keys = [k for k, _, _ in normalized]
        directions = [d for _, d, _ in normalized]
        nulls = [n for _, _, n in normalized]

        assert_cursor_projection_includes_sort_keys(
            return_fields=return_fields,
            sort_keys=sort_keys,
        )

        with self.state.lock:
            docs = [
                dict(doc) for doc in self._store().values() if self._doc_visible(doc)
            ]

        filtered = [doc for doc in docs if _match_filters(doc, filters)]
        page_docs, has_more, next_c, prev_c = _mock_keyset_window(
            filtered,
            cursor=cursor,
            sort_keys=sort_keys,
            directions=directions,
            nulls=nulls,
        )
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
        self._validate_filter_types(filters)

        with self.state.lock:
            docs = [
                dict(doc) for doc in self._store().values() if self._doc_visible(doc)
            ]
        return sum(1 for doc in docs if _match_filters(doc, filters))

"""Internal offset/cursor pagination for document queries."""

from typing import Any, AsyncGenerator, Generic, Sequence, cast

import attrs

from forze.application.contracts.base import CursorPage, page_from_limit_offset
from forze.application.contracts.document import DocumentReadGatewayPort
from forze.application.contracts.querying import (
    AggregatesExpression,
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    assemble_keyset_cursor_page,
    assert_cursor_projection_includes_sort_keys,
    normalize_sorts_for_keyset,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.domain.constants import ID_FIELD

from ._limits import assert_cursor_advanced, check_page_limit
from ._types import R, T

# ----------------------- #


@attrs.frozen
class OffsetQuery:
    return_count: bool
    aggregates: AggregatesExpression | None
    return_model: type[Any] | None
    return_fields: Sequence[str] | None


# ....................... #


@attrs.frozen
class CursorQuery:
    return_model: type[Any] | None
    return_fields: Sequence[str] | None


# ....................... #


@attrs.frozen
class StreamQuery:
    return_model: type[Any] | None
    return_fields: Sequence[str] | None


# ....................... #


class DocumentPaginationMixin(Generic[R]):
    """Offset/cursor paging helpers for :class:`~.adapter.DocumentAdapter`."""

    read_gw: DocumentReadGatewayPort[R]
    enforce_primary_key_cursor_sort: bool

    # ....................... #

    @property
    def _read_fields(self) -> frozenset[str]: ...  # type: ignore[empty-body]

    @property
    def eff_batch_size(self) -> int: ...  # type: ignore[empty-body]

    @property
    def max_scan_pages(self) -> int | None: ...  # type: ignore[empty-body]

    @property
    def max_stream_pages(self) -> int | None: ...  # type: ignore[empty-body]

    def _eff_stream_chunk_size(self, chunk_size: int) -> int: ...  # type: ignore[empty-body]

    def _resolve_sorts(  # type: ignore[empty-body]
        self,
        sorts: QuerySortExpression | None,
    ) -> QuerySortExpression: ...

    async def _offset_page(
        self,
        query: OffsetQuery,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
    ) -> Any:
        if query.aggregates is not None and query.return_fields is not None:
            raise exc.precondition("Aggregates cannot be combined with return_fields")

        pagination = pagination or {}
        parsed_filters = self.read_gw.compile_filters(filters)
        cnt = 0
        if query.return_count:
            cnt = (
                await self.read_gw.count_aggregates(
                    filters,
                    aggregates=query.aggregates,
                    parsed=parsed_filters,
                )
                if query.aggregates is not None
                else await self.read_gw.count(filters, parsed=parsed_filters)
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

        if limit is None:
            chunk = self.eff_batch_size
            off = 0 if offset is None else offset
            sorts_for_scan = self._resolve_sorts(sorts)
            res = []
            page_num = 0

            while True:
                check_page_limit(
                    pages=page_num,
                    max_pages=self.max_scan_pages,
                    label="Document offset scan",
                )

                if query.aggregates is not None:
                    batch = await self.read_gw.find_many_aggregates(
                        filters=filters,
                        limit=chunk,
                        offset=off,
                        sorts=sorts_for_scan,
                        aggregates=query.aggregates,
                        return_model=query.return_model,
                        parsed=parsed_filters,
                    )
                else:
                    batch = await self.read_gw.find_many(  # type: ignore[misc]
                        filters=filters,
                        limit=chunk,
                        offset=off,
                        sorts=sorts_for_scan,
                        return_model=query.return_model,  # type: ignore[arg-type]
                        return_fields=query.return_fields,  # type: ignore[arg-type]
                        parsed=parsed_filters,
                    )

                res.extend(batch)  # type: ignore[arg-type]

                if len(batch) < chunk:  # type: ignore[arg-type]
                    break

                off += chunk
                page_num += 1

        elif query.aggregates is not None:
            res = await self.read_gw.find_many_aggregates(
                filters=filters,
                limit=limit,
                offset=offset,
                sorts=sorts,
                aggregates=query.aggregates,
                return_model=query.return_model,
                parsed=parsed_filters,
            )
        else:
            res = await self.read_gw.find_many(  # type: ignore[misc]
                filters=filters,
                limit=limit,
                offset=offset,
                sorts=sorts,
                return_model=query.return_model,  # type: ignore[arg-type]
                return_fields=query.return_fields,  # type: ignore[arg-type]
                parsed=parsed_filters,
            )

        return page_from_limit_offset(
            list(res),  # type: ignore[arg-type]
            pagination,
            total=cnt if query.return_count else None,
        )

    async def _cursor_page(
        self,
        query: CursorQuery,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None,
        sorts: QuerySortExpression | None,
    ) -> CursorPage[R] | CursorPage[JsonDict] | CursorPage[T]:
        if query.return_model is not None and query.return_fields is not None:
            raise exc.precondition("return_model and return_fields cannot be combined")

        effective = self._resolve_sorts(sorts)
        normalized = normalize_sorts_for_keyset(
            effective,
            read_fields=self._read_fields,
            model=self.read_gw.model_type,
        )

        sort_keys = [k for k, _, _ in normalized]
        directions = [d for _, d, _ in normalized]
        nulls = [n for _, _, n in normalized]

        assert_cursor_projection_includes_sort_keys(
            return_fields=query.return_fields,
            sort_keys=sort_keys,
        )

        if self.enforce_primary_key_cursor_sort and (
            sort_keys != [ID_FIELD] or len(sort_keys) != 1
        ):
            raise exc.precondition(
                "find_cursor (strict) requires sorting only by primary key: "
                "omit ``sorts`` or pass a single {id: asc|desc}.",
            )

        raw = await self.read_gw.find_many_with_cursor(  # type: ignore[call-overload, misc]
            filters,
            cursor=cursor,
            sorts=effective,
            return_model=query.return_model,  # type: ignore[arg-type]
            return_fields=query.return_fields,  # type: ignore[typeddict, arg-type, misc]
        )

        def _dump(o: R | JsonDict | T) -> JsonDict:
            if isinstance(o, dict):
                return o

            return o.model_dump(mode="json")  # type: ignore[union-attr, err]

        page_raw, has_more, next_tok, prev_tok = assemble_keyset_cursor_page(
            raw,
            cursor=cursor,
            sort_keys=sort_keys,
            directions=directions,
            nulls=nulls,
            dump_row=_dump,
        )

        if query.return_model is not None:
            return CursorPage(
                hits=cast(list[T], list(page_raw)),
                next_cursor=next_tok,
                prev_cursor=prev_tok,
                has_more=has_more,
            )

        if query.return_fields is not None:
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

    async def _stream(
        self,
        query: StreamQuery,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None,
        chunk_size: int,
    ) -> AsyncGenerator[Sequence[R] | Sequence[JsonDict] | Sequence[T]]:
        eff = self._eff_stream_chunk_size(chunk_size)
        cursor: CursorPaginationExpression = {"limit": eff}
        page: CursorPage[R] | CursorPage[JsonDict] | CursorPage[T]
        page_num = 0
        prev_cursor: str | None = None

        while True:
            check_page_limit(
                pages=page_num,
                max_pages=self.max_stream_pages,
                label="Document cursor stream",
            )

            if query.return_model is not None:
                page = await self._cursor_page(
                    CursorQuery(return_model=query.return_model, return_fields=None),
                    filters=filters,
                    cursor=cursor,
                    sorts=sorts,
                )
            elif query.return_fields is not None:
                page = await self._cursor_page(
                    CursorQuery(return_model=None, return_fields=query.return_fields),
                    filters=filters,
                    cursor=cursor,
                    sorts=sorts,
                )
            else:
                page = await self._cursor_page(
                    CursorQuery(return_model=None, return_fields=None),
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
            cursor = {"limit": eff, "after": page.next_cursor}
            page_num += 1

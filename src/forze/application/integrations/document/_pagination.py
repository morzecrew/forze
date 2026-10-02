"""Internal offset/cursor pagination for document queries."""

from collections.abc import AsyncGenerator, Mapping, Sequence
from typing import Any, Generic, cast, get_args

import attrs
from pydantic import BaseModel
from pydantic_core import to_jsonable_python

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
    with_id_tiebreaker,
)
from forze.application.contracts.querying.pagination.cursor_page import (
    _sort_key_in_projection,  # pyright: ignore[reportPrivateUsage]
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.domain.constants import ID_FIELD

from ..persistence import document_cursor_binding
from ._limits import assert_cursor_advanced, check_page_limit
from ._types import R

# ----------------------- #


def _model_in(annotation: Any) -> Any:
    """The model class *annotation* holds, through ``Optional`` and unions; else *annotation*."""

    for candidate in (annotation, *get_args(annotation)):
        if isinstance(candidate, type) and issubclass(candidate, BaseModel):
            return candidate

    return annotation


def _model_carries(model: type[BaseModel], key: str) -> bool:
    """Whether every segment of the sort *key* is a field of *model* and its nested models.

    Past the last model on the path the value is a mapping or scalar the backend returns as
    stored, so a missing segment there is a missing stored value, which sorts as null.
    """

    node: Any = model

    for part in key.split("."):
        if not (isinstance(node, type) and issubclass(node, BaseModel)):
            return True

        field = node.model_fields.get(part)

        if field is None:
            return False

        node = _model_in(field.annotation)

    return True


def _seek_values(row: BaseModel, sort_keys: Sequence[str]) -> JsonDict:
    """The sort keys' values on *row*, nested the way a cursor token reads them.

    Read off the model's attributes rather than its dump: a dump can leave a key out
    (``exclude``), rename it (an alias) or rewrite it (a serializer), and a token built from it
    would seek from the wrong place, dropping rows without an error.

    :raises CoreException: ``precondition`` when a model on a key's path has no field for it.
    """

    out: JsonDict = {}

    for key in sort_keys:
        parts = key.split(".")
        node: Any = row

        for part in parts:
            if isinstance(node, BaseModel):
                if part not in type(node).model_fields:
                    raise exc.precondition(
                        f"The returned model does not carry sort key {key!r}, so a cursor "
                        "cannot continue from it; return a model that holds the key.",
                    )

                node = getattr(node, part)

            elif isinstance(node, Mapping):
                node = cast(Mapping[str, Any], node).get(part)

            else:
                # A null parent: the key reads as null, which is how it sorts.
                node = None
                break

        target = out

        for part in parts[:-1]:
            nested = target.get(part)

            if not isinstance(nested, dict):
                nested = target[part] = {}

            target = cast(JsonDict, nested)

        target[parts[-1]] = to_jsonable_python(node)

    return out


# ....................... #


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

    # ....................... #

    @property
    def _sealed_fields(self) -> frozenset[str]:
        """The read gateway's ciphertext-at-rest fields, refused as cursor sort keys.

        Read off the gateway rather than declared on the port: ``DocumentReadGatewayPort`` is a
        published protocol, and a third-party gateway that predates the field would otherwise stop
        satisfying it. Same duck-typed shape the persistence gateway mixins use for
        ``lenient_read_fields``; a gateway without one simply seals nothing.
        """

        return getattr(self.read_gw, "sealed_fields", frozenset())

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

        if limit is None and query.aggregates is None:
            scan_sorts = with_id_tiebreaker(
                self._resolve_sorts(sorts), read_fields=self._read_fields
            )

            if self._seekable(query, scan_sorts):
                skip = offset or 0

                if skip < 0:
                    # A slice would count from the end and return the last rows.
                    raise exc.precondition("Pagination offset must not be negative.")

                res = await self._keyset_scan(query, filters=filters, sorts=scan_sorts)
                res = res[skip:]

            else:
                res = await self._offset_scan(
                    query,
                    filters=filters,
                    sorts=scan_sorts,
                    offset=offset,
                    parsed_filters=parsed_filters,
                )

        elif limit is None:
            res = await self._offset_scan(
                query,
                filters=filters,
                sorts=self._resolve_sorts(sorts),
                offset=offset,
                parsed_filters=parsed_filters,
            )

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

    def _seekable(self, query: OffsetQuery, sorts: QuerySortExpression) -> bool:
        """Whether a read with no limit can seek past each batch instead of offsetting.

        Seeking needs a unique last key, ``id``, and every sort key's value in the rows it
        returns, since the next batch starts after the last row's values. A projection that
        leaves a key out is paged by offset, as is any sort but ``id`` alone where the cursor
        seeks on nothing else — a strict primary-key cursor, or a gateway declaring
        ``cursor_sorts_by_id_only``.
        """

        keys = list(sorts)

        if keys[-1] != ID_FIELD:
            return False

        id_only = self.enforce_primary_key_cursor_sort or (
            getattr(self.read_gw, "cursor_sorts_by_id_only", False) is True
        )

        if id_only and keys != [ID_FIELD]:
            return False

        if query.return_model is not None:
            return all(_model_carries(query.return_model, k) for k in keys)

        projection = query.return_fields

        return projection is None or all(_sort_key_in_projection(k, projection) for k in keys)

    # ....................... #

    async def _keyset_scan(
        self,
        query: OffsetQuery,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        sorts: QuerySortExpression,
    ) -> list[Any]:
        """Every row, batch by batch, each batch seeking past the last row of the one before."""

        rows: list[Any] = []

        async for batch in self._keyset_batches(
            CursorQuery(return_model=query.return_model, return_fields=query.return_fields),
            filters=filters,
            sorts=sorts,
            chunk=self.eff_batch_size,
            max_pages=self.max_scan_pages,
            label="Document scan",
        ):
            rows.extend(batch)

        return rows

    # ....................... #

    async def _offset_scan(
        self,
        query: OffsetQuery,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        sorts: QuerySortExpression,
        offset: int | None,
        parsed_filters: Any,
    ) -> list[Any]:
        """Every row, batch by batch at growing offsets — for reads that cannot seek."""

        chunk = self.eff_batch_size
        off = 0 if offset is None else offset
        res: list[Any] = []
        page_num = 0

        while True:
            check_page_limit(
                pages=page_num,
                max_pages=self.max_scan_pages,
                label="Document scan",
            )

            if query.aggregates is not None:
                batch = await self.read_gw.find_many_aggregates(
                    filters=filters,
                    limit=chunk,
                    offset=off,
                    sorts=sorts,
                    aggregates=query.aggregates,
                    return_model=query.return_model,
                    parsed=parsed_filters,
                )
            else:
                batch = await self.read_gw.find_many(  # type: ignore[misc]
                    filters=filters,
                    limit=chunk,
                    offset=off,
                    sorts=sorts,
                    return_model=query.return_model,  # type: ignore[arg-type]
                    return_fields=query.return_fields,  # type: ignore[arg-type]
                    parsed=parsed_filters,
                )

            res.extend(batch)  # type: ignore[arg-type]

            if len(batch) < chunk:  # type: ignore[arg-type]
                break

            off += chunk
            page_num += 1

        return res

    # ....................... #

    async def _cursor_page(
        self,
        query: CursorQuery,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None,
        sorts: QuerySortExpression | None,
    ) -> CursorPage[R] | CursorPage[JsonDict] | CursorPage[BaseModel]:
        if query.return_model is not None and query.return_fields is not None:
            raise exc.precondition("return_model and return_fields cannot be combined")

        effective = self._resolve_sorts(sorts)
        normalized = normalize_sorts_for_keyset(
            effective,
            read_fields=self._read_fields,
            model=self.read_gw.model_type,
            sealed=self._sealed_fields,
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

        def _dump(o: R | JsonDict | BaseModel) -> JsonDict:
            # A projection's dict holds the stored values; a model's are read off its fields.
            if isinstance(o, dict):
                return o

            return _seek_values(o, sort_keys)

        page_raw, has_more, next_tok, prev_tok = assemble_keyset_cursor_page(
            raw,
            cursor=cursor,
            sort_keys=sort_keys,
            directions=directions,
            nulls=nulls,
            dump_row=_dump,
            # Same gateway + filters as the verify inside ``find_many_with_cursor`` above, so
            # the minted and checked bindings are identical (nothing is threaded across).
            binding=document_cursor_binding(self.read_gw, filters),
        )

        if query.return_model is not None:
            return CursorPage(
                hits=cast(list[BaseModel], list(page_raw)),
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
    ) -> AsyncGenerator[Sequence[R] | Sequence[JsonDict] | Sequence[BaseModel]]:
        async for hits in self._keyset_batches(
            CursorQuery(return_model=query.return_model, return_fields=query.return_fields),
            filters=filters,
            sorts=sorts,
            chunk=self._eff_stream_chunk_size(chunk_size),
            max_pages=self.max_stream_pages,
            label="Document cursor stream",
        ):
            yield hits

    # ....................... #

    async def _keyset_batches(
        self,
        query: CursorQuery,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None,
        chunk: int,
        max_pages: int | None,
        label: str,
    ) -> AsyncGenerator[Sequence[R] | Sequence[JsonDict] | Sequence[BaseModel]]:
        cursor: CursorPaginationExpression = {"limit": chunk}
        page_num = 0
        prev_cursor: str | None = None

        while True:
            check_page_limit(pages=page_num, max_pages=max_pages, label=label)

            page = await self._cursor_page(query, filters=filters, cursor=cursor, sorts=sorts)

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
            cursor = {"limit": chunk, "after": page.next_cursor}
            page_num += 1

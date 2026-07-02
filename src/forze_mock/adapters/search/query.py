"""In-memory search adapter."""

from __future__ import annotations

from typing import (
    Any,
    AsyncGenerator,
    Final,
    Literal,
    Sequence,
    cast,
    final,
    overload,
)
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCountlessPage,
    SearchCursorPage,
    SearchPage,
    search_page_from_limit_offset,
)
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    compile_filter,
)
from forze.application.contracts.search import (
    PhraseCombine,
    SearchCapabilities,
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    SearchSpec,
    effective_phrase_combine,
    highlight_fragment_bounds,
    normalize_search_queries,
    resolve_facet_fields,
    resolve_highlight,
    search_options_for_simple_adapter,
    validate_stream_supported,
)
from forze.application.integrations.search import (
    SearchResultSnapshot,
    stream_search_pages,
)
from forze.base.primitives import JsonDict
from forze.base.serialization import (
    default_model_codec,
)
from forze_mock.query._types import (
    M,
    T,
)
from forze_mock.query.cursors import (
    _mock_cursor_start_and_limit,  # type: ignore[reportPrivateUsage]
    _mock_cursor_tokens,  # type: ignore[reportPrivateUsage]
)
from forze_mock.adapters.search._facets_highlights import (
    compute_facets,
    compute_highlights,
)
from forze_mock.query.matching import (
    _path_text,  # type: ignore[reportPrivateUsage]
    _project,  # type: ignore[reportPrivateUsage]
    _sort_docs,  # type: ignore[reportPrivateUsage]
)
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin

# ----------------------- #

_MOCK_RANK: Final[str] = "_mock_rank"
"""Transient per-doc relevance score, tagged during the ranked scan and read out at
page-build (mirrors the real backends' ``_fts_rank`` / ``_mongo_rank`` columns). Stripped
before model decode / projection so it never leaks into a hit."""


def _without_mock_rank(doc: JsonDict) -> JsonDict:
    return {k: v for k, v in doc.items() if k != _MOCK_RANK}


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockSearchAdapter(MockTenancyMixin, SearchQueryPort[M]):
    """In-memory search adapter over documents in :class:`MockState`."""

    state: MockState
    spec: SearchSpec[M]
    result_snapshot: SearchResultSnapshot | None = None

    # ....................... #

    def _store(self) -> dict[UUID, JsonDict]:
        ns = self._partitioned_namespace(str(self.spec.name))
        with self.state.lock:
            return self.state.documents.setdefault(ns, {})

    # ....................... #

    def _resolve_fields(
        self,
        options: SearchOptions | None,
    ) -> tuple[list[str], dict[str, float] | None]:
        """Return field paths to search and optional per-field weights."""

        opts = options or {}
        allowed = list(self.spec.fields)

        weights_opt = opts.get("weights")
        if weights_opt:
            fields = [f for f in allowed if weights_opt.get(f, 0.0) > 0.0]
            if not fields:
                fields = allowed
            w = {f: float(weights_opt.get(f, 0.0)) for f in fields}
            return fields, w

        fields_opt = opts.get("fields")

        if fields_opt:
            sub = [f for f in fields_opt if f in allowed]
            allowed = sub if sub else allowed

        def_weights = (
            dict(self.spec.default_weights) if self.spec.default_weights else None
        )

        return allowed, def_weights

    # ....................... #

    def _text_score(
        self,
        query: str,
        doc: JsonDict,
        field_paths: Sequence[str],
        mode: str,
    ) -> float:
        q = query.strip().lower()
        if not q:
            return 1.0

        tokens = [x for x in q.split() if x]
        if not tokens:
            return 1.0

        joined = " ".join(_path_text(doc, p).lower() for p in field_paths)
        if not joined.strip():
            return 0.0

        if mode == "exact":
            return 1.0 if q == joined else 0.0

        if mode == "prefix":
            words = joined.split()
            matched = sum(
                1 for token in tokens if any(w.startswith(token) for w in words)
            )
            return matched / len(tokens)

        # fulltext and phrase use token containment for mock behavior.
        matched = sum(1 for token in tokens if token in joined)
        return matched / len(tokens)

    # ....................... #

    def _document_score_multi_phrase(
        self,
        terms: tuple[str, ...],
        doc: JsonDict,
        fields: Sequence[str],
        weights: dict[str, float] | None,
        *,
        combine: PhraseCombine,
    ) -> float:
        if not terms:
            return self._document_score("", doc, fields, weights)
        scores = [self._document_score(q, doc, fields, weights) for q in terms]
        return max(scores) if combine == "any" else min(scores)

    # ....................... #

    def _document_score(
        self,
        query: str,
        doc: JsonDict,
        fields: Sequence[str],
        weights: dict[str, float] | None,
    ) -> float:
        mode = "fulltext"
        if not fields:
            return 0.0
        if weights:
            total_w = sum(weights.values())
            if total_w <= 0.0:
                return 0.0
            acc = 0.0
            for f in fields:
                w = weights.get(f, 0.0)
                if w <= 0.0:
                    continue
                acc += w * self._text_score(query, doc, [f], mode)
            return acc / total_w
        return self._text_score(query, doc, fields, mode)

    # ....................... #

    def _full_ordered_search_documents(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
    ) -> list[JsonDict]:
        options = search_options_for_simple_adapter(options)
        fields, weights = self._resolve_fields(options)
        terms = normalize_search_queries(query)
        combine = effective_phrase_combine(options)

        with self.state.lock:
            docs = [dict(doc) for doc in self._store().values()]

        # Parse the filter once into a reusable predicate rather than re-parsing it
        # per document inside the scan.
        matches = compile_filter(filters)

        ranked: list[tuple[float, JsonDict]] = []
        for doc in docs:
            if not matches(doc):
                continue

            score = self._document_score_multi_phrase(
                terms, doc, fields, weights, combine=combine
            )
            if score <= 0.0:
                continue
            ranked.append((score, {**doc, _MOCK_RANK: score}))

        ranked.sort(key=lambda x: x[0], reverse=True)
        ordered = [d for _, d in ranked]

        if sorts:
            ordered = _sort_docs(ordered, sorts)

        return ordered

    # ....................... #

    def _facets_and_highlights(
        self,
        query: str | Sequence[str],
        options: SearchOptions | None,
        *,
        all_rows: Sequence[JsonDict],
        page_rows: Sequence[JsonDict],
    ) -> tuple[Any | None, list[Any] | None]:
        """Compute optional facets (over the full matching set ``all_rows``) and per-hit
        highlights (over the returned ``page_rows``); ``None`` when not requested."""

        facet_fields = resolve_facet_fields(self.spec, options)
        facets = (
            compute_facets(all_rows, facet_fields, options=options)
            if facet_fields
            else None
        )

        highlight = resolve_highlight(self.spec, options)
        fragment_size, max_fragments = highlight_fragment_bounds(options)
        highlights = (
            compute_highlights(
                page_rows,
                normalize_search_queries(query),
                highlight[0],
                pre_tag=highlight[1],
                post_tag=highlight[2],
                fragment_size=fragment_size,
                max_fragments=max_fragments,
            )
            if highlight is not None
            else None
        )

        return facets, highlights

    # ....................... #

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: None = None,
        return_fields: None = None,
    ) -> SearchCountlessPage[M]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: None = None,
        return_fields: None = None,
    ) -> SearchPage[M]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: type[T],
        return_fields: None = None,
    ) -> SearchCountlessPage[T]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: type[T],
        return_fields: None = None,
    ) -> SearchPage[T]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: None = None,
        return_fields: Sequence[str],
    ) -> SearchCountlessPage[JsonDict]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: None = None,
        return_fields: Sequence[str],
    ) -> SearchPage[JsonDict]: ...

    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: bool,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        rs_spec = self.spec.snapshot
        if self.result_snapshot is not None and rs_spec is not None:
            fp = SearchResultSnapshot.simple_search_fingerprint(
                query,
                filters,
                sorts,
                spec_name=str(self.spec.name),
                variant="offset",
            )
            maybe_snap: Any = await self.result_snapshot.read_simple_result_snapshot(
                rs_spec=rs_spec,
                snap_opt=snapshot,
                fp_computed=fp,
                spec=self.spec,
                pagination=dict(pagination or {}),
                return_type=return_type,
                return_fields=return_fields,
                return_count=return_count,
            )
            if maybe_snap is not None:
                return maybe_snap
        ordered = self._full_ordered_search_documents(query, filters, sorts, options)
        total = len(ordered)
        pagination = pagination or {}
        limit = pagination.get("limit")
        offset = pagination.get("offset")

        all_rows = ordered
        if offset:
            ordered = ordered[offset:]

        if limit is not None:
            ordered = ordered[:limit]

        # Per-hit relevance score for the page window (ranked queries only; a filter-only
        # browse has no meaningful score). Strip the transient tag before decode/projection.
        scores = (
            [float(doc.get(_MOCK_RANK, 0.0)) for doc in ordered]
            if normalize_search_queries(query)
            else None
        )
        ordered = [_without_mock_rank(doc) for doc in ordered]

        facets, highlights = self._facets_and_highlights(
            query, options, all_rows=all_rows, page_rows=ordered
        )

        if return_fields is not None:
            hits: list[Any] = [_project(doc, return_fields) for doc in ordered]
        elif return_type is not None:
            hits = default_model_codec(return_type).decode_mapping_many(ordered)
        else:
            allowed = set(self.spec.model_type.model_fields.keys())
            typed_docs = [
                {k: v for k, v in doc.items() if k in allowed} for doc in ordered
            ]
            hits = cast(
                list[Any],
                self.spec.resolved_read_codec.decode_mapping_many(typed_docs),
            )

        return search_page_from_limit_offset(
            hits,
            pagination,
            total=total if return_count else None,
            facets=facets,
            highlights=highlights,
            scores=scores,
        )

    # ....................... #

    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchCountlessPage[M]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=False,
            return_type=None,
            return_fields=None,
        )

    async def search_page(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchPage[M]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=True,
            return_type=None,
            return_fields=None,
        )

    async def project_search(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchCountlessPage[JsonDict]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=False,
            return_type=None,
            return_fields=tuple(fields),
        )

    async def project_search_page(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchPage[JsonDict]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=True,
            return_type=None,
            return_fields=tuple(fields),
        )

    async def select_search(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchCountlessPage[T]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=False,
            return_type=return_type,
            return_fields=None,
        )

    async def select_search_page(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchPage[T]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=True,
            return_type=return_type,
            return_fields=None,
        )

    # ....................... #

    @overload
    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: None = None,
        return_fields: None = None,
    ) -> SearchCursorPage[M]: ...

    @overload
    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: None = None,
        return_fields: Sequence[str],
    ) -> SearchCursorPage[JsonDict]: ...

    @overload
    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T],
        return_fields: None = None,
    ) -> SearchCursorPage[T]: ...

    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        ordered = self._full_ordered_search_documents(query, filters, sorts, options)
        start, lim = _mock_cursor_start_and_limit(cursor)
        window = ordered[start : start + lim + 1]
        has_more = len(window) > lim
        page_rows = window[:lim]
        next_c, prev_c = _mock_cursor_tokens(start, len(page_rows), has_more=has_more)

        scores = (
            [float(doc.get(_MOCK_RANK, 0.0)) for doc in page_rows]
            if normalize_search_queries(query)
            else None
        )
        page_rows = [_without_mock_rank(doc) for doc in page_rows]

        facets, highlights = self._facets_and_highlights(
            query, options, all_rows=ordered, page_rows=page_rows
        )

        if return_fields is not None:
            hits: list[Any] = [_project(doc, return_fields) for doc in page_rows]
        elif return_type is not None:
            hits = default_model_codec(return_type).decode_mapping_many(page_rows)
        else:
            allowed = set(self.spec.model_type.model_fields.keys())
            typed_docs = [
                {k: v for k, v in doc.items() if k in allowed} for doc in page_rows
            ]
            hits = cast(
                list[Any],
                self.spec.resolved_read_codec.decode_mapping_many(typed_docs),
            )

        return SearchCursorPage(
            hits=hits,
            next_cursor=next_c,
            prev_cursor=prev_c,
            has_more=has_more,
            facets=facets,
            highlights=highlights,
            scores=scores,
        )

    async def search_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> SearchCursorPage[M]:
        return await self._cursor_search_impl(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_type=None,
            return_fields=None,
        )

    async def project_search_cursor(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> SearchCursorPage[JsonDict]:
        return await self._cursor_search_impl(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_type=None,
            return_fields=tuple(fields),
        )

    async def select_search_cursor(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> SearchCursorPage[T]:
        return await self._cursor_search_impl(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_type=return_type,
            return_fields=None,
        )

    # ....................... #

    @property
    def search_capabilities(self) -> SearchCapabilities:
        # Single-index keyword reference: supports keyset iteration → bounded-memory export.
        return SearchCapabilities(supports_stream=True)

    async def search_stream(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[M]]:
        validate_stream_supported(self.search_capabilities, backend="mock")
        async for chunk in stream_search_pages(
            lambda cursor: self.search_cursor(
                query, filters, cursor, sorts, options=options
            ),
            chunk_size=chunk_size,
        ):
            yield chunk

    async def project_search_stream(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[JsonDict]]:
        validate_stream_supported(self.search_capabilities, backend="mock")
        async for chunk in stream_search_pages(
            lambda cursor: self.project_search_cursor(
                fields, query, filters, cursor, sorts, options=options
            ),
            chunk_size=chunk_size,
        ):
            yield chunk

    async def select_search_stream(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[T]]:
        validate_stream_supported(self.search_capabilities, backend="mock")
        async for chunk in stream_search_pages(
            lambda cursor: self.select_search_cursor(
                return_type, query, filters, cursor, sorts, options=options
            ),
            chunk_size=chunk_size,
        ):
            yield chunk

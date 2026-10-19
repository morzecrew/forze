"""Base class for single-index Mongo search adapters."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    SearchCapabilities,
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    effective_phrase_combine,
    normalize_search_queries,
    reject_unsupported_facets,
    reject_unsupported_highlight,
    resolve_search_sorts,
    search_options_for_simple_adapter,
)
from forze.application.integrations.search import (
    SearchResultSnapshot,
    reject_encrypted_sort_fields,
)
from forze_mongo.kernel.client.port import MongoClientPort

from ._cursor_run import execute_mongo_ranked_cursor_search
from ._offset_run import execute_mongo_ranked_offset_search
from ._port import MongoSearchPortMixin
from .base import MongoSearchGateway

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoSimpleSearchAdapter[M: BaseModel](
    MongoSearchGateway[M],
    MongoSearchPortMixin[M],
    SearchQueryPort[M],
):
    """Shared offset/cursor execution for Mongo simple (single-index) search."""

    client: MongoClientPort
    """Mongo client for aggregation queries."""

    result_snapshot: SearchResultSnapshot | None = attrs.field(default=None)
    """Optional result-ID snapshot coordinator."""

    search_variant: str = "mongo"
    """Fingerprint variant label for snapshots."""

    # ....................... #

    @property
    def search_capabilities(self) -> SearchCapabilities:
        # Text / Atlas ``$search`` rank over a full keyset cursor → bounded-memory export.
        # The ``$vectorSearch`` subclass overrides this (top-k, no whole-corpus stream).
        return SearchCapabilities(supports_stream=True)

    # ....................... #

    @property
    def _query_vector_dimensions(self) -> int | None:
        """The length a caller's ``query_vector`` must have; ``None`` where none is served."""

        return None

    def _search_options(
        self,
        options: SearchOptions | None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchOptions:
        return search_options_for_simple_adapter(
            options,
            spec=self.spec,
            snapshot=snapshot,
            capabilities=self.search_capabilities,
            backend=type(self).__name__,
            vector_dimensions=self._query_vector_dimensions,
        )

    def _ranks(self, terms: tuple[str, ...], options: SearchOptions | None) -> bool:
        """Whether the request ranks; a blank query browses instead."""

        _ = options
        return bool(terms)

    # ....................... #

    async def _ranked_pipeline(
        self,
        *,
        terms: tuple[str, ...],
        combine: str,
        pre_filter: dict[str, Any],
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    # ....................... #

    def _user_sorts(
        self,
        sorts: QuerySortExpression | None,
    ) -> list[tuple[str, int]] | None:
        return self.render_sorts(
            resolve_search_sorts(
                sorts,
                default_sort=self.spec.default_sort,
                read_fields=self.read_fields,
                model=self.model_type,
                spec_name=self.spec.name,
            )
        )

    # ....................... #

    async def _offset_search_impl(  # type: ignore[override]
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: bool = False,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        options = self._search_options(options, snapshot)

        # Mongo search does not produce facets or highlights; fail closed rather than
        # silently ignore a request (which would let a caller think sidecars were coming).
        reject_unsupported_facets(options, backend="Mongo")
        reject_unsupported_highlight(self.spec, options, backend="Mongo")

        # The encrypted-sort guard runs in the shared offset executor below.
        terms = tuple(normalize_search_queries(query))
        combine = effective_phrase_combine(options)
        pre_filter = self.render_filters(filters)
        pipeline = await self._ranked_pipeline(
            terms=terms,
            combine=combine,
            pre_filter=pre_filter,
            sorts=sorts,
            options=options,
        )
        fingerprint_extras: dict[str, object] = {"phrase_combine": str(combine)}

        if (vector := options.get("query_vector")) is not None:
            # A stored snapshot for one vector must not answer another.
            fingerprint_extras["query_vector"] = list(vector)

        return await execute_mongo_ranked_offset_search(
            self,
            client=self.client,
            ranked_pipeline=pipeline,
            ranked=self._ranks(terms, options),
            query=query,
            filters=filters,
            sorts=sorts,
            spec=self.spec,
            variant=self.search_variant,
            fingerprint_extras=fingerprint_extras,
            pagination=pagination,
            snapshot=snapshot,
            return_count=return_count,
            return_type=return_type,
            return_fields=return_fields,
            result_snapshot=self.result_snapshot,
        )

    # ....................... #

    async def _cursor_search_impl(  # type: ignore[override]
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        options = self._search_options(options)
        reject_unsupported_facets(options, backend="Mongo")
        reject_unsupported_highlight(self.spec, options, backend="Mongo")
        reject_encrypted_sort_fields(
            sorts, encryption=self.spec.encryption, spec_name=self.spec.name
        )
        terms = tuple(normalize_search_queries(query))
        combine = effective_phrase_combine(options)
        pre_filter = self.render_filters(filters)
        pipeline = await self._ranked_pipeline(
            terms=terms,
            combine=combine,
            pre_filter=pre_filter,
            sorts=sorts,
            options=options,
        )

        return await execute_mongo_ranked_cursor_search(
            self,
            client=self.client,
            ranked_pipeline=pipeline,
            ranked=self._ranks(terms, options),
            query=query,
            filters=filters,
            sorts=sorts,
            cursor=cursor,
            return_type=return_type,
            return_fields=return_fields,
        )

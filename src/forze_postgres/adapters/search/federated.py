"""Federated multi-index search: per-member adapters merged with weighted RRF."""

import asyncio
from functools import partial
from typing import (
    Any,
    Awaitable,
    Callable,
    Final,
    Literal,
    NoReturn,
    Sequence,
    TypeVar,
    cast,
    final,
    overload,
)

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCapabilities,
    SearchCountlessPage,
    SearchPage,
    SearchSnapshotHandle,
    search_page_from_limit_offset,
)
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    FederatedSearchReadModel,
    FederatedSearchSpec,
    MultiSourceSearchOptions,
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    SearchResultSnapshotSpec,
    prepare_federated_search_options,
    reject_federated_facets,
    resolve_fusion,
)
from forze.application.integrations.search import (
    SearchResultSnapshot,
    build_federated_highlight_index,
    execute_federated_thin_offset,
    federated_highlights_for_hits,
    federated_snapshot_rehydrator,
    federated_thin_eligible,
    federated_thin_format,
)
from forze.base.exceptions import exc
from forze.base.serialization import default_model_codec

from ._materialize_hits import decode_search_hits

from ...kernel.client import PostgresClientPort, gather_db_work
from ._port import PostgresSearchPortMixin
from ._search_count import effective_search_count

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #

_DEFAULT_RRF_K: Final[int] = 60
_DEFAULT_PER_LEG_LIMIT: Final[int] = 5000

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresFederatedSearchAdapter[M: BaseModel](
    PostgresSearchPortMixin[FederatedSearchReadModel[M]],
    SearchQueryPort[FederatedSearchReadModel[M]],
):
    """Search several independent indexes and merge results using weighted RRF.

    Per-request :class:`SearchOptions` ``member_weights`` / ``members`` select
    and weight federation members (``0`` disables a member). Field-level
    ``weights`` / ``fields`` are ignored; tune each :class:`~forze.application.contracts.search.SearchSpec` instead.

    Each member query uses relevance ordering only; caller ``sorts`` apply after
    RRF as a stable secondary ordering (RRF score remains primary).

    Pagination applies to the merged list. Each leg fetches at most
    :attr:`rrf_per_leg_limit` rows; :attr:`total` is the length of the merged
    candidate pool (thus exact only when no leg truncates). Cursor keyset methods
    are not supported; use offset pagination (:meth:`search`, :meth:`search_page`).
    """

    federated_spec: FederatedSearchSpec[M]
    """Federated search specification."""

    legs: Sequence[tuple[str, SearchQueryPort[M]]]
    """``(member, port)`` pairs: ``member`` is each leg :class:`~forze.application.contracts.search.SearchSpec` ``name``."""

    rrf_k: int = _DEFAULT_RRF_K
    """RRF smoothing constant (typical default 60)."""

    rrf_per_leg_limit: int = _DEFAULT_PER_LEG_LIMIT
    """Maximum hits pulled per member for merging (truncation bounds offset search totals)."""

    postgres_client: PostgresClientPort | None = None
    """When set, leg queries respect pool / transaction concurrency rules."""

    result_snapshot: SearchResultSnapshot | None = None
    """Federation snapshot run helper."""

    spec: FederatedSearchSpec[M] = attrs.field(
        default=attrs.Factory(lambda self: self.federated_spec, takes_self=True),
        init=False,
    )
    """Alias of :attr:`federated_spec` exposed for the port mixin."""

    model_type: type[FederatedSearchReadModel[M]] = attrs.field(
        default=cast("type[FederatedSearchReadModel[M]]", FederatedSearchReadModel),
        init=False,
    )
    """Read model for port mixin typing."""

    # ....................... #

    @property
    def search_capabilities(self) -> SearchCapabilities:
        # Cross-index fusion by weighted reciprocal rank fusion (rank-only). Weighted
        # (relative-score) fusion is not offered here, so it is not advertised.
        return SearchCapabilities(hybrid_fusion=frozenset({"rrf"}))

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if len(self.legs) != len(self.federated_spec.members):
            raise exc.internal(
                "Federated adapter legs must match FederatedSearchSpec.members length.",
            )

        for (leg_member, _), m in zip(
            self.legs, self.federated_spec.members, strict=True
        ):
            if leg_member != m.name:
                raise exc.internal(
                    f"Federated leg member {leg_member!r} does not match SearchSpec.name {m.name!r}.",
                )

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
    ) -> SearchCountlessPage[FederatedSearchReadModel[M]]: ...

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
    ) -> SearchPage[FederatedSearchReadModel[M]]: ...

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
        return_count: bool,
        return_type: None = None,
        return_fields: Sequence[str],
    ) -> NoReturn: ...

    async def _offset_search_impl(  # pyright: ignore[reportIncompatibleMethodOverride]
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
        if return_fields is not None:
            raise exc.precondition(
                "Field projection is not supported for federated (RRF) search; use "
                "select_search / select_search_page with a return_type instead.",
                code="query_feature_unsupported",
            )

        reject_federated_facets(options)
        resolve_fusion(
            cast("MultiSourceSearchOptions", options or {}).get("fusion"),
            self.search_capabilities,
            backend="postgres_federated",
        )

        leg_opts, member_weights = prepare_federated_search_options(
            self.federated_spec,
            options,
        )

        rs_spec: SearchResultSnapshotSpec | None = self.federated_spec.snapshot
        offset = int((pagination or {}).get("offset") or 0)
        limit = (pagination or {}).get("limit")

        count_policy = effective_search_count(options)
        snapshot_return_count = return_count and count_policy != "none"

        # Per-hit highlights are rebuilt from leg results, which a snapshot replay skips, so a
        # highlight request runs live (no snapshot read or write) to keep them.
        _hl = (options or {}).get("highlight")
        wants_highlights = _hl is not None and _hl is not False

        # Spec-level: thin specs store/replay tiny ``(member, id)`` snapshot keys; the
        # marker keeps a thin snapshot from ever being read as a full-record one.
        effective_thin = federated_thin_format(
            self.federated_spec.members, thin_merge=self.federated_spec.thin_merge
        )

        fp_computed = SearchResultSnapshot.federated_fingerprint(
            query,
            filters,
            sorts,
            spec_name=self.federated_spec.name,
            rrf_k=int(self.rrf_k),
            extras={"thin": True} if effective_thin else None,
        )

        if (
            self.result_snapshot is not None
            and rs_spec is not None
            and not wants_highlights
            and snapshot is not None
            and "id" in snapshot
        ):
            if effective_thin:
                maybe_page = await self.result_snapshot.read_federated_thin_snapshot_page_if_requested(
                    rs_spec=rs_spec,
                    snapshot=snapshot,
                    fp_computed=fp_computed,
                    pagination=dict(pagination or {}),
                    return_type=return_type,
                    return_count=snapshot_return_count,
                    rehydrate=federated_snapshot_rehydrator(
                        ports={name: port for name, port in self.legs},
                        leg_opts=leg_opts,
                        run_legs=self._run_legs,
                    ),
                )

            else:
                maybe_page = await self.result_snapshot.read_federated_snapshot_page_if_requested(
                    federated_spec=self.federated_spec,
                    rs_spec=rs_spec,
                    snapshot=snapshot,
                    fp_computed=fp_computed,
                    pagination=dict(pagination or {}),
                    return_type=return_type,
                    return_count=snapshot_return_count,
                )

            if maybe_page is not None:
                return maybe_page

        active = [
            (name, port, member_weights[i])
            for i, (name, port) in enumerate(self.legs)
            if member_weights[i] > 0.0
        ]

        if not active:
            empty_hits: list[FederatedSearchReadModel[M]] = []
            if return_count:
                return search_page_from_limit_offset(
                    empty_hits,
                    pagination or {},
                    total=0,
                )

            return search_page_from_limit_offset(empty_hits, pagination or {}, total=None)

        leg_cap = max(1, int(self.rrf_per_leg_limit))
        leg_page: PaginationExpression = {"limit": leg_cap}

        snapshot_write = (
            self.result_snapshot is not None
            and rs_spec is not None
            and self.result_snapshot.should_write_result_snapshot(snapshot, rs_spec)
        )

        if federated_thin_eligible(
            members=self.federated_spec.members,
            thin_merge=self.federated_spec.thin_merge,
            wants_highlights=wants_highlights,
            sorts=sorts,
        ):
            return await execute_federated_thin_offset(
                legs=active,
                query=query,
                filters=filters,
                pagination=pagination,
                sorts=sorts,
                leg_opts=leg_opts,
                rrf_k=int(self.rrf_k),
                per_leg_limit=leg_cap,
                return_count=return_count,
                return_type=return_type,
                run_legs=self._run_legs,
                result_snapshot=self.result_snapshot,
                rs_spec=rs_spec,
                snapshot=snapshot,
                fp_computed=fp_computed,
                write_snapshot=snapshot_write,
            )

        async def _run_leg(
            name: str,
            port: SearchQueryPort[M],
            weight: float,
        ) -> tuple[str, Any, float]:
            page = await port.search(
                query,
                filters,
                leg_page,
                None,
                options=leg_opts,
            )
            return name, page, weight

        if self.postgres_client is not None:
            leg_results = await gather_db_work(
                self.postgres_client,
                [partial(_run_leg, n, p, w) for n, p, w in active],
            )

        else:
            leg_results = await asyncio.gather(
                *(_run_leg(n, p, w) for n, p, w in active),
            )

        hl_index = build_federated_highlight_index(
            [(name, page) for name, page, _w in leg_results]
        )
        merged = SearchResultSnapshot.weighted_rrf_merge_rows(
            leg_rows=[(name, page.hits, w) for name, page, w in leg_results],
            k=int(self.rrf_k),
        )

        SearchResultSnapshot.order_federated_full_merge(merged, sorts)

        total = len(merged)
        handle_out: SearchSnapshotHandle | None = None

        if (
            self.result_snapshot is not None
            and rs_spec is not None
            and not wants_highlights
            and not effective_thin  # thin specs only ever write the thin format
            and self.result_snapshot.should_write_result_snapshot(snapshot, rs_spec)
        ):
            handle_out = await self.result_snapshot.put_ordered_snapshot_keys(
                (
                    SearchResultSnapshot.federated_record_key_string(
                        item[0].member,
                        item[0].hit,
                    )
                    for item in merged
                ),
                snap_opt=snapshot,
                rs_spec=rs_spec,
                fp_computed=fp_computed,
                pool_len_before_cap=total,
            )

        window = merged[offset:]

        if limit is not None:
            window = window[: int(limit)]

        # Per-hit highlights from the originating leg, aligned with the windowed hits.
        highlights = federated_highlights_for_hits(
            [it[0] for it in window], hl_index
        )
        # Fused RRF score per windowed hit (index-aligned with the hits either branch builds).
        scores = [float(it[1]) for it in window]

        def _finish(hits: list[Any]) -> Any:
            result = search_page_from_limit_offset(
                hits,
                pagination,
                total=total if return_count else None,
                snapshot=handle_out,
                scores=scores,
            )
            if highlights is None:
                return result
            return attrs.evolve(result, highlights=highlights)

        if return_type is not None:
            rows = [
                {
                    "hit": it[0].hit.model_dump(mode="json"),
                    "member": it[0].member,
                }
                for it in window
            ]
            v = decode_search_hits(
                rows=rows,
                model_type=return_type,
                codec=default_model_codec(return_type),
                return_type=return_type,
                trust_source=False,
            )
            return _finish(v)

        return _finish([it[0] for it in window])

    # ....................... #

    async def _run_legs(
        self,
        makers: Sequence[Callable[[], Awaitable[Any]]],
    ) -> list[Any]:
        """Run leg thunks honouring pool/transaction concurrency when a client is set."""

        if self.postgres_client is not None:
            return await gather_db_work(self.postgres_client, list(makers))

        return list(await asyncio.gather(*(maker() for maker in makers)))

    # ....................... #

    def _raise_federated_cursor_not_supported(self) -> NoReturn:
        raise exc.precondition(
            "search_cursor is not implemented for federated (RRF) search; use "
            "search or search_page with limit/offset.",
            code="query_feature_unsupported",
        )

    async def _cursor_search_impl(
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
        del query, filters, cursor, sorts, options, return_type, return_fields
        self._raise_federated_cursor_not_supported()

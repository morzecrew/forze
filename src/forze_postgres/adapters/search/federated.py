"""Federated multi-index search: per-member adapters merged with weighted RRF."""

import asyncio
from functools import partial
from typing import Any, Final, Literal, NoReturn, Sequence, TypeVar, final, overload

import attrs
from pydantic import BaseModel

from forze.application.contracts.base import (
    CountlessPage,
    CursorPage,
    Page,
    SearchSnapshotHandle,
    page_from_limit_offset,
)
from forze.application.contracts.query import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    FederatedSearchReadModel,
    FederatedSearchSpec,
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    SearchResultSnapshotSpec,
    prepare_federated_search_options,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.application.coordinators import SearchResultSnapshotCoordinator
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many

from ...kernel.db_gather import gather_db_work
from ...kernel.platform import PostgresClientPort
from ..txmanager import PostgresTxScopeKey

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #

_DEFAULT_RRF_K: Final[int] = 60
_DEFAULT_PER_LEG_LIMIT: Final[int] = 5000


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresFederatedSearchAdapter[M: BaseModel](
    SearchQueryPort[FederatedSearchReadModel[M]],
    TxScopedPort,
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

    snapshot_coord: SearchResultSnapshotCoordinator | None = None
    """Coordinator for federation snapshot runs."""

    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if len(self.legs) != len(self.federated_spec.members):
            raise CoreError(
                "Federated adapter legs must match FederatedSearchSpec.members length.",
            )

        for (leg_member, _), m in zip(
            self.legs, self.federated_spec.members, strict=True
        ):
            if leg_member != m.name:
                raise CoreError(
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
    ) -> CountlessPage[FederatedSearchReadModel[M]]: ...

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
    ) -> Page[FederatedSearchReadModel[M]]: ...

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
    ) -> CountlessPage[T]: ...

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
    ) -> Page[T]: ...

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
        if return_fields is not None:
            raise CoreError(
                "Field projection is not supported for federated search "
                "(``project_search`` / ``project_search_page``). "
                "Use ``select_search`` / ``select_search_page`` with a ``return_type`` instead.",
            )

        leg_opts, member_weights = prepare_federated_search_options(
            self.federated_spec,
            options,
        )

        rs_spec: SearchResultSnapshotSpec | None = self.federated_spec.snapshot
        offset = int((pagination or {}).get("offset") or 0)
        limit = (pagination or {}).get("limit")

        fp_computed = SearchResultSnapshotCoordinator.federated_fingerprint(
            query,
            filters,
            sorts,
            spec_name=self.federated_spec.name,
            rrf_k=int(self.rrf_k),
        )

        if (
            self.snapshot_coord is not None
            and rs_spec is not None
            and snapshot is not None
            and "id" in snapshot
        ):
            maybe_page = (
                await self.snapshot_coord.read_federated_snapshot_page_if_requested(
                    federated_spec=self.federated_spec,
                    rs_spec=rs_spec,
                    snapshot=snapshot,
                    fp_computed=fp_computed,
                    pagination=dict(pagination or {}),
                    return_type=return_type,
                    return_count=return_count,
                )
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
                return page_from_limit_offset(
                    empty_hits,
                    pagination or {},
                    total=0,
                )

            return page_from_limit_offset(empty_hits, pagination or {}, total=None)

        leg_cap = max(1, int(self.rrf_per_leg_limit))
        leg_page: PaginationExpression = {"limit": leg_cap}

        async def _run_leg(
            name: str,
            port: SearchQueryPort[M],
            weight: float,
        ) -> tuple[str, list[M], float]:
            page = await port.search(
                query,
                filters,
                leg_page,
                None,
                options=leg_opts,
            )
            return name, page.hits, weight

        if self.postgres_client is not None:
            leg_results = await gather_db_work(
                self.postgres_client,
                [partial(_run_leg, n, p, w) for n, p, w in active],
            )

        else:
            leg_results = await asyncio.gather(
                *(_run_leg(n, p, w) for n, p, w in active),
            )

        merged = SearchResultSnapshotCoordinator.weighted_rrf_merge_rows(
            leg_rows=leg_results,
            k=int(self.rrf_k),
        )

        if sorts:
            for field, direction in reversed(list(sorts.items())):
                merged.sort(
                    key=partial(
                        SearchResultSnapshotCoordinator.federated_merged_hit_field,
                        field=field,
                    ),
                    reverse=(direction == "desc"),
                )

        merged.sort(key=lambda it: -it[1])

        total = len(merged)
        handle_out: SearchSnapshotHandle | None = None

        if (
            self.snapshot_coord is not None
            and rs_spec is not None
            and self.snapshot_coord.should_write_result_snapshot(snapshot, rs_spec)
        ):
            max_n = self.snapshot_coord.effective_snapshot_max_ids(snapshot, rs_spec)
            to_store = merged[:max_n]

            row_keys = [
                SearchResultSnapshotCoordinator.federated_record_key_string(
                    item[0].member,
                    item[0].hit,
                )
                for item in to_store
            ]

            handle_out = await self.snapshot_coord.put_ordered_snapshot_keys(
                row_keys,
                snap_opt=snapshot,
                rs_spec=rs_spec,
                fp_computed=fp_computed,
                pool_len_before_cap=total,
            )

        window = merged[offset:]

        if limit is not None:
            window = window[: int(limit)]

        if return_type is not None:
            rows = [
                {
                    "hit": it[0].hit.model_dump(mode="json"),
                    "member": it[0].member,
                }
                for it in window
            ]
            v = pydantic_validate_many(return_type, rows)
            if return_count:
                return page_from_limit_offset(
                    v,
                    pagination,
                    total=total,
                    snapshot=handle_out,
                )

            return page_from_limit_offset(
                v,
                pagination,
                total=None,
                snapshot=handle_out,
            )

        out = [it[0] for it in window]

        if return_count:
            return page_from_limit_offset(
                out,
                pagination,
                total=total,
                snapshot=handle_out,
            )

        return page_from_limit_offset(
            out,
            pagination,
            total=None,
            snapshot=handle_out,
        )

    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> CountlessPage[FederatedSearchReadModel[M]]:
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
    ) -> Page[FederatedSearchReadModel[M]]:
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
    ) -> CountlessPage[JsonDict]:
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
    ) -> Page[JsonDict]:
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
    ) -> CountlessPage[T]:
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
    ) -> Page[T]:
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

    def _raise_federated_cursor_not_supported(self) -> NoReturn:
        raise CoreError(
            "search_cursor is not implemented for federated (RRF) search; use "
            "search or search_page with limit/offset.",
        )

    async def search_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CursorPage[FederatedSearchReadModel[M]]:
        del query, filters, cursor, sorts, options
        self._raise_federated_cursor_not_supported()

    async def project_search_cursor(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CursorPage[JsonDict]:
        del fields, query, filters, cursor, sorts, options
        self._raise_federated_cursor_not_supported()

    async def select_search_cursor(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CursorPage[T]:
        del return_type, query, filters, cursor, sorts, options
        self._raise_federated_cursor_not_supported()

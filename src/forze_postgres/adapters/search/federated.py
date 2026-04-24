"""Federated multi-index search: per-member adapters merged with weighted RRF."""

import asyncio
import uuid
from functools import partial
from typing import Any, Final, Literal, Sequence, TypeVar, final, overload

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
    SearchResultSnapshotPort,
    SearchResultSnapshotSpec,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many

from ...kernel.db_gather import gather_db_work
from ...kernel.platform.client import PostgresClient
from ..txmanager import PostgresTxScopeKey
from ._options import prepare_federated_search_options
from .federated_snapshot import (
    effective_snapshot_chunk_size,
    effective_snapshot_max_ids,
    effective_snapshot_ttl,
    federated_fingerprint,
    federated_row_key_string,
    hydrate_federated_row_key,
    should_write_federated_snapshot,
)

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

_DEFAULT_RRF_K: Final[int] = 60
_DEFAULT_PER_LEG_LIMIT: Final[int] = 5000


def weighted_rrf_merge_rows(
    *,
    leg_rows: Sequence[tuple[str, Sequence[BaseModel], float]],
    k: int,
) -> list[tuple[FederatedSearchReadModel[Any], float]]:
    """Merge ranked hit lists with weighted reciprocal rank fusion (RRF).

    Each tuple is ``(member, hits in relevance order, member_weight)`` where
    ``member`` is the leg :class:`~forze.application.contracts.search.SearchSpec`
    ``name``. Rows with non-positive member weights are skipped. RRF contribution
    per row is ``weight / (k + rank)`` with **1-based** ``rank``.
    """

    scores: dict[str, float] = {}
    models: dict[str, FederatedSearchReadModel[Any]] = {}

    for member, hits, weight in leg_rows:
        if weight <= 0.0:
            continue

        for rank, hit in enumerate(hits, start=1):
            key = _federated_row_key(member, hit)
            contrib = float(weight) / (float(k) + float(rank))
            scores[key] = scores.get(key, 0.0) + contrib

            if key not in models:
                models[key] = FederatedSearchReadModel(
                    hit=hit,
                    member=member,
                )

    ordered = sorted(
        scores.keys(),
        key=lambda rk: (-scores[rk], models[rk].member, rk),
    )

    return [(models[rk], scores[rk]) for rk in ordered]


def _federated_row_key(member: str, hit: BaseModel) -> str:
    return federated_row_key_string(member, hit)


def _federated_merged_hit_field(
    item: tuple[FederatedSearchReadModel[Any], float],
    *,
    field: str,
) -> Any:
    return getattr(item[0].hit, field)


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
    candidate pool (thus exact only when no leg truncates).
    """

    federated_spec: FederatedSearchSpec[M]
    """Federated search specification."""

    legs: Sequence[tuple[str, SearchQueryPort[M]]]
    """``(member, port)`` pairs: ``member`` is each leg :class:`~forze.application.contracts.search.SearchSpec` ``name``."""

    rrf_k: int = _DEFAULT_RRF_K
    """RRF smoothing constant (typical default 60)."""

    rrf_per_leg_limit: int = _DEFAULT_PER_LEG_LIMIT
    """Maximum hits pulled per member for merging (truncation bounds :meth:`search` totals)."""

    postgres_client: PostgresClient | None = None
    """When set, leg queries respect pool / transaction concurrency rules."""

    snapshot_store: SearchResultSnapshotPort | None = None
    """Optional store for ordered RRF row keys to accelerate subsequent pages (same search request)."""

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
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        snapshot: SearchResultSnapshotOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[FederatedSearchReadModel[M]]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        snapshot: SearchResultSnapshotOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[T]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        snapshot: SearchResultSnapshotOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
        return_count: Literal[False] = ...,
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        snapshot: SearchResultSnapshotOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[True] = ...,
    ) -> Page[FederatedSearchReadModel[M]]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        snapshot: SearchResultSnapshotOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[True] = ...,
    ) -> Page[T]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        snapshot: SearchResultSnapshotOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
        return_count: Literal[True] = ...,
    ) -> Page[JsonDict]: ...

    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
        return_count: bool = False,
    ) -> (
        CountlessPage[FederatedSearchReadModel[M]]
        | CountlessPage[T]
        | CountlessPage[JsonDict]
        | Page[FederatedSearchReadModel[M]]
        | Page[T]
        | Page[JsonDict]
    ):
        if return_fields is not None:
            raise CoreError(
                "Fields selection with `return_fields` is not supported for federated search. "
                "Use `return_type` instead",
            )

        leg_opts, member_weights = prepare_federated_search_options(
            self.federated_spec,
            options,
        )

        rs_spec: SearchResultSnapshotSpec | None = self.federated_spec.result_snapshot
        offset = int((pagination or {}).get("offset") or 0)
        limit = (pagination or {}).get("limit")
        page_limit = max(1, int(limit)) if limit is not None else 20

        fp_computed = federated_fingerprint(
            query,
            filters,
            sorts,
            spec_name=self.federated_spec.name,
            rrf_k=int(self.rrf_k),
        )

        if (
            self.snapshot_store is not None
            and rs_spec is not None
            and snapshot is not None
            and "id" in snapshot
        ):
            if "fingerprint" in snapshot:
                sub_fp = str(snapshot["fingerprint"])

            else:
                sub_fp = None

            raw_keys = await self.snapshot_store.get_id_range(
                str(snapshot["id"]),
                offset,
                page_limit,
                expected_fingerprint=sub_fp,
            )

            if raw_keys is not None:
                sm = await self.snapshot_store.get_meta(str(snapshot["id"]))
                total_snap = (
                    int(sm.total) if sm and sm.complete else offset + len(raw_keys)
                )
                fp_h = (sm and sm.fingerprint) or fp_computed
                handle = SearchSnapshotHandle(
                    id=str(snapshot["id"]),
                    fingerprint=fp_h,
                    total=total_snap,
                    capped=False,
                )

                hydrated = [
                    hydrate_federated_row_key(k, self.federated_spec) for k in raw_keys
                ]

                if return_type is not None:
                    rows2 = [
                        {
                            "hit": it.hit.model_dump(mode="json"),
                            "member": it.member,
                        }
                        for it in hydrated
                    ]
                    v2 = pydantic_validate_many(return_type, rows2)

                    if return_count:
                        return page_from_limit_offset(
                            v2,
                            pagination,
                            total=total_snap,
                            result_snapshot=handle,
                        )

                    return page_from_limit_offset(
                        v2,
                        pagination,
                        total=None,
                        result_snapshot=handle,
                    )

                if return_count:
                    return page_from_limit_offset(
                        hydrated,
                        pagination,
                        total=total_snap,
                        result_snapshot=handle,
                    )

                return page_from_limit_offset(
                    hydrated, pagination, total=None, result_snapshot=handle
                )

        active = [
            (name, port, member_weights[i])
            for i, (name, port) in enumerate(self.legs)
            if member_weights[i] > 0.0
        ]

        if not active:
            if return_count:
                return page_from_limit_offset(
                    [],
                    pagination or {},
                    total=0,
                )

            return page_from_limit_offset([], pagination or {}, total=None)

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
                return_count=False,
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

        merged = weighted_rrf_merge_rows(leg_rows=leg_results, k=int(self.rrf_k))

        if sorts:
            for field, direction in reversed(list(sorts.items())):
                merged.sort(
                    key=partial(_federated_merged_hit_field, field=field),
                    reverse=(direction == "desc"),
                )

        merged.sort(key=lambda it: -it[1])

        total = len(merged)
        handle_out: SearchSnapshotHandle | None = None

        if (
            self.snapshot_store is not None
            and rs_spec is not None
            and should_write_federated_snapshot(snapshot, rs_spec)
        ):
            max_n = effective_snapshot_max_ids(snapshot, rs_spec)
            to_store = merged[:max_n]
            capped = total > len(to_store)
            row_keys = [
                federated_row_key_string(item[0].member, item[0].hit)
                for item in to_store
            ]

            run_id = str(uuid.uuid4())
            put_ttl = effective_snapshot_ttl(snapshot, rs_spec)
            put_chunk = effective_snapshot_chunk_size(snapshot, rs_spec)

            await self.snapshot_store.put_run(
                run_id=run_id,
                fingerprint=fp_computed,
                ordered_ids=row_keys,
                ttl=put_ttl,
                chunk_size=put_chunk,
            )

            handle_out = SearchSnapshotHandle(
                id=run_id,
                fingerprint=fp_computed,
                total=len(row_keys),
                capped=capped,
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
                    v, pagination, total=total, result_snapshot=handle_out
                )

            return page_from_limit_offset(
                v, pagination, total=None, result_snapshot=handle_out
            )

        out = [it[0] for it in window]

        if return_count:
            return page_from_limit_offset(
                out, pagination, total=total, result_snapshot=handle_out
            )

        return page_from_limit_offset(
            out, pagination, total=None, result_snapshot=handle_out
        )

    # ....................... #

    @overload
    async def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
    ) -> CursorPage[FederatedSearchReadModel[M]]: ...

    @overload
    async def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
    ) -> CursorPage[T]: ...

    @overload
    async def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
    ) -> CursorPage[JsonDict]: ...

    async def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> CursorPage[FederatedSearchReadModel[M]] | CursorPage[T] | CursorPage[JsonDict]:
        del query, filters, cursor, sorts, options, return_type, return_fields
        raise CoreError(
            "search_with_cursor is not implemented for federated (RRF) search; use search() "
            "with limit/offset.",
        )

"""Federated multi-index search: per-member adapters merged with weighted RRF."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from functools import partial
from typing import Any, Final, TypeVar, final, overload

import attrs
from pydantic import BaseModel

from forze.application.contracts.query import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    FederatedSearchReadModel,
    FederatedSearchSpec,
    SearchOptions,
    SearchQueryPort,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many

from ..txmanager import PostgresTxScopeKey
from ._options import prepare_federated_search_options

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
    payload = json.dumps(hit.model_dump(mode="json"), sort_keys=True)
    return f"{member}\0{payload}"


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

    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if len(self.legs) != len(self.federated_spec.members):
            raise CoreError(
                "Federated adapter legs must match FederatedSearchSpec.members length.",
            )

        for (leg_member, _), m in zip(self.legs, self.federated_spec.members, strict=True):
            if leg_member != m.name:
                raise CoreError(
                    f"Federated leg member {leg_member!r} does not match SearchSpec.name {m.name!r}.",
                )

    # ....................... #

    @overload
    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
    ) -> tuple[list[FederatedSearchReadModel[M]], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
    ) -> tuple[list[T], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]: ...

    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> tuple[
        list[FederatedSearchReadModel[M]] | list[T] | list[JsonDict],
        int,
    ]:
        if return_fields is not None:
            raise CoreError(
                "return_fields is not supported for federated search; use return_type=None.",
            )

        leg_opts, member_weights = prepare_federated_search_options(
            self.federated_spec,
            options,
        )

        active = [
            (name, port, member_weights[i])
            for i, (name, port) in enumerate(self.legs)
            if member_weights[i] > 0.0
        ]

        if not active:
            return [], 0

        leg_cap = max(1, int(self.rrf_per_leg_limit))
        leg_page: PaginationExpression = {"limit": leg_cap}

        async def _run_leg(
            name: str,
            port: SearchQueryPort[M],
            weight: float,
        ) -> tuple[str, list[M], float]:
            hits, _t = await port.search(
                query,
                filters,
                leg_page,
                None,
                options=leg_opts,
            )
            return name, hits, weight

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
        offset = int((pagination or {}).get("offset") or 0)
        limit = (pagination or {}).get("limit")

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
            return pydantic_validate_many(return_type, rows), total

        out = [it[0] for it in window]

        return out, total

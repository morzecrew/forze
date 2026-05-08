"""Coordinated KV reads/writes for search result snapshots plus federated RRF merge.

Adapters delegate fingerprinting, policy, pagination windows, hydration,
federated rank fusion, and ``SearchResultSnapshotPort`` access here.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import timedelta
from typing import Any, Mapping, Sequence, TypeVar, cast

import attrs
from pydantic import BaseModel

from forze.application.contracts.base import (
    CountlessPage,
    Page,
    SearchSnapshotHandle,
    page_from_limit_offset,
)
from forze.application.contracts.query import (
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    FederatedSearchReadModel,
    FederatedSearchSpec,
    SearchResultSnapshotMeta,
    SearchResultSnapshotOptions,
    SearchResultSnapshotPort,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many

# ----------------------- #

M_co = TypeVar("M_co", bound=BaseModel)
T_co = TypeVar("T_co", bound=BaseModel)

# ....................... #


def _sha256_fingerprint_payload(payload: dict[str, object]) -> str:
    body = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    h = hashlib.sha256(body.encode("utf-8")).hexdigest()

    return f"sha256:{h}"


# ....................... #


def _snapshot_write_policy(
    result_snapshot: SearchResultSnapshotOptions | None,
    rs_spec: SearchResultSnapshotSpec | None,
) -> bool:
    if rs_spec is None:
        return False

    if not result_snapshot:
        return rs_spec.enabled is True

    if result_snapshot.get("mode") is False:
        return False

    if result_snapshot.get("mode") is True:
        return True

    if result_snapshot.get("mode", "auto") in ("auto",):
        return rs_spec.enabled is True

    return rs_spec.enabled is True


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchResultSnapshotCoordinator:
    """Coordinates ordered search-result snapshots for ranked adapters."""

    store: SearchResultSnapshotPort
    """KV backend for snapshot runs."""

    # ....................... #

    @staticmethod
    def should_write_result_snapshot(
        result_snapshot: SearchResultSnapshotOptions | None,
        rs_spec: SearchResultSnapshotSpec | None,
    ) -> bool:
        return _snapshot_write_policy(result_snapshot, rs_spec)

    # ....................... #

    @staticmethod
    def effective_snapshot_max_ids(
        opt: SearchResultSnapshotOptions | None,
        spec: SearchResultSnapshotSpec | None,
    ) -> int:
        if opt and "max_ids" in opt:
            return max(1, int(opt["max_ids"]))

        if spec is not None:
            return max(1, int(spec.max_ids))

        return 50_000

    # ....................... #

    @staticmethod
    def effective_snapshot_chunk_size(
        opt: SearchResultSnapshotOptions | None,
        spec: SearchResultSnapshotSpec | None,
    ) -> int:
        if opt and "chunk_size" in opt:
            return max(1, int(opt["chunk_size"]))

        if spec is not None:
            return max(1, int(spec.chunk_size))

        return 5_000

    # ....................... #

    @staticmethod
    def effective_snapshot_ttl(
        opt: SearchResultSnapshotOptions | None,
        spec: SearchResultSnapshotSpec | None,
    ) -> timedelta:
        if opt and "ttl_seconds" in opt:
            return timedelta(seconds=max(1, int(opt["ttl_seconds"])))

        if spec is not None:
            return spec.ttl

        return timedelta(minutes=5)

    # ....................... #
    # Simple / hub fingerprints

    @staticmethod
    def simple_search_fingerprint(
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
        *,
        spec_name: str,
        variant: str,
        extras: dict[str, object] | None = None,
    ) -> str:
        if isinstance(query, (list, tuple)):
            qpart: object = [str(x) for x in query]

        else:
            qpart = str(query)

        payload: dict[str, object] = {
            "kind": "simple",
            "variant": variant,
            "spec": spec_name,
            "query": qpart,
            "filters": filters,
            "sorts": dict(sorts) if sorts is not None else None,
            "extras": dict(extras) if extras else None,
        }

        return _sha256_fingerprint_payload(payload)

    # ....................... #

    @staticmethod
    def hub_search_fingerprint(
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
        *,
        spec_name: str,
        members_weighted: list[tuple[str, float]],
        score_merge: str,
        combine: str,
    ) -> str:
        if isinstance(query, (list, tuple)):
            qpart: object = [str(x) for x in query]

        else:
            qpart = str(query)

        payload: dict[str, object] = {
            "kind": "hub",
            "hub": spec_name,
            "query": qpart,
            "filters": filters,
            "sorts": dict(sorts) if sorts is not None else None,
            "members": members_weighted,
            "score_merge": score_merge,
            "combine": combine,
        }

        return _sha256_fingerprint_payload(payload)

    # ....................... #

    @staticmethod
    def federated_fingerprint(
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
        *,
        spec_name: str,
        rrf_k: int,
    ) -> str:
        if isinstance(query, (list, tuple)):
            qpart: object = [str(x) for x in query]

        else:
            qpart = str(query)

        payload: dict[str, object] = {
            "federated": spec_name,
            "rrf_k": rrf_k,
            "query": qpart,
            "filters": filters,
            "sorts": dict(sorts) if sorts is not None else None,
        }
        body = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
        h = hashlib.sha256(body.encode("utf-8")).hexdigest()

        return f"sha256:{h}"

    # ....................... #
    # Record keys (serialized projection hits / federation partitions)

    @staticmethod
    def result_record_key_string(hit: BaseModel) -> str:
        return json.dumps(
            hit.model_dump(mode="json"), sort_keys=True, ensure_ascii=False
        )

    # ....................... #

    @staticmethod
    def hydrate_result_record_key(key: str, model_type: type[M_co]) -> M_co:
        data = json.loads(key)

        return model_type.model_validate(data)

    # ....................... #

    @staticmethod
    def federated_record_key_string(member: str, hit: BaseModel) -> str:
        payload = json.dumps(hit.model_dump(mode="json"), sort_keys=True)

        return f"{member}\0{payload}"

    # ....................... #

    @staticmethod
    def hydrate_federated_record_key(
        key: str,
        federated_spec: FederatedSearchSpec[M_co],
    ) -> FederatedSearchReadModel[M_co]:
        if "\0" not in key:
            raise CoreError(
                "Invalid federated snapshot record key (missing partition)."
            )

        member, rest = key.split("\0", 1)

        for sm in federated_spec.members:
            if sm.name != member:
                continue

            data = json.loads(rest)
            model: type[BaseModel] = sm.model_type
            hit = cast(M_co, model.model_validate(data))

            return FederatedSearchReadModel(hit=hit, member=member)

        raise CoreError(f"Unknown federated member in snapshot key: {member!r}.")

    # ....................... #
    # Federated RRF (merge ranked leg lists; keys match :meth:`federated_record_key_string`)

    @staticmethod
    def weighted_rrf_merge_rows(
        *,
        leg_rows: Sequence[tuple[str, Sequence[BaseModel], float]],
        k: int,
    ) -> list[tuple[FederatedSearchReadModel[Any], float]]:
        """Merge ranked hit lists with weighted reciprocal rank fusion (RRF).

        Each tuple is ``(member, hits in relevance order, member_weight)`` where
        ``member`` is the leg :class:`~forze.application.contracts.search.SearchSpec`
        ``name``. Legs with non-positive member weights are skipped. RRF
        contribution per hit is ``weight / (k + rank)`` with **1-based** ``rank``.
        Deduping uses the same string keys as snapshot storage
        (:meth:`federated_record_key_string`).
        """

        scores: dict[str, float] = {}
        models: dict[str, FederatedSearchReadModel[Any]] = {}

        for member, hits, weight in leg_rows:
            if weight <= 0.0:
                continue

            for rank, hit in enumerate(hits, start=1):
                key = SearchResultSnapshotCoordinator.federated_record_key_string(
                    member,
                    hit,
                )
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

    # ....................... #

    @staticmethod
    def federated_merged_hit_field(
        item: tuple[FederatedSearchReadModel[Any], float],
        *,
        field: str,
    ) -> Any:
        """Value of ``field`` on the merged hit (for stable secondary ``sorts``)."""

        return getattr(item[0].hit, field)

    # ....................... #

    @staticmethod
    def snapshot_pagination(
        want_snap: bool,
        max_ids: int,
        pagination: Mapping[str, Any] | None,
    ) -> tuple[int | None, int, int]:
        p = dict(pagination or {})
        limit = p.get("limit")

        user_offset = int(p.get("offset") or 0)

        page_limit = max(1, int(limit)) if limit is not None else 20

        if want_snap:

            return max(1, max_ids), 0, page_limit

        return (int(limit) if limit is not None else None, user_offset, page_limit)

    # ....................... #
    # Instance: projection (simple engine + hub homogeneous row model)

    async def read_simple_result_snapshot(
        self,
        *,
        rs_spec: SearchResultSnapshotSpec,
        snap_opt: SearchResultSnapshotOptions | None,
        fp_computed: str,
        spec: SearchSpec[Any],
        pagination: dict[str, Any] | None,
        return_type: type[T_co] | None,
        return_fields: Sequence[str] | None,
        return_count: bool,
    ) -> (
        Page[M_co]
        | CountlessPage[M_co]
        | Page[T_co]
        | CountlessPage[T_co]
        | Page[JsonDict]
        | CountlessPage[JsonDict]
        | None
    ):
        return await self._read_projection_snapshot_page(
            snap_opt=snap_opt,
            fp_computed=fp_computed,
            hydrate_as=spec.model_type,
            pagination=pagination,
            return_type=return_type,
            return_fields=return_fields,
            return_count=return_count,
        )

    # ....................... #

    async def read_hub_result_snapshot(
        self,
        *,
        rs_spec: SearchResultSnapshotSpec,
        snap_opt: SearchResultSnapshotOptions | None,
        fp_computed: str,
        model_type: type[M_co],
        pagination: dict[str, Any] | None,
        return_type: type[T_co] | None,
        return_fields: Sequence[str] | None,
        return_count: bool,
    ) -> (
        Page[M_co]
        | CountlessPage[M_co]
        | Page[T_co]
        | CountlessPage[T_co]
        | Page[JsonDict]
        | CountlessPage[JsonDict]
        | None
    ):
        return await self._read_projection_snapshot_page(
            snap_opt=snap_opt,
            fp_computed=fp_computed,
            hydrate_as=model_type,
            pagination=pagination,
            return_type=return_type,
            return_fields=return_fields,
            return_count=return_count,
        )

    # ....................... #

    async def _read_projection_snapshot_page(
        self,
        *,
        snap_opt: SearchResultSnapshotOptions | None,
        fp_computed: str,
        hydrate_as: type[M_co],
        pagination: dict[str, Any] | None,
        return_type: type[T_co] | None,
        return_fields: Sequence[str] | None,
        return_count: bool,
    ) -> (
        Page[M_co]
        | CountlessPage[M_co]
        | Page[T_co]
        | CountlessPage[T_co]
        | Page[JsonDict]
        | CountlessPage[JsonDict]
        | None
    ):
        if snap_opt is None or "id" not in snap_opt:
            return None

        sub_fp = str(snap_opt["fingerprint"]) if "fingerprint" in snap_opt else None
        pagination_d = dict(pagination or {})
        offset = int(pagination_d.get("offset") or 0)
        limit = pagination_d.get("limit")
        page_limit = max(1, int(limit)) if limit is not None else 20

        raw_keys = await self.store.get_id_range(
            str(snap_opt["id"]),
            offset,
            page_limit,
            expected_fingerprint=sub_fp,
        )

        if raw_keys is None:
            return None

        sm: SearchResultSnapshotMeta | None = await self.store.get_meta(
            str(snap_opt["id"])
        )
        total_snap = int(sm.total) if sm and sm.complete else offset + len(raw_keys)
        fp_h = (sm and sm.fingerprint) or fp_computed

        handle = SearchSnapshotHandle(
            id=str(snap_opt["id"]),
            fingerprint=fp_h,
            total=total_snap,
            capped=False,
        )
        hydrated: list[BaseModel] = [
            self.hydrate_result_record_key(k, hydrate_as) for k in raw_keys
        ]

        if return_type is not None:
            v = pydantic_validate_many(
                return_type, [h.model_dump(mode="json") for h in hydrated]
            )

            if return_count:
                return page_from_limit_offset(
                    v,
                    pagination_d,
                    total=total_snap,
                    snapshot=handle,
                )

            return page_from_limit_offset(
                v,
                pagination_d,
                total=None,
                snapshot=handle,
            )

        if return_fields is not None:
            raw = [{k: getattr(h, k, None) for k in return_fields} for h in hydrated]

            if return_count:

                return page_from_limit_offset(
                    raw,
                    pagination_d,
                    total=total_snap,
                    snapshot=handle,
                )

            return page_from_limit_offset(
                raw,
                pagination_d,
                total=None,
                snapshot=handle,
            )

        if return_count:

            return page_from_limit_offset(  # type: ignore[return-value]
                hydrated,
                pagination_d,
                total=total_snap,
                snapshot=handle,
            )

        return page_from_limit_offset(  # type: ignore[return-value]
            hydrated,
            pagination_d,
            total=None,
            snapshot=handle,
        )

    # ....................... #

    async def put_simple_ordered_hits(
        self,
        ordered_hits: Sequence[BaseModel],
        *,
        snap_opt: SearchResultSnapshotOptions | None,
        rs_spec: SearchResultSnapshotSpec,
        fp_computed: str,
        pool_len_before_cap: int,
    ) -> SearchSnapshotHandle:
        max_n = self.effective_snapshot_max_ids(snap_opt, rs_spec)
        to_store = list(ordered_hits)[:max_n]
        capped = pool_len_before_cap > len(to_store)
        run_id = str(uuid.uuid4())

        await self.store.put_run(
            run_id=run_id,
            fingerprint=fp_computed,
            ordered_ids=[self.result_record_key_string(h) for h in to_store],
            ttl=self.effective_snapshot_ttl(snap_opt, rs_spec),
            chunk_size=self.effective_snapshot_chunk_size(snap_opt, rs_spec),
        )

        return SearchSnapshotHandle(
            id=run_id,
            fingerprint=fp_computed,
            total=len(to_store),
            capped=capped,
        )

    # ....................... #

    async def put_ordered_snapshot_keys(
        self,
        ordered_ids: Sequence[str],
        *,
        snap_opt: SearchResultSnapshotOptions | None,
        rs_spec: SearchResultSnapshotSpec,
        fp_computed: str,
        pool_len_before_cap: int,
    ) -> SearchSnapshotHandle:
        max_n = self.effective_snapshot_max_ids(snap_opt, rs_spec)

        sliced = list(ordered_ids)[:max_n]

        capped = pool_len_before_cap > len(sliced)

        run_id = str(uuid.uuid4())

        await self.store.put_run(
            run_id=run_id,
            fingerprint=fp_computed,
            ordered_ids=sliced,
            ttl=self.effective_snapshot_ttl(snap_opt, rs_spec),
            chunk_size=self.effective_snapshot_chunk_size(snap_opt, rs_spec),
        )

        return SearchSnapshotHandle(
            id=run_id,
            fingerprint=fp_computed,
            total=len(sliced),
            capped=capped,
        )

    # ....................... #

    async def read_federated_snapshot_page_if_requested(
        self,
        *,
        federated_spec: FederatedSearchSpec[M_co],
        rs_spec: SearchResultSnapshotSpec | None,
        snapshot: SearchResultSnapshotOptions | None,
        fp_computed: str,
        pagination: Mapping[str, Any] | None,
        return_type: type[T_co] | None,
        return_count: bool,
    ) -> (
        CountlessPage[FederatedSearchReadModel[M_co]]
        | CountlessPage[T_co]
        | Page[FederatedSearchReadModel[M_co]]
        | Page[T_co]
        | None
    ):
        pagination_d = dict(pagination or {})
        offset = int(pagination_d.get("offset") or 0)
        limit = pagination_d.get("limit")
        page_limit = max(1, int(limit)) if limit is not None else 20

        if rs_spec is None or snapshot is None or "id" not in snapshot:
            return None

        sub_fp = str(snapshot["fingerprint"]) if "fingerprint" in snapshot else None

        raw_keys = await self.store.get_id_range(
            str(snapshot["id"]),
            offset,
            page_limit,
            expected_fingerprint=sub_fp,
        )

        if raw_keys is None:
            return None

        sm = await self.store.get_meta(str(snapshot["id"]))
        total_snap = int(sm.total) if sm and sm.complete else offset + len(raw_keys)

        fp_h = (sm and sm.fingerprint) or fp_computed

        handle = SearchSnapshotHandle(
            id=str(snapshot["id"]),
            fingerprint=fp_h,
            total=total_snap,
            capped=False,
        )

        hydrated = [
            self.hydrate_federated_record_key(k, federated_spec) for k in raw_keys
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
                    pagination_d,
                    total=total_snap,
                    snapshot=handle,
                )

            return page_from_limit_offset(
                v2,
                pagination_d,
                total=None,
                snapshot=handle,
            )

        if return_count:

            return page_from_limit_offset(
                hydrated,
                pagination_d,
                total=total_snap,
                snapshot=handle,
            )

        return page_from_limit_offset(
            hydrated,
            pagination_d,
            total=None,
            snapshot=handle,
        )

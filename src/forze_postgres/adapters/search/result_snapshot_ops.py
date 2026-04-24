"""Result-ID snapshot read/write for simple (single-index) and hub search adapters."""

import hashlib
import json
import uuid
from typing import Any, Mapping, Sequence, TypeVar

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
    SearchResultSnapshotMeta,
    SearchResultSnapshotOptions,
    SearchResultSnapshotPort,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many

from .federated_snapshot import (
    effective_snapshot_chunk_size,
    effective_snapshot_max_ids,
    effective_snapshot_ttl,
    should_write_federated_snapshot,
)

# ----------------------- #

M = TypeVar("M", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)

# ....................... #


def should_write_result_snapshot(
    result_snapshot: SearchResultSnapshotOptions | None,
    rs_spec: SearchResultSnapshotSpec | None,
) -> bool:
    """Whether to materialize a new snapshot (same rules as federated)."""

    return should_write_federated_snapshot(result_snapshot, rs_spec)


# ....................... #


def result_row_key_string(hit: BaseModel) -> str:
    """Stable serialized identity for a search row (single read model, no leg prefix)."""

    return json.dumps(hit.model_dump(mode="json"), sort_keys=True, ensure_ascii=False)


# ....................... #


def hydrate_result_row_key(key: str, model_type: type[M]) -> M:
    """Rebuild a Pydantic model from :func:`result_row_key_string`."""

    data = json.loads(key)
    return model_type.model_validate(data)


# ....................... #


def _sha256_fingerprint_payload(payload: dict[str, object]) -> str:
    body = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    h = hashlib.sha256(body.encode("utf-8")).hexdigest()

    return f"sha256:{h}"


# ....................... #


def simple_search_fingerprint(
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    *,
    spec_name: str,
    variant: str,
    extras: dict[str, object] | None = None,
) -> str:
    """Fingerprint for simple (per-engine) search; include ``variant`` and engine-specific ``extras``."""

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
    """Fingerprint for hub search including resolved member weights and merge options."""

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


def snapshot_sql_pagination(
    want_snap: bool,
    max_ids: int,
    pagination: Mapping[str, Any] | None,
) -> tuple[int | None, int, int]:
    """``(sql_limit, sql_offset, page_limit)`` for data query vs in-memory page window."""
    p = dict(pagination or {})
    limit = p.get("limit")
    user_offset = int(p.get("offset") or 0)
    page_limit = max(1, int(limit)) if limit is not None else 20
    if want_snap:
        return max(1, max_ids), 0, page_limit
    return (int(limit) if limit is not None else None, user_offset, page_limit)


# ....................... #


async def read_simple_result_snapshot(
    *,
    store: SearchResultSnapshotPort,
    rs_spec: SearchResultSnapshotSpec,
    snap_opt: SearchResultSnapshotOptions | None,
    fp_computed: str,
    spec: SearchSpec[Any],
    pagination: dict[str, Any] | None,
    return_type: type[T] | None,
    return_fields: Sequence[str] | None,
    return_count: bool,
) -> (
    Page[M]
    | CountlessPage[M]
    | Page[T]
    | CountlessPage[T]
    | Page[JsonDict]
    | CountlessPage[JsonDict]
    | None
):
    """Serve a page from KV when the client provides ``result_snapshot`` ``id``; else ``None``."""
    if snap_opt is None or "id" not in snap_opt:
        return None

    if "fingerprint" in snap_opt:
        sub_fp = str(snap_opt["fingerprint"])

    else:
        sub_fp = None

    pagination_d = dict(pagination or {})
    offset = int(pagination_d.get("offset") or 0)
    limit = pagination_d.get("limit")
    page_limit = max(1, int(limit)) if limit is not None else 20

    raw_keys = await store.get_id_range(
        str(snap_opt["id"]),
        offset,
        page_limit,
        expected_fingerprint=sub_fp,
    )

    if raw_keys is None:
        return None

    sm: SearchResultSnapshotMeta | None = await store.get_meta(str(snap_opt["id"]))
    total_snap = int(sm.total) if sm and sm.complete else offset + len(raw_keys)
    fp_h = (sm and sm.fingerprint) or fp_computed

    handle = SearchSnapshotHandle(
        id=str(snap_opt["id"]),
        fingerprint=fp_h,
        total=total_snap,
        capped=False,
    )
    hydrated: list[BaseModel] = [
        hydrate_result_row_key(k, spec.model_type) for k in raw_keys
    ]

    if return_type is not None:
        v = pydantic_validate_many(
            return_type, [h.model_dump(mode="json") for h in hydrated]
        )

        if return_count:
            return page_from_limit_offset(
                v, pagination_d, total=total_snap, result_snapshot=handle
            )

        return page_from_limit_offset(
            v, pagination_d, total=None, result_snapshot=handle
        )

    if return_fields is not None:
        raw = [{k: getattr(h, k, None) for k in return_fields} for h in hydrated]

        if return_count:
            return page_from_limit_offset(
                raw, pagination_d, total=total_snap, result_snapshot=handle
            )

        return page_from_limit_offset(
            raw, pagination_d, total=None, result_snapshot=handle
        )

    if return_count:
        return page_from_limit_offset(  # type: ignore[return-value]
            hydrated, pagination_d, total=total_snap, result_snapshot=handle
        )

    return page_from_limit_offset(  # type: ignore[return-value]
        hydrated, pagination_d, total=None, result_snapshot=handle
    )


# ....................... #


async def read_hub_result_snapshot(
    *,
    store: SearchResultSnapshotPort,
    rs_spec: SearchResultSnapshotSpec,
    snap_opt: SearchResultSnapshotOptions | None,
    fp_computed: str,
    model_type: type[M],
    pagination: dict[str, Any] | None,
    return_type: type[T] | None,
    return_fields: Sequence[str] | None,
    return_count: bool,
) -> (
    Page[M]
    | CountlessPage[M]
    | Page[T]
    | CountlessPage[T]
    | Page[JsonDict]
    | CountlessPage[JsonDict]
    | None
):
    """Hub read path: same as simple, using the hub's homogeneous ``model_type``."""
    if snap_opt is None or "id" not in snap_opt:
        return None

    if "fingerprint" in snap_opt:
        sub_fp = str(snap_opt["fingerprint"])

    else:
        sub_fp = None

    pagination_d = dict(pagination or {})
    offset = int(pagination_d.get("offset") or 0)
    limit = pagination_d.get("limit")
    page_limit = max(1, int(limit)) if limit is not None else 20

    raw_keys = await store.get_id_range(
        str(snap_opt["id"]),
        offset,
        page_limit,
        expected_fingerprint=sub_fp,
    )
    if raw_keys is None:
        return None

    sm = await store.get_meta(str(snap_opt["id"]))
    total_snap = int(sm.total) if sm and sm.complete else offset + len(raw_keys)
    fp_h = (sm and sm.fingerprint) or fp_computed
    handle = SearchSnapshotHandle(
        id=str(snap_opt["id"]),
        fingerprint=fp_h,
        total=total_snap,
        capped=False,
    )
    hydrated = [hydrate_result_row_key(k, model_type) for k in raw_keys]

    if return_type is not None:
        v = pydantic_validate_many(
            return_type, [h.model_dump(mode="json") for h in hydrated]
        )
        if return_count:
            return page_from_limit_offset(
                v, pagination_d, total=total_snap, result_snapshot=handle
            )

        return page_from_limit_offset(
            v, pagination_d, total=None, result_snapshot=handle
        )

    if return_fields is not None:
        raw = [{k: getattr(h, k, None) for k in return_fields} for h in hydrated]
        if return_count:
            return page_from_limit_offset(
                raw, pagination_d, total=total_snap, result_snapshot=handle
            )

        return page_from_limit_offset(
            raw, pagination_d, total=None, result_snapshot=handle
        )

    if return_count:
        return page_from_limit_offset(  # type: ignore[return-value]
            hydrated, pagination_d, total=total_snap, result_snapshot=handle
        )

    return page_from_limit_offset(  # type: ignore[return-value]
        hydrated, pagination_d, total=None, result_snapshot=handle
    )


# ....................... #


async def put_simple_result_snapshot(
    store: SearchResultSnapshotPort,
    ordered_hits: Sequence[BaseModel],
    *,
    snap_opt: SearchResultSnapshotOptions | None,
    rs_spec: SearchResultSnapshotSpec,
    fp_computed: str,
    pool_len_before_cap: int,
) -> SearchSnapshotHandle:
    """Store ordered row keys; ``pool_len_before_cap`` is the in-memory pool size (``<= max_ids``)."""
    max_n = effective_snapshot_max_ids(snap_opt, rs_spec)
    to_store = list(ordered_hits)[:max_n]
    capped = pool_len_before_cap > len(to_store)
    run_id = str(uuid.uuid4())
    await store.put_run(
        run_id=run_id,
        fingerprint=fp_computed,
        ordered_ids=[result_row_key_string(h) for h in to_store],
        ttl=effective_snapshot_ttl(snap_opt, rs_spec),
        chunk_size=effective_snapshot_chunk_size(snap_opt, rs_spec),
    )
    return SearchSnapshotHandle(
        id=run_id,
        fingerprint=fp_computed,
        total=len(to_store),
        capped=capped,
    )

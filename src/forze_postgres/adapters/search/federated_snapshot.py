"""Federated (RRF) result snapshot: fingerprint, policy, and key hydration."""

import hashlib
import json
from datetime import timedelta
from typing import Sequence, cast

from pydantic import BaseModel

from forze.application.contracts.query import (
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    FederatedSearchReadModel,
    FederatedSearchSpec,
    SearchResultSnapshotOptions,
    SearchResultSnapshotSpec,
)
from forze.base.errors import CoreError

# ----------------------- #


def federated_row_key_string(member: str, hit: BaseModel) -> str:
    """Match :func:`federated.federated` row identity (``member`` + stable JSON of ``hit``)."""

    payload = json.dumps(hit.model_dump(mode="json"), sort_keys=True)

    return f"{member}\0{payload}"


# ....................... #


def federated_fingerprint(
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    *,
    spec_name: str,
    rrf_k: int,
) -> str:
    """Deterministic request fingerprint for snapshot validation and cache keys."""

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


def should_write_federated_snapshot(
    result_snapshot: SearchResultSnapshotOptions | None,
    rs_spec: SearchResultSnapshotSpec | None,
) -> bool:
    """Return whether a new KV snapshot should be materialized for this request."""

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


def hydrate_federated_row_key[M: BaseModel](
    key: str,
    federated_spec: FederatedSearchSpec[M],
) -> FederatedSearchReadModel[M]:
    """Rebuild :class:`FederatedSearchReadModel` from a stored RRF key string."""

    if "\0" not in key:
        raise CoreError("Invalid federated snapshot row key (missing partition).")

    member, rest = key.split("\0", 1)

    for sm in federated_spec.members:
        if sm.name != member:
            continue

        data = json.loads(rest)
        model: type[BaseModel] = sm.model_type
        hit = cast(M, model.model_validate(data))

        return FederatedSearchReadModel(hit=hit, member=member)

    raise CoreError(f"Unknown federated member in snapshot key: {member!r}.")

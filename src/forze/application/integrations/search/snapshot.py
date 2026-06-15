"""Coordinated KV reads/writes for search result snapshots plus federated RRF merge.

Adapters delegate fingerprinting, policy, pagination windows, hydration,
federated rank fusion, and ``SearchResultSnapshotPort`` access here.
"""

from __future__ import annotations

import base64
import binascii
import json
import uuid
from collections.abc import Callable
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
from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.querying import (
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import payload_aad
from forze.application.contracts.search import (
    FederatedSearchReadModel,
    FederatedSearchSpec,
    SearchResultSnapshotMeta,
    SearchResultSnapshotOptions,
    SearchResultSnapshotPort,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, stable_payload_fingerprint
from forze.base.serialization import default_model_codec

# ----------------------- #

M_co = TypeVar("M_co", bound=BaseModel)
T_co = TypeVar("T_co", bound=BaseModel)

SNAPSHOT_PAYLOAD_DOMAIN = "search.snapshot"
"""AAD domain isolating snapshot-record ciphertext from other contexts."""

_SEALED_PREFIX = "\x01fz.snap:"
"""Sentinel marking a sealed record key. A plaintext key is model JSON (``{``) or a
``member\\0json`` federated key, so it never starts with this control byte — letting reads
tell sealed from legacy-plaintext records during a zero-downtime rollout."""

# ....................... #


def _sha256_fingerprint_payload(payload: dict[str, object]) -> str:
    return stable_payload_fingerprint(payload)


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
class SearchResultSnapshot:
    """Coordinates ordered search-result snapshots for ranked adapters."""

    store: SearchResultSnapshotPort
    """KV backend for snapshot runs."""

    cipher: BytesCipherPort | None = attrs.field(default=None, repr=False)
    """Keyring sealing the stored record models at rest — set only when the search route
    field-encrypts, so the snapshot store does not re-expose what the document sealed.
    ``None`` keeps record keys plaintext (the default, unencrypted routes)."""

    cipher_tenant: Callable[[], TenantIdentity | None] | None = attrs.field(
        default=None, repr=False
    )
    """Resolver for the bound tenant, anchoring the per-record AAD to ``cipher``."""

    # ....................... #

    async def _seal_ids(self, ids: list[str], *, run_id: str) -> list[str]:
        """Seal each record key under ``(tenant, run_id)`` so the store holds no plaintext.

        Whole-key sealing (the key is opaque model JSON) keeps list order — encryption is
        per-position — so re-pagination over the sealed run is unchanged. A no-op without a
        cipher.
        """

        if self.cipher is None:
            return ids

        tenant = self.cipher_tenant() if self.cipher_tenant is not None else None
        aad = payload_aad(
            SNAPSHOT_PAYLOAD_DOMAIN,
            tenant.tenant_id if tenant is not None else None,
            run_id,
        )

        sealed: list[str] = []
        for raw in ids:
            blob = await self.cipher.encrypt(raw.encode("utf-8"), tenant=tenant, aad=aad)
            sealed.append(_SEALED_PREFIX + base64.b64encode(blob).decode("ascii"))

        return sealed

    # ....................... #

    async def _open_ids(self, ids: Sequence[str], *, run_id: str) -> list[str]:
        """Open record keys sealed by :meth:`_seal_ids`; pass legacy plaintext through.

        Fail-closed: a sealed key with no cipher wired is a misconfiguration (the key cannot
        be opened), so it raises rather than handing back ciphertext.
        """

        if not any(k.startswith(_SEALED_PREFIX) for k in ids):
            return list(ids)

        if self.cipher is None:
            raise exc.configuration(
                "Search snapshot holds sealed records but no keyring is wired to open them. "
                "Register a CryptoDepsModule or clear the route's encryption.",
                code="core.search.snapshot_encryption_wiring",
            )

        tenant = self.cipher_tenant() if self.cipher_tenant is not None else None
        aad = payload_aad(
            SNAPSHOT_PAYLOAD_DOMAIN,
            tenant.tenant_id if tenant is not None else None,
            run_id,
        )

        opened: list[str] = []
        for key in ids:
            if not key.startswith(_SEALED_PREFIX):
                opened.append(key)  # legacy plaintext record, replayed as-is
                continue

            try:
                blob = base64.b64decode(key[len(_SEALED_PREFIX) :], validate=True)
            except (binascii.Error, ValueError) as error:
                raise exc.validation(
                    "Sealed snapshot record key is not valid base64",
                    code="core.search.snapshot_base64_invalid",
                ) from error

            raw = await self.cipher.decrypt(blob, aad=aad)
            opened.append(raw.decode("utf-8"))

        return opened

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

        return max(1, int(spec.max_ids)) if spec is not None else 50_000

    # ....................... #

    @staticmethod
    def effective_snapshot_chunk_size(
        opt: SearchResultSnapshotOptions | None,
        spec: SearchResultSnapshotSpec | None,
    ) -> int:
        if opt and "chunk_size" in opt:
            return max(1, int(opt["chunk_size"]))

        return max(1, int(spec.chunk_size)) if spec is not None else 5_000

    # ....................... #

    @staticmethod
    def effective_snapshot_ttl(
        opt: SearchResultSnapshotOptions | None,
        spec: SearchResultSnapshotSpec | None,
    ) -> timedelta:
        if opt and "ttl_seconds" in opt:
            return timedelta(seconds=max(1, int(opt["ttl_seconds"])))

        return spec.ttl if spec is not None else timedelta(minutes=5)

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
        extras: JsonDict | None = None,
    ) -> str:
        qpart: list[str] | str

        if isinstance(query, (list, tuple)):
            qpart = [str(x) for x in query]

        else:
            qpart = str(query)

        payload: JsonDict = {
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
        per_leg_limit: int | None = None,
        execution: str | None = None,
        combo_limit: int | None = None,
        search_count: str | None = None,
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
            "per_leg_limit": per_leg_limit,
        }

        if execution is not None:
            payload["execution"] = execution

        if combo_limit is not None:
            payload["combo_limit"] = combo_limit

        if search_count is not None:
            payload["search_count"] = search_count

        return _sha256_fingerprint_payload(payload)

    # ....................... #

    @staticmethod
    def federated_fingerprint(
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
        *,
        spec_name: str,
        rrf_k: int | None = None,
        extras: Mapping[str, object] | None = None,
    ) -> str:
        if isinstance(query, (list, tuple)):
            qpart: object = [str(x) for x in query]

        else:
            qpart = str(query)

        payload: dict[str, object] = {
            "federated": spec_name,
            "query": qpart,
            "filters": filters,
            "sorts": dict(sorts) if sorts is not None else None,
        }

        if rrf_k is not None:
            payload["rrf_k"] = rrf_k

        if extras:
            payload |= dict(extras)

        return stable_payload_fingerprint(payload)

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
            raise exc.internal(
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

        raise exc.internal(f"Unknown federated member in snapshot key: {member!r}.")

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
                key = SearchResultSnapshot.federated_record_key_string(
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
        page_limit = max(1, int(limit)) if limit is not None else 20

        if want_snap:
            return max(1, max_ids), 0, page_limit

        user_offset = int(p.get("offset") or 0)

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

        raw_keys = await self._open_ids(raw_keys, run_id=str(snap_opt["id"]))

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
            v = default_model_codec(return_type).decode_mapping_many(
                [h.model_dump(mode="json") for h in hydrated]
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
            ordered_ids=await self._seal_ids(
                [self.result_record_key_string(h) for h in to_store],
                run_id=run_id,
            ),
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
            ordered_ids=await self._seal_ids(sliced, run_id=run_id),
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

        raw_keys = await self._open_ids(raw_keys, run_id=str(snapshot["id"]))

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
            v2 = default_model_codec(return_type).decode_mapping_many(rows2)

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

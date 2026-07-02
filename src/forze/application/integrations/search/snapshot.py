"""Coordinated KV reads/writes for search result snapshots plus federated RRF merge.

Adapters delegate fingerprinting, policy, pagination windows, hydration,
federated rank fusion, and ``SearchResultSnapshotPort`` access here.
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Awaitable, Callable, Iterable
from datetime import timedelta
from functools import cmp_to_key
from typing import Any, Mapping, Sequence, TypeVar, cast

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCountlessPage,
    SearchPage,
    SearchSnapshotHandle,
    search_page_from_limit_offset,
)
from forze.application.contracts.crypto import KeyringPort
from forze.application.contracts.querying import (
    QueryFilterExpression,
    QuerySortExpression,
    ordered_compare,
    parse_sort_value,
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
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import payload_aad
from forze.base.crypto import unpack_envelope
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import (
    MISSING,
    JsonDict,
    path_get,
    stable_payload_fingerprint,
    utcnow,
    uuid4,
)
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

_CIPHER_NOT_WARM = "core.crypto.cipher_not_warm"
"""Raised by the sync cipher when its data key rotated out from under a warmed batch."""

# ....................... #


def _shape_snapshot_page(
    items: Sequence[Any],
    *,
    pagination: Mapping[str, Any],
    total: int | None,
    snapshot: SearchSnapshotHandle,
    return_type: type[BaseModel] | None,
    to_return_rows: Callable[[Sequence[Any]], list[JsonDict]],
    return_fields: Sequence[str] | None = None,
) -> Any:
    """Shape a hydrated snapshot page into the requested form, then paginate.

    Collapses the ``return_type / return_fields / plain`` × ``return_count`` cascade both
    snapshot read paths shared into one place. ``total`` is already resolved (``None`` when
    no count was requested); ``to_return_rows`` builds the mapping rows for the
    ``return_type`` case — the only thing the projection and federated paths differ on
    (a hydrated model vs. a ``{hit, member}`` pair).
    """

    if return_type is not None:
        decoded = default_model_codec(return_type).decode_mapping_many(
            to_return_rows(items)
        )

        return search_page_from_limit_offset(
            decoded,
            pagination,
            total=total,
            snapshot=snapshot,
        )

    if return_fields is not None:
        raw = [{k: getattr(it, k, None) for k in return_fields} for it in items]

        return search_page_from_limit_offset(
            raw,
            pagination,
            total=total,
            snapshot=snapshot,
        )

    return search_page_from_limit_offset(
        cast(list[Any], items),
        pagination,
        total=total,
        snapshot=snapshot,
    )


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

    cipher: KeyringPort | None = attrs.field(default=None, repr=False)
    """Keyring sealing the stored record models at rest — set only when the search route
    field-encrypts, so the snapshot store does not re-expose what the document sealed.
    ``None`` keeps record keys plaintext (the default, unencrypted routes)."""

    cipher_tenant: Callable[[], TenantIdentity | None] | None = attrs.field(
        default=None,
        repr=False,
    )
    """Resolver for the bound tenant, anchoring the per-record AAD to ``cipher``."""

    # ....................... #

    async def _seal_ids(self, ids: list[str], *, run_id: str) -> list[str]:
        """Seal each record key under ``(tenant, run_id)`` so the store holds no plaintext.

        Whole-key sealing (the key is opaque model JSON) keeps list order — encryption is
        per-position — so re-pagination over the sealed run is unchanged. A no-op without a
        cipher. The data key is **warmed once** for the whole run, then each key is sealed
        with the no-await sync cipher (the same batch path the field codec uses): one key
        resolution and zero per-item awaits/locks instead of one async round per record.
        """

        if self.cipher is None:
            return ids

        tenant = self.cipher_tenant() if self.cipher_tenant is not None else None
        aad = payload_aad(
            SNAPSHOT_PAYLOAD_DOMAIN,
            tenant.tenant_id if tenant is not None else None,
            run_id,
        )

        cipher = self.cipher
        await cipher.warm(tenant)

        sealed: list[str] = []

        for raw in ids:
            blob = await self._encrypt_one(cipher, raw.encode("utf-8"), tenant, aad)
            sealed.append(_SEALED_PREFIX + base64.b64encode(blob).decode("ascii"))

        return sealed

    # ....................... #

    @staticmethod
    async def _encrypt_one(
        cipher: KeyringPort,
        plaintext: bytes,
        tenant: TenantIdentity | None,
        aad: bytes,
    ) -> bytes:
        """Sync-encrypt against the warmed key, re-warming once if the key rotated mid-run.

        A run never exhausts a data key in practice (``max_dek_messages`` ≫ the snapshot
        cap), but if a shared key tips over its budget mid-loop the sync path raises
        ``cipher_not_warm``; one re-warm mints the next key and the encrypt retries.
        """

        try:
            return cipher.encrypt_sync(plaintext, tenant=tenant, aad=aad)

        except CoreException as error:
            if error.code != _CIPHER_NOT_WARM:
                raise

            await cipher.warm(tenant)
            return cipher.encrypt_sync(plaintext, tenant=tenant, aad=aad)

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

        # Decode every sealed blob first (None marks a passed-through plaintext position),
        # warm the decrypt cache for all their data keys in one pass (a run reuses only a
        # handful of distinct keys), then sync-decrypt with no per-record awaits.
        blobs: list[bytes | None] = []
        for key in ids:
            if not key.startswith(_SEALED_PREFIX):
                blobs.append(None)
                continue

            try:
                blobs.append(
                    base64.b64decode(key[len(_SEALED_PREFIX) :], validate=True)
                )

            except (binascii.Error, ValueError) as error:
                raise exc.validation(
                    "Sealed snapshot record key is not valid base64",
                    code="core.search.snapshot_base64_invalid",
                ) from error

        await self.cipher.ensure_unwrapped(
            unpack_envelope(blob) for blob in blobs if blob is not None
        )

        opened: list[str] = []

        for key, blob in zip(ids, blobs, strict=True):
            if blob is None:
                opened.append(key)  # legacy plaintext record, replayed as-is

            else:
                opened.append(self.cipher.decrypt_sync(blob, aad=aad).decode("utf-8"))

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
    def federated_thin_record_key(member: str, record_id: str) -> str:
        """``member \\0 id`` — the thin federated snapshot key (no full record).

        The :attr:`~forze.application.contracts.search.FederatedSearchSpec.thin_merge`
        snapshot stores these instead of the full-record key, so the snapshot is tiny
        and the merge never holds full hits; replay re-fetches the page's hits by id
        from each member."""

        return f"{member}\0{record_id}"

    # ....................... #

    @staticmethod
    def parse_federated_thin_record_key(key: str) -> tuple[str, str]:
        """Split a thin federated key into ``(member, id)``."""

        if "\0" not in key:
            raise exc.internal(
                "Invalid thin federated snapshot key (missing partition)."
            )

        member, record_id = key.split("\0", 1)

        return member, record_id

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
    def weighted_rrf_merge_ids(
        *,
        leg_rows: Sequence[tuple[str, Sequence[str], float]],
        k: int,
    ) -> list[tuple[str, str, float]]:
        """Thin (id-only) weighted RRF: fuse ordered ``(member, ids, weight)`` legs.

        The late-materialization counterpart of :meth:`weighted_rrf_merge_rows`: it
        fuses on the ``(member, id)`` identity instead of the full-record key, so the
        caller never has to hold the full hits to merge. Each leg's ``ids`` are in
        relevance order (rank = 1-based position); the contribution per hit is
        ``weight / (k + rank)``. Returns ``(member, id, score)`` ordered by descending
        score, breaking ties by ``(member, id)`` (deterministic; the full-record path's
        tie-break by serialized record differs only among identical-score hits).
        """

        scores: dict[tuple[str, str], float] = {}

        for member, ids, weight in leg_rows:
            if weight <= 0.0:
                continue

            for rank, rid in enumerate(ids, start=1):
                key = (member, rid)
                scores[key] = scores.get(key, 0.0) + float(weight) / (
                    float(k) + float(rank)
                )

        ordered = sorted(scores.keys(), key=lambda kk: (-scores[kk], kk[0], kk[1]))

        return [(member, rid, scores[(member, rid)]) for member, rid in ordered]

    # ....................... #

    @staticmethod
    def order_federated_full_merge(
        merged: list[tuple[FederatedSearchReadModel[Any], float]],
        sorts: QuerySortExpression | None,
    ) -> None:
        """Order a full-record RRF merge in place: RRF score primary, ``sorts`` tie-break.

        Reads each (dotted) ``sorts`` value off the hit via a one-per-hit ``model_dump`` +
        :func:`path_get`, so a nested key resolves identically to the thin path's reads over
        the projected dict. An absent path reads as ``None`` (same as thin)."""

        dumped: dict[int, JsonDict] = {}

        def _value_of(
            item: tuple[FederatedSearchReadModel[Any], float],
            field: str,
        ) -> Any:
            doc = dumped.get(id(item))

            if doc is None:
                doc = item[0].hit.model_dump(mode="python")
                dumped[id(item)] = doc

            value = path_get(doc, field)

            return None if value is MISSING else value

        SearchResultSnapshot.order_federated_secondary_sorts(
            merged,
            sorts,
            value_of=_value_of,
            score_of=lambda item: -item[1],
        )

    # ....................... #

    @staticmethod
    def order_federated_secondary_sorts[Item](
        merged: list[Item],
        sorts: QuerySortExpression | None,
        *,
        value_of: Callable[[Item, str], Any],
        score_of: Callable[[Item], Any],
    ) -> None:
        """Order a merged federated result in place: RRF score primary, ``sorts`` tie-break.

        Shared by the full-fetch and thin (id-only) paths so both produce identical order.
        The ``sorts`` fields are applied least-significant-first and the fused score last, so
        (stable sort) the RRF score dominates and each ``sorts`` field only breaks ties among
        equal-score hits. ``value_of``/``score_of`` read from whatever tuple shape the caller
        holds (full-record ``(model, score)`` vs. thin ``(member, id, score)``).
        """

        if sorts:
            for field, sort_value in reversed(list(sorts.items())):
                # Resolve the shorthand (``"desc"``) or explicit (``{"dir","nulls"}``) spec, then
                # order via the canonical keyset comparator: it honors the requested null
                # placement absolutely (``nulls`` first/last, defaulting first-asc/last-desc),
                # flips only the non-null comparison by direction, and turns a cross-type/``None``
                # comparison into a validation error rather than a raw ``TypeError`` — the same
                # order every backend conforms to, so full-fetch, thin, and mock paths agree.
                direction, nulls = parse_sort_value(sort_value)

                def _cmp(
                    a: Item,
                    b: Item,
                    field: str = field,
                    direction: str = direction,
                    nulls: str = nulls,
                ) -> int:
                    return ordered_compare(
                        value_of(a, field),
                        value_of(b, field),
                        direction=direction,
                        nulls=nulls,
                    )

                merged.sort(key=cmp_to_key(_cmp))

        merged.sort(key=score_of)

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
        SearchPage[M_co]
        | SearchCountlessPage[M_co]
        | SearchPage[T_co]
        | SearchCountlessPage[T_co]
        | SearchPage[JsonDict]
        | SearchCountlessPage[JsonDict]
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
        SearchPage[M_co]
        | SearchCountlessPage[M_co]
        | SearchPage[T_co]
        | SearchCountlessPage[T_co]
        | SearchPage[JsonDict]
        | SearchCountlessPage[JsonDict]
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
        SearchPage[M_co]
        | SearchCountlessPage[M_co]
        | SearchPage[T_co]
        | SearchCountlessPage[T_co]
        | SearchPage[JsonDict]
        | SearchCountlessPage[JsonDict]
        | None
    ):
        return await self._read_snapshot_page(
            snap_opt,
            fp_computed=fp_computed,
            pagination=pagination,
            hydrate=lambda k: self.hydrate_result_record_key(k, hydrate_as),
            to_return_rows=lambda items: [h.model_dump(mode="json") for h in items],
            return_type=return_type,
            return_fields=return_fields,
            return_count=return_count,
        )

    # ....................... #

    async def _read_snapshot_page(
        self,
        snapshot_opts: SearchResultSnapshotOptions | None,
        *,
        fp_computed: str,
        pagination: Mapping[str, Any] | None,
        hydrate: Callable[[str], BaseModel],
        to_return_rows: Callable[[Sequence[Any]], list[JsonDict]],
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
        return_count: bool,
    ) -> Any:
        """Read one snapshot page: window → open ids → meta → hydrate → shape.

        The shared body behind the projection and federated snapshot reads; the two differ
        only in how a record key hydrates (*hydrate*) and how a hydrated item becomes a
        ``return_type`` row (*to_return_rows*). Returns ``None`` when the snapshot is absent
        or its id range cannot be served (stale/expired run).
        """

        window = await self._open_snapshot_window(
            snapshot_opts, fp_computed=fp_computed, pagination=pagination
        )

        if window is None:
            return None

        raw_keys, handle, total_snap, pagination_d = window

        return _shape_snapshot_page(
            [hydrate(k) for k in raw_keys],
            pagination=pagination_d,
            total=total_snap if return_count else None,
            snapshot=handle,
            return_type=return_type,
            return_fields=return_fields,
            to_return_rows=to_return_rows,
        )

    # ....................... #

    async def _open_snapshot_window(
        self,
        snapshot_opts: SearchResultSnapshotOptions | None,
        *,
        fp_computed: str,
        pagination: Mapping[str, Any] | None,
    ) -> tuple[list[str], SearchSnapshotHandle, int, dict[str, Any]] | None:
        """Resolve a snapshot read window: the page's ordered record keys + handle.

        Shared by the sync-hydrate read (:meth:`_read_snapshot_page`) and the async
        re-fetch read (:meth:`read_federated_thin_snapshot_page_if_requested`). Returns
        ``None`` when the snapshot is absent or its id range cannot be served.
        """

        if snapshot_opts is None or "id" not in snapshot_opts:
            return None

        run_id = str(snapshot_opts["id"])
        pagination_d = dict(pagination or {})
        offset = int(pagination_d.get("offset") or 0)
        limit = pagination_d.get("limit")
        page_limit = max(1, int(limit)) if limit is not None else 20

        # Bind the stored snapshot to *this* request's server-computed fingerprint,
        # not the client-supplied one: a caller passing a stale snapshot id (or no
        # fingerprint at all) must not replay another request's results. A mismatch
        # returns ``None`` and the caller recomputes live.
        raw_keys = await self.store.get_id_range(
            run_id, offset, page_limit, expected_fingerprint=fp_computed
        )

        if raw_keys is None:
            return None

        raw_keys = await self._open_ids(raw_keys, run_id=run_id)

        sm: SearchResultSnapshotMeta | None = await self.store.get_meta(run_id)
        total_snap = int(sm.total) if sm and sm.complete else offset + len(raw_keys)
        fp_h = (sm and sm.fingerprint) or fp_computed

        handle = SearchSnapshotHandle(
            id=run_id,
            fingerprint=fp_h,
            total=total_snap,
            capped=False,
            expires_at=sm.expires_at if sm else None,
        )

        return raw_keys, handle, total_snap, pagination_d

    # ....................... #

    def open_simple_hit_sink(
        self,
        *,
        snap_opt: SearchResultSnapshotOptions | None,
        rs_spec: SearchResultSnapshotSpec,
        fp_computed: str,
    ) -> _OrderedHitSink:
        """Open a streaming sink that writes ordered record keys one chunk at a time.

        Callers feed record keys incrementally and the sink seals + appends them in
        ``chunk_size`` blocks, so peak memory is one chunk regardless of ``max_ids`` — the
        ranked offset/PGroonga paths drive it from a windowed pool fetch, the federated path
        from the in-memory RRF merge via :meth:`put_ordered_snapshot_keys`.
        """

        ttl = self.effective_snapshot_ttl(snap_opt, rs_spec)

        return _OrderedHitSink(
            coordinator=self,
            run_id=str(uuid4()),
            fingerprint=fp_computed,
            chunk_size=self.effective_snapshot_chunk_size(snap_opt, rs_spec),
            max_ids=self.effective_snapshot_max_ids(snap_opt, rs_spec),
            ttl=ttl,
            expires_at=int(utcnow().timestamp()) + int(ttl.total_seconds()),
        )

    # ....................... #

    async def put_ordered_snapshot_keys(
        self,
        ordered_ids: Iterable[str],
        *,
        snap_opt: SearchResultSnapshotOptions | None,
        rs_spec: SearchResultSnapshotSpec,
        fp_computed: str,
        pool_len_before_cap: int,
    ) -> SearchSnapshotHandle:
        """Seal and store pre-built ordered record keys, streamed in ``chunk_size`` blocks.

        The federated path's counterpart to the windowed pool build: the RRF merge already
        holds the ordered keys in memory, but sealing + storing them streams through the sink
        so the full sealed copy is never materialized. ``ordered_ids`` may be a lazy generator
        — only one chunk of keys is sealed at a time, and iteration stops once ``max_ids`` is
        reached.
        """

        sink = self.open_simple_hit_sink(
            snap_opt=snap_opt, rs_spec=rs_spec, fp_computed=fp_computed
        )
        batch: list[str] = []

        for key in ordered_ids:
            batch.append(key)

            if len(batch) >= sink.chunk_size:
                full = await sink.add(batch)
                batch = []

                if full:
                    break

        else:
            if batch:
                await sink.add(batch)

        return await sink.finish(pool_len_before_cap=pool_len_before_cap)

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
        SearchCountlessPage[FederatedSearchReadModel[M_co]]
        | SearchCountlessPage[T_co]
        | SearchPage[FederatedSearchReadModel[M_co]]
        | SearchPage[T_co]
        | None
    ):
        if rs_spec is None:
            return None

        return await self._read_snapshot_page(
            snapshot,
            fp_computed=fp_computed,
            pagination=pagination,
            hydrate=lambda k: self.hydrate_federated_record_key(k, federated_spec),
            to_return_rows=lambda items: [
                {"hit": it.hit.model_dump(mode="json"), "member": it.member}
                for it in items
            ],
            return_type=return_type,
            return_fields=None,
            return_count=return_count,
        )

    # ....................... #

    async def read_federated_thin_snapshot_page_if_requested(
        self,
        *,
        rs_spec: SearchResultSnapshotSpec | None,
        snapshot: SearchResultSnapshotOptions | None,
        fp_computed: str,
        pagination: Mapping[str, Any] | None,
        return_type: type[T_co] | None,
        return_count: bool,
        rehydrate: Callable[
            [Sequence[tuple[str, str]]],
            Awaitable[Sequence[FederatedSearchReadModel[Any]]],
        ],
    ) -> Any:
        """Replay a thin federated snapshot page by re-fetching its hits from the legs.

        The thin snapshot stores only ``(member, id)`` keys, so replay parses the page's
        keys and hands them to *rehydrate* (which batch-fetches the full hits from each
        member by id). Unlike the full-record snapshot, the replayed **content is current**
        (re-fetched), the order/identities are frozen, and a since-deleted hit drops out.
        Returns ``None`` when the snapshot is absent or its id range cannot be served.
        """

        if rs_spec is None:
            return None

        window = await self._open_snapshot_window(
            snapshot, fp_computed=fp_computed, pagination=pagination
        )

        if window is None:
            return None

        raw_keys, handle, total_snap, pagination_d = window
        parsed = [self.parse_federated_thin_record_key(k) for k in raw_keys]
        items = await rehydrate(parsed)

        return _shape_snapshot_page(
            items,
            pagination=pagination_d,
            total=total_snap if return_count else None,
            snapshot=handle,
            return_type=return_type,
            return_fields=None,
            to_return_rows=lambda hits: [
                {"hit": it.hit.model_dump(mode="json"), "member": it.member}
                for it in hits
            ],
        )


# ....................... #


@attrs.define(slots=True)
class _OrderedHitSink:
    """Streams ordered record keys into a snapshot run one ``chunk_size`` block at a time.

    The store requires every non-final chunk to hold exactly ``chunk_size`` ids, so keys are
    buffered and flushed in full blocks (sealed under the run id), the remainder going out as
    the final chunk. Peak retained memory is one chunk, independent of the pool size.
    ``begin_run`` is deferred to the first flush, so an empty pool still writes a canonical
    empty run via :meth:`SearchResultSnapshotPort.put_run`.
    """

    coordinator: SearchResultSnapshot
    run_id: str
    fingerprint: str
    chunk_size: int
    max_ids: int
    ttl: timedelta
    expires_at: int
    """Absolute UTC unix-second expiry, mirroring what the store persists in run meta."""

    # ....................... #

    _buffer: list[str] = attrs.field(factory=list, init=False)
    _stored: int = attrs.field(default=0, init=False)
    _chunk_index: int = attrs.field(default=0, init=False)
    _began: bool = attrs.field(default=False, init=False)
    _capped: bool = attrs.field(default=False, init=False)

    # ....................... #

    async def add(self, record_keys: Sequence[str]) -> bool:
        """Buffer ``record_keys`` (dropping any past ``max_ids``), flushing full chunks.

        Returns ``True`` once the cap is reached and further keys would be dropped.
        """

        for key in record_keys:
            if self._stored + len(self._buffer) >= self.max_ids:
                self._capped = True
                break

            self._buffer.append(key)

            if len(self._buffer) >= self.chunk_size:
                await self._flush(is_last=False)

        return self._stored + len(self._buffer) >= self.max_ids

    # ....................... #

    async def _flush(self, *, is_last: bool) -> None:
        if not self._began:
            await self.coordinator.store.begin_run(
                run_id=self.run_id,
                fingerprint=self.fingerprint,
                chunk_size=self.chunk_size,
                ttl=self.ttl,
            )
            self._began = True

        take = len(self._buffer) if is_last else self.chunk_size
        block = self._buffer[:take]

        sealed = (
            await self.coordinator._seal_ids(  # pyright: ignore[reportPrivateUsage]
                block,
                run_id=self.run_id,
            )
        )

        await self.coordinator.store.append_chunk(
            run_id=self.run_id,
            chunk_index=self._chunk_index,
            ids=sealed,
            is_last=is_last,
        )

        self._chunk_index += 1
        self._stored += len(block)
        del self._buffer[:take]

    # ....................... #

    async def finish(self, *, pool_len_before_cap: int) -> SearchSnapshotHandle:
        """Flush the final chunk and return the run handle.

        ``pool_len_before_cap`` is the number of hits the source produced before capping; it
        marks the snapshot ``capped`` when it exceeds what was stored.
        """

        if not self._began and not self._buffer:
            await self.coordinator.store.put_run(
                run_id=self.run_id,
                fingerprint=self.fingerprint,
                ordered_ids=[],
                ttl=self.ttl,
                chunk_size=self.chunk_size,
            )

        else:
            await self._flush(is_last=True)

        return SearchSnapshotHandle(
            id=self.run_id,
            fingerprint=self.fingerprint,
            total=self._stored,
            capped=self._capped or pool_len_before_cap > self._stored,
            expires_at=self.expires_at,
        )

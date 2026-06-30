"""In-memory search result snapshot store (chunked)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Sequence, final

import attrs

from forze.application.contracts.search import (
    SearchResultSnapshotMeta,
    SearchResultSnapshotPort,
    SearchResultSnapshotSpec,
)
from forze.base.exceptions import exc
from forze.base.primitives import utcnow
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin

# ----------------------- #


def _validate_chunk_size(chunk_size: int) -> None:
    if chunk_size < 1:
        raise exc.internal("chunk_size must be at least 1.")


def _expires_at(ttl: timedelta) -> int:
    """Absolute UTC unix-second expiry for a run, computed once at write time."""

    return int(utcnow().timestamp()) + int(ttl.total_seconds())


def _is_expired(meta: dict[str, Any]) -> bool:
    """Whether a run's stored expiry has passed (Redis lets the keys lapse; the mock has no
    TTL eviction, so reads must check expiry explicitly to match that behavior)."""

    expires_at = meta.get("expires_at")

    return expires_at is not None and int(expires_at) <= int(utcnow().timestamp())


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockSearchResultSnapshotAdapter(MockTenancyMixin, SearchResultSnapshotPort):
    """Chunked meta + chunk dicts mirroring Redis snapshot layout."""

    state: MockState
    spec: SearchResultSnapshotSpec
    namespace: str = "search_snapshot"

    default_ttl: timedelta | None = None
    default_chunk_size: int | None = None

    # ....................... #

    def _route(self) -> str:
        return self._partitioned_namespace(self.namespace)

    def _meta_store(self) -> dict[str, Any]:
        with self.state.lock:
            return self.state.search_snapshots.setdefault(self._route(), {})

    def _chunk_store(self) -> dict[tuple[str, int], list[str]]:
        with self.state.lock:
            return self.state.search_snapshot_chunks.setdefault(self._route(), {})

    # ....................... #

    def _resolve_ttl(self, ttl: timedelta | None) -> timedelta:
        if ttl is not None:
            return ttl
        if self.default_ttl is not None:
            return self.default_ttl
        return self.spec.ttl

    def _resolve_chunk_size(self, chunk_size: int | None) -> int:
        if chunk_size is not None:
            return chunk_size
        if self.default_chunk_size is not None:
            return self.default_chunk_size
        return self.spec.chunk_size

    # ....................... #

    @staticmethod
    def _as_meta(run_id: str, data: dict[str, Any]) -> SearchResultSnapshotMeta:
        raw_expires = data.get("expires_at")

        return SearchResultSnapshotMeta(
            run_id=run_id,
            fingerprint=str(data.get("fingerprint", "")),
            total=int(data.get("total", data.get("total_ids", 0))),
            chunk_size=int(data.get("chunk_size", 0)),
            complete=bool(data.get("complete", False)),
            expires_at=int(raw_expires) if raw_expires is not None else None,
        )

    # ....................... #

    async def put_run(
        self,
        *,
        run_id: str,
        fingerprint: str,
        ordered_ids: Sequence[str],
        ttl: timedelta | None = None,
        chunk_size: int | None = None,
    ) -> None:
        ttl_eff = self._resolve_ttl(ttl)
        cs = self._resolve_chunk_size(chunk_size)
        _validate_chunk_size(cs)
        n = len(ordered_ids)
        if n == 0:
            with self.state.lock:
                self._meta_store()[run_id] = {
                    "fingerprint": fingerprint,
                    "chunk_size": cs,
                    "total": 0,
                    "num_chunks": 0,
                    "complete": True,
                    "expires_at": _expires_at(ttl_eff),
                }
            return
        await self.begin_run(
            run_id=run_id,
            fingerprint=fingerprint,
            chunk_size=cs,
            ttl=ttl,
        )
        for start in range(0, n, cs):
            sl = list(ordered_ids[start : start + cs])
            idx = start // cs
            is_last = start + cs >= n
            await self.append_chunk(
                run_id=run_id,
                chunk_index=idx,
                ids=sl,
                is_last=is_last,
            )

    async def begin_run(
        self,
        *,
        run_id: str,
        fingerprint: str,
        chunk_size: int | None = None,
        ttl: timedelta | None = None,
    ) -> None:
        ttl_eff = self._resolve_ttl(ttl)
        cs = self._resolve_chunk_size(chunk_size)
        _validate_chunk_size(cs)
        with self.state.lock:
            self._meta_store()[run_id] = {
                "fingerprint": fingerprint,
                "chunk_size": cs,
                "complete": False,
                "next_chunk_index": 0,
                "total_ids": 0,
                "expires_at": _expires_at(ttl_eff),
            }

    async def append_chunk(
        self,
        *,
        run_id: str,
        chunk_index: int,
        ids: Sequence[str],
        is_last: bool,
    ) -> None:
        with self.state.lock:
            meta = self._meta_store().get(run_id)
            if meta is None:
                raise exc.internal(
                    "begin_run is required before append_chunk (missing meta)."
                )
            if meta.get("complete"):
                raise exc.internal("Cannot append_chunk to a completed snapshot run.")
            if chunk_index != int(meta.get("next_chunk_index", 0)):
                raise exc.internal(
                    f"append_chunk expected chunk_index {meta.get('next_chunk_index')!r}, got {chunk_index!r}."
                )
            chunk_size = int(meta["chunk_size"])
            if len(ids) > chunk_size:
                raise exc.internal(
                    f"Chunk has {len(ids)} ids; chunk_size is {chunk_size!r}."
                )
            if not is_last and len(ids) != chunk_size:
                raise exc.internal(
                    "All non-final chunks must contain exactly ``chunk_size`` ids."
                )
            self._chunk_store()[(run_id, chunk_index)] = list(ids)
            total_ids = int(meta.get("total_ids", 0)) + len(ids)
            next_idx = chunk_index + 1
            expires_at = meta.get("expires_at")
            if is_last:
                self._meta_store()[run_id] = {
                    "fingerprint": meta["fingerprint"],
                    "chunk_size": chunk_size,
                    "total": total_ids,
                    "num_chunks": next_idx,
                    "complete": True,
                    "expires_at": expires_at,
                }
            else:
                self._meta_store()[run_id] = {
                    "fingerprint": meta["fingerprint"],
                    "chunk_size": chunk_size,
                    "complete": False,
                    "next_chunk_index": next_idx,
                    "total_ids": total_ids,
                    "expires_at": expires_at,
                }

    async def get_id_range(
        self,
        run_id: str,
        offset: int,
        limit: int,
        *,
        expected_fingerprint: str | None = None,
    ) -> list[str] | None:
        if offset < 0 or limit < 1:
            raise exc.internal("get_id_range requires offset >= 0 and limit >= 1.")
        with self.state.lock:
            meta = self._meta_store().get(run_id)
            if meta is None or not meta.get("complete") or _is_expired(meta):
                return None
            if expected_fingerprint is not None and str(
                meta.get("fingerprint")
            ) != str(expected_fingerprint):
                return None
            total = int(meta["total"])
            if offset >= total:
                return []
            chunk_size = int(meta["chunk_size"])
            num_chunks = int(meta.get("num_chunks", 0))
            out: list[str] = []
            remaining = limit
            pos = offset
            while remaining > 0 and pos < total:
                ci = pos // chunk_size
                if ci >= num_chunks:
                    break
                chunk = self._chunk_store().get((run_id, ci), [])
                start_in_chunk = pos % chunk_size
                take = min(remaining, len(chunk) - start_in_chunk)
                if take <= 0:
                    break
                out.extend(chunk[start_in_chunk : start_in_chunk + take])
                remaining -= take
                pos += take
            return out

    async def get_meta(self, run_id: str) -> SearchResultSnapshotMeta | None:
        with self.state.lock:
            meta = self._meta_store().get(run_id)
            if meta is None or _is_expired(meta):
                return None
            return self._as_meta(run_id, meta)

    async def delete_run(self, run_id: str) -> None:
        with self.state.lock:
            self._meta_store().pop(run_id, None)
            keys = [k for k in self._chunk_store() if k[0] == run_id]
            for k in keys:
                del self._chunk_store()[k]

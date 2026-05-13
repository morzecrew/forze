"""Redis implementation of :class:`~forze.application.contracts.search.search_result_snapshot.SearchResultSnapshotPort`."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from datetime import timedelta
from typing import Any, Final, Sequence, cast, final

import attrs

from forze.application.contracts.search import (
    SearchResultSnapshotMeta,
    SearchResultSnapshotPort,
)
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict

from forze_redis.kernel.scripts import APPEND_SNAPSHOT_CHUNK

from .base import RedisBaseAdapter
from .codecs import default_json_codec

# ----------------------- #

_SNAPSHOT_SCOPE: Final[tuple[str, str]] = ("search", "result_snapshot")
_META: Final[str] = "meta"
_CHUNK: Final[str] = "chunk"

# ....................... #


def _validate_chunk_size(chunk_size: int) -> None:
    if chunk_size < 1:
        raise CoreError("chunk_size must be at least 1.")


def _ex_seconds(ttl: timedelta) -> int:
    sec = int(ttl.total_seconds())
    if sec < 1:
        raise CoreError("ttl must be at least one second for Redis key expiry.")
    return sec


def _json_dumps_bytes(value: Any) -> bytes:
    return default_json_codec.dumps(value)


def _decode_get_json(raw: bytes | str | None) -> Any:
    if raw is None:
        return None

    if isinstance(raw, (bytes, bytearray)):
        return default_json_codec.loads(bytes(raw))

    return default_json_codec.loads(str(raw))


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisSearchResultSnapshotAdapter(
    SearchResultSnapshotPort,
    RedisBaseAdapter,
):
    """Chunked keys: one JSON meta value and one JSON array per chunk of ordered ids.

    :class:`.ConfigurableRedisSearchResultSnapshot` supplies ``default_ttl``,
    ``default_max_ids``, and ``default_chunk_size`` from :class:`.SearchResultSnapshotSpec`
    (``default_max_ids`` is for observability; truncation is a search-layer concern).
    """

    default_ttl: timedelta = timedelta(minutes=5)
    """Fallback when :meth:`put_run` / :meth:`begin_run` receive ``ttl is None``."""

    default_max_ids: int = 50_000
    """Reflected from the spec at DI; used by the search use case, not by every Redis op."""

    default_chunk_size: int = 5_000
    """Fallback when ``chunk_size is None`` on :meth:`put_run` / :meth:`begin_run`."""

    # ....................... #

    def _resolve_ttl(self, ttl: timedelta | None) -> timedelta:
        return self.default_ttl if ttl is None else ttl

    # ....................... #

    def __key_meta(self, run_id: str) -> str:
        return self.construct_key(_SNAPSHOT_SCOPE, _META, run_id)

    def __key_chunk(self, run_id: str, chunk_index: int) -> str:
        return self.construct_key(_SNAPSHOT_SCOPE, _CHUNK, run_id, str(chunk_index))

    # ....................... #

    async def __load_meta(self, run_id: str) -> JsonDict | None:
        _raw, data = await self.__load_meta_raw(run_id)

        return data

    async def __load_meta_raw(self, run_id: str) -> tuple[bytes | None, JsonDict | None]:
        raw = await self.client.get(self.__key_meta(run_id))

        if raw is None:
            return None, None

        data = _decode_get_json(raw)

        if not isinstance(data, dict):
            return raw, None

        return raw, cast(JsonDict, data)

    # ....................... #

    @staticmethod
    def __as_meta_model(run_id: str, data: dict[str, Any]) -> SearchResultSnapshotMeta:
        return SearchResultSnapshotMeta(
            run_id=run_id,
            fingerprint=str(data.get("fingerprint", "")),
            total=int(data.get("total", data.get("total_ids", 0))),
            chunk_size=int(data.get("chunk_size", 0)),
            complete=bool(data.get("complete", False)),
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
        eff = self._resolve_ttl(ttl)
        ex = _ex_seconds(eff)
        cs = self.default_chunk_size if chunk_size is None else chunk_size
        _validate_chunk_size(cs)

        n = len(ordered_ids)
        if n == 0:
            payload = {
                "fingerprint": fingerprint,
                "chunk_size": cs,
                "total": 0,
                "num_chunks": 0,
                "complete": True,
                "ttl_seconds": ex,
            }
            await self.client.set(
                self.__key_meta(run_id),
                _json_dumps_bytes(payload),
                ex=ex,
            )
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
                run_id=run_id, chunk_index=idx, ids=sl, is_last=is_last
            )

    # ....................... #

    async def begin_run(
        self,
        *,
        run_id: str,
        fingerprint: str,
        chunk_size: int | None = None,
        ttl: timedelta | None = None,
    ) -> None:
        eff = self._resolve_ttl(ttl)
        ex = _ex_seconds(eff)
        cs = self.default_chunk_size if chunk_size is None else chunk_size
        _validate_chunk_size(cs)

        payload = {
            "fingerprint": fingerprint,
            "chunk_size": cs,
            "ttl_seconds": ex,
            "complete": False,
            "next_chunk_index": 0,
            "total_ids": 0,
        }

        await self.client.set(
            self.__key_meta(run_id),
            _json_dumps_bytes(payload),
            ex=ex,
        )

    # ....................... #

    async def append_chunk(
        self,
        *,
        run_id: str,
        chunk_index: int,
        ids: Sequence[str],
        is_last: bool,
    ) -> None:
        meta_key = self.__key_meta(run_id)
        raw_meta, meta = await self.__load_meta_raw(run_id)

        if meta is None or raw_meta is None:
            raise CoreError("begin_run is required before append_chunk (missing meta).")

        if meta.get("complete"):
            raise CoreError("Cannot append_chunk to a completed snapshot run.")

        if chunk_index != int(meta.get("next_chunk_index", 0)):
            raise CoreError(
                f"append_chunk expected chunk_index {meta.get('next_chunk_index')!r}, got {chunk_index!r}."
            )

        chunk_size = int(meta["chunk_size"])
        ex = int(meta["ttl_seconds"])

        if len(ids) > chunk_size:
            raise CoreError(f"Chunk has {len(ids)} ids; chunk_size is {chunk_size!r}.")

        if not is_last and len(ids) != chunk_size:
            raise CoreError(
                "All non-final chunks must contain exactly ``chunk_size`` ids."
            )

        chunk_key = self.__key_chunk(run_id, chunk_index)
        total_ids = int(meta.get("total_ids", 0)) + len(ids)
        next_idx = chunk_index + 1

        if is_last:
            new_meta = {
                "fingerprint": meta["fingerprint"],
                "chunk_size": chunk_size,
                "ttl_seconds": ex,
                "total": total_ids,
                "num_chunks": next_idx,
                "complete": True,
            }

        else:
            new_meta = {
                "fingerprint": meta["fingerprint"],
                "chunk_size": chunk_size,
                "ttl_seconds": ex,
                "complete": False,
                "next_chunk_index": next_idx,
                "total_ids": total_ids,
            }

        chunk_b = _json_dumps_bytes(list(ids))
        new_meta_b = _json_dumps_bytes(new_meta)

        raw = await self.client.run_script(
            APPEND_SNAPSHOT_CHUNK,
            [meta_key, chunk_key],
            [raw_meta, chunk_b, str(ex), new_meta_b],
        )

        if str(raw).strip() != "1":
            raise CoreError(
                "Concurrent snapshot append detected (meta changed); retry append_chunk."
            )

    # ....................... #

    async def get_id_range(
        self,
        run_id: str,
        offset: int,
        limit: int,
        *,
        expected_fingerprint: str | None = None,
    ) -> list[str] | None:
        if offset < 0 or limit < 1:
            raise CoreError("get_id_range requires offset >= 0 and limit >= 1.")

        meta = await self.__load_meta(run_id)

        if meta is None:
            return None

        if not meta.get("complete"):
            return None

        if expected_fingerprint is not None and str(meta.get("fingerprint")) != str(
            expected_fingerprint
        ):
            return None

        total = int(meta["total"])

        if offset >= total:
            return []

        chunk_size = int(meta["chunk_size"])
        end = min(offset + limit, total)
        n_chunks = int(meta["num_chunks"])
        first_chunk = offset // chunk_size
        last_chunk = (end - 1) // chunk_size

        if last_chunk >= n_chunks:
            return None

        out: list[str] = []

        for ci in range(first_chunk, last_chunk + 1):
            if ci >= n_chunks:
                return None
            raw = await self.client.get(self.__key_chunk(run_id, ci))
            part = _decode_get_json(raw)

            if not isinstance(part, list):
                return None

            g0 = ci * chunk_size
            g1 = g0 + len(part)  # type: ignore[arg-type]
            seg_start = max(offset, g0)
            seg_end = min(end, g1)

            if seg_start < seg_end:
                lo = seg_start - g0
                hi = seg_end - g0
                out.extend(str(x) for x in part[lo:hi])  # type: ignore[arg-type]

        return out

    # ....................... #

    async def get_meta(self, run_id: str) -> SearchResultSnapshotMeta | None:
        meta = await self.__load_meta(run_id)

        if meta is None:
            return None

        if meta.get("complete"):
            return self.__as_meta_model(run_id, meta)

        return SearchResultSnapshotMeta(
            run_id=run_id,
            fingerprint=str(meta.get("fingerprint", "")),
            total=int(meta.get("total_ids", 0)),
            chunk_size=int(meta.get("chunk_size", 0)),
            complete=False,
        )

    # ....................... #

    async def delete_run(self, run_id: str) -> None:
        meta = await self.__load_meta(run_id)

        if meta is None:
            return

        keys: list[str] = [self.__key_meta(run_id)]

        if meta.get("complete"):
            n = int(meta.get("num_chunks", 0))

        else:
            n = int(meta.get("next_chunk_index", 0))

        for i in range(n):
            keys.append(self.__key_chunk(run_id, i))

        await self.client.unlink(*keys)

"""Redis-backed :class:`~forze.application.contracts.cache.CachePort` adapter."""

import asyncio

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from datetime import timedelta
from typing import Any, Callable, Final, Iterable, Mapping, Sequence, final

import attrs

from forze.application.contracts.cache import CachePort

from ._logger import logger
from .base import RedisBaseAdapter
from .codecs import default_json_codec, default_text_codec

# ----------------------- #

_CACHE_SCOPE: Final[str] = "cache"
_KV_SCOPE: Final[str] = "kv"
_POINTER_SCOPE: Final[str] = "pointer"
_BODY_SCOPE: Final[str] = "body"

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisCacheAdapter(CachePort, RedisBaseAdapter):
    """Redis implementation of :class:`~forze.application.contracts.cache.CachePort`.

    Supports two caching strategies:

    * **Plain key-value** — a single Redis key per cache entry.
    * **Versioned** — a pointer key that maps to a version-tagged body key,
      enabling atomic cache invalidation without deleting the body.
    """

    ttl_pointer: timedelta = timedelta(seconds=60)
    """TTL for the cache pointers (when using versioned cache)."""

    ttl_body: timedelta = timedelta(seconds=300)
    """TTL for the cache bodies (when using versioned cache)."""

    ttl_kv: timedelta = timedelta(seconds=300)
    """TTL for the cache key-value pairs (when using plain cache)."""

    # ....................... #
    # Helpers

    def _decode_batch(
        self,
        keys: Iterable[str],
        raw: Sequence[bytes | str | None],
        loads: Callable[[bytes | str], Any],
    ) -> dict[str, Any]:
        res: dict[str, Any] = {}

        for k, rv in zip(keys, raw, strict=True):
            if rv is not None:
                try:
                    res[k] = loads(rv)

                except (ValueError, TypeError):
                    logger.warning("Cache decode failed for key=%s", k, exc_info=False)
                    continue

        return res

    # ....................... #

    def __kv_key(self, key: str) -> str:
        return self.construct_key((_CACHE_SCOPE, _KV_SCOPE), key)

    # ....................... #

    def __pointer_key(self, key: str) -> str:
        return self.construct_key((_CACHE_SCOPE, _POINTER_SCOPE), key)

    # ....................... #

    def __body_key(self, key: str, version: str) -> str:
        return self.construct_key((_CACHE_SCOPE, _BODY_SCOPE), key, version)

    # ....................... #
    # Internal: pointer

    async def __mget_pointers(self, keys: Sequence[str]) -> dict[str, str]:
        if not keys:
            return {}

        redis_keys = [self.__pointer_key(k) for k in keys]
        raw = await self.client.mget(redis_keys)

        if len(keys) > 64:
            return await asyncio.to_thread(
                self._decode_batch,
                keys,
                raw,
                default_text_codec.loads,
            )

        return self._decode_batch(keys, raw, default_text_codec.loads)

    # ....................... #

    async def __mset_pointers(self, mapping: dict[str, str], *, ttl: timedelta) -> None:
        if not mapping:
            return

        redis_mapping = {self.__pointer_key(k): v for k, v in mapping.items()}
        await self.client.mset(redis_mapping, ex=int(ttl.total_seconds()))

    # ....................... #

    async def __mdelete_pointers(self, keys: Sequence[str]) -> None:
        if not keys:
            return

        redis_keys = [self.__pointer_key(k) for k in keys]
        await self.client.unlink(*redis_keys)

    # ....................... #
    # Internal: body

    async def __mget_bodies(self, mapping: dict[str, str]) -> dict[str, Any]:
        if not mapping:
            return {}

        redis_keys = [self.__body_key(k, v) for k, v in mapping.items()]
        raw = await self.client.mget(redis_keys)

        if len(mapping) > 64:
            return await asyncio.to_thread(
                self._decode_batch,
                mapping.keys(),
                raw,
                default_json_codec.loads,
            )

        return self._decode_batch(mapping.keys(), raw, default_json_codec.loads)

    # ....................... #

    async def __mset_bodies(
        self,
        mapping: Mapping[tuple[str, str], Any],
        *,
        ttl: timedelta,
    ) -> None:
        if not mapping:
            return

        redis_mapping = {
            self.__body_key(k, v): default_json_codec.dumps(val)
            for (k, v), val in mapping.items()
        }
        await self.client.mset(redis_mapping, ex=int(ttl.total_seconds()))

    # ....................... #

    async def __mdelete_bodies(self, mapping: dict[str, str]) -> None:
        if not mapping:
            return

        redis_keys = [self.__body_key(k, v) for k, v in mapping.items()]
        await self.client.unlink(*redis_keys)

    # ....................... #
    # Internal: plain KV

    async def __mget_kv(self, keys: Sequence[str]) -> dict[str, Any]:
        if not keys:
            return {}

        redis_keys = [self.__kv_key(k) for k in keys]
        raw = await self.client.mget(redis_keys)

        if len(keys) > 64:
            return await asyncio.to_thread(
                self._decode_batch,
                keys,
                raw,
                default_json_codec.loads,
            )

        return self._decode_batch(keys, raw, default_json_codec.loads)

    # ....................... #

    async def __mset_kv(self, mapping: dict[str, Any], *, ttl: timedelta) -> None:
        if not mapping:
            return

        redis_mapping = {
            self.__kv_key(k): default_json_codec.dumps(v) for k, v in mapping.items()
        }
        await self.client.mset(redis_mapping, ex=int(ttl.total_seconds()))

    # ....................... #

    async def __mdelete_kv(self, keys: Sequence[str]) -> None:
        if not keys:
            return

        redis_keys = [self.__kv_key(k) for k in keys]
        await self.client.unlink(*redis_keys)

    # ....................... #
    # Public: read

    async def get(self, key: str) -> Any | None:
        # Try versioned first
        logger.debug("Cache lookup for key=%s", key)

        pointers = await self.__mget_pointers([key])

        if pointers:
            bodies = await self.__mget_bodies({key: pointers[key]})
            if key in bodies:
                logger.debug("Cache hit (versioned) key=%s", key)
                return bodies[key]

        # Fallback to plain
        kv = await self.__mget_kv([key])

        if key in kv:
            logger.debug("Cache hit (plain) key=%s", key)
            return kv[key]

        logger.debug("Cache miss key=%s", key)
        return None

    # ....................... #

    async def get_many(self, keys: Sequence[str]) -> tuple[dict[str, Any], list[str]]:
        if not keys:
            logger.debug("Empty list of keys, skipping")
            return {}, []

        logger.debug("Cache batch lookup for %s keys", len(keys))

        # 1) versioned hits where pointer exists + body exists
        pointers = await self.__mget_pointers(keys)
        versioned_hits: dict[str, Any] = {}

        if pointers:
            versioned_hits = await self.__mget_bodies(pointers)

        # 2) for the rest, try plain KV
        remaining = [k for k in keys if k not in versioned_hits]
        kv_hits = await self.__mget_kv(remaining) if remaining else {}

        hits = {**versioned_hits, **kv_hits}
        misses = [k for k in keys if k not in hits]

        logger.debug(
            "Cache hits=%s, misses=%s",
            len(hits),
            len(misses),
        )

        return hits, misses

    # ....................... #
    # Public: write

    async def set(self, key: str, value: Any) -> None:
        # Plain set. (We do not touch pointer/body.)
        await self.__mset_kv({key: value}, ttl=self.ttl_kv)

    # ....................... #

    async def set_versioned(self, key: str, version: str, value: Any) -> None:
        async with self.client.pipeline(transaction=True):
            await self.__mset_bodies({(key, version): value}, ttl=self.ttl_body)
            await self.__mset_pointers({key: version}, ttl=self.ttl_pointer)

    # ....................... #

    async def set_many(self, key_mapping: dict[str, Any]) -> None:
        if not key_mapping:
            return

        await self.__mset_kv(key_mapping, ttl=self.ttl_kv)

    # ....................... #

    async def set_many_versioned(
        self,
        key_version_mapping: Mapping[tuple[str, str], Any],
    ) -> None:
        if not key_version_mapping:
            return

        pointer_mapping = {
            key: version for (key, version) in key_version_mapping.keys()
        }

        async with self.client.pipeline(transaction=True):
            await self.__mset_bodies(key_version_mapping, ttl=self.ttl_body)
            await self.__mset_pointers(pointer_mapping, ttl=self.ttl_pointer)

    # ....................... #

    async def delete(self, key: str, *, hard: bool) -> None:
        if hard:
            # Overlap kv-delete with pointer lookup; body-delete waits for the pointer.
            pointers, _ = await asyncio.gather(
                self.__mget_pointers([key]),
                self.__mdelete_kv([key]),
            )
            await asyncio.gather(
                self.__mdelete_bodies(pointers) if pointers else asyncio.sleep(0),
                self.__mdelete_pointers([key]),
            )
        else:
            await asyncio.gather(
                self.__mdelete_kv([key]),
                self.__mdelete_pointers([key]),
            )

    # ....................... #

    async def delete_many(self, keys: Sequence[str], *, hard: bool) -> None:
        if not keys:
            return

        if hard:
            # Overlap kv-delete with pointer lookups; body-delete waits for the pointers.
            pointers, _ = await asyncio.gather(
                self.__mget_pointers(keys),
                self.__mdelete_kv(keys),
            )
            await asyncio.gather(
                self.__mdelete_bodies(pointers) if pointers else asyncio.sleep(0),
                self.__mdelete_pointers(keys),
            )
        else:
            await asyncio.gather(
                self.__mdelete_kv(keys),
                self.__mdelete_pointers(keys),
            )

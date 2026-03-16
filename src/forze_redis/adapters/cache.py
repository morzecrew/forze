"""Redis-backed :class:`~forze.application.contracts.cache.CachePort` adapter."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from datetime import timedelta
from typing import Any, Optional, Sequence, final

import attrs

from forze.application.contracts.cache import CachePort
from forze.application.contracts.tenant import TenantContextPort
from forze.base.codecs import JsonCodec, KeyCodec, TextCodec
from forze.base.logging_v2 import getLogger

from ..kernel.platform import RedisClient

# ----------------------- #

logger = getLogger(__name__).bind(scope="redis.cache")

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisCacheAdapter(CachePort):
    """Redis implementation of :class:`~forze.application.contracts.cache.CachePort`.

    Supports two caching strategies:

    * **Plain key-value** — a single Redis key per cache entry.
    * **Versioned** — a pointer key that maps to a version-tagged body key,
      enabling atomic cache invalidation without deleting the body.

    Keys are namespaced via :class:`~forze.base.codecs.KeyCodec` and optionally
    prefixed with a tenant identifier when a
    :class:`~forze.application.contracts.tenant.TenantContextPort` is provided.
    """

    client: RedisClient
    key_codec: KeyCodec
    tenant_context: Optional[TenantContextPort] = None

    # Non initable fields
    json_codec: JsonCodec = attrs.field(factory=JsonCodec, init=False)
    text_codec: TextCodec = attrs.field(factory=TextCodec, init=False)

    # Defaults (overrideable)
    ttl_pointer: timedelta = timedelta(seconds=60)
    ttl_body: timedelta = timedelta(seconds=300)
    ttl_kv: timedelta = timedelta(seconds=300)

    # ....................... #
    # Helpers

    def __kv_key(self, key: str) -> str:
        if self.tenant_context is not None:
            return self.key_codec.join(
                str(self.tenant_context.get()), "cache", "kv", key
            )

        return self.key_codec.join("cache", "kv", key)

    def __pointer_key(self, key: str) -> str:
        if self.tenant_context is not None:
            return self.key_codec.join(
                str(self.tenant_context.get()), "cache", "pointer", key
            )

        return self.key_codec.join("cache", "pointer", key)

    def __body_key(self, key: str, version: str) -> str:
        if self.tenant_context is not None:
            return self.key_codec.join(
                str(self.tenant_context.get()), "cache", "body", key, version
            )

        return self.key_codec.join("cache", "body", key, version)

    # ....................... #
    # Internal: pointer

    async def __mget_pointers(self, keys: Sequence[str]) -> dict[str, str]:
        if not keys:
            return {}

        redis_keys = [self.__pointer_key(k) for k in keys]
        raw = await self.client.mget(redis_keys)

        res: dict[str, str] = {}

        for k, rv in zip(keys, raw, strict=True):
            if rv is None:
                continue

            try:
                res[k] = self.text_codec.loads(rv)

            except ValueError:
                continue

        return res

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

        res: dict[str, Any] = {}

        for k, rb in zip(mapping.keys(), raw, strict=True):
            if rb is None:
                continue

            try:
                res[k] = self.json_codec.loads(rb)

            except ValueError:
                continue

        return res

    # ....................... #

    async def __mset_bodies(
        self,
        mapping: dict[tuple[str, str], Any],
        *,
        ttl: timedelta,
    ) -> None:
        if not mapping:
            return

        redis_mapping = {
            self.__body_key(k, v): self.json_codec.dumps(val)
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

        res: dict[str, Any] = {}

        for k, rv in zip(keys, raw, strict=True):
            if rv is None:
                continue

            try:
                res[k] = self.json_codec.loads(rv)

            except ValueError:
                continue

        return res

    # ....................... #

    async def __mset_kv(self, mapping: dict[str, Any], *, ttl: timedelta) -> None:
        if not mapping:
            return

        redis_mapping = {
            self.__kv_key(k): self.json_codec.dumps(v) for k, v in mapping.items()
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

    async def get(self, key: str) -> Optional[Any]:
        # Try versioned first
        logger.debug("Cache lookup for key=%s", key)

        with logger.section():
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

        logger.debug("Cache batch lookup for %d keys", len(keys))

        with logger.section():
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

            logger.debug("Cache hits=%d, misses=%d", len(hits), len(misses))

            return hits, misses

    # ....................... #
    # Public: write

    async def set(self, key: str, value: Any) -> None:
        # Plain set. (We do not touch pointer/body.)
        await self.__mset_kv({key: value}, ttl=self.ttl_kv)

    # ....................... #

    async def set_versioned(self, key: str, version: str, value: Any) -> None:
        async with self.client.pipeline(transaction=False):
            await self.__mset_pointers({key: version}, ttl=self.ttl_pointer)
            await self.__mset_bodies({(key, version): value}, ttl=self.ttl_body)

    # ....................... #

    async def set_many(self, key_mapping: dict[str, Any]) -> None:
        if not key_mapping:
            return

        await self.__mset_kv(key_mapping, ttl=self.ttl_kv)

    # ....................... #

    async def set_many_versioned(
        self,
        key_version_mapping: dict[tuple[str, str], Any],
    ) -> None:
        if not key_version_mapping:
            return

        pointer_mapping = {
            key: version for (key, version) in key_version_mapping.keys()
        }

        async with self.client.pipeline(transaction=False):
            await self.__mset_pointers(pointer_mapping, ttl=self.ttl_pointer)
            await self.__mset_bodies(key_version_mapping, ttl=self.ttl_body)

    # ....................... #

    async def delete(self, key: str, *, hard: bool) -> None:
        await self.__mdelete_kv([key])

        if hard:
            pointers = await self.__mget_pointers([key])
            if pointers:
                await self.__mdelete_bodies(pointers)

        await self.__mdelete_pointers([key])

    # ....................... #

    async def delete_many(self, keys: Sequence[str], *, hard: bool) -> None:
        if not keys:
            return

        await self.__mdelete_kv(keys)

        if hard:
            pointers = await self.__mget_pointers(keys)

            if pointers:
                await self.__mdelete_bodies(pointers)

        await self.__mdelete_pointers(keys)

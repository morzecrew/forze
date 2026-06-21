"""Redis-backed :class:`~forze.application.contracts.cache.CachePort` adapter."""

import asyncio
import math

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from datetime import timedelta
from typing import Any, Awaitable, Callable, Final, Iterable, Mapping, Sequence, final

import attrs

from forze.application.contracts.cache import (
    CacheInvalidation,
    CachePort,
    InvalidationCallback,
)
from forze.application.contracts.resolution import is_static_named_resource
from forze.base.exceptions import exc
from forze.base.primitives import run_cpu

from ._logger import logger
from .base import RedisBaseAdapter
from .codecs import default_json_codec, default_text_codec

# ----------------------- #

_CACHE_SCOPE: Final[str] = "cache"
_KV_SCOPE: Final[str] = "kv"
_POINTER_SCOPE: Final[str] = "pointer"
_BODY_SCOPE: Final[str] = "body"


def _ttl_seconds(ttl: "timedelta") -> int:
    """Whole seconds for Redis expiry, never truncating a live TTL to zero.

    ``int()`` would turn a sub-second TTL into ``EX 0`` (rejected by Redis) or
    an ``EXPIRE 0`` (immediate deletion): round up and floor at one second.
    """

    return max(1, math.ceil(ttl.total_seconds()))


def _loads_cache_body(raw: bytes | str) -> Any:
    """Deserialize a versioned cache body for :class:`CachePort` consumers.

    Bodies may be stored as JSON bytes (plain cache usage or legacy document dict
    payloads). :class:`~forze.application.integrations.document.DocumentCache`
    may store pre-encoded JSON bytes; parsing here yields a ``dict`` that its
    codec can still decode (same as the legacy dict wire format).
    """

    return default_json_codec.loads(raw)


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

    invalidation_push: bool = False
    """Opt-in client-side-caching invalidation push (Redis 6+ ``CLIENT
    TRACKING``). When enabled, :meth:`subscribe_invalidations` streams key
    invalidations to in-process subscribers (the document L1); when disabled
    (default) the capability reports unavailable and subscribers keep their
    TTL-only semantics."""

    sliding_ttl: timedelta | None = None
    """Opt-in sliding expiration: a versioned hit extends the *pointer* key's
    lifetime to this idle window (``EXPIRE ... GT``, Redis 7+ — extend-only).
    The *body* key keeps its write-time TTL, which therefore remains the
    absolute revalidation cap. Extension failures are swallowed: a hit must
    never fail because the slide did."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl_pointer.total_seconds() < 1:
            raise exc.configuration("TTL pointer must be at least 1 second")

        if self.ttl_body.total_seconds() < 1:
            raise exc.configuration("TTL body must be at least 1 second")

        if self.ttl_kv.total_seconds() < 1:
            raise exc.configuration("TTL kv must be at least 1 second")

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
            # Bounded CPU pool + correlated logs; deadline-naive — a cache decode is
            # best-effort (a deadline failure here would only force a redundant fetch).
            return await run_cpu(
                self._decode_batch,
                keys,
                raw,
                default_text_codec.loads,
                deadline=False,
            )

        return self._decode_batch(keys, raw, default_text_codec.loads)

    # ....................... #

    async def __mset_pointers(self, mapping: dict[str, str], *, ttl: timedelta) -> None:
        if not mapping:
            return

        redis_mapping = {self.__pointer_key(k): v for k, v in mapping.items()}
        await self.client.mset(redis_mapping, ex=_ttl_seconds(ttl))

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
            return await run_cpu(
                self._decode_batch,
                mapping.keys(),
                raw,
                _loads_cache_body,
                deadline=False,
            )

        return self._decode_batch(mapping.keys(), raw, _loads_cache_body)

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
            self.__body_key(k, v): (
                val if isinstance(val, bytes) else default_json_codec.dumps(val)
            )
            for (k, v), val in mapping.items()
        }
        await self.client.mset(redis_mapping, ex=_ttl_seconds(ttl))

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
            return await run_cpu(
                self._decode_batch,
                keys,
                raw,
                default_json_codec.loads,
                deadline=False,
            )

        return self._decode_batch(keys, raw, default_json_codec.loads)

    # ....................... #

    async def __mset_kv(self, mapping: Mapping[str, Any], *, ttl: timedelta) -> None:
        if not mapping:
            return

        redis_mapping = {
            self.__kv_key(k): v if isinstance(v, bytes) else default_json_codec.dumps(v)
            for k, v in mapping.items()
        }
        await self.client.mset(redis_mapping, ex=_ttl_seconds(ttl))

    # ....................... #

    async def __mdelete_kv(self, keys: Sequence[str]) -> None:
        if not keys:
            return

        redis_keys = [self.__kv_key(k) for k in keys]
        await self.client.unlink(*redis_keys)

    # ....................... #
    # Public: invalidation push (SupportsInvalidationPush)

    async def subscribe_invalidations(
        self,
        callback: InvalidationCallback,
    ) -> Callable[[], Awaitable[None]] | None:
        """Stream pointer-key invalidations to *callback* (L1 consumers).

        Tracks the **pointer** scope only: every versioned write re-sets the
        pointer, every delete unlinks it, and pointer TTL expiry broadcasts
        too — so the pointer is a complete invalidation signal for versioned
        read-through entries. Tenant-aware adapters track the ``tenant:``
        prefix and filter; the parsed tenant rides the event so subscribers
        can compose their own tenant-scoped keys.

        Returns ``None`` (push unavailable) when the feature is disabled, the
        namespace is dynamic (per-tenant-resolved namespaces have no stable
        broadcast prefix), or the client cannot track (tenant-routed clients).
        """

        if not self.invalidation_push:
            return None

        if not is_static_named_resource(self.namespace):
            logger.warning(
                "Invalidation push requires a static namespace; falling back "
                "to TTL-only L1 semantics",
            )

            return None

        namespace = await self._resolved_namespace()
        sep = self.key_sep
        pointer_prefix = sep.join((_CACHE_SCOPE, _POINTER_SCOPE, namespace)) + sep
        tenant_marker = f"tenant{sep}"

        if self.tenant_aware:
            # Tenant ids prefix the key, so there is no per-namespace
            # broadcast prefix — subscribe to the tenant scope and filter.
            prefixes: tuple[str, ...] = (tenant_marker,)

        else:
            prefixes = (pointer_prefix,)

        def _parse(server_key: str) -> CacheInvalidation | None:
            tenant: str | None = None
            rest = server_key

            if self.tenant_aware:
                if not rest.startswith(tenant_marker):
                    return None

                parts = rest.split(sep, 2)

                if len(parts) < 3:
                    return None

                tenant, rest = parts[1], parts[2]

            if not rest.startswith(pointer_prefix):
                return None

            logical = rest[len(pointer_prefix) :]

            if not logical:
                return None

            return CacheInvalidation(key=logical, tenant=tenant)

        def _on_keys(keys: Sequence[str]) -> None:
            for key in keys:
                inv = _parse(key)

                if inv is not None:
                    callback(inv)

        def _on_reset() -> None:
            callback(CacheInvalidation(key=None))

        return await self.client.track_invalidations(
            prefixes=prefixes,
            on_keys=_on_keys,
            on_reset=_on_reset,
        )

    # ....................... #
    # Internal: sliding expiration

    async def __slide_pointers(self, keys: Sequence[str]) -> None:
        """Extend hit pointers' lifetime to the sliding window (extend-only).

        Best-effort: a failed slide must never fail the hit it rides on. The
        body key is deliberately untouched — its write-time TTL is the
        absolute revalidation cap.
        """

        if self.sliding_ttl is None or not keys:
            return

        seconds = _ttl_seconds(self.sliding_ttl)

        try:
            if len(keys) == 1:
                await self.client.expire(self.__pointer_key(keys[0]), seconds, gt=True)

            else:
                async with self.client.pipeline(transaction=False):
                    for key in keys:
                        await self.client.expire(
                            self.__pointer_key(key), seconds, gt=True
                        )

        except Exception:
            logger.debug("Sliding TTL extension failed, continuing", exc_info=True)

    # ....................... #
    # Public: read

    async def get(self, key: str) -> Any | None:
        await self._prepare_keys()
        # Try versioned first
        logger.debug("Cache lookup for key=%s", key)

        pointers = await self.__mget_pointers([key])

        if pointers:
            bodies = await self.__mget_bodies({key: pointers[key]})
            if key in bodies:
                logger.debug("Cache hit (versioned) key=%s", key)
                await self.__slide_pointers([key])
                return bodies[key]

        # Fallback to plain
        kv = await self.__mget_kv([key])

        if key in kv:
            logger.debug("Cache hit (plain) key=%s", key)
            return kv[key]

        logger.debug("Cache miss key=%s", key)
        return None

    # ....................... #

    async def exists(self, key: str) -> bool:
        """Presence check without payload transfer or decode.

        Mirrors :meth:`get`'s effective semantics: a versioned entry counts
        only when its pointer *and* the pointed body are both live; otherwise
        falls back to the plain key-value scope.
        """

        await self._prepare_keys()

        pointers = await self.__mget_pointers([key])

        if pointers and await self.client.exists(
            self.__body_key(key, pointers[key])
        ):
            return True

        return await self.client.exists(self.__kv_key(key))

    # ....................... #

    async def get_many(self, keys: Sequence[str]) -> tuple[dict[str, Any], list[str]]:
        await self._prepare_keys()
        if not keys:
            logger.debug("Empty list of keys, skipping")
            return {}, []

        logger.debug("Cache batch lookup for %s keys", len(keys))

        # 1) versioned hits where pointer exists + body exists
        pointers = await self.__mget_pointers(keys)
        versioned_hits: dict[str, Any] = {}

        if pointers:
            versioned_hits = await self.__mget_bodies(pointers)

        if versioned_hits:
            await self.__slide_pointers(list(versioned_hits.keys()))

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

    async def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: timedelta | None = None,
    ) -> None:
        await self._prepare_keys()
        # Plain set. (We do not touch pointer/body.)
        await self.__mset_kv({key: value}, ttl=ttl if ttl is not None else self.ttl_kv)

    # ....................... #

    async def set_versioned(
        self,
        key: str,
        version: str,
        value: Any,
        *,
        ttl: timedelta | None = None,
    ) -> None:
        await self._prepare_keys()
        # A per-entry ttl is the entry's whole lifetime: it overrides both the
        # pointer (revalidation cadence) and the body (retention cap).
        async with self.client.pipeline(transaction=True):
            await self.__mset_bodies(
                {(key, version): value},
                ttl=ttl if ttl is not None else self.ttl_body,
            )
            await self.__mset_pointers(
                {key: version},
                ttl=ttl if ttl is not None else self.ttl_pointer,
            )

    # ....................... #

    async def set_many(
        self,
        key_mapping: Mapping[str, Any],
        *,
        ttl: timedelta | None = None,
    ) -> None:
        await self._prepare_keys()
        if not key_mapping:
            return

        await self.__mset_kv(key_mapping, ttl=ttl if ttl is not None else self.ttl_kv)

    # ....................... #

    async def set_many_versioned(
        self,
        key_version_mapping: Mapping[tuple[str, str], Any],
        *,
        ttl: timedelta | None = None,
    ) -> None:
        await self._prepare_keys()
        if not key_version_mapping:
            return

        pointer_mapping = {
            key: version for (key, version) in key_version_mapping.keys()
        }

        async with self.client.pipeline(transaction=True):
            await self.__mset_bodies(
                key_version_mapping,
                ttl=ttl if ttl is not None else self.ttl_body,
            )
            await self.__mset_pointers(
                pointer_mapping,
                ttl=ttl if ttl is not None else self.ttl_pointer,
            )

    # ....................... #

    async def delete(self, key: str, *, hard: bool) -> None:
        await self._prepare_keys()
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
        await self._prepare_keys()
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

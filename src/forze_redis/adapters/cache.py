from forze_redis._compat import require_redis

require_redis()

# ....................... #

from typing import Any, Optional, Sequence, TypeVar, final
from uuid import UUID

import attrs

from forze.application.kernel.ports import DocumentCachePort
from forze.utils.codecs import JsonCodec, KeyCodec, TextCodec

from ..kernel.platform import RedisClient

# ----------------------- #

K = TypeVar("K", bound=UUID | str)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisDocumentCacheAdapter(DocumentCachePort):
    client: RedisClient
    key_codec: KeyCodec

    # Non initable fields
    json_codec: JsonCodec = attrs.field(factory=JsonCodec, init=False)
    text_codec: TextCodec = attrs.field(factory=TextCodec, init=False)

    # Defaults (overrideable)
    ttl_s_pointer: int = 60
    ttl_s_body: int = 300

    # ....................... #

    def __pointer_key(self, pk: UUID | str) -> str:
        return self.key_codec.join("cache", "pointer", str(pk))

    # ....................... #

    async def __mget_pointers(self, pks: Sequence[K]) -> dict[K, int]:
        keys = [self.__pointer_key(pk) for pk in pks]
        raw_pointers = await self.client.mget(keys)

        res: dict[K, int] = {}

        for pk, rp in zip(pks, raw_pointers, strict=True):
            if rp is None:
                continue

            try:
                res[pk] = int(self.text_codec.loads(rp))
            except ValueError:
                continue

        return res

    # ....................... #

    async def __mset_pointers(
        self,
        mapping: dict[K, int],
        *,
        ttl_s: Optional[int] = None,
    ) -> None:
        ttl_s = ttl_s or self.ttl_s_pointer

        pointer_keys = [self.__pointer_key(pk) for pk in mapping.keys()]
        pointer_values = [str(rev) for rev in mapping.values()]

        await self.client.mset(dict(zip(pointer_keys, pointer_values)), ex=ttl_s)

    # ....................... #

    async def __mdelete_pointers(self, pks: Sequence[K]) -> None:
        keys = [self.__pointer_key(pk) for pk in pks]
        await self.client.unlink(*keys)

    # ....................... #

    def __body_key(self, pk: UUID | str, rev: int) -> str:
        return self.key_codec.join("cache", "body", str(pk), str(rev))

    # ....................... #

    async def __mget_bodies(self, mapping: dict[K, int]) -> dict[K, Any]:
        keys = [self.__body_key(pk, rev) for pk, rev in mapping.items()]
        raw_bodies = await self.client.mget(keys)

        res: dict[K, Any] = {}

        for pk, rb in zip(mapping.keys(), raw_bodies, strict=True):
            if rb is None:
                continue

            try:
                res[pk] = self.json_codec.loads(rb)
            except ValueError:
                continue

        return res

    # ....................... #

    async def __mset_bodies(
        self,
        mapping: dict[tuple[K, int], Any],
        *,
        ttl_s: Optional[int] = None,
    ) -> None:
        ttl_s = ttl_s or self.ttl_s_body

        body_keys = [self.__body_key(pk, rev) for pk, rev in mapping.keys()]
        body_values = [self.json_codec.dumps(value) for value in mapping.values()]

        await self.client.mset(dict(zip(body_keys, body_values)), ex=ttl_s)

    # ....................... #

    async def __mdelete_bodies(self, mapping: dict[K, int]) -> None:
        keys = [self.__body_key(pk, rev) for pk, rev in mapping.items()]
        await self.client.unlink(*keys)

    # ....................... #
    # Public methods

    async def get(self, pk: UUID | str) -> Optional[Any]:
        pointers = await self.__mget_pointers([pk])

        if not pointers:
            return None

        bodies = await self.__mget_bodies({pk: pointers[pk]})

        if not bodies:
            return None

        return bodies[pk]

    # ....................... #

    async def set(
        self,
        pk: UUID | str,
        rev: int,
        value: Any,
        *,
        ttl_s_pointer: Optional[int] = None,
        ttl_s_body: Optional[int] = None,
    ) -> None:
        ttl_pointer = ttl_s_pointer or self.ttl_s_pointer
        ttl_body = ttl_s_body or self.ttl_s_body

        async with self.client.pipeline(transaction=False):
            await self.__mset_pointers({pk: rev}, ttl_s=ttl_pointer)
            await self.__mset_bodies({(pk, rev): value}, ttl_s=ttl_body)

    # ....................... #

    async def delete(self, pk: UUID | str, *, hard: bool = False) -> None:
        if hard:
            pointers = await self.__mget_pointers([pk])

            if pointers:
                await self.__mdelete_bodies(pointers)

        await self.__mdelete_pointers([pk])

    # ....................... #

    async def get_many(self, pks: Sequence[K]) -> tuple[dict[K, Any], list[K]]:
        if not pks:
            return {}, []

        pointers = await self.__mget_pointers(pks)

        if not pointers:
            return {}, list(pks)

        hits = await self.__mget_bodies(pointers)
        misses = [p for p in pks if p not in hits]

        return hits, misses

    # ....................... #

    async def set_many(
        self,
        mapping: dict[tuple[K, int], Any],
        *,
        ttl_s_pointer: Optional[int] = None,
        ttl_s_body: Optional[int] = None,
    ) -> None:
        if not mapping:
            return

        ttl_pointer = ttl_s_pointer or self.ttl_s_pointer
        ttl_body = ttl_s_body or self.ttl_s_body

        pointer_mapping = {pk: rev for pk, rev in mapping.keys()}

        async with self.client.pipeline(transaction=False):
            await self.__mset_pointers(pointer_mapping, ttl_s=ttl_pointer)
            await self.__mset_bodies(mapping, ttl_s=ttl_body)

    # ....................... #

    async def delete_many(self, pks: Sequence[K], *, hard: bool = False) -> None:
        if not pks:
            return

        if hard:
            pointers = await self.__mget_pointers(pks)

            if pointers:
                await self.__mdelete_bodies(pointers)

        await self.__mdelete_pointers(pks)

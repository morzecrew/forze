from forze_redis._compat import require_redis

require_redis()

# ....................... #

from typing import Optional, Self, final

import attrs

from forze.application.contracts.counter import CounterPort
from forze.base.errors import ValidationError
from forze.utils.codecs import KeyCodec

from ..kernel.platform import RedisClient

# ----------------------- #
#! TODO: add tenant context support

@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisCounterAdapter(CounterPort):
    client: RedisClient
    key_codec: KeyCodec

    # ....................... #

    @classmethod
    def from_namespace(cls, client: RedisClient, namespace: str) -> Self:
        return cls(client=client, key_codec=KeyCodec(namespace=namespace))

    # ....................... #

    async def incr(self, by: int = 1, *, suffix: Optional[str] = None) -> int:
        return await self.client.incr(self.key_codec.cond_join(suffix), by)

    # ....................... #

    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: Optional[str] = None,
    ) -> list[int]:
        if size <= 1:
            raise ValidationError("Size must be greater than 1")

        max_cnt = await self.client.incr(self.key_codec.cond_join(suffix), size)

        return list(range(max_cnt - size + 1, max_cnt + 1))

    # ....................... #

    async def decr(self, by: int = 1, *, suffix: Optional[str] = None) -> int:
        return await self.client.decr(self.key_codec.cond_join(suffix), by)

    # ....................... #

    async def reset(self, value: int = 1, *, suffix: Optional[str] = None) -> int:
        return await self.client.reset(self.key_codec.cond_join(suffix), value)

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from typing import Optional, final

import attrs

from forze.application.contracts.counter import CounterPort
from forze.application.contracts.tenant import TenantContextPort
from forze.base.errors import ValidationError
from forze.utils.codecs import KeyCodec

from ..kernel.platform import RedisClient

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisCounterAdapter(CounterPort):
    client: RedisClient
    key_codec: KeyCodec
    tenant_context: Optional[TenantContextPort] = None

    # ....................... #

    def _build_key(self, suffix: Optional[str]) -> str:
        tenant_id = str(self.tenant_context.get()) if self.tenant_context else None
        return self.key_codec.cond_join(tenant_id, suffix)

    # ....................... #

    async def incr(self, by: int = 1, *, suffix: Optional[str] = None) -> int:
        return await self.client.incr(self._build_key(suffix), by)

    # ....................... #

    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: Optional[str] = None,
    ) -> list[int]:
        if size <= 1:
            raise ValidationError("Size must be greater than 1")

        max_cnt = await self.client.incr(self._build_key(suffix), size)

        return list(range(max_cnt - size + 1, max_cnt + 1))

    # ....................... #

    async def decr(self, by: int = 1, *, suffix: Optional[str] = None) -> int:
        return await self.client.decr(self._build_key(suffix), by)

    # ....................... #

    async def reset(self, value: int = 1, *, suffix: Optional[str] = None) -> int:
        return await self.client.reset(self._build_key(suffix), value)

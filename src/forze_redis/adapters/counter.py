"""Redis-backed :class:`~forze.application.contracts.counter.CounterPort` adapter."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from typing import final

import attrs

from forze.application.contracts.counter import CounterPort
from forze.application.contracts.tenant import TenantContextPort
from forze.base.codecs import KeyCodec
from forze.base.errors import ValidationError

from ..kernel.platform import RedisClient
from ._logger import logger

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisCounterAdapter(CounterPort):
    """Redis implementation of :class:`~forze.application.contracts.counter.CounterPort`.

    Uses ``INCRBY`` / ``DECRBY`` / ``GETSET`` for atomic counter operations.
    Keys are namespaced via :class:`~forze.base.codecs.KeyCodec` and optionally
    prefixed with a tenant identifier.
    """

    client: RedisClient
    key_codec: KeyCodec
    tenant_context: TenantContextPort | None = None

    # ....................... #

    def _build_key(self, suffix: str | None) -> str:
        tenant_id = str(self.tenant_context.get()) if self.tenant_context else None
        return self.key_codec.cond_join(tenant_id, suffix)

    # ....................... #

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        key = self._build_key(suffix)

        logger.debug("Incrementing counter '%s' by %s", key, by)

        return await self.client.incr(key, by)

    # ....................... #

    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: str | None = None,
    ) -> list[int]:
        if size <= 1:
            raise ValidationError("Size must be greater than 1")

        key = self._build_key(suffix)

        logger.debug(
            "Incrementing counter '%s' by %s, returning batch range",
            key,
            size,
        )

        max_cnt = await self.client.incr(key, size)

        return list(range(max_cnt - size + 1, max_cnt + 1))

    # ....................... #

    async def decr(self, by: int = 1, *, suffix: str | None = None) -> int:
        key = self._build_key(suffix)

        logger.debug("Decrementing counter '%s' by %s", key, by)

        return await self.client.decr(key, by)

    # ....................... #

    async def reset(self, value: int = 1, *, suffix: str | None = None) -> int:
        key = self._build_key(suffix)

        logger.debug("Resetting counter '%s' to %s", key, value)

        return await self.client.reset(key, value)

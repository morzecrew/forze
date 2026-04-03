"""Redis-backed :class:`~forze.application.contracts.counter.CounterPort` adapter."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from typing import Final, final

from forze.application.contracts.counter import CounterPort

from ._logger import logger
from .base import RedisBaseAdapter

# ----------------------- #

_COUNTER_SCOPE: Final[str] = "counter"

# ....................... #


@final
class RedisCounterAdapter(CounterPort, RedisBaseAdapter):
    """Redis implementation of :class:`~forze.application.contracts.counter.CounterPort`.

    Uses ``INCRBY`` / ``DECRBY`` / ``GETSET`` for atomic counter operations.
    """

    def __key(self, suffix: str | None) -> str:
        return self.construct_key(_COUNTER_SCOPE, suffix)

    # ....................... #

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        key = self.__key(suffix)

        logger.debug("Incrementing counter '%s' by %s", key, by)

        return await self.client.incr(key, by)

    # ....................... #

    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: str | None = None,
    ) -> list[int]:
        key = self.__key(suffix)

        logger.debug(
            "Incrementing counter '%s' by %s, returning batch range",
            key,
            size,
        )

        max_cnt = await self.client.incr(key, size)

        return list(range(max_cnt - size + 1, max_cnt + 1))

    # ....................... #

    async def decr(self, by: int = 1, *, suffix: str | None = None) -> int:
        key = self.__key(suffix)

        logger.debug("Decrementing counter '%s' by %s", key, by)

        return await self.client.decr(key, by)

    # ....................... #

    async def reset(self, value: int = 1, *, suffix: str | None = None) -> int:
        key = self.__key(suffix)

        logger.debug("Resetting counter '%s' to %s", key, value)

        return await self.client.reset(key, value)

from datetime import timedelta

import attrs

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from typing import Final, final

from forze.application.contracts.dlock import (
    DistributedLockCommandPort,
    DistributedLockQueryPort,
    DistributedLockSpec,
)

from ..kernel.scripts import RELEASE_DLOCK, RESET_DLOCK
from .base import RedisBaseAdapter

# ----------------------- #

_DLOCK_SCOPE: Final[str] = "dlock"

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisDistributedLockAdapter(
    DistributedLockCommandPort,
    DistributedLockQueryPort,
    RedisBaseAdapter,
):
    spec: DistributedLockSpec
    """Specification for the distributed lock."""

    # ....................... #

    def __key(self, key: str) -> str:
        return self.construct_key(_DLOCK_SCOPE, key)

    # ....................... #

    def __ttl_ms(self) -> int:
        return int(self.spec.ttl.total_seconds() * 1000)

    # ....................... #

    async def is_locked(self, key: str) -> bool:
        _k = self.__key(key)

        return await self.client.exists(_k)

    # ....................... #

    async def get_owner(self, key: str) -> str | None:
        _k = self.__key(key)

        res = await self.client.get(_k)

        if res is None:
            return None

        return (
            res.decode("utf-8")
            if isinstance(res, bytes)  # pyright: ignore[reportUnnecessaryIsInstance]
            else res
        )

    # ....................... #

    async def get_ttl(self, key: str) -> timedelta | None:
        _k = self.__key(key)

        res = await self.client.pttl(_k)

        if res is None:
            return None

        return timedelta(milliseconds=res)

    # ....................... #

    async def acquire(self, key: str, owner: str) -> bool:
        _k = self.__key(key)

        return await self.client.set(
            _k,
            owner,
            nx=True,
            px=self.__ttl_ms(),
        )

    # ....................... #

    async def release(self, key: str, owner: str) -> bool:
        _k = self.__key(key)

        res = await self.client.run_script(
            RELEASE_DLOCK,
            [_k],
            [owner],
        )

        if res:
            _res = bool(int(res))

        else:
            _res = False

        return _res

    # ....................... #

    async def reset(self, key: str, owner: str) -> bool:
        _k = self.__key(key)

        res = await self.client.run_script(
            RESET_DLOCK,
            [_k],
            [owner, self.__ttl_ms()],
        )

        if res:
            _res = bool(int(res))

        else:
            _res = False

        return _res

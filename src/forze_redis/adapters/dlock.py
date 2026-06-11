from datetime import timedelta

import attrs

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from typing import Final, final

from forze.application.contracts.dlock import (
    AcquiredLock,
    DistributedLockCommandPort,
    DistributedLockQueryPort,
    DistributedLockSpec,
)

from ..kernel.scripts import ACQUIRE_DLOCK, RELEASE_DLOCK, RESET_DLOCK
from .base import RedisBaseAdapter

# ----------------------- #

_DLOCK_SCOPE: Final[str] = "dlock"

_FENCE_SUFFIX: Final[str] = "fence"

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

    def __fence_key(self, key: str) -> str:
        return self.construct_key(_DLOCK_SCOPE, key, _FENCE_SUFFIX)

    # ....................... #

    def __ttl_ms(self) -> int:
        return int(self.spec.ttl.total_seconds() * 1000)

    # ....................... #

    async def is_locked(self, key: str) -> bool:
        await self._prepare_keys()
        _k = self.__key(key)

        return await self.client.exists(_k)

    # ....................... #

    async def get_owner(self, key: str) -> str | None:
        await self._prepare_keys()
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
        await self._prepare_keys()
        _k = self.__key(key)

        res = await self.client.pttl(_k)

        if res is None:
            return None

        return timedelta(milliseconds=res)

    # ....................... #

    async def acquire(self, key: str, owner: str) -> AcquiredLock | None:
        """Acquire the lock and issue a fencing token atomically.

        Runs ``ACQUIRE_DLOCK``: ``SET NX PX`` on the lock key plus, on success,
        ``INCR`` of the per-key fencing counter (``<lock key>:fence``). The
        counter has no TTL so tokens stay monotonic across lock generations —
        one small permanent key per lock key; :meth:`release` never deletes it.
        """

        await self._prepare_keys()
        _k = self.__key(key)
        _fk = self.__fence_key(key)

        res = await self.client.run_script(
            ACQUIRE_DLOCK,
            [_k, _fk],
            [owner, self.__ttl_ms()],
        )

        token = int(res) if res else 0

        if token <= 0:
            return None

        return AcquiredLock(key=key, owner=owner, token=token)

    # ....................... #

    async def release(self, key: str, owner: str) -> bool:
        await self._prepare_keys()
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
        await self._prepare_keys()
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

"""Redis-backed :class:`~forze.application.contracts.counter.CounterPort` adapter."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

import math
from collections.abc import Sequence
from datetime import timedelta
from typing import Final, final

from forze.application.contracts.counter import (
    CounterAdminPort,
    CounterCapabilities,
    CounterEntry,
    CounterPort,
    validate_counter_ttl,
    validate_counter_value,
)
from forze.base.exceptions import exc

from ..kernel.scripts import INCR_COUNTER_WITH_TTL
from ._logger import logger
from .base import RedisBaseAdapter
from .codecs import KEY_SEP

# ----------------------- #

_COUNTER_SCOPE: Final[str] = "counter"

_SCAN_COUNT: Final[int] = 500
"""``SCAN`` work-per-step hint (not a page size — a step may still return nothing)."""

_GLOB_META: Final[str] = "\\*?[]^"
"""Redis glob metacharacters, escaped with a backslash inside a ``MATCH`` pattern.

Backslash first: escaping it after the others would double-escape the backslashes they just
introduced.
"""

# ....................... #


def _glob_escape(literal: str) -> str:
    """Quote *literal* so ``SCAN MATCH`` treats it as itself and not as a pattern.

    A counter's key prefix interpolates the route's namespace (and a tenant id), which are
    application-supplied strings. Leave a ``[`` in one unescaped and Redis reads it as an
    unterminated character class: the pattern matches nothing, the scan returns no keys, and
    the counters are reported as *absent* rather than as an error — an export would then
    carry no sequence numbers and look complete.
    """

    return "".join(f"\\{ch}" if ch in _GLOB_META else ch for ch in literal)


# ....................... #


def _ttl_ms(ttl: timedelta) -> int:
    """*ttl* in whole milliseconds, rounded up so a sub-millisecond lifetime is not zero."""

    return math.ceil(ttl.total_seconds() * 1000)


# ....................... #


@final
class RedisCounterAdapter(CounterPort, RedisBaseAdapter):
    """Redis implementation of :class:`~forze.application.contracts.counter.CounterPort`.

    Uses ``INCRBY`` / ``DECRBY`` / ``GETSET`` for atomic counter operations. A ``ttl`` is a
    native key expiry: an increment that creates the key sets it in the same script
    (:data:`~forze_redis.kernel.scripts.INCR_COUNTER_WITH_TTL`), and a reset writes it with
    ``SET ... PX``. A zero step is a ``GET``, so a read never creates the key.
    """

    def __key(self, suffix: str | None) -> str:
        return self.construct_key(_COUNTER_SCOPE, suffix)

    # ....................... #

    def capabilities(self) -> CounterCapabilities:
        """Report expiry support (native key TTL)."""

        return CounterCapabilities(expiry=True)

    # ....................... #

    async def _bump(self, by: int, suffix: str | None, ttl: timedelta | None) -> int:
        validate_counter_ttl(ttl)

        await self._prepare_keys()
        key = self.__key(suffix)

        if by == 0:
            raw = await self.client.get(key)
            return int(raw) if raw is not None else 0

        if ttl is None:
            return await self.client.incr(key, by)

        return int(await self.client.run_script(INCR_COUNTER_WITH_TTL, [key], [by, _ttl_ms(ttl)]))

    # ....................... #

    async def incr(
        self,
        by: int = 1,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> int:
        logger.debug("Incrementing counter suffix '%s' by %s", suffix, by)

        return await self._bump(by, suffix, ttl)

    # ....................... #

    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> list[int]:
        if size < 1:
            raise exc.precondition("Batch size must be at least 1")

        logger.debug(
            "Incrementing counter suffix '%s' by %s, returning batch range",
            suffix,
            size,
        )

        max_cnt = await self._bump(size, suffix, ttl)

        return list(range(max_cnt - size + 1, max_cnt + 1))

    # ....................... #

    async def decr(
        self,
        by: int = 1,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> int:
        logger.debug("Decrementing counter suffix '%s' by %s", suffix, by)

        return await self._bump(-by, suffix, ttl)

    # ....................... #

    async def reset(
        self,
        value: int = 1,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> int:
        # Known up front, so it is refused here rather than by the backend — or, on
        # Redis, silently accepted and left to break the *next* allocation.
        validate_counter_value(value, operation="reset")
        validate_counter_ttl(ttl)

        await self._prepare_keys()
        key = self.__key(suffix)

        logger.debug("Resetting counter '%s' to %s", key, value)

        if ttl is not None:
            await self.client.set(key, str(value), px=_ttl_ms(ttl))
            return value

        # ``GETSET`` swaps atomically but hands back the *previous* value; the port
        # contract returns the value the counter now holds. Like any ``SET`` it drops an
        # expiry the key had, which is what a reset without a ttl means.
        await self.client.reset(key, value)
        return value


# ....................... #


@final
class RedisCounterAdminAdapter(CounterAdminPort, RedisBaseAdapter):
    """Enumerate the counters allocated under one Redis namespace (``SCAN`` + ``MGET``)."""

    async def list_counters(self) -> Sequence[CounterEntry]:
        await self._prepare_keys()

        # The unsuffixed counter's key *is* the prefix every suffixed one extends, because
        # ``construct_key`` simply drops a ``None`` trailing part. So one scan finds both.
        base = self.construct_key(_COUNTER_SCOPE)
        suffixed = base + KEY_SEP
        pattern = f"{_glob_escape(base)}*"

        # Keyed by the Redis key, so a key ``SCAN`` hands back on more than one step (which
        # it is allowed to do) collapses instead of being read and reported twice.
        found: dict[str, str | None] = {}
        cursor = 0

        while True:
            cursor, batch = await self.client.scan(cursor, match=pattern, count=_SCAN_COUNT)

            for key in batch:
                if key == base:
                    found[key] = None

                elif key.startswith(suffixed):
                    found[key] = key[len(suffixed) :]

                # Anything else merely *starts* like us: ``MATCH`` is a prefix glob, so a
                # spec named ``orders`` would otherwise swallow every counter of one named
                # ``orders_archive`` and export another route's sequence numbers as its own.

            # Only a zero cursor means the iteration is complete. An empty ``batch`` says
            # nothing — ``count`` bounds the work a step does, not the keys it returns — so
            # breaking on one would silently report a subset of the counters as all of them.
            if cursor == 0:
                break

        if not found:
            return []

        keys = list(found)
        values = await self.client.mget(keys)

        return [
            CounterEntry(suffix=found[key], value=int(raw))
            # A key that vanished between the scan and the read (a ``DEL``, or an expiry the
            # application set itself) reads back as ``None``. That is not a counter whose
            # value is zero — it is not a counter any more — so it is dropped rather than
            # exported as 0, which on import would rewind a live sequence to the start.
            for key, raw in zip(keys, values, strict=True)
            if raw is not None
        ]

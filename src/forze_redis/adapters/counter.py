"""Redis-backed :class:`~forze.application.contracts.counter.CounterPort` adapter."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from collections.abc import Sequence
from typing import Final, final

from forze.application.contracts.counter import (
    CounterAdminPort,
    CounterEntry,
    CounterPort,
)
from forze.base.exceptions import exc

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


@final
class RedisCounterAdapter(CounterPort, RedisBaseAdapter):
    """Redis implementation of :class:`~forze.application.contracts.counter.CounterPort`.

    Uses ``INCRBY`` / ``DECRBY`` / ``GETSET`` for atomic counter operations.
    """

    def __key(self, suffix: str | None) -> str:
        return self.construct_key(_COUNTER_SCOPE, suffix)

    # ....................... #

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        await self._prepare_keys()
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
        if size < 1:
            raise exc.precondition("Batch size must be at least 1")

        await self._prepare_keys()
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
        await self._prepare_keys()
        key = self.__key(suffix)

        logger.debug("Decrementing counter '%s' by %s", key, by)

        return await self.client.decr(key, by)

    # ....................... #

    async def reset(self, value: int = 1, *, suffix: str | None = None) -> int:
        await self._prepare_keys()
        key = self.__key(suffix)

        logger.debug("Resetting counter '%s' to %s", key, value)

        # ``GETSET`` swaps atomically but hands back the *previous* value; the port
        # contract returns the value the counter now holds.
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

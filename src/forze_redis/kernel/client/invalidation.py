"""Client-side caching invalidation hub (Redis ``CLIENT TRACKING``, RESP3 push).

One hub per :class:`~forze_redis.kernel.client.client.RedisClient`. It pins a
single dedicated connection from the pool, enables
``CLIENT TRACKING ON BCAST PREFIX ...`` on it, and consumes the RESP3
``invalidate`` push frames the server then sends for **every** write (by any
client), expiration, or eviction of a key matching a registered prefix.

redis-py (8+) negotiates RESP3 by default and routes ``invalidate`` push
frames to the connection parser's *invalidation push handler* — the hub
installs its own and drives frame consumption with a timeout read loop (plus
a periodic PING that keeps the connection alive; server-side tracking lives
and dies with it).

Failure posture is fail-open: any setup or stream error notifies subscribers
with a **reset** (their caches flush — events may have been missed) and the
hub rebuilds with jittered exponential backoff. Subscribers keep their own
TTL backstop, so a hub outage degrades to TTL-only staleness, never to
incorrectness.
"""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

import asyncio
import random
import time
from typing import Any, Awaitable, Callable, Sequence

import attrs

from forze_redis.kernel._logger import logger

# ----------------------- #

_POLL_TIMEOUT_S = 1.0
_KEEPALIVE_EVERY_S = 15.0
_BACKOFF_BASE_S = 0.5
_BACKOFF_MAX_S = 15.0


@attrs.define(slots=True)
class _Subscription:
    prefixes: tuple[str, ...]
    on_keys: Callable[[Sequence[str]], None]
    on_reset: Callable[[], None]
    active: bool = True


# ....................... #


@attrs.define(slots=True, kw_only=True)
class InvalidationHub:
    """Fan-out of ``invalidate`` push frames to local subscribers."""

    acquire_connection: Callable[[], Awaitable[Any]]
    """Check a dedicated connection out of the pool (stays pinned)."""

    release_connection: Callable[[Any], Awaitable[None]]
    """Disconnect and return the pinned connection to the pool."""

    clock: Callable[[], float] = time.monotonic

    _subs: list[_Subscription] = attrs.field(factory=list, init=False)
    _task: "asyncio.Task[None] | None" = attrs.field(default=None, init=False)
    _resubscribe: asyncio.Event = attrs.field(factory=asyncio.Event, init=False)
    _stopped: bool = attrs.field(default=False, init=False)

    # ....................... #

    async def subscribe(
        self,
        *,
        prefixes: Sequence[str],
        on_keys: Callable[[Sequence[str]], None],
        on_reset: Callable[[], None],
    ) -> Callable[[], Awaitable[None]]:
        """Register a subscriber; starts (or re-arms) the listener task."""

        sub = _Subscription(
            prefixes=tuple(prefixes),
            on_keys=on_keys,
            on_reset=on_reset,
        )
        self._subs.append(sub)

        if self._task is None or self._task.done():
            self._stopped = False
            self._task = asyncio.get_running_loop().create_task(self._run())

        else:
            # Tracking prefixes are fixed per CLIENT TRACKING session: re-arm
            # the loop so the union of prefixes is re-registered.
            self._resubscribe.set()

        async def _unsubscribe() -> None:
            sub.active = False

        return _unsubscribe

    # ....................... #

    async def stop(self) -> None:
        """Cancel the listener task and drop all subscriptions."""

        self._stopped = True
        task = self._task

        if task is not None and not task.done():
            task.cancel()

            try:
                await task

            except asyncio.CancelledError:
                pass

        self._task = None
        self._subs.clear()

    # ....................... #

    def _active_subs(self) -> list[_Subscription]:
        live = [sub for sub in self._subs if sub.active]

        if len(live) != len(self._subs):
            self._subs[:] = live

        return live

    # ....................... #

    def _notify_reset(self) -> None:
        for sub in self._active_subs():
            try:
                sub.on_reset()

            except Exception:  # noqa: BLE001 — subscriber bugs must not kill the hub
                logger.warning("Invalidation reset callback failed", exc_info=True)

    # ....................... #

    def _notify_keys(self, keys: Sequence[str]) -> None:
        for sub in self._active_subs():
            try:
                sub.on_keys(keys)

            except Exception:  # noqa: BLE001 — subscriber bugs must not kill the hub
                logger.warning("Invalidation key callback failed", exc_info=True)

    # ....................... #

    @staticmethod
    def _decode_keys(data: Any) -> list[str]:
        if isinstance(data, (bytes, str)):
            data = [data]

        keys: list[str] = []

        for item in data:
            if isinstance(item, bytes):
                keys.append(item.decode("utf-8", errors="replace"))

            else:
                keys.append(str(item))

        return keys

    # ....................... #

    async def _on_push(self, response: Any) -> None:
        """Parser-installed sink for ``invalidate`` push frames."""

        payload: Any = None

        if isinstance(response, (list, tuple)) and len(response) > 1:  # pyright: ignore[reportUnknownArgumentType]
            payload = response[1]  # pyright: ignore[reportUnknownVariableType]

        if payload is None:
            # FLUSHALL/FLUSHDB-style invalidation: everything may be stale.
            self._notify_reset()
            return

        self._notify_keys(self._decode_keys(payload))

    # ....................... #

    async def _setup(self) -> Any:
        conn = await self.acquire_connection()

        try:
            # The parser-level handler is where redis-py (8+) routes RESP3
            # ``invalidate`` push frames; without one they are dropped.
            conn._parser.set_invalidation_push_handler(self._on_push)  # noqa: SLF001

            prefixes = sorted(
                {p for sub in self._active_subs() for p in sub.prefixes}
            )
            args: list[Any] = ["CLIENT", "TRACKING", "ON", "BCAST"]

            for prefix in prefixes:
                args.extend(("PREFIX", prefix))

            await conn.send_command(*args)
            response = await conn.read_response()

            if response not in (b"OK", "OK", True):
                raise RuntimeError(f"CLIENT TRACKING refused: {response!r}")

            logger.debug("Invalidation tracking enabled (prefixes=%s)", prefixes)

            return conn

        except BaseException:
            await self._teardown(conn)
            raise

    # ....................... #

    async def _teardown(self, conn: Any) -> None:
        try:
            if conn is not None:
                # Disconnecting turns tracking off server-side; the connection
                # returns to the pool and reconnects cleanly on next use.
                await self.release_connection(conn)

        except Exception:  # noqa: BLE001 — best-effort cleanup
            logger.debug("Invalidation connection release failed", exc_info=True)

    # ....................... #

    async def _run(self) -> None:
        backoff = _BACKOFF_BASE_S

        while not self._stopped:
            conn: Any = None
            self._resubscribe.clear()

            try:
                conn = await self._setup()

                # Events before/while (re)connecting may have been missed:
                # every subscriber flushes rather than trusting stale state.
                self._notify_reset()
                backoff = _BACKOFF_BASE_S
                last_ping = self.clock()

                while not self._stopped and not self._resubscribe.is_set():
                    # Push frames are dispatched to ``_on_push`` by the parser
                    # inside this read; PONGs and timeouts surface here and
                    # are ignored. The read is what drives the stream.
                    await conn.read_response(
                        timeout=_POLL_TIMEOUT_S,
                        push_request=True,
                    )

                    if self.clock() - last_ping >= _KEEPALIVE_EVERY_S:
                        # Tracking lives and dies with this connection — keep
                        # it alive and fail fast when it silently died.
                        await conn.send_command("PING")
                        last_ping = self.clock()

            except asyncio.CancelledError:
                raise

            except Exception:
                if self._stopped:
                    break

                logger.warning(
                    "Invalidation stream failed; flushing subscribers and "
                    "reconnecting in ~%.1fs",
                    backoff,
                    exc_info=True,
                )
                self._notify_reset()
                # Reconnect jitter — not security randomness.
                await asyncio.sleep(backoff * random.uniform(0.8, 1.2))  # nosec B311
                backoff = min(backoff * 2.0, _BACKOFF_MAX_S)

            finally:
                await self._teardown(conn)

            if not self._active_subs():
                # Every subscriber detached: stop listening until the next one.
                break

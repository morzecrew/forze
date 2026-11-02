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

Failure posture is fail-open: any setup or stream error, or a keepalive PING
left unanswered, notifies subscribers with a **reset** (their caches flush —
events may have been missed) and the hub rebuilds with jittered exponential
backoff, which resets only once a connection has answered a keepalive PING.
Subscribers keep their own TTL backstop, so a hub outage degrades to
TTL-only staleness, never to incorrectness. A subscriber that must not start
on a stream that never came up asks to wait until it is live; stopping the
hub tells every subscriber the stream has ended.
"""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Sequence
from datetime import timedelta
from typing import Any

import attrs

from forze.base.primitives import current_entropy_source, monotonic
from forze_redis.kernel._logger import logger

# ----------------------- #

_POLL_TIMEOUT_S = 1.0
_KEEPALIVE_EVERY_S = 15.0
_PONG_TIMEOUT_S = 5.0
_BACKOFF_BASE_S = 0.5
_BACKOFF_MAX_S = 15.0


@attrs.define(slots=True)
class _Subscription:
    prefixes: tuple[str, ...]
    on_keys: Callable[[Sequence[str]], None]
    on_reset: Callable[[], None]
    on_end: Callable[[], None] | None = None
    live: "asyncio.Future[bool] | None" = None
    active: bool = True


# ....................... #


@attrs.define(slots=True, kw_only=True)
class InvalidationHub:
    """Fan-out of ``invalidate`` push frames to local subscribers."""

    acquire_connection: Callable[[], Awaitable[Any]]
    """Check a dedicated connection out of the pool (stays pinned)."""

    release_connection: Callable[[Any], Awaitable[None]]
    """Disconnect and return the pinned connection to the pool."""

    clock: Callable[[], float] = monotonic

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
        on_end: Callable[[], None] | None = None,
        live_within: timedelta | None = None,
    ) -> Callable[[], Awaitable[None]] | None:
        """Register a subscriber; starts (or re-arms) the listener task.

        With *live_within*, returns once tracking is set up with this subscriber's prefixes,
        or ``None``, detached, when that setup fails, takes longer or the hub stops; a wait
        that is cancelled detaches too. *on_end* is told when the hub stops; without it,
        *on_reset* is.
        """

        sub = _Subscription(
            prefixes=tuple(prefixes),
            on_keys=on_keys,
            on_reset=on_reset,
            on_end=on_end,
            live=asyncio.get_running_loop().create_future() if live_within is not None else None,
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

            if not self._active_subs():
                # Re-arm the loop: the inner read exits, teardown releases the
                # pinned connection, and `_run` then stops because no active
                # subscriber remains (same path as a prefix-set change).
                self._resubscribe.set()

        if sub.live is not None and live_within is not None:
            live = False

            try:
                # Shielded, so a timeout leaves the outcome readable.
                live = await asyncio.wait_for(
                    asyncio.shield(sub.live), timeout=live_within.total_seconds()
                )

            except TimeoutError:
                # The stream may have come up at the deadline itself.
                live = sub.live.done() and not sub.live.cancelled() and sub.live.result()

            finally:
                # Timed out, refused, stopped or cancelled: nobody holds this subscription.
                if not live:
                    await _unsubscribe()

            if not live:
                return None

        return _unsubscribe

    # ....................... #

    async def stop(self) -> None:
        """Cancel the listener task, tell every subscriber the stream ended, and drop them."""

        self._stopped = True
        task = self._task

        if task is not None and not task.done():
            task.cancel()

            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._task = None

        for sub in self._active_subs():
            try:
                (sub.on_end or sub.on_reset)()

            except Exception:
                logger.warning("Invalidation end callback failed", exc_info=True)

        self._settle(self._subs, live=False)
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

            except Exception:
                logger.warning("Invalidation reset callback failed", exc_info=True)

    # ....................... #

    @staticmethod
    def _settle(subs: Sequence[_Subscription], *, live: bool) -> None:
        for sub in subs:
            if sub.live is not None and not sub.live.done():
                sub.live.set_result(live)

    # ....................... #

    def _notify_keys(self, keys: Sequence[str]) -> None:
        for sub in self._active_subs():
            try:
                sub.on_keys(keys)

            except Exception:
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
        """The tracking connection, or ``None`` when no subscriber is left to track for."""

        conn = await self.acquire_connection()

        try:
            # RESP3 is required for server ``invalidate`` push frames. redis-py 8 negotiates
            # it by default; guard against a client explicitly pinned to RESP2.
            if getattr(conn, "protocol", None) != 3:
                raise RuntimeError(
                    "Client-side caching requires a RESP3 connection; configure the Redis "
                    "client with protocol=3."
                )

            # The parser-level handler is where redis-py (8+) routes RESP3
            # ``invalidate`` push frames; without one they are dropped.
            conn._parser.set_invalidation_push_handler(self._on_push)

            prefixes = sorted({p for sub in self._active_subs() for p in sub.prefixes})

            if not prefixes:
                # Every subscriber left while the connection was coming: BCAST with no
                # prefix would track every key.
                await self._teardown(conn)
                return None

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

        except Exception:
            logger.debug("Invalidation connection release failed", exc_info=True)

    # ....................... #

    async def _run(self) -> None:
        backoff = _BACKOFF_BASE_S

        while not self._stopped:
            if not self._active_subs():
                # Every subscriber detached: stop listening until the next one.
                break

            conn: Any = None
            self._resubscribe.clear()
            # The subscribers whose prefixes this setup registers.
            included = list(self._active_subs())

            try:
                conn = await self._setup()

                if conn is None:
                    continue

                # Events before/while (re)connecting may have been missed:
                # every subscriber flushes rather than trusting stale state.
                self._notify_reset()
                self._settle(included, live=True)
                last_ping = self.clock()
                pinged_at: float | None = None

                while not self._stopped and not self._resubscribe.is_set():
                    # Push frames are dispatched to ``_on_push`` by the parser
                    # inside this read; PONGs and timeouts surface here. The
                    # read is what drives the stream.
                    response = await conn.read_response(
                        timeout=_POLL_TIMEOUT_S,
                        push_request=True,
                    )

                    if response in (b"PONG", "PONG"):
                        pinged_at = None
                        # Only a connection that has answered counts as recovered: one
                        # that sets up and then stalls keeps backing off.
                        backoff = _BACKOFF_BASE_S

                    now = self.clock()

                    if pinged_at is not None and now - pinged_at >= _PONG_TIMEOUT_S:
                        # A half-open connection reads as quiet, not closed:
                        # a missing PONG is the only sign of the gap.
                        raise ConnectionError("Invalidation keepalive PING went unanswered")

                    if pinged_at is None and now - last_ping >= _KEEPALIVE_EVERY_S:
                        # Tracking lives and dies with this connection — keep
                        # it alive and fail fast when it silently died.
                        await conn.send_command("PING")
                        last_ping = pinged_at = now

            except asyncio.CancelledError:
                raise

            except Exception:
                if self._stopped:
                    break

                logger.warning(
                    "Invalidation stream failed; flushing subscribers and reconnecting in ~%.1fs",
                    backoff,
                    exc_info=True,
                )
                self._notify_reset()
                self._settle(included, live=False)
                # Reconnect jitter — not security randomness.
                await asyncio.sleep(
                    backoff * current_entropy_source().as_random().uniform(0.8, 1.2)
                )
                backoff = min(backoff * 2.0, _BACKOFF_MAX_S)

            finally:
                await self._teardown(conn)

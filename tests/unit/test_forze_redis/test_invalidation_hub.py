"""The invalidation hub over a fake connection: when it is live, when it notices a dead one,
and what its subscribers hear when it stops."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import attrs
import pytest
import structlog
from redis.exceptions import ResponseError

from forze_redis.kernel.client.invalidation import InvalidationHub

pytestmark = pytest.mark.unit

# ----------------------- #


@attrs.define
class _Clock:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now


@attrs.define
class _Parser:
    handler: Any = None

    def set_invalidation_push_handler(self, handler: Any) -> None:
        self.handler = handler


@attrs.define
class _Conn:
    """A tracking connection; each timed read waits out its timeout on the fake clock."""

    clock: _Clock
    protocol: int = 3
    tracking_reply: Any = b"OK"
    answers_ping: bool = True
    push_before_pong: bool = False
    sent: list[tuple[Any, ...]] = attrs.field(factory=list)
    _parser: _Parser = attrs.field(factory=_Parser)
    _pong: bool = False
    _pushed: bool = False

    async def send_command(self, *args: Any) -> None:
        self.sent.append(args)
        self._pong = args == ("PING",) and self.answers_ping

    async def read_response(self, timeout: float | None = None, **_kwargs: Any) -> Any:
        await asyncio.sleep(0)

        if timeout is None:  # the CLIENT TRACKING reply
            if isinstance(self.tracking_reply, Exception):
                raise self.tracking_reply

            return self.tracking_reply

        if self._pong and self.push_before_pong and not self._pushed:
            # An invalidation the server sent before its PONG: the parser hands the frame to
            # the handler, and the read returns it.
            self._pushed = True
            frame = [b"invalidate", [b"p:k"]]
            await self._parser.handler(frame)
            return frame

        if self._pong:
            self._pong = self._pushed = False
            return b"PONG"

        self.clock.now += timeout
        return None

    def pings(self) -> int:
        return self.sent.count(("PING",))


@attrs.define
class _Subscriber:
    resets: int = 0
    ends: int = 0
    keys: list[str] = attrs.field(factory=list)

    def reset(self) -> None:
        self.resets += 1

    def end(self) -> None:
        self.ends += 1


def _hub(
    conn: _Conn, *, gate: asyncio.Event | None = None, drops: int = 0
) -> tuple[InvalidationHub, list[_Conn]]:
    """A hub over *conn*; with *gate*, checking the connection out waits for it, and the first
    *drops* checkouts fail as a dropped connection does."""

    released: list[_Conn] = []
    failures = [drops]

    async def acquire() -> _Conn:
        if gate is not None:
            await gate.wait()

        if failures[0] > 0:
            failures[0] -= 1
            raise ConnectionError("connection dropped")

        return conn

    async def release(c: _Conn) -> None:
        released.append(c)

    return InvalidationHub(
        acquire_connection=acquire, release_connection=release, clock=conn.clock
    ), released


async def _until(predicate: Callable[[], bool]) -> None:
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.005)


async def _subscribe(hub: InvalidationHub, sub: _Subscriber, **kwargs: Any) -> Any:
    return await hub.subscribe(prefixes=["p:"], on_keys=sub.keys.extend, on_reset=sub.reset, **kwargs)


def _active(hub: InvalidationHub) -> list[Any]:
    return hub._active_subs()  # pyright: ignore[reportPrivateUsage]


def _tracking(conn: _Conn) -> list[tuple[Any, ...]]:
    return [args for args in conn.sent if args[:2] == ("CLIENT", "TRACKING")]


class TestGoingLive:
    async def test_with_a_bound_it_returns_once_tracking_is_set_up(self) -> None:
        hub, _released = _hub(_Conn(clock=_Clock()))
        sub = _Subscriber()

        unsubscribe = await _subscribe(hub, sub, live_within=timedelta(seconds=1))

        assert unsubscribe is not None
        assert sub.resets == 1  # the connect flush came first
        await hub.stop()

    async def test_a_refused_setup_returns_none_at_once(self) -> None:
        hub, _released = _hub(_Conn(clock=_Clock(), protocol=2))  # RESP2: no push frames

        async with asyncio.timeout(5):  # not the 30 s bound: the failure answers
            unsubscribe = await _subscribe(hub, _Subscriber(), live_within=timedelta(seconds=30))

        assert unsubscribe is None
        await hub.stop()

    async def test_a_tracking_command_the_server_refuses_returns_none_at_once(self) -> None:
        conn = _Conn(clock=_Clock(), tracking_reply=ResponseError("NOPERM no CLIENT TRACKING"))
        hub, _released = _hub(conn)

        async with asyncio.timeout(5):
            unsubscribe = await _subscribe(hub, _Subscriber(), live_within=timedelta(seconds=30))

        assert unsubscribe is None
        await hub.stop()

    async def test_a_dropped_connection_is_retried_within_the_bound(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("forze_redis.kernel.client.invalidation._BACKOFF_BASE_S", 0.01)
        hub, _released = _hub(_Conn(clock=_Clock()), drops=2)

        unsubscribe = await _subscribe(hub, _Subscriber(), live_within=timedelta(seconds=5))

        assert unsubscribe is not None
        await hub.stop()

    async def test_a_connection_that_never_comes_waits_out_the_bound(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("forze_redis.kernel.client.invalidation._BACKOFF_BASE_S", 0.01)
        hub, _released = _hub(_Conn(clock=_Clock()), drops=1_000_000)
        loop = asyncio.get_running_loop()
        started = loop.time()

        unsubscribe = await _subscribe(hub, _Subscriber(), live_within=timedelta(seconds=0.2))

        assert unsubscribe is None
        assert loop.time() - started >= 0.19  # the bound, not the first failure
        assert _active(hub) == []
        await hub.stop()

    async def test_without_a_bound_it_returns_before_the_setup(self) -> None:
        hub, _released = _hub(_Conn(clock=_Clock(), protocol=2))

        assert await _subscribe(hub, _Subscriber()) is not None
        await hub.stop()

    async def test_a_setup_that_hangs_past_the_bound_returns_none_and_detaches(self) -> None:
        hub, _released = _hub(_Conn(clock=_Clock()), gate=asyncio.Event())

        unsubscribe = await _subscribe(hub, _Subscriber(), live_within=timedelta(milliseconds=50))

        assert unsubscribe is None
        assert _active(hub) == []
        await hub.stop()

    async def test_a_cancelled_wait_detaches(self) -> None:
        hub, _released = _hub(_Conn(clock=_Clock()), gate=asyncio.Event())
        waiting = asyncio.create_task(
            _subscribe(hub, _Subscriber(), live_within=timedelta(seconds=30))
        )
        await asyncio.sleep(0.01)

        waiting.cancel()

        with pytest.raises(asyncio.CancelledError):
            await waiting

        assert _active(hub) == []
        await hub.stop()

    async def test_stopping_the_hub_answers_a_waiter(self) -> None:
        hub, _released = _hub(_Conn(clock=_Clock()), gate=asyncio.Event())
        waiting = asyncio.create_task(
            _subscribe(hub, _Subscriber(), live_within=timedelta(seconds=30))
        )
        await asyncio.sleep(0.01)

        await hub.stop()

        async with asyncio.timeout(5):
            assert await waiting is None


class TestNoSubscriberLeft:
    async def test_no_tracking_is_armed_for_nobody(self) -> None:
        # The only subscriber gave up while the connection was being checked out: tracking
        # with no prefix would broadcast every key in the database.
        conn, gate = _Conn(clock=_Clock()), asyncio.Event()
        hub, released = _hub(conn, gate=gate)
        await _subscribe(hub, _Subscriber(), live_within=timedelta(milliseconds=50))

        gate.set()
        await _until(lambda: released == [conn])

        assert _tracking(conn) == []
        await _until(lambda: hub._task is None or hub._task.done())  # pyright: ignore[reportPrivateUsage]


class TestKeepalive:
    async def test_an_unanswered_ping_is_a_gap(self) -> None:
        conn = _Conn(clock=_Clock(), answers_ping=False)
        hub, released = _hub(conn)
        sub = _Subscriber()
        await _subscribe(hub, sub)

        await _until(lambda: sub.resets == 2)  # the connect flush, then the gap's
        assert conn.pings() == 1

        await _until(lambda: released == [conn])  # the dead connection is let go
        await hub.stop()

    async def test_an_answered_ping_keeps_the_stream(self) -> None:
        conn = _Conn(clock=_Clock())
        hub, released = _hub(conn)
        sub = _Subscriber()
        await _subscribe(hub, sub)

        await _until(lambda: conn.pings() >= 3)

        assert sub.resets == 1
        assert released == []
        await hub.stop()

    async def test_a_pong_behind_an_invalidation_is_still_seen(self) -> None:
        conn = _Conn(clock=_Clock(), push_before_pong=True)
        hub, released = _hub(conn)
        sub = _Subscriber()
        await _subscribe(hub, sub)

        await _until(lambda: conn.pings() >= 3)

        assert sub.keys[:1] == ["p:k"]
        assert sub.resets == 1
        assert released == []
        await hub.stop()

    async def test_repeated_gaps_back_off_further_each_time(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Each connection sets up and then never answers: setting up is not recovering.
        monkeypatch.setattr("forze_redis.kernel.client.invalidation._BACKOFF_BASE_S", 0.1)
        conn = _Conn(clock=_Clock(), answers_ping=False)
        hub, _released = _hub(conn)

        with structlog.testing.capture_logs() as logs:
            await _subscribe(hub, _Subscriber())

            def failures() -> list[str]:
                return [
                    log["event"]
                    for log in logs
                    if str(log["event"]).startswith("Invalidation stream failed")
                ]

            await _until(lambda: len(failures()) >= 3)
            await hub.stop()

        assert [event.rsplit("~", 1)[1] for event in failures()[:3]] == ["0.1s", "0.2s", "0.4s"]


class TestStopping:
    async def test_every_subscriber_hears_the_end(self) -> None:
        hub, _released = _hub(_Conn(clock=_Clock()))
        with_end, without_end = _Subscriber(), _Subscriber()
        await _subscribe(hub, with_end, on_end=with_end.end, live_within=timedelta(seconds=1))
        await _subscribe(hub, without_end, live_within=timedelta(seconds=1))
        resets = without_end.resets

        await hub.stop()

        assert (with_end.ends, without_end.ends) == (1, 0)
        assert without_end.resets == resets + 1  # told by its reset instead

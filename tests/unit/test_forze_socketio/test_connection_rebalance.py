"""Rebalancing: a replica well above its fleet's mean sheds movable transports, at a rate.

# covers: forze_socketio.rebalance (connection_rebalance_lifecycle_step, connection_report)

Run against python-socketio's own server and manager, with Engine.IO sockets stood in by a
fake whose close fires the library's own Engine.IO disconnect, as a real socket does. The
fleet is the mock, and the step runs in a runtime on the virtual-time loop, so minutes of
ticks take milliseconds and every count below is exact.
"""

from __future__ import annotations

import asyncio
import io
import json
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import pytest
from socketio.async_server import AsyncServer

from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.fleet import FleetSpec, MemberState, Report
from forze.application.execution import DeploymentProfile, build_runtime
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import instance_id
from forze_dst.runtime import run_simulation
from forze_mock import MockDepsModule
from forze_mock.adapters import MockFleetAdapter
from forze_mock.state import MockState
from forze_socketio import connection_rebalance_lifecycle_step, connection_report
from forze_socketio.drain import _FINISHING, _ClosesFinishing
from forze_socketio.rebalance import _Rebalance

# ----------------------- #

SPEC = FleetSpec(name="gateways", ttl=timedelta(hours=1))
SECOND = timedelta(seconds=1)
_REPORTED: Report = {"connections": 0, "movable": 0}
"""This replica's own report: present, so it takes part, but never read — live counts win."""


class _Socket:
    """An Engine.IO socket stand-in: its close fires the disconnect after *handlers* app
    disconnect handlers that take *delay* each. A cancel ends the handler it interrupts and
    the close goes on to the next, as python-socketio's does; a second close while one is
    under way does nothing, as python-engineio's does."""

    def __init__(self, server: AsyncServer, eio_sid: str, delay: float, handlers: int) -> None:
        self.server = server
        self.sid = eio_sid
        self.delay = delay
        self.handlers = handlers
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self.closing = False
        self.closed = False
        self.closes = 0
        self.cancelled = False

    async def close(self, wait: bool = True, abort: bool = False, reason: Any = None) -> None:
        self.closes += 1

        if self.closing or self.closed:
            return

        self.closing = True

        for _handler in range(self.handlers):
            try:
                await asyncio.sleep(self.delay)

            except asyncio.CancelledError:
                self.cancelled = True

        await self.server.eio._trigger_event("disconnect", self.sid, reason, run_async=False)
        self.closed = True


class _Node:
    """One replica's server and the sockets it was given."""

    def __init__(self) -> None:
        self.server = AsyncServer()
        self.sockets: dict[str, _Socket] = {}

    async def connect(self, count: int, *namespaces: str, delay: float = 0.0) -> list[str]:
        """Open *count* Engine.IO connections, each carrying every one of *namespaces*."""

        eio_sids = [f"eio-{len(self.sockets) + index}" for index in range(count)]

        for eio_sid in eio_sids:
            self.sockets[eio_sid] = _Socket(self.server, eio_sid, delay, len(namespaces))
            self.server.eio.sockets[eio_sid] = self.sockets[eio_sid]
            self.server.environ[eio_sid] = {}

            for namespace in namespaces:
                await self.server.manager.connect(eio_sid, namespace)

        return eio_sids

    def closed(self, eio_sids: list[str] | None = None) -> int:
        """How many of *eio_sids* (or of every socket) closed."""

        chosen = self.sockets if eio_sids is None else eio_sids

        return sum(1 for eio_sid in chosen if self.sockets[eio_sid].closed)

    def close_calls(self) -> int:
        return sum(socket.closes for socket in self.sockets.values())

    def namespace_connections(self) -> int:
        return sum(
            1
            for namespace in list(self.server.manager.get_namespaces())
            for _ in self.server.manager.get_participants(namespace, None)
        )


def _lobby(namespace: str, sid: str) -> bool:
    return namespace == "/lobby"


def _step(node: _Node, **overrides: Any) -> Any:
    options: dict[str, Any] = {
        "fleet": SPEC,
        "every": SECOND,
        "member_id": "me",
        "namespaces": ("/lobby", "/game"),
        "movable": _lobby,
        "per_minute": 600,
        "settle": timedelta(0),
        "cooldown": timedelta(0),
    }
    options.update(overrides)

    return connection_rebalance_lifecycle_step(node.server, **options)


async def _beat(
    state: MockState,
    member_id: str,
    report: Report,
    member_state: MemberState = "serving",
) -> None:
    fleet = MockFleetAdapter(spec=SPEC, state=state, namespace=str(SPEC.name))
    await fleet.beat(member_id, member_state, report)


async def _peers(state: MockState, *loads: int) -> None:
    for index, load in enumerate(loads):
        await _beat(state, f"peer-{index}", {"connections": load, "movable": load})


Probe = Callable[[ExecutionRuntime], Awaitable[None]]


def _slow_views(surface: str | None, route: str | None, op: str) -> float:
    """Every fleet view takes two seconds of virtual time."""

    return 2.0 if op == "view" else 0.0


async def _serve(state: MockState, step: Any, probe: Probe) -> None:
    """Run *step* in a runtime on the mock fleet while *probe* looks on."""

    runtime = build_runtime(
        MockDepsModule(state=state), lifecycle_steps=step, drain_timeout=timedelta(0)
    )

    async with runtime.scope():
        await probe(runtime)


def _sleep(seconds: float) -> Probe:
    async def probe(_runtime: ExecutionRuntime) -> None:
        await asyncio.sleep(seconds)

    return probe


# ----------------------- #


class TestShedding:
    def test_sheds_toward_the_mean_at_the_rate_and_only_movable(self) -> None:
        async def scenario() -> tuple[int, int, int, int]:
            state, node = MockState(), _Node()
            lobby = await node.connect(30, "/lobby")
            game = await node.connect(10, "/game")
            await _peers(state, 20, 20)
            await _beat(state, "me", _REPORTED)
            seen: list[int] = []

            async def probe(_runtime: ExecutionRuntime) -> None:
                await asyncio.sleep(60.5)
                seen.append(node.closed())
                await asyncio.sleep(600)

            await _serve(state, _step(node, per_minute=6), probe)

            return seen[0], node.closed(lobby), node.closed(game), len(node.server.eio.sockets)

        first_minute, lobby_closed, game_closed, still_open = run_simulation(scenario)

        # 40 against peers at 20: the mean is 26.7, so 40 is 50% above it. The allowance
        # starts empty and fills at 6 a minute, one every ten seconds.
        assert first_minute == 6
        # It stops at 24, the most within 15% of the mean (24 + 20 + 20) / 3.
        assert lobby_closed == 16
        assert game_closed == 0
        assert still_open == 24

    def test_the_oldest_movable_transports_go_first(self) -> None:
        async def scenario() -> list[str]:
            state, node = MockState(), _Node()
            await node.connect(100, "/lobby")
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)

            await _serve(state, _step(node), _sleep(1.95))

            return [eio_sid for eio_sid, socket in node.sockets.items() if socket.closed]

        # The 1 s tick sheds ten: the first ten to connect.
        assert run_simulation(scenario) == [f"eio-{index}" for index in range(10)]

    def test_a_transport_no_longer_movable_when_its_close_comes_is_kept(self) -> None:
        async def scenario() -> tuple[int, int]:
            state, node = MockState(), _Node()
            await node.connect(100, "/lobby")
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)
            in_game: list[str] = []

            async def probe(_runtime: ExecutionRuntime) -> None:
                # The 10 s tick picks 67 and closes them over ten seconds; half a second in,
                # every lobby client still open joins a game.
                await asyncio.sleep(10.5)

                for eio_sid, socket in node.sockets.items():
                    if not socket.closes:
                        await node.server.manager.connect(eio_sid, "/game")
                        in_game.append(eio_sid)

                await asyncio.sleep(15)

            await _serve(state, _step(node, every=timedelta(seconds=10)), probe)

            return node.closed(), node.closed(in_game)

        closed, closed_in_game = run_simulation(scenario)

        # Only the closes already started by 10.5 s went ahead.
        assert closed_in_game == 0
        assert closed == 4


class TestNoDoubleShedding:
    def test_a_tick_while_a_shed_is_closing_does_nothing(self) -> None:
        async def scenario() -> list[int]:
            state, node = MockState(), _Node()
            # Each close takes 0.8 s: ten spread over one second end at 2.7 s, past the
            # next tick at 2 s.
            await node.connect(100, "/lobby", delay=0.8)
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)
            seen: list[int] = []

            async def probe(_runtime: ExecutionRuntime) -> None:
                for moment in (1.55, 2.5, 3.02):
                    await asyncio.sleep(moment - asyncio.get_running_loop().time())
                    seen.append(node.close_calls())

            await _serve(state, _step(node, per_minute=600), probe)

            return seen

        # Ten a second: the 1 s tick sheds ten; the 2 s tick finds them closing and leaves
        # them; the 3 s tick sheds again once they are done.
        assert run_simulation(scenario) == [6, 10, 11]

    def test_a_transport_still_closing_is_not_counted_or_picked_again(self) -> None:
        async def scenario() -> int:
            state, node = MockState(), _Node()
            # Each close takes 5 s: the 1 s shed's ten outlive its tail, which ends at 2.9 s.
            await node.connect(100, "/lobby", delay=5)
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)

            await _serve(state, _step(node, per_minute=600), _sleep(3.52))

            return sum(1 for socket in node.sockets.values() if socket.closes)

        # The 3 s tick sheds twenty, starting one every 0.05 s, from the eleventh transport
        # on: the first ten are still closing, so they are neither counted nor picked.
        assert run_simulation(scenario) == 21

    def test_a_tick_inside_the_cooldown_does_nothing(self) -> None:
        async def scenario() -> list[int]:
            state, node = MockState(), _Node()
            await node.connect(100, "/lobby")
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)
            seen: list[int] = []

            async def probe(_runtime: ExecutionRuntime) -> None:
                for moment in (2.5, 11.5, 12.01):
                    await asyncio.sleep(moment - asyncio.get_running_loop().time())
                    seen.append(node.closed())

            await _serve(state, _step(node, cooldown=timedelta(seconds=10)), probe)

            return seen

        # The 1 s shed ends at 1.9 s; the next may start ten seconds after, at the 12 s tick.
        assert run_simulation(scenario) == [10, 10, 11]

    def test_the_live_count_sets_the_shed_not_a_stale_report(self) -> None:
        async def scenario() -> int:
            state, node = MockState(), _Node()
            await node.connect(30, "/lobby")
            await _peers(state, 30, 30)
            # Its own last report is from before a shed: 100 open, far above the mean.
            await _beat(state, "me", {"connections": 100, "movable": 100})

            await _serve(state, _step(node), _sleep(10))

            return node.closed()

        assert run_simulation(scenario) == 0


class TestRate:
    @pytest.mark.parametrize("every", [SECOND, timedelta(seconds=30)])
    def test_ten_a_minute_whatever_the_interval(self, every: timedelta) -> None:
        async def scenario() -> int:
            state, node = MockState(), _Node()
            await node.connect(200, "/lobby")
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)

            # Ten minutes of ticks, then up to just before the next one, so the last
            # shed's closes have landed.
            seconds = 600 + every.total_seconds() - 0.1
            await _serve(state, _step(node, every=every, per_minute=10), _sleep(seconds))

            return node.closed()

        # Every second: one each sixth tick. Every 30 s: five each tick. A hundred either way.
        assert run_simulation(scenario) == 100

    def test_slow_ticks_keep_the_rate(self) -> None:
        async def scenario() -> int:
            state, node = MockState(), _Node()
            await node.connect(200, "/lobby")
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)

            # Ticks start every 3 s; the one at 600 s reads until 602 s and sheds.
            await _serve(state, _step(node, per_minute=10), _sleep(602.5))

            return node.closed()

        # Each view takes 2 s, so ticks one second apart start 3 s apart: the allowance
        # follows the time that passed, not the ticks, and a minute still sheds ten.
        assert run_simulation(scenario, latency=_slow_views) == 100

    def test_a_second_scope_starts_with_an_empty_allowance(self) -> None:
        async def scenario() -> int:
            state, node = MockState(), _Node()
            await node.connect(200, "/lobby")
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)
            step = _step(node, per_minute=6)

            await _serve(state, step, _sleep(60.5))
            closed = node.closed()
            # A minute later, the same step in a new scope: it waits for its allowance again.
            await asyncio.sleep(60)
            await _serve(state, step, _sleep(9.5))

            return node.closed() - closed

        assert run_simulation(scenario) == 0

    def test_the_default_cooldown_keeps_the_rate(self) -> None:
        async def scenario() -> int:
            state, node = MockState(), _Node()
            await node.connect(3000, "/lobby")
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)
            # settle and cooldown at their defaults: 120 s.
            step = connection_rebalance_lifecycle_step(
                node.server,
                fleet=SPEC,
                every=timedelta(seconds=10),
                member_id="me",
                namespaces=("/lobby",),
                movable=_lobby,
                per_minute=60,
            )

            await _serve(state, step, _sleep(1200))

            return node.closed()

        # Settled at 120 s, then a shed every 130 s (ten closing, 120 cooling down) that
        # spends what the 130 s refilled: 1,160 by 20 minutes, about 60 a minute after the
        # settle. Banking only a minute would have shed 60 each time: 540.
        assert run_simulation(scenario) == 1160

    @pytest.mark.parametrize(
        ("cooldown", "banked"),
        [
            # A minute's worth of sixty a minute, not ten minutes', then the one the next
            # second refills.
            (timedelta(0), 61),
            # The cooldown plus three one-second intervals: 303 s.
            (timedelta(minutes=5), 303),
        ],
    )
    def test_a_long_wait_banks_at_most_one_shed_cycle(
        self, cooldown: timedelta, banked: int
    ) -> None:
        async def scenario() -> int:
            state, node = MockState(), _Node()
            await node.connect(1000, "/lobby")
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)

            # Ten minutes before the fleet counts as settled, then the first shed.
            step = _step(node, per_minute=60, settle=timedelta(minutes=10), cooldown=cooldown)
            await _serve(state, step, _sleep(601.5))

            return node.closed()

        assert run_simulation(scenario) == banked


class TestSharedTransports:
    def test_a_transport_with_a_namespace_that_may_not_move_is_never_shed(self) -> None:
        async def scenario() -> int:
            state, node = MockState(), _Node()
            await node.connect(40, "/lobby", "/game")
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)

            await _serve(state, _step(node), _sleep(10))

            return node.closed()

        assert run_simulation(scenario) == 0

    def test_an_excess_of_twenty_closes_twenty_transports(self) -> None:
        async def scenario() -> tuple[int, int]:
            state, node = MockState(), _Node()
            await node.connect(40, "/lobby", "/chat")
            await _peers(state, 10, 10)
            await _beat(state, "me", _REPORTED)

            await _serve(
                state,
                _step(
                    node,
                    namespaces=("/lobby", "/chat"),
                    movable=lambda namespace, sid: True,
                    per_minute=6000,
                    cooldown=timedelta(hours=1),
                ),
                _sleep(5),
            )

            return node.closed(), node.namespace_connections()

        # 40 transports against a mean of 20 is 20 over: twenty transports close, and the
        # forty connections they carried with them.
        assert run_simulation(scenario) == (20, 40)


class TestNoShedding:
    @pytest.mark.parametrize(
        ("case", "settle"),
        [
            ("unsettled", timedelta(hours=1)),
            ("below the tolerance", timedelta(0)),
            ("peers without a report", timedelta(0)),
            ("no entry of its own", timedelta(0)),
            ("no report of its own", timedelta(0)),
            ("not serving", timedelta(0)),
        ],
    )
    def test_nothing_sheds(self, case: str, settle: timedelta) -> None:
        async def scenario() -> int:
            state, node = MockState(), _Node()
            await node.connect(34, "/lobby")

            if case == "below the tolerance":
                await _peers(state, 30, 30)  # the mean is 31.3; 15% above is 36

            elif case == "peers without a report":
                await _beat(state, "peer-0", {})
                await _beat(state, "peer-1", {"movable": 0})

            else:
                await _peers(state, 0, 0)

            if case == "not serving":
                await _beat(state, "me", _REPORTED, "starting")

            elif case == "no report of its own":
                await _beat(state, "me", {"movable": 34})

            elif case != "no entry of its own":
                await _beat(state, "me", _REPORTED)

            await _serve(state, _step(node, settle=settle), _sleep(60))

            return node.closed()

        assert run_simulation(scenario) == 0

    def test_a_drain_announced_during_the_read_starts_no_shed(self) -> None:
        async def scenario() -> int:
            state, node = MockState(), _Node()
            await node.connect(100, "/lobby")
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)

            async def probe(runtime: ExecutionRuntime) -> None:
                # The tick at 3 s holds an allowance and reads the view until 5 s.
                await asyncio.sleep(4)
                runtime.begin_draining()
                await asyncio.sleep(30)

            await _serve(state, _step(node), probe)

            return node.closed()

        assert run_simulation(scenario, latency=_slow_views) == 0

    @pytest.mark.parametrize(
        ("own", "settle", "warnings"),
        [
            (None, timedelta(0), ["warning"]),
            ({"movable": 34}, timedelta(0), ["warning"]),
            # Before the fleet settles a missing entry may be a replica still joining.
            (None, timedelta(hours=1), []),
        ],
        ids=["no entry", "no load", "unsettled"],
    )
    def test_a_replica_left_out_says_so_once(
        self,
        own: Report | None,
        settle: timedelta,
        warnings: list[str],
        error_log_buf: io.StringIO,
    ) -> None:
        async def scenario() -> int:
            state, node = MockState(), _Node()
            await node.connect(34, "/lobby")
            await _peers(state, 0, 0)

            if own is not None:
                await _beat(state, "me", own)

            await _serve(state, _step(node, settle=settle), _sleep(60))

            return node.closed()

        assert run_simulation(scenario) == 0

        rows = [
            json.loads(line)
            for line in error_log_buf.getvalue().splitlines()
            if line.startswith("{")
        ]
        # Sixty ticks, one warning: a member id that is not the membership step's, or a
        # membership step without the report, would otherwise do nothing, silently.
        assert [row["level"] for row in rows] == warnings
        assert all("'me'" in row["event"] for row in rows)

    def test_nothing_sheds_or_is_read_while_draining(self) -> None:
        reads: list[float] = []

        def record(surface: str | None, route: str | None, op: str) -> float:
            if op == "view":
                reads.append(asyncio.get_running_loop().time())

            return 0.0

        async def scenario() -> int:
            state, node = MockState(), _Node()
            await node.connect(100, "/lobby")
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)

            async def probe(runtime: ExecutionRuntime) -> None:
                await asyncio.sleep(0.5)  # before the first tick that could shed
                runtime.begin_draining()
                await asyncio.sleep(60)

            await _serve(state, _step(node), probe)

            return node.closed()

        assert run_simulation(scenario, latency=record) == 0
        # A replica on its way down does not read the fleet at all.
        assert all(at < 0.5 for at in reads)


class TestStop:
    def test_shutdown_stops_a_shed_and_teardown_waits_for_its_started_close(self) -> None:
        async def scenario() -> tuple[list[tuple[bool, bool, int]], float]:
            state, node = MockState(), _Node()
            first, second = await node.connect(2, "/lobby", "/chat", delay=3)
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)
            seen_in_teardown: list[tuple[bool, bool, int]] = []

            async def record(_ctx: object) -> None:
                started = node.sockets[first]
                seen_in_teardown.append(
                    (started.cancelled, started.closed, node.sockets[second].closes)
                )

            # Started after the rebalance step, so torn down before it.
            later = LifecycleStep(
                id="later", shutdown=record, depends_on=("realtime_connection_rebalance",)
            )
            # At 10 s the allowance holds two: closes at 10 s and 15 s, each running two
            # 3 s disconnect handlers. The scope ends at 12 s, inside the first close.
            rebalance = _step(
                node,
                namespaces=("/lobby", "/chat"),
                movable=lambda namespace, sid: True,
                every=timedelta(seconds=10),
                per_minute=12,
            )
            await _serve(state, [rebalance, later], _sleep(12))

            return seen_in_teardown, asyncio.get_running_loop().time()

        seen_in_teardown, left_at = run_simulation(scenario)

        # Before any step's teardown, with the background loops: the shed was stopped, so
        # its second close never started, and the first ran to its end, never cancelled.
        assert seen_in_teardown == [(False, True, 0)]
        assert left_at < 17

    def test_stopping_as_a_close_is_queued_waits_for_that_close(self) -> None:
        async def scenario() -> tuple[float, int, bool]:
            state, node = MockState(), _Node()
            _first, second = await node.connect(2, "/lobby", "/chat", delay=3)
            await _peers(state, 0, 0)
            await _beat(state, "me", _REPORTED)
            seen: list[tuple[float, int, bool]] = []

            async def probe(runtime: ExecutionRuntime) -> None:
                # The shed's second close is due at 15 s. Half a nanosecond earlier, inside
                # the clock's resolution, falls in the same turn of the loop and runs first:
                # the runtime stops its background loops just after the shed has queued that
                # close, and before the close has started and joined those teardown waits for.
                await asyncio.sleep(15 - 5e-10)
                await runtime.get_context().drainables.stop_all(grace=10)
                late = node.sockets[second]
                seen.append((asyncio.get_running_loop().time(), late.closes, late.closed))

            rebalance = _step(
                node,
                namespaces=("/lobby", "/chat"),
                movable=lambda namespace, sid: True,
                every=timedelta(seconds=10),
                per_minute=12,
            )
            await _serve(state, rebalance, probe)

            return seen[0]

        stopped_at, closes, closed = run_simulation(scenario)

        # The queued close went ahead, and stopping waited for its two 3 s handlers, well
        # within the grace, rather than returning with them still running.
        assert (closes, closed) == (1, True)
        assert stopped_at == pytest.approx(21)

    def test_the_closes_wait_looks_again_for_a_close_that_joins_meanwhile(self) -> None:
        async def scenario() -> float:
            server = _Node().server
            finishing = _FINISHING.setdefault(server, set())
            loop = asyncio.get_running_loop()

            async def close_for(seconds: float) -> None:
                await asyncio.sleep(seconds)

            first = asyncio.ensure_future(close_for(1))
            finishing.add(first)
            first.add_done_callback(finishing.discard)

            def join() -> None:
                late = asyncio.ensure_future(close_for(1.5))
                finishing.add(late)
                late.add_done_callback(finishing.discard)

            # A second close joins half a second in, while the first is being waited for.
            loop.call_later(0.5, join)
            waiter = _ClosesFinishing(sio=server, loop_name="closes")
            assert await waiter.stop(deadline=loop.time() + 10)

            return loop.time()

        assert run_simulation(scenario) == pytest.approx(2)

    def test_the_closes_wait_gives_up_at_its_deadline_and_cancels_nothing(self) -> None:
        async def scenario() -> tuple[bool, float, bool]:
            server = _Node().server
            finishing = _FINISHING.setdefault(server, set())
            loop = asyncio.get_running_loop()
            slow = asyncio.ensure_future(asyncio.sleep(5))
            finishing.add(slow)
            slow.add_done_callback(finishing.discard)

            waiter = _ClosesFinishing(sio=server, loop_name="closes")
            stopped = await waiter.stop(deadline=loop.time() + 1)
            gave_up_at = loop.time()
            await slow

            return stopped, gave_up_at, slow.cancelled()

        assert run_simulation(scenario) == (False, pytest.approx(1), False)


# ....................... #


class TestReport:
    def test_counts_transports_and_the_movable_ones(self) -> None:
        async def scenario() -> Report:
            node = _Node()
            await node.connect(2, "/lobby")
            await node.connect(1, "/lobby", "/game")
            await node.connect(1, "/game")
            await node.connect(1, "/other")

            report = connection_report(node.server, namespaces=("/lobby", "/game"), movable=_lobby)

            return report()

        # Four carry a counted namespace; only the two lobby-only ones may move.
        assert run_simulation(scenario) == {"connections": 4, "movable": 2}

    def test_a_failing_selector_keeps_each_connection_and_is_logged_once(
        self, error_log_buf: io.StringIO
    ) -> None:
        def broken(namespace: str, sid: str) -> bool:
            raise RuntimeError("selector bug")

        async def scenario() -> Report:
            node = _Node()
            await node.connect(50, "/lobby")

            return connection_report(node.server, namespaces=("/lobby",), movable=broken)()

        assert run_simulation(scenario) == {"connections": 50, "movable": 0}

        rows = [
            json.loads(line)
            for line in error_log_buf.getvalue().splitlines()
            if line.startswith("{")
        ]
        # Listed on every tick and every beat: the count once, and the first error once.
        assert [row["level"] for row in rows] == ["warning", "critical"]
        assert "50 Engine.IO connection(s)" in rows[0]["event"]


class TestWiring:
    @pytest.mark.parametrize(
        ("overrides", "match"),
        [
            ({"every": timedelta(0)}, "interval must be positive"),
            ({"per_minute": 0}, "at least 1 a minute"),
            ({"tolerance": -0.1}, "tolerance"),
            ({"tolerance": float("nan")}, "tolerance"),
            ({"settle": -SECOND}, "must not be negative"),
            ({"cooldown": -SECOND}, "must not be negative"),
            ({"namespaces": ()}, "at least one namespace"),
        ],
    )
    def test_bad_arguments_are_refused_at_construction(
        self, overrides: dict[str, Any], match: str
    ) -> None:
        with pytest.raises(CoreException, match=match):
            _step(_Node(), **overrides)

    def test_one_report_key_for_both_counts_is_refused(self) -> None:
        node = _Node()

        with pytest.raises(CoreException, match="different report keys"):
            _step(node, load="count", movable_report="count")

        with pytest.raises(CoreException, match="different report keys"):
            connection_report(node.server, movable=_lobby, load="count", movable_report="count")

    def test_an_invalid_member_id_is_refused_at_construction(self) -> None:
        with pytest.raises(CoreException) as caught:
            _step(_Node(), member_id="a\tb")

        assert caught.value.code == "fleet.member_id_invalid"

    def test_an_engine_io_the_close_cannot_use_is_refused_at_construction(self) -> None:
        node = _Node()
        node.server.eio.sockets = None  # type: ignore[assignment]

        with pytest.raises(CoreException) as caught:
            _step(node)

        assert caught.value.code == "realtime_drain_engineio_unsupported"

    def test_the_default_member_id_is_the_instance_id(self) -> None:
        async def scenario() -> int:
            state, node = MockState(), _Node()
            await node.connect(100, "/lobby")
            await _peers(state, 0, 0)
            # The membership step's default id.
            await _beat(state, instance_id(), _REPORTED)

            await _serve(state, _step(node, member_id=None), _sleep(1.95))

            return node.closed()

        assert run_simulation(scenario) == 10

    def test_a_tick_before_startup_is_an_internal_error(self) -> None:
        rebalance = _Rebalance(
            sio=_Node().server,
            spec=SPEC,
            every=SECOND,
            member_id=None,
            namespaces=("/",),
            load="connections",
            movable_report="movable",
            movable=_lobby,
            per_minute=1,
            tolerance=0.15,
            settle=timedelta(0),
            cooldown=timedelta(0),
        )

        with pytest.raises(CoreException) as caught:
            asyncio.run(rebalance.tick())

        assert caught.value.kind == ExceptionKind.INTERNAL

    def test_a_serverless_runtime_refuses_it(self) -> None:
        with pytest.raises(CoreException, match="SERVERLESS"):
            build_runtime(
                MockDepsModule(),
                lifecycle_steps=_step(_Node()),
                deployment=DeploymentProfile.SERVERLESS,
            )

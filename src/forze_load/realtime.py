"""Realtime runs: subscribers, send-to-delivery latency and a reconnect storm (RFC 0084 §5.7).

The writer and every subscriber run in this one process, on one monotonic clock, so a frame's
delivery is timed against its write with no clock alignment: from the write's send (an upper
bound, the write's own latency included) and from its answer (negative when the live leg beats
the HTTP answer). True commit time is not observable: no frame carries it.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import math
import time
from collections.abc import Callable, Collection, Hashable, Sequence
from typing import TYPE_CHECKING, Any, Literal

import attrs
import httpx

from forze.base.exceptions import exc

from ._seams import seeded_random
from ._target import no_cookies, require_path, target_guard
from ._wire import Frame, SseParser, WsHandshakeError, WsRefused, sse_lines, ws_connect
from .engine import _assignments  # pyright: ignore[reportPrivateUsage]
from .stats import StepRecorder, _Series  # pyright: ignore[reportPrivateUsage]

if TYPE_CHECKING:
    import random

    from .engine import _Run  # pyright: ignore[reportPrivateUsage]
    from .plan import LoadPlan, Principal
    from .report import RunWriter

# ----------------------- #

LAST_EVENT_ID_HEADER = "Last-Event-ID"
"""The SSE resume header (``forze_fastapi.realtime.sse``)."""

ERROR_CODE_HEADER = "X-Error-Code"
"""The header forze's error answers carry their code in (``forze_fastapi.exceptions``)."""

DRAINING_CODE = "realtime_draining"
"""The code a draining replica answers a new SSE stream with, beside a ``retry:``."""

WS_RECONNECT_CLOSE = 1012
"""The close code a draining replica, or an unconfirmed replay, ends a WebSocket with."""

WS_POLICY_CLOSE = 1008
"""The close code a refused WebSocket connect, or an expired credential, gets."""

RETRY_S = 1.0
"""Seconds before reopening a stream when the server named no ``retry:``."""

_POLL_S = 0.01

_HOLD_S = 1.0
"""Seconds past the request timeout a frame waits for its write's answer before it is let go."""

_MIN_RETRY_S = 0.05
"""The shortest wait before a reopen, whatever ``retry:`` says: no stream reopens in a hot loop."""

_MAX_BACKOFF_S = 30.0
"""The longest base wait between reopens of a stream that keeps failing, before jitter."""

_MAX_DOUBLINGS = 32
"""Doublings past which the backoff stops growing: any ``retry:`` base has reached
:data:`_MAX_BACKOFF_S` by then, and ``2.0 ** 1024`` would overflow."""

_QUIET_S = 0.25
"""Seconds without a frame that end the wait for the first opens' replay, before any write."""

_KEEP_CLOSED = 10
"""Closed keys are kept this many ``timeout_s`` after they close, so a late frame can be told
from other traffic; a frame for a key let go after that counts nowhere, as other traffic's."""

type Key = Hashable
type Match = Callable[[Frame], Key | None]
type KeyOf = Callable[[httpx.Response], Key | None]
type AudienceOf = Callable[[httpx.Response], Collection[str] | None]


# ....................... #


def _invalid(summary: str) -> Exception:
    return exc.configuration(summary, code="load_plan_invalid")


def _names(values: Sequence[str] | None) -> tuple[str, ...] | None:
    return None if values is None else tuple(values)


@attrs.frozen(kw_only=True)
class Realtime:
    """A realtime run: *subscribers* streams, writes at *rate*, then a reconnect storm.

    Pass it to :func:`forze_load.run` as the profile. The run opens the streams in batches,
    spread over :attr:`principals` by weight, each with its own ``device_id``. It then
    sends writes for :attr:`duration_s`: open-model arrivals at :attr:`rate`, each a writer
    picked by weight and a scenario from its mix. A write's answer gives its key
    (:attr:`key_of`) and audience (:attr:`audience`), and each frame its key (:attr:`match`).
    With :attr:`storm`, every stream then drops at once and comes back in one batch, resuming
    from the last durable id it saw, while writes run on for another :attr:`duration_s`.

    The meta line records the numbers, not :attr:`path`, :attr:`topics` or the callables.
    """

    transport: Literal["sse", "ws"]
    """``"sse"`` for ``attach_realtime_sse_route``, ``"ws"`` for ``attach_realtime_ws_route``."""

    path: str = attrs.field(metadata={"recorded": False})
    """The stream route's path on the target, ``/realtime/sse`` say."""

    subscribers: int
    """Streams held open."""

    rate: float
    """Writes per second."""

    match: Match
    """A frame's key, or ``None`` for a frame no write of the run caused."""

    key_of: KeyOf
    """A write's key, from its answer, or ``None`` when it emits no event."""

    audience: AudienceOf | None = None
    """The principal names a write's event reaches, or ``None`` for every subscriber."""

    principals: tuple[str, ...] | None = attrs.field(default=None, converter=_names)
    """Principals the subscribers run as; every principal of the plan by default."""

    writers: tuple[str, ...] | None = attrs.field(default=None, converter=_names)
    """Principals that send the writes; every principal of the plan by default."""

    topics: tuple[str, ...] = attrs.field(default=(), converter=tuple, metadata={"recorded": False})
    """Topics each stream subscribes to (``?topics=``); the app must grant them."""

    duration_s: float = 30.0
    """Seconds of writes per phase."""

    storm: bool = True
    """Drop every stream at once after the steady phase, and measure the resume."""

    resume: bool = True
    """Reopen streams from the last durable id seen. ``False`` for a live-only SSE route,
    which ignores ``Last-Event-ID``: the storm then records ``resume: "none"``."""

    timeout_s: float = 10.0
    """Seconds after a write's answer before an expected delivery counts as missing."""

    batch: int = 50
    """Streams opened at once at the start."""

    idle_s: float | None = 60.0
    """Seconds without a byte after which a stream counts as cut, and is reopened. Keep it
    above the server's heartbeat: forze's SSE keepalive comment (15 s by default) or the ASGI
    server's WebSocket ping (uvicorn: 20 s). ``None`` for a server that sends neither."""

    def __attrs_post_init__(self) -> None:
        if self.transport not in ("sse", "ws"):
            raise _invalid("Realtime transport must be 'sse' or 'ws'")

        require_path(self.path, "Realtime.path")

        for name in ("subscribers", "batch"):
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise _invalid(f"Realtime {name} must be a whole number of at least 1")

        for name in ("rate", "duration_s", "timeout_s", "idle_s"):
            value = getattr(self, name)

            if (value is not None or name != "idle_s") and not (
                isinstance(value, int | float)
                and not isinstance(value, bool)
                and math.isfinite(value)
                and value > 0
            ):
                raise _invalid(f"Realtime {name} must be a positive, finite number")

    def check(self, plan: LoadPlan) -> None:
        """Refuse principals or writers the plan does not have; run before sign-in."""

        known = {principal.name for principal in plan.principals}

        for what, chosen in (("principals", self.principals), ("writers", self.writers)):
            if chosen is not None and (not chosen or not set(chosen) <= known):
                raise _invalid(f"Realtime {what} must name principals of the plan")

    async def drive(
        self,
        state: _Run,
        writer: RunWriter,
        transport: httpx.AsyncBaseTransport | None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        """Run this profile after sign-in; :func:`forze_load.run` calls it."""

        plan = state.plan

        async with httpx.AsyncClient(
            base_url=plan.base_url,
            # Each stream holds its connection: no pool limit, no keep-alive, and a read waits
            # up to `idle_s`, so a silent half-open stream is cut rather than kept "up".
            timeout=httpx.Timeout(plan.timeout_s, read=self.idle_s),
            limits=httpx.Limits(max_connections=None, max_keepalive_connections=0),
            transport=transport,
            cookies=no_cookies(),
            event_hooks={"request": [target_guard(plan.base_url)]},
        ) as client:
            rows, stopped = await _RealtimeRun(spec=self, state=state, client=client).run()

        for row in rows:
            writer.write(row)

        return rows, stopped


# ....................... #


@attrs.define(eq=False)
class _Phase:
    """One window of writes, and what the streams and deliveries did during it."""

    name: str
    recorder: StepRecorder = attrs.field(factory=lambda: StepRecorder(warm_until=math.inf))
    started: float = 0.0
    measured: float = 0.0
    client: dict[str, Any] = attrs.field(factory=lambda: dict[str, Any]())
    open_now: int = 0
    opened: int = 0
    refused: int = 0
    errors: int = 0
    draining: int = 0
    ended: int = 0
    open_ms: _Series = attrs.field(factory=_Series)
    expected: int = 0
    delivered: int = 0
    duplicates: int = 0
    missing: int = 0
    gaps: int = 0
    replayed: int = 0
    late: int = 0
    send_ms: _Series = attrs.field(factory=_Series)
    answer_ms: _Series = attrs.field(factory=_Series)
    negative: int = 0
    negative_min: float | None = None
    dropped_at: float | None = None
    """When the storm dropped every stream; ``None`` outside the storm."""

    restarted: list[_Subscriber] = attrs.field(factory=lambda: list[_Subscriber]())
    resumed: int = 0
    storm: dict[str, Any] = attrs.field(factory=lambda: dict[str, Any]())
    extra: dict[str, Any] = attrs.field(factory=lambda: dict[str, Any]())


@attrs.define(eq=False)
class _Subscriber:
    index: int
    principal: str
    ups: list[list[float]] = attrs.field(factory=lambda: list[list[float]]())
    """``[opened, closed]`` per stream it held, ``closed`` infinite while open."""

    last_id: str | None = None
    refused: bool = False
    settled: asyncio.Event = attrs.field(factory=asyncio.Event)
    task: asyncio.Task[None] | None = None
    failures: int = 0
    """Failed attempts since its last open: the reopen backoff doubles with each."""

    errored: bool = False

    def up_at(self, moment: float) -> bool:
        return any(opened <= moment < closed for opened, closed in self.ups)

    def down(self, moment: float) -> None:
        if self.ups and self.ups[-1][1] == math.inf:
            self.ups[-1][1] = moment


@attrs.define(eq=False)
class _Write:
    phase: _Phase
    sent: float
    answered: float
    expected: int
    """The subscribers the write's event should reach, as a bit set of their indexes."""

    seen: int = 0
    """The subscribers it reached, as a bit set."""


@attrs.define(eq=False)
class _RealtimeRun:
    spec: Realtime
    state: _Run
    client: httpx.AsyncClient
    subscribers: list[_Subscriber] = attrs.field(init=False)
    rng: random.Random = attrs.field(init=False)
    """Jitters reopens; its own stream, so it moves neither picks nor arrivals."""

    last_frame_at: float = 0.0
    phases: list[_Phase] = attrs.field(factory=lambda: list[_Phase]())
    phase: _Phase = attrs.field(init=False)
    writes: dict[Key, _Write] = attrs.field(factory=lambda: dict[Key, _Write]())
    """Writes whose deliveries are still awaited, in the order they were answered."""

    early: dict[Key, tuple[float, dict[int, list[float]]]] = attrs.field(
        factory=lambda: dict[Key, tuple[float, dict[int, list[float]]]]()
    )
    """Frames whose write has not answered yet: key to (first arrival, sub to [at, repeats])."""

    finished: dict[Key, tuple[float, _Phase, int]] = attrs.field(
        factory=lambda: dict[Key, tuple[float, _Phase, int]]()
    )
    """Closed keys, in the order they closed: when, the write's phase, and the subscribers it
    never reached. A frame for one of those is late; any other frame for a closed key counts
    nowhere."""

    outstanding: int = 0
    opened: int = 0
    """Subscribers that have opened a stream and were never refused, as a bit set."""

    joined: list[tuple[float, int]] = attrs.field(factory=lambda: list[tuple[float, int]]())
    """Subscribers whose first stream opened once the writes had begun, with when."""

    writing: bool = False
    by_principal: dict[str, int] = attrs.field(factory=lambda: dict[str, int]())
    """Each principal's subscribers, as a bit set."""

    def __attrs_post_init__(self) -> None:
        assign = _assignments(self._principals(self.spec.principals))
        self.subscribers = [
            _Subscriber(index=i, principal=next(assign).name) for i in range(self.spec.subscribers)
        ]

        for sub in self.subscribers:
            self.by_principal[sub.principal] = (
                self.by_principal.get(sub.principal, 0) | 1 << sub.index
            )

        self.rng = seeded_random(self.state.plan.seed, "reconnects")

        self.phase = self._phase("steady")

    # ....................... #

    async def run(self) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        state = self.state
        inflight: set[asyncio.Task[None]] = set()
        arrivals: asyncio.Task[None] | None = None
        stopped: dict[str, Any] | None = None

        try:
            await self._open()
            # A batch stops waiting for streams that only fail; the run gives them, together,
            # one `timeout_s` more to open before it stops.
            await self._until(
                lambda: (
                    any(sub.ups for sub in self.subscribers)
                    or all(sub.refused for sub in self.subscribers)
                ),
                time.perf_counter() + self.spec.timeout_s,
            )

            if not any(sub.ups for sub in self.subscribers):
                self._start(self.phase)
                self._end(self.phase)
                stopped = {"phase": "steady", "reason": "no_stream"}

            else:
                await self._quiet()
                state.on_response = self._answered
                self.writing = True
                self._start(self.phase)
                assign = _assignments(self._principals(self.spec.writers))
                arrivals = asyncio.create_task(
                    state.arrivals(self.spec.rate, "constant", inflight, assign)
                )
                stopped = await self._window(self.phase)

                if stopped is None and self.spec.storm:
                    self.phase = self._phase("storm")
                    self._start(self.phase)
                    await self._storm(self.phase)
                    stopped = await self._window(self.phase)

                arrivals.cancel()
                await asyncio.gather(arrivals, return_exceptions=True)

                for phase in self.phases:
                    await phase.recorder.drain(state.plan.timeout_s + 1.0)

                await self._settle()

        finally:
            tasks = [t for t in (arrivals, *inflight, *(s.task for s in self.subscribers)) if t]

            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)
            state.on_response = None

        return [self._row(phase) for phase in self.phases], stopped

    # ....................... #

    def _principals(self, names: tuple[str, ...] | None) -> list[Principal]:
        return [p for p in self.state.plan.principals if names is None or p.name in names]

    def _phase(self, name: str) -> _Phase:
        phase = _Phase(name=name)
        self.phases.append(phase)

        return phase

    def _start(self, phase: _Phase) -> None:
        phase.started = time.perf_counter()
        phase.recorder.warm_until = phase.started
        self.state.current = phase.recorder
        self.state.probe.begin()

    def _end(self, phase: _Phase) -> None:
        state = self.state
        recorder = phase.recorder
        phase.measured = time.perf_counter() - phase.started
        phase.client = state.probe.report(
            recorder.scheduled, recorder.behind, cpu_judged=state.cpu_judged
        )
        phase.open_now = sum(1 for sub in self.subscribers if sub.up_at(time.perf_counter()))

        if state.exhausted:
            phase.extra["sample_exhausted"], state.exhausted = state.exhausted, []

        if phase.dropped_at is not None:
            dropped = phase.dropped_at
            back = [
                min(opened for opened, _ in sub.ups if opened >= dropped)
                for sub in phase.restarted
                if sub.ups[-1][0] >= dropped
            ]
            phase.storm = {
                "back": len(back),
                # Until every stream is back: unknown while one is still away.
                "back_in_s": round(max(back) - dropped, 3)
                if back and len(back) == len(phase.restarted)
                else None,
                "resume": "last_event_id" if self.spec.resume else "none",
                "resumed": phase.resumed,
            }

    async def _until(self, done: Callable[[], bool], deadline: float) -> None:
        state = self.state

        while not done() and state.failure is None and state.halt is None:
            left = deadline - time.perf_counter()

            if left <= 0:
                break

            await asyncio.sleep(min(_POLL_S, left))

        if state.failure is not None:
            raise state.failure

    async def _window(self, phase: _Phase) -> dict[str, Any] | None:
        await self._until(lambda: False, phase.started + self.spec.duration_s)
        self._end(phase)
        halt = self.state.halt

        if halt is None:
            return None

        phase.extra["cut_short"] = True

        return {"phase": phase.name, **halt}

    # ....................... #

    async def _open(self) -> None:
        """Open the streams in batches, each batch waited for up to ``timeout_s``."""

        for start in range(0, len(self.subscribers), self.spec.batch):
            group = self.subscribers[start : start + self.spec.batch]

            for sub in group:
                sub.task = asyncio.create_task(self._subscribe(sub, None))

            await self._until(
                functools.partial(_all_settled, group),
                time.perf_counter() + self.spec.timeout_s,
            )

    async def _quiet(self) -> None:
        """Let the first opens' replay pass before the first write: the kit never acks, so each
        first open replays the retained mailbox. Waits until no frame has come for
        :data:`_QUIET_S`, at most ``timeout_s``."""

        start = time.perf_counter()

        await self._until(
            lambda: time.perf_counter() - max(self.last_frame_at, start) >= _QUIET_S,
            start + self.spec.timeout_s,
        )

    async def _storm(self, phase: _Phase) -> None:
        """Drop every stream at once, then reopen them all in one batch."""

        dropped = time.perf_counter()
        restarted = [sub for sub in self.subscribers if not sub.refused]

        tasks = [sub.task for sub in restarted if sub.task is not None]

        for sub in restarted:
            sub.down(dropped)

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        resume = self.spec.resume

        for sub in restarted:
            sub.task = asyncio.create_task(self._subscribe(sub, sub.last_id if resume else None))

        # A stream that had never opened was not dropped: it does not hold `back_in_s` open.
        phase.dropped_at, phase.restarted = dropped, [sub for sub in restarted if sub.ups]
        phase.resumed = sum(1 for sub in restarted if resume and sub.last_id is not None)

    async def _subscribe(self, sub: _Subscriber, last_id: str | None) -> None:
        """Hold *sub*'s stream: reopen it when the server ends it, until refused."""

        try:
            while True:
                delay = await self._stream(sub, last_id)

                if delay is None:
                    return

                last_id = sub.last_id if self.spec.resume else None
                await asyncio.sleep(self._delay(sub, delay))

        except Exception as error:  # a plan's `match` bug, or a credential that expired
            self.state.fail(error)

    def _delay(self, sub: _Subscriber, base: float) -> float:
        """The wait before *sub* reopens: *base* (the server's ``retry:``, or
        :data:`RETRY_S`), doubled for each failure in a row up to :data:`_MAX_BACKOFF_S`, then
        jittered by half either way so the streams do not come back in lockstep."""

        # The exponent is capped first: 2.0 ** 1024 overflows long before `min` would apply.
        doublings = min(max(sub.failures - 1, 0), _MAX_DOUBLINGS)
        backoff = min(base * 2.0**doublings, _MAX_BACKOFF_S)

        return max(backoff * self.rng.uniform(0.5, 1.5), _MIN_RETRY_S)

    async def _stream(self, sub: _Subscriber, last_id: str | None) -> float | None:
        """One connection: the seconds to wait before reopening, or ``None`` if refused."""

        headers = await self.state.sessions[sub.principal].headers()
        params = {"device_id": f"forze-load-{sub.index}"}

        if self.spec.topics:
            params["topics"] = ",".join(self.spec.topics)

        started = time.perf_counter()

        try:
            if self.spec.transport == "sse":
                return await self._sse(sub, started, headers, params, last_id)

            return await self._ws(sub, started, headers, params, last_id)

        except httpx.TransportError:  # a refused connection, a reset, a timeout
            self._error(sub)

            return RETRY_S

        finally:
            sub.down(time.perf_counter())

    async def _sse(
        self,
        sub: _Subscriber,
        started: float,
        headers: dict[str, str],
        params: dict[str, str],
        last_id: str | None,
    ) -> float | None:
        if last_id is not None:
            headers = {**headers, LAST_EVENT_ID_HEADER: last_id}

        parser = SseParser()

        async with self.client.stream(
            "GET", self.spec.path, params=params, headers=headers
        ) as response:
            # `EventSource` gives up on any other status or type: a wrong path answering 200
            # with JSON would otherwise be reopened forever.
            media_type = response.headers.get("content-type", "").partition(";")[0].strip()

            if response.status_code >= 500:  # a balancer's 502 or 503 is passing: retried
                self._error(sub)

                return RETRY_S

            if response.status_code != 200 or media_type != "text/event-stream":
                self._refused(sub)

                return None

            lines = sse_lines(response.aiter_bytes(), idle_s=self.spec.idle_s)

            if response.headers.get(ERROR_CODE_HEADER) == DRAINING_CODE:
                self.phase.draining += 1

                async for line in lines:
                    parser.feed(line)

                return parser.retry_s if parser.retry_s is not None else RETRY_S

            self._opened(sub, started, time.perf_counter())

            # A stream that breaks mid-read ends as one the server closed, as a WebSocket does.
            with contextlib.suppress(httpx.TransportError):
                async for line in lines:
                    frame = parser.feed(line)

                    if frame is not None:
                        self._arrive(sub, frame)

        self.phase.ended += 1

        return parser.retry_s if parser.retry_s is not None else RETRY_S

    async def _ws(
        self,
        sub: _Subscriber,
        started: float,
        headers: dict[str, str],
        params: dict[str, str],
        last_id: str | None,
    ) -> float | None:
        if last_id is not None:
            params = {**params, "last_event_id": last_id}

        try:
            async with ws_connect(
                self.client, self.spec.path, params=params, headers=headers, idle_s=self.spec.idle_s
            ) as ws:
                upgraded = time.perf_counter()
                # forze accepts a refused or draining socket before closing it, so the 101
                # proves nothing: the open counts once a pong or a message comes back first.
                await ws.ping()
                confirmed = False

                while True:
                    event = await ws.receive()

                    if event[0] == "close":
                        code = event[1]
                        break

                    if not confirmed:
                        confirmed = True
                        self._opened(sub, started, upgraded)

                    if event[0] == "text" and (frame := _ws_frame(event[1])) is not None:
                        self._arrive(sub, frame)

        except WsRefused as refused:
            if refused.status < 500:
                self._refused(sub)

                return None

            self._error(sub)  # a balancer's 502 or 503 is passing: retried

            return RETRY_S

        except WsHandshakeError:
            self._error(sub)

            return RETRY_S

        if confirmed:
            self.phase.ended += 1

        elif code == WS_RECONNECT_CLOSE:
            self.phase.draining += 1

        elif code == WS_POLICY_CLOSE:
            self._refused(sub)

            return None

        else:
            self._error(sub)

        return RETRY_S

    def _error(self, sub: _Subscriber) -> None:
        self.phase.errors += 1
        sub.failures += 1
        sub.errored = True

    def _opened(self, sub: _Subscriber, started: float, opened: float) -> None:
        if not sub.ups:
            self.opened |= 1 << sub.index

            if self.writing:
                self.joined.append((opened, sub.index))

        sub.ups.append([opened, math.inf])
        sub.failures = 0
        sub.settled.set()
        self.phase.opened += 1
        self.phase.open_ms.add("ok", (opened - started) * 1000)

    def _refused(self, sub: _Subscriber) -> None:
        self.opened &= ~(1 << sub.index)
        sub.refused = True
        sub.settled.set()
        self.phase.refused += 1

    # ....................... #

    def _answered(
        self, step: StepRecorder | None, response: httpx.Response, sent: float, answered: float
    ) -> None:
        """A write answered: expect its key at the subscribers its event addresses."""

        phase = next((p for p in self.phases if p.recorder is step), None)
        self._sweep(answered)
        key = self.spec.key_of(response)

        # A key already awaited or finished is expected once: keys are one per write.
        if phase is None or key is None or key in self.writes or key in self.finished:
            return

        names = self.spec.audience(response) if self.spec.audience is not None else None
        expected = self.opened

        if names is not None:
            audience = 0

            for name in set(names):
                audience |= self.by_principal.get(name, 0)

            expected &= audience

        # A stream that first opened after the write was sent was not there to receive it. Only
        # streams whose first open came once the writes had begun are listed: usually none.
        for opened_at, index in self.joined:
            if opened_at > sent:
                expected &= ~(1 << index)

        write = _Write(phase=phase, sent=sent, answered=answered, expected=expected)
        self.writes[key] = write
        phase.expected += expected.bit_count()
        self.outstanding += expected.bit_count()
        _, early = self.early.pop(key, (0.0, {}))

        for index, (at, repeats) in early.items():
            if expected >> index & 1:
                self._deliver(write, index, at)
                phase.duplicates += int(repeats)

    def _arrive(self, sub: _Subscriber, frame: Frame) -> None:
        """A frame reached *sub*: time it against its write, or hold it until the write
        answers."""

        at = time.perf_counter()
        self.last_frame_at = at
        # Writes close as later ones answer: with none answering, a frame must close them itself.
        self._sweep(at)

        if frame.id is not None:
            sub.last_id = frame.id

        key = self.spec.match(frame)

        if key is None:
            return

        write = self.writes.get(key)

        bit = 1 << sub.index

        if write is None:
            closed = self.finished.get(key)

            # Before the first write a frame can only be a replay of earlier traffic.
            if closed is None and self.writing:
                self._hold(key, sub.index, at)

            elif closed is not None and closed[2] & bit:
                closed_at, phase, unseen = closed
                phase.late += 1  # in the write's phase, as its `missing` and `gaps` are
                self.finished[key] = (closed_at, phase, unseen & ~bit)

            return

        if not write.expected & bit:
            return

        if len(sub.ups) > 1 and write.sent < sub.ups[-1][0]:
            write.phase.replayed += 1  # written before this reopened stream: the resume sent it

        if write.seen & bit:
            write.phase.duplicates += 1

        else:
            self._deliver(write, sub.index, at)

    def _hold(self, key: Key, index: int, at: float) -> None:
        # Frames of other traffic never meet a write: let go of any older than a request can
        # take to answer, oldest first.
        horizon = at - self.state.plan.timeout_s - _HOLD_S

        while self.early and next(iter(self.early.values()))[0] < horizon:
            del self.early[next(iter(self.early))]

        subs = self.early.setdefault(key, (at, {}))[1]

        if index in subs:
            subs[index][1] += 1

        else:
            subs[index] = [at, 0]

    def _deliver(self, write: _Write, index: int, at: float) -> None:
        write.seen |= 1 << index
        self.outstanding -= 1
        phase = write.phase
        phase.delivered += 1
        phase.send_ms.add("ok", max(at - write.sent, 0.0) * 1000)
        answer_ms = (at - write.answered) * 1000

        # DDSketch takes no negative value: a frame that beat its answer is counted beside it.
        if answer_ms >= 0:
            phase.answer_ms.add("ok", answer_ms)

        else:
            phase.negative += 1
            phase.negative_min = min(answer_ms, phase.negative_min or 0.0)

    def _sweep(self, now: float) -> None:
        """Close every write answered ``timeout_s`` ago: what has not arrived is missing,
        or a gap where the subscriber was down when the write was sent."""

        while self.writes:
            key, write = next(iter(self.writes.items()))

            if write.answered + self.spec.timeout_s > now:
                break

            del self.writes[key]
            pending = write.expected & ~write.seen
            self.finished[key] = (now, write.phase, pending)

            while pending:
                lowest = pending & -pending
                pending ^= lowest
                index = lowest.bit_length() - 1
                self.outstanding -= 1

                if self.subscribers[index].up_at(write.sent):
                    write.phase.missing += 1

                else:
                    write.phase.gaps += 1

        keep = _KEEP_CLOSED * self.spec.timeout_s

        while self.finished and next(iter(self.finished.values()))[0] + keep <= now:
            del self.finished[next(iter(self.finished))]

    async def _settle(self) -> None:
        """Wait until every expected delivery arrived, or its time ran out."""

        last = max((write.answered for write in self.writes.values()), default=0.0)
        deadline = last + self.spec.timeout_s

        while self.outstanding > 0 and (now := time.perf_counter()) < deadline:
            self._sweep(now)
            await asyncio.sleep(_POLL_S)

        if self.state.failure is not None:
            raise self.state.failure

        self._sweep(math.inf)

    # ....................... #

    def _row(self, phase: _Phase) -> dict[str, Any]:
        summary = phase.recorder.summary(phase.measured)
        scenarios = summary.pop("scenarios")
        delivered = phase.delivered
        streams: dict[str, Any] = {
            "subscribers": self.spec.subscribers,
            "open": phase.open_now,
            "opened": phase.opened,
            "open_ms": phase.open_ms.latency(),
            "refused": phase.refused,
            "errors": phase.errors,
            "draining": phase.draining,
            "ended": phase.ended,
            **phase.storm,
        }

        return {
            "kind": "phase",
            "phase": phase.name,
            "rate": self.spec.rate,
            "latency": "open",
            **summary,
            "client": phase.client,
            "scenarios": scenarios,
            "streams": streams,
            "delivery": {
                "expected": phase.expected,
                "delivered": delivered,
                "share": round(delivered / phase.expected, 4) if phase.expected else None,
                "duplicates": phase.duplicates,
                "missing": phase.missing,
                "gaps": phase.gaps,
                "replayed": phase.replayed,
                "late": phase.late,
                "send_ms": phase.send_ms.latency(),
                "answer_ms": phase.answer_ms.latency(),
                "answer_negative": {
                    "count": phase.negative,
                    "share": round(phase.negative / delivered, 4) if delivered else None,
                    "min_ms": None if phase.negative_min is None else round(phase.negative_min, 2),
                },
            },
            **phase.extra,
        }


def _all_settled(group: Sequence[_Subscriber]) -> bool:
    # A stream that only fails keeps retrying, but no longer holds up the opening.
    return all(sub.settled.is_set() or sub.errored for sub in group)


def _ws_frame(text: str) -> Frame | None:
    """A WebSocket egress frame, ``{"event", "id", "data"}``; anything else is not one."""

    try:
        envelope = json.loads(text)

    except ValueError:
        return None

    if not isinstance(envelope, dict) or not isinstance(envelope.get("event"), str):
        return None

    event_id = envelope.get("id")

    return Frame(
        event=envelope["event"],
        id=event_id if isinstance(event_id, str) else None,
        text=text,
        data=envelope.get("data"),
    )

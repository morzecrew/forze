"""Realtime runs against a fake app: matching, audiences, latencies, the storm, drains, refusals.

The fake serves ``/notify`` and a stream route, SSE or WebSocket, over ``httpx.MockTransport``.
Like forze's hub it sends a principal-addressed frame to that principal's open streams only,
and like its mailbox it replays durable frames past a resumed ``Last-Event-ID`` (SSE) or
``last_event_id`` (WebSocket). Knobs make it deliver frames before or after the answer, twice,
to nobody, or reopen streams slowly.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import attrs
import httpx
import pytest

from forze.base.exceptions import CoreException
from forze_load import Call, Frame, LoadPlan, Principal, Realtime, Scenario, read_run, run, sample
from forze_load import realtime as realtime_module
from forze_load.realtime import _RealtimeRun

pytestmark = pytest.mark.unit

# ----------------------- #


@attrs.define(eq=False)
class _Conn:
    principal: str
    device: str
    resumed_from: str | None
    queue: asyncio.Queue[bytes | None] = attrs.field(factory=asyncio.Queue)
    delivered: list[str] = attrs.field(factory=list)
    closed: bool = False


@attrs.define(eq=False)
class _Fake:
    kind: str = "sse"
    durable: bool = True
    emit: bool = True
    answer_after_s: float = 0.0
    """Frames go out, then the answer waits this long: every frame beats its answer."""

    frames_after_s: float = 0.0
    duplicate: bool = False
    deaf: frozenset[str] = frozenset()
    draining: int = 0
    """The next opens get the draining answer."""

    refuse: bool = False
    not_a_stream: bool = False
    """The stream route answers 200 with JSON, as a wrong path might."""

    end_after: int = 0
    """End every open stream, as a drain does, when this write arrives."""

    slow_device: str | None = None
    """This subscriber's stream opens only after ``slow_open_s``."""

    slow_open_s: float = 0.0
    break_after: int = 0
    """Break every open stream's connection mid-read when this write arrives."""

    upgrade_status: int = 101
    close_first: list[int] = attrs.field(factory=list)
    """WebSocket close codes the next opens get before anything else."""

    foreign: bool = False
    """Each write also sends a frame whose key no write of the run has."""

    reopen_delay_s: float = 0.0
    initial: int = 0
    """Opens before the reopen delay applies."""

    conns: list[_Conn] = attrs.field(factory=list)
    mailbox: list[tuple[str, str, bytes]] = attrs.field(factory=list)
    opens: int = 0
    writes: int = 0

    # ....................... #

    async def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path

        if path == "/notify":
            return await self._notify(request)

        if path == "/openapi.json":
            return httpx.Response(200, json=_DOC)

        if path == "/items/list":
            return httpx.Response(200, json={"hits": [{"id": "i1"}]})

        if path.startswith("/items/"):  # the sampled item is gone
            return httpx.Response(404)

        return await self._open(request)

    async def _notify(self, request: httpx.Request) -> httpx.Response:
        self.writes += 1

        if self.writes == self.end_after:
            self.end_all()

        if self.writes == self.break_after:
            self.end_all(broken=True)

        body = json.loads(request.content)
        to, n = body.get("to"), self.writes
        key, event_id = f"key-{n}", f"event-{n}" if self.durable else None

        if not self.emit:
            return httpx.Response(200, json={"key": None})

        envelope = {"id": event_id, "data": {"key": key}}
        frame = (
            (f"id: {event_id}\n" if event_id else "")
            + f"event: notified\ndata: {json.dumps(envelope)}\n\n"
        ).encode()

        if self.kind == "ws":
            frame = _ws_text(json.dumps({"event": "notified", **envelope}))

        principals = {to} if to else {conn.principal for conn in self.conns}

        if event_id is not None:
            self.mailbox.extend((event_id, p, frame) for p in sorted(principals))

        targets = [c for c in self.conns if c.principal in principals and not c.closed]

        def deliver() -> None:
            for conn in targets:
                if conn.closed or conn.principal in self.deaf:
                    continue

                if self.foreign:
                    conn.queue.put_nowait(frame.replace(key.encode(), f"foreign-{n}".encode()))

                conn.delivered.extend([event_id or ""] * (2 if self.duplicate else 1))
                conn.queue.put_nowait(frame)

                if self.duplicate:
                    conn.queue.put_nowait(frame)

        if self.frames_after_s:
            asyncio.get_running_loop().call_later(self.frames_after_s, deliver)

        else:
            deliver()

        await asyncio.sleep(self.answer_after_s)

        return httpx.Response(200, json={"key": key, "to": to})

    async def _open(self, request: httpx.Request) -> httpx.Response:
        self.opens += 1

        if self.opens > self.initial and self.reopen_delay_s:
            await asyncio.sleep(self.reopen_delay_s)

        if request.url.params["device_id"] == self.slow_device:
            await asyncio.sleep(self.slow_open_s)

        principal = request.headers["X-User"]
        resumed_from = request.headers.get("Last-Event-ID") or request.url.params.get(
            "last_event_id"
        )
        conn = _Conn(principal, request.url.params["device_id"], resumed_from)

        if self.kind == "ws":
            return self._ws(conn)

        if self.refuse:
            return httpx.Response(401)

        if self.not_a_stream:
            return httpx.Response(200, json={})

        if self.draining:
            self.draining -= 1
            headers = {**_EVENT_STREAM, "X-Error-Code": "realtime_draining"}

            return httpx.Response(200, headers=headers, content=b"retry: 20\n\n")

        conn.queue.put_nowait(b"event: welcome\ndata: {}\n\n")  # a frame no write caused
        self._replay(conn)

        return httpx.Response(200, headers=_EVENT_STREAM, content=self._body(conn))

    def _ws(self, conn: _Conn) -> httpx.Response:
        if self.upgrade_status != 101:
            return httpx.Response(self.upgrade_status)

        if self.refuse or self.draining or self.close_first:
            self.draining = max(self.draining - 1, 0)
            code = 1008 if self.refuse else self.close_first.pop(0) if self.close_first else 1012
            conn.queue.put_nowait(bytes([0x88, 2]) + code.to_bytes(2))

        else:
            # Two texts that are not frames of the protocol, then a frame no write caused.
            for text in ("not json", '{"type": "error"}', '{"event": "welcome", "data": {}}'):
                conn.queue.put_nowait(_ws_text(text))

            self._replay(conn)

        return httpx.Response(101, extensions={"network_stream": _FakeStream(conn, self)})

    def _replay(self, conn: _Conn) -> None:
        self.conns.append(conn)

        if conn.resumed_from is None:
            return

        ids = [event_id for event_id, _, _ in self.mailbox]
        start = ids.index(conn.resumed_from) + 1 if conn.resumed_from in ids else 0

        for event_id, principal, frame in self.mailbox[start:]:
            if principal == conn.principal:
                conn.delivered.append(event_id)
                conn.queue.put_nowait(frame)

    async def _body(self, conn: _Conn) -> AsyncIterator[bytes]:
        try:
            while (chunk := await conn.queue.get()) is not None:
                if chunk == _BREAK:
                    raise httpx.ReadError("connection reset")

                yield chunk

        finally:
            conn.closed = True

    def end_all(self, *, broken: bool = False) -> None:
        """What the drain step does to open streams, or, *broken*, a reset connection."""

        ended = None if self.kind == "sse" else bytes([0x88, 2]) + (1012).to_bytes(2)

        for conn in self.conns:
            if not conn.closed:
                conn.closed = True
                conn.queue.put_nowait(_BREAK if broken else ended)


_EVENT_STREAM = {"content-type": "text/event-stream; charset=utf-8"}
_BREAK = b"<connection reset>"

_DOC: dict[str, Any] = {
    "openapi": "3.1.0",
    "paths": {
        "/items/list": {"post": {"operationId": "items.list", "x-forze-kind": "query"}},
        "/items/{id}/ping": {
            "post": {
                "operationId": "items.ping",
                "x-forze-kind": "command",
                "parameters": [{"name": "id", "in": "path", "required": True}],
            }
        },
    },
}


def _ws_text(text: str) -> bytes:
    payload = text.encode()

    return bytes([0x81, 126]) + len(payload).to_bytes(2) + payload


class _FakeStream:
    def __init__(self, conn: _Conn, fake: _Fake) -> None:
        self.conn = conn
        self.fake = fake

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        chunk = await self.conn.queue.get()

        if chunk == _BREAK:
            raise ConnectionResetError("connection reset")

        return chunk or b""

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if buffer[0] & 0x0F == 0x9:
            self.conn.queue.put_nowait(b"\x8a\x00")  # the pong

    async def aclose(self) -> None:
        self.conn.closed = True


# ....................... #


def _user(name: str) -> Any:
    async def headers(_client: httpx.AsyncClient) -> dict[str, str]:
        return {"X-User": name}

    return headers


async def _to_ada(call: Call) -> httpx.Response:
    return await call.request("POST", "/notify", json={"to": "ada"})


async def _notify_alternately(call: Call) -> httpx.Response:
    to = "ada" if call.rng.random() < 0.5 else "bob"

    return await call.request("POST", "/notify", json={"to": to})


def _plan(send: Any = _notify_alternately, mix: Any = None, **kw: Any) -> LoadPlan:
    scenarios = kw.pop("scenarios", None) or [Scenario("notify", send, writes=True)]

    return LoadPlan(
        base_url="http://127.0.0.1:9",
        principals=[
            Principal(name="ada", mix=mix or {"notify": 1}, auth=_user("ada")),
            Principal(name="bob", mix=mix or {"notify": 1}, auth=_user("bob")),
        ],
        scenarios=scenarios,
        **{"allow_writes": True, **kw},
    )


def _key(frame: Frame) -> Any:
    return frame.data["key"] if frame.event == "notified" else None


def _to(response: httpx.Response) -> list[str]:
    return [response.json()["to"]]


def _realtime(kind: str = "sse", **kw: Any) -> Realtime:
    options: dict[str, Any] = {
        "subscribers": 4,
        "rate": 50,
        "duration_s": 0.25,
        "timeout_s": 0.25,
        "storm": False,
        "audience": _to,
        "match": _key,
        "key_of": lambda response: response.json()["key"],
        **kw,
    }

    return Realtime(transport=kind, path="/rt", **options)  # type: ignore[arg-type]


async def _run(fake: _Fake, profile: Realtime, **kw: Any) -> Any:
    return await run(
        _plan(**kw.pop("plan", {})), profile, transport=httpx.MockTransport(fake.handle), **kw
    )


@pytest.fixture(autouse=True)
def _fast_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(realtime_module, "RETRY_S", 0.02)


# ....................... #


class TestMatching:
    @pytest.mark.parametrize("kind", ["sse", "ws"])
    async def test_each_write_is_timed_at_every_subscriber_its_event_addresses(
        self, kind: str
    ) -> None:
        fake = _Fake(kind=kind)
        result = await _run(fake, _realtime(kind))

        (row,) = result.rows
        delivery = row["delivery"]
        assert row["kind"] == "phase"
        assert row["phase"] == "steady"
        assert fake.writes == row["outcomes"]["ok"] > 0
        # Two subscribers per principal, and each write reaches one principal's.
        assert delivery["expected"] == 2 * fake.writes
        assert delivery["delivered"] == delivery["expected"]
        assert delivery["share"] == 1.0
        assert delivery["missing"] == delivery["gaps"] == delivery["duplicates"] == 0
        assert delivery["send_ms"]["p50"] is not None
        assert row["streams"]["opened"] == row["streams"]["open"] == 4

    async def test_without_an_audience_every_subscriber_is_expected(self) -> None:
        fake = _Fake()
        result = await _run(fake, _realtime(audience=None))

        delivery = result.rows[0]["delivery"]
        assert delivery["expected"] == 4 * fake.writes
        # The other principal's streams never get the frame, so half is missing.
        assert delivery["missing"] == delivery["delivered"] == 2 * fake.writes

    async def test_a_principal_addressed_write_is_missing_only_at_that_principal(self) -> None:
        async def to_ada(call: Call) -> httpx.Response:
            return await call.request("POST", "/notify", json={"to": "ada"})

        fake = _Fake(deaf=frozenset({"ada"}))
        result = await _run(fake, _realtime(), plan={"send": to_ada})

        delivery = result.rows[0]["delivery"]
        assert delivery["expected"] == delivery["missing"] == 2 * fake.writes
        assert delivery["delivered"] == delivery["gaps"] == 0

    @pytest.mark.parametrize(("answer_after_s", "frames_after_s"), [(0.02, 0.0), (0.0, 0.02)])
    async def test_a_frame_at_a_subscriber_outside_the_audience_is_not_counted(
        self, answer_after_s: float, frames_after_s: float
    ) -> None:
        async def to_everyone(call: Call) -> httpx.Response:
            return await call.request("POST", "/notify", json={})

        fake = _Fake(answer_after_s=answer_after_s, frames_after_s=frames_after_s)
        profile = _realtime(audience=lambda _response: ["ada"])
        result = await _run(fake, profile, plan={"send": to_everyone})

        delivery = result.rows[0]["delivery"]
        assert delivery["expected"] == delivery["delivered"] == 2 * fake.writes
        assert delivery["duplicates"] == delivery["missing"] == 0

    async def test_frames_of_other_traffic_are_let_go(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(realtime_module, "_HOLD_S", 0.0)
        fake = _Fake(foreign=True)
        result = await _run(fake, _realtime(), plan={"timeout_s": 0.02})

        delivery = result.rows[0]["delivery"]
        assert delivery["delivered"] == delivery["expected"] > 0
        assert delivery["late"] == 0

    async def test_a_stream_that_first_opens_after_a_write_is_sent_is_not_expected_for_it(
        self,
    ) -> None:
        # The open phase waits 0.1 s for the batch; ada's first stream opens later, with
        # writes to ada in flight, and misses the frames of the writes sent before it opened.
        fake = _Fake(slow_device="forze-load-0", slow_open_s=0.2, answer_after_s=0.15)
        result = await _run(fake, _realtime(timeout_s=0.1, duration_s=0.3), plan={"send": _to_ada})

        (row,) = result.rows
        assert row["streams"]["opened"] == 4
        assert row["delivery"]["delivered"] == row["delivery"]["expected"] > 0
        assert row["delivery"]["missing"] == row["delivery"]["gaps"] == 0

    async def test_a_write_whose_key_is_none_is_expected_nowhere(self) -> None:
        fake = _Fake(emit=False)
        result = await _run(fake, _realtime())

        delivery = result.rows[0]["delivery"]
        assert fake.writes > 0
        assert delivery["expected"] == delivery["delivered"] == delivery["missing"] == 0
        assert delivery["share"] is None

    @pytest.mark.parametrize("frames_after_s", [0.0, 0.01])
    async def test_a_duplicate_is_counted_and_not_timed_again(self, frames_after_s: float) -> None:
        fake = _Fake(duplicate=True, frames_after_s=frames_after_s)
        result = await _run(fake, _realtime())

        delivery = result.rows[0]["delivery"]
        assert delivery["delivered"] == delivery["expected"]
        assert delivery["duplicates"] == delivery["delivered"]

    @pytest.mark.parametrize("duplicate", [False, True])
    async def test_a_frame_after_its_key_timed_out_is_late_and_the_key_missing(
        self, duplicate: bool
    ) -> None:
        fake = _Fake(frames_after_s=0.2, duplicate=duplicate)
        result = await _run(fake, _realtime(timeout_s=0.03, duration_s=0.3))

        delivery = result.rows[0]["delivery"]
        assert delivery["missing"] == delivery["expected"] > 0
        # Once per stream and key: a late frame's duplicate is not late again.
        assert 0 < delivery["late"] <= delivery["missing"]
        assert delivery["delivered"] == delivery["duplicates"] == 0

    def test_frames_of_other_traffic_are_let_go_oldest_first(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(realtime_module, "_HOLD_S", 0.0)
        state = SimpleNamespace(plan=_plan(timeout_s=0.5))
        rt = _RealtimeRun(spec=_realtime(subscribers=1), state=state, client=None)  # type: ignore[arg-type]

        rt._hold("a", 0, 1.0)
        rt._hold("b", 0, 1.2)
        rt._hold("b", 0, 1.3)
        rt._hold("c", 0, 1.6)

        assert list(rt.early) == ["b", "c"]  # "a" is older than a request can take
        assert rt.early["b"][1][0] == [1.2, 1]  # first arrival, one repeat

    def test_a_late_frame_counts_once_and_only_at_a_stream_that_missed_it(self) -> None:
        state = SimpleNamespace(plan=_plan())
        rt = _RealtimeRun(spec=_realtime(subscribers=2), state=state, client=None)  # type: ignore[arg-type]
        rt.finished["key-1"] = 1 << 0  # closed: stream 0 never got it, stream 1 did
        frame = Frame(event="notified", id=None, text='{"data": {"key": "key-1"}}')

        rt._arrive(rt.subscribers[0], frame)
        rt._arrive(rt.subscribers[0], frame)  # its duplicate
        rt._arrive(rt.subscribers[1], frame)  # a re-send to a stream that had it

        assert rt.phase.late == 1
        assert rt.finished["key-1"] == 0

    def test_a_frame_before_the_first_write_is_not_held(self) -> None:
        state = SimpleNamespace(plan=_plan())
        rt = _RealtimeRun(spec=_realtime(subscribers=1), state=state, client=None)  # type: ignore[arg-type]
        frame = Frame(event="notified", id="event-0", text='{"data": {"key": "old"}}')

        rt._arrive(rt.subscribers[0], frame)

        assert rt.early == {}  # an earlier run's mailbox, replayed on the first open
        assert rt.subscribers[0].last_id == "event-0"


class TestTwoLatencies:
    async def test_a_frame_that_beats_its_answer_is_negative_and_kept_out_of_the_sketch(
        self,
    ) -> None:
        fake = _Fake(answer_after_s=0.03)
        result = await _run(fake, _realtime())

        delivery = result.rows[0]["delivery"]
        negative = delivery["answer_negative"]
        assert delivery["delivered"] > 0
        assert negative["count"] == delivery["delivered"]
        assert negative["share"] == 1.0
        assert negative["min_ms"] < -20
        assert all(value is None for value in delivery["answer_ms"].values())
        assert delivery["send_ms"]["p50"] is not None  # send → delivery is never negative

    async def test_a_frame_after_its_answer_is_timed_from_it(self) -> None:
        fake = _Fake(frames_after_s=0.03)
        result = await _run(fake, _realtime())

        delivery = result.rows[0]["delivery"]
        assert delivery["answer_negative"]["count"] == 0
        assert delivery["answer_negative"]["min_ms"] is None
        assert delivery["answer_ms"]["p50"] >= 20


class TestStorm:
    @pytest.mark.parametrize("kind", ["sse", "ws"])
    async def test_each_stream_resumes_from_the_last_durable_id_it_saw(self, kind: str) -> None:
        fake = _Fake(kind=kind, initial=4, reopen_delay_s=0.1)
        result = await _run(fake, _realtime(kind, storm=True))

        steady, storm = result.rows
        streams = storm["streams"]
        assert streams["resume"] == "last_event_id"
        assert streams["resumed"] == streams["back"] == 4
        assert streams["back_in_s"] >= 0.1

        for device in {conn.device for conn in fake.conns}:
            first, second = [conn for conn in fake.conns if conn.device == device]
            assert first.delivered, "the subscriber saw a durable id before the drop"
            assert second.resumed_from == first.delivered[-1]

        # What was written while the streams were down came back through the resume.
        delivery = storm["delivery"]
        assert delivery["gaps"] == delivery["missing"] == 0
        assert delivery["replayed"] > 0
        assert delivery["delivered"] == delivery["expected"]
        assert steady["delivery"]["delivered"] == steady["delivery"]["expected"]

    async def test_without_a_resume_the_keys_written_while_down_are_gaps(self) -> None:
        fake = _Fake(initial=4, reopen_delay_s=0.1)
        result = await _run(fake, _realtime(storm=True, resume=False))

        streams, delivery = result.rows[1]["streams"], result.rows[1]["delivery"]
        assert streams["resume"] == "none"
        assert streams["resumed"] == 0
        assert all(conn.resumed_from is None for conn in fake.conns)
        assert delivery["gaps"] > 0
        assert delivery["missing"] == 0
        assert delivery["delivered"] + delivery["gaps"] == delivery["expected"]

    async def test_a_subscriber_that_saw_no_durable_id_resumes_from_none(self) -> None:
        fake = _Fake(durable=False)
        result = await _run(fake, _realtime(storm=True))

        streams = result.rows[1]["streams"]
        assert streams["resume"] == "last_event_id"
        assert streams["resumed"] == 0
        assert all(conn.resumed_from is None for conn in fake.conns)

    async def test_a_stream_that_never_opened_does_not_hold_back_in_s_open(self) -> None:
        fake = _Fake()

        async def first_never_opens(request: httpx.Request) -> httpx.Response:
            if request.url.params.get("device_id") == "forze-load-0":
                raise httpx.ConnectError("refused")

            return await fake.handle(request)

        result = await run(
            _plan(), _realtime(storm=True), transport=httpx.MockTransport(first_never_opens)
        )

        steady, storm = result.rows
        assert steady["streams"]["opened"] == 3
        assert steady["streams"]["errors"] > 0
        assert storm["streams"]["back"] == 3
        assert storm["streams"]["back_in_s"] is not None

    async def test_a_stream_still_away_leaves_back_in_s_unknown(self) -> None:
        fake = _Fake(initial=4, reopen_delay_s=1.0)
        result = await _run(fake, _realtime(storm=True, duration_s=0.15, timeout_s=0.1))

        streams = result.rows[1]["streams"]
        assert streams["back"] == 0
        assert streams["back_in_s"] is None


class TestReconnects:
    @pytest.mark.parametrize("kind", ["sse", "ws"])
    async def test_a_draining_answer_is_a_reconnect_never_an_error(self, kind: str) -> None:
        fake = _Fake(kind=kind, draining=2)
        result = await _run(fake, _realtime(kind))

        streams = result.rows[0]["streams"]
        assert streams["draining"] == 2
        assert streams["opened"] == 4
        assert streams["refused"] == streams["errors"] == 0

    @pytest.mark.parametrize("kind", ["sse", "ws"])
    async def test_a_stream_the_server_ends_is_reopened_with_its_resume(self, kind: str) -> None:
        fake = _Fake(kind=kind, end_after=3)
        result = await _run(fake, _realtime(kind, duration_s=0.3))

        streams = result.rows[0]["streams"]
        assert streams["ended"] == 4
        assert streams["opened"] == 8

        for device in {conn.device for conn in fake.conns}:
            first, second = [conn for conn in fake.conns if conn.device == device]
            assert second.resumed_from == (first.delivered[-1] if first.delivered else None)

    @pytest.mark.parametrize(
        "fake",
        [
            _Fake(refuse=True),
            _Fake(not_a_stream=True),
            _Fake(kind="ws", refuse=True),
            _Fake(kind="ws", upgrade_status=403),
        ],
        ids=["sse-401", "sse-not-a-stream", "ws-1008", "ws-403"],
    )
    async def test_every_stream_refused_stops_the_run_before_any_write(self, fake: _Fake) -> None:
        result = await _run(fake, _realtime(fake.kind, storm=True))

        (row,) = result.rows
        assert result.summary["stopped"] == {"phase": "steady", "reason": "no_stream"}
        assert row["streams"]["refused"] == 4
        assert row["calls"] == fake.writes == 0

    @pytest.mark.parametrize("kind", ["sse", "ws"])
    async def test_a_stream_that_breaks_mid_read_ended_and_is_not_an_error(self, kind: str) -> None:
        fake = _Fake(kind=kind, break_after=3)
        result = await _run(fake, _realtime(kind, duration_s=0.3))

        streams = result.rows[0]["streams"]
        assert streams["ended"] == 4
        assert streams["errors"] == 0
        assert streams["opened"] == 8

    async def test_a_socket_closed_before_its_pong_for_another_reason_is_retried(self) -> None:
        fake = _Fake(kind="ws", close_first=[1011])
        result = await _run(fake, _realtime("ws", subscribers=1))

        streams = result.rows[0]["streams"]
        assert streams["errors"] == 1
        assert streams["opened"] == 1
        assert streams["refused"] == streams["draining"] == 0

    async def test_a_transport_error_on_open_is_retried(self) -> None:
        fake = _Fake()
        failures = iter([True, False])

        async def flaky(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/rt" and next(failures, False):
                raise httpx.ConnectError("refused")

            return await fake.handle(request)

        result = await run(_plan(), _realtime(subscribers=1), transport=httpx.MockTransport(flaky))

        streams = result.rows[0]["streams"]
        assert streams["errors"] == 1
        assert streams["opened"] == 1


class TestTheRun:
    async def test_the_file_records_the_profile_s_numbers_and_never_a_key_or_id(
        self, tmp_path: Path
    ) -> None:
        out = tmp_path / "rt.jsonl"
        await _run(_Fake(), _realtime(storm=True, topics=("orders",)), out=out)

        text = out.read_text()
        meta = json.loads(text.splitlines()[0])
        assert meta["model"] == "realtime"
        assert meta["profile"]["transport"] == "sse"
        assert {"match", "key_of", "audience", "path", "topics"}.isdisjoint(meta["profile"])
        assert "key-1" not in text and "event-1" not in text
        assert read_run(out).rows == []  # `compare` reads steps; a realtime run has phases
        summary = json.loads(text.splitlines()[-1])
        assert summary["knee"] is None and summary["note"] is None

    async def test_a_busy_client_flags_its_rows_saturated(self) -> None:
        def burning_match(frame: Frame) -> Any:
            # Past the probe's 50 ms period: every burn shows as loop lag well over 25 ms.
            end = time.perf_counter() + 0.1

            while time.perf_counter() < end:
                pass

            return _key(frame)

        fake = _Fake()
        profile = attrs.evolve(_realtime(subscribers=2, rate=20), match=burning_match)
        result = await _run(fake, profile)

        client = result.rows[0]["client"]
        assert client["saturated"]
        assert not client["loop_cpu_judged"]  # served in process: judged on loop lag

    @pytest.mark.parametrize("frames_after_s", [0.0, 0.3])
    async def test_a_match_that_raises_stops_the_run(self, frames_after_s: float) -> None:
        # Raised at once, or only once the writes are over and the run awaits deliveries.
        def broken(frame: Frame) -> Any:
            if frame.event == "notified":
                raise RuntimeError("plan bug")

        fake = _Fake(frames_after_s=frames_after_s)

        with pytest.raises(RuntimeError, match="plan bug"):
            await _run(fake, attrs.evolve(_realtime(duration_s=0.2, timeout_s=0.5), match=broken))

    async def test_a_writer_whose_samples_run_out_stops_the_run(self) -> None:
        plan = {
            "send": None,
            "scenarios": [
                Scenario.op("items.ping", writes=True, path={"id": sample("items.list")})
            ],
            "mix": {"items.ping": 1},
        }
        fake = _Fake()
        profile = _realtime(writers=["ada"], key_of=lambda _response: None, storm=True)
        result = await _run(fake, profile, plan=plan)

        (row,) = result.rows
        assert result.summary["stopped"] == {
            "phase": "steady",
            "reason": "sample_exhausted",
            "principal": "ada",
        }
        assert row["cut_short"] is True
        assert row["sample_exhausted"] == [{"principal": "ada", "scenario": "items.ping"}]

    async def test_subscribers_and_writers_are_spread_over_the_principals_named(self) -> None:
        fake = _Fake()
        result = await _run(fake, _realtime(principals=["bob"], writers=["ada"], audience=None))

        assert {conn.principal for conn in fake.conns} == {"bob"}
        assert result.rows[0]["delivery"]["expected"] == 4 * fake.writes


class TestRefusals:
    @pytest.mark.parametrize(
        "options",
        [
            {"transport": "grpc"},
            {"path": "http://elsewhere/rt"},
            {"subscribers": 0},
            {"subscribers": True},
            {"batch": 1.5},
            {"rate": float("nan")},
            {"duration_s": 0},
            {"timeout_s": float("inf")},
        ],
    )
    def test_a_malformed_profile_is_refused(self, options: dict[str, Any]) -> None:
        values: dict[str, Any] = {
            "transport": "sse",
            "path": "/rt",
            "subscribers": 1,
            "rate": 1.0,
            "match": _key,
            "key_of": _to,
            **options,
        }

        with pytest.raises(CoreException) as refused:
            Realtime(**values)

        assert refused.value.code == "load_plan_invalid"

    @pytest.mark.parametrize("options", [{"principals": ["carol"]}, {"writers": []}])
    async def test_principals_the_plan_lacks_are_refused_before_anything_is_sent(
        self, options: dict[str, Any]
    ) -> None:
        fake = _Fake()

        with pytest.raises(CoreException) as refused:
            await _run(fake, _realtime(**options))

        assert refused.value.code == "load_plan_invalid"
        assert fake.opens == fake.writes == 0

    async def test_the_write_guard_still_holds(self) -> None:
        fake = _Fake()

        with pytest.raises(CoreException) as refused:
            await _run(fake, _realtime(), plan={"allow_writes": False})

        assert refused.value.code == "load_writes_refused"
        assert fake.opens == 0

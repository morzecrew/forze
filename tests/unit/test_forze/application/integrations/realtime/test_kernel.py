"""The transport-neutral realtime kernel — client-key ladder, replay, ack, protocol.

# covers: forze.application.integrations.realtime (resolve_client_key, iter_replay,
#         acknowledge_up_to, negotiate_realtime_protocol, in-memory mailbox/cursors)

The kernel is what makes a second transport cheap: the Socket.IO connection layer and
the SSE route both consume these helpers, so their delivery semantics cannot drift.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import aclosing
from enum import Enum
from typing import Any
from uuid import UUID

import pytest
import structlog

from forze.application.contracts.authn import ClientIdentity
from forze.application.contracts.realtime import Audience, MailboxEntry, RealtimeSignal
from forze.application.integrations.realtime import (
    REALTIME_PROTOCOL_VERSION,
    SUPPORTED_REALTIME_PROTOCOLS,
    BacklogDrain,
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    acknowledge_up_to,
    iter_backlog,
    iter_replay,
    negotiate_realtime_protocol,
    resolve_client_key,
)
from forze.base.exceptions import CoreException
from forze.base.primitives import HlcTimestamp

# ----------------------- #


def _hlc(ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=ms, logical=0)


def _signal(n: int) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("p1"), "e", {"n": n})


async def _seed(mailbox: InMemoryRealtimeMailbox, count: int = 3) -> list[str]:
    ids = [f"00000000-0000-0000-0000-00000000000{i + 1}" for i in range(count)]

    for i, event_id in enumerate(ids):
        await mailbox.store(principal="p1", event_id=event_id, hlc=_hlc(i + 1), signal=_signal(i))

    return ids


# ----------------------- #


class TestClientKeyLadder:
    def test_device_id_wins(self) -> None:
        client = ClientIdentity(device_id="dev", session_id="sess")
        assert resolve_client_key(client, fallback="sid") == "dev"

    def test_session_id_next(self) -> None:
        client = ClientIdentity(session_id="sess")
        assert resolve_client_key(client, fallback="sid") == "sess"

    def test_fallback_last(self) -> None:
        assert resolve_client_key(ClientIdentity(), fallback="sid") == "sid"
        assert resolve_client_key(None, fallback="sid") == "sid"


# ----------------------- #


class TestIterReplay:
    async def test_prefers_the_paged_replay_since(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        ids = await _seed(mailbox)

        got = [e.event_id async for e in iter_replay(mailbox, principal="p1", since=None)]
        assert got == ids


class _WindowedMailbox:
    """A mailbox whose replay window stops at ``cap`` while more entries may remain —
    the durable-store shape. ``InMemoryRealtimeMailbox``'s own cap EVICTS instead of
    bounding the window, so it cannot reproduce truncation."""

    def __init__(self, inner: InMemoryRealtimeMailbox, cap: int) -> None:
        self.inner = inner
        self.cap = cap
        self.on_replay: Callable[[int], None] | None = None

    async def replay_since(self, *, principal: str, since: HlcTimestamp | None):  # type: ignore[no-untyped-def]
        fetched = 0

        async with aclosing(self.inner.replay_since(principal=principal, since=since)) as entries:
            async for entry in entries:
                if fetched >= self.cap:
                    return

                fetched += 1
                yield entry

    def __getattr__(self, name: str) -> object:
        return getattr(self.inner, name)


class _FailingMailbox:
    """A store that delivers one entry, then fails mid-replay."""

    def __init__(self) -> None:
        self.on_replay: Callable[[int], None] | None = None

    async def replay_since(self, *, principal: str, since: HlcTimestamp | None):  # type: ignore[no-untyped-def]
        yield MailboxEntry(event_id="a", hlc=_hlc(1), event="e", payload={})
        raise RuntimeError("store down")


def _raise(_count: int) -> None:
    raise ValueError("metrics backend broken")


async def _store(mailbox: InMemoryRealtimeMailbox, event_id: str, ms: int) -> None:
    await mailbox.store(principal="p1", event_id=event_id, hlc=_hlc(ms), signal=_signal(ms))


class TestIterBacklog:
    """The multi-round drain that settles a cap-filled replay window safely."""

    async def _drain(self, mailbox: object, **kwargs: object) -> tuple[list[str], BacklogDrain]:
        outcome = BacklogDrain()
        got = [
            e.event_id
            async for e in iter_backlog(
                mailbox,  # type: ignore[arg-type]
                principal="p1",
                since=None,
                outcome=outcome,
                **kwargs,  # type: ignore[arg-type]
            )
        ]

        return got, outcome

    async def test_uncapped_mailbox_drains_in_one_pass(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        ids = await _seed(mailbox)

        got, outcome = await self._drain(mailbox)

        assert got == ids
        assert outcome.complete is True
        assert outcome.claim_floor == _hlc(3)

    async def test_cap_filled_but_drained_backlog_confirms_via_a_refetch(self) -> None:
        # Exactly ``cap`` retained entries fill the window; the follow-up pass from
        # the last complete run underfills it, proving the drain (no duplicates out).
        inner = InMemoryRealtimeMailbox()
        await _store(inner, "a", 1)
        await _store(inner, "b", 2)

        got, outcome = await self._drain(_WindowedMailbox(inner, cap=2))

        assert got == ["a", "b"]
        assert outcome.complete is True
        assert outcome.claim_floor == _hlc(2)

    async def test_split_equal_hlc_run_is_recovered(self) -> None:
        # The cap boundary lands inside an equal-HLC run: strict-greater resume from
        # the run's HLC would skip its undelivered sibling forever. The re-fetch from
        # the last COMPLETE run re-reads the split run and delivers the remainder
        # exactly once.
        inner = InMemoryRealtimeMailbox()
        await _store(inner, "a", 1)
        await _store(inner, "b", 2)
        await _store(inner, "c", 2)  # the window (cap=3) cuts between "c" and "d"
        await _store(inner, "d", 3)
        await _store(inner, "f", 4)

        got, outcome = await self._drain(_WindowedMailbox(inner, cap=3))

        assert got == ["a", "b", "c", "d", "f"]  # nothing skipped, nothing duplicated
        assert outcome.complete is True
        assert outcome.claim_floor == _hlc(4)

    async def test_unresolvable_equal_hlc_run_stays_unclaimed(self) -> None:
        # A run at least ``cap`` long can never be fully seen through a
        # strict-greater window: the drain stops, stays incomplete, and the claim
        # floor holds BELOW the run — claiming it would trim the unseen sibling.
        inner = InMemoryRealtimeMailbox()
        await _store(inner, "a", 1)
        await _store(inner, "s1", 2)
        await _store(inner, "s2", 2)
        await _store(inner, "s3", 2)  # unreachable: the window always ends at s2

        got, outcome = await self._drain(_WindowedMailbox(inner, cap=2))

        assert got == ["a", "s1", "s2"]
        assert outcome.complete is False
        assert outcome.claim_floor == _hlc(1)  # never the partially delivered run

    async def test_round_budget_leaves_the_drain_unconfirmed(self) -> None:
        inner = InMemoryRealtimeMailbox()

        for n in range(1, 7):
            await _store(inner, f"e{n}", n)

        got, outcome = await self._drain(_WindowedMailbox(inner, cap=2), max_rounds=2)

        # round 1: e1,e2; round 2 re-fetches e2, delivers e3 — then the budget ends
        assert got == ["e1", "e2", "e3"]
        assert outcome.complete is False
        assert outcome.claim_floor == _hlc(2)  # e3's run is not proven complete

    async def test_closing_the_drain_closes_the_nested_replay_stream(self) -> None:
        # Closing iter_backlog early must deterministically close the mailbox's own
        # replay_since generator — an ``async for`` does not close its iterator on
        # early exit, so without propagation the nested generator stays suspended
        # until the event loop's asyncgen finalizer acloses it at some later tick
        # (never, under a torn-down loop).
        closed: list[bool] = []

        class _TrackedMailbox:
            async def replay_since(self, *, principal: str, since: HlcTimestamp | None):  # type: ignore[no-untyped-def]
                try:
                    yield MailboxEntry(event_id="a", hlc=_hlc(1), event="e", payload={})
                    yield MailboxEntry(event_id="b", hlc=_hlc(2), event="e", payload={})

                finally:
                    closed.append(True)

            async def read_since(self, *, principal: str, since: HlcTimestamp | None):  # type: ignore[no-untyped-def]
                return []  # pragma: no cover — replay_since is preferred

        stream = iter_backlog(
            _TrackedMailbox(),  # type: ignore[arg-type]
            principal="p1",
            since=None,
            outcome=BacklogDrain(),
        )
        assert (await anext(stream)).event_id == "a"
        await stream.aclose()

        assert closed == [True]  # cleanup ran synchronously with the aclose

    async def test_on_replay_hears_one_count_per_drain(self) -> None:
        # Two cap-filled rounds re-fetch e2; the hook still hears once, counting it once.
        inner = InMemoryRealtimeMailbox()

        for n in range(1, 7):
            await _store(inner, f"e{n}", n)

        mailbox = _WindowedMailbox(inner, cap=2)
        sizes: list[int] = []
        mailbox.on_replay = sizes.append

        got, _ = await self._drain(mailbox, max_rounds=2)

        assert got == ["e1", "e2", "e3"]
        assert sizes == [3]

    async def test_a_drain_stopped_early_records_nothing(self) -> None:
        # A client gone mid-replay says nothing about the backlog; a partial count
        # beside healthy connects would read as devices catching up.
        inner = InMemoryRealtimeMailbox()
        await _seed(inner)
        mailbox = _WindowedMailbox(inner, cap=10)
        sizes: list[int] = []
        mailbox.on_replay = sizes.append

        stream = iter_backlog(
            mailbox,  # type: ignore[arg-type]
            principal="p1",
            since=None,
            outcome=BacklogDrain(),
        )
        await anext(stream)
        await stream.aclose()

        assert sizes == []

    async def test_a_failed_drain_records_nothing(self) -> None:
        mailbox = _FailingMailbox()
        sizes: list[int] = []
        mailbox.on_replay = sizes.append

        with pytest.raises(RuntimeError, match="store down"):
            await self._drain(mailbox)

        assert sizes == []  # an outage must not read as a caught-up device

    async def test_a_raising_hook_leaves_a_finished_drain_intact(self) -> None:
        inner = InMemoryRealtimeMailbox()
        ids = await _seed(inner)
        mailbox = _WindowedMailbox(inner, cap=10)
        mailbox.on_replay = _raise

        with structlog.testing.capture_logs() as logs:
            got, outcome = await self._drain(mailbox)

        assert got == ids
        assert outcome.complete is True
        assert [one["log_level"] for one in logs if "on_replay" in str(one["event"])] == ["warning"]

    async def test_a_raising_hook_never_replaces_the_drains_own_error(self) -> None:
        mailbox = _FailingMailbox()
        mailbox.on_replay = _raise

        with pytest.raises(RuntimeError, match="store down"):
            await self._drain(mailbox)

    async def test_falls_back_to_read_since(self) -> None:
        class _BufferedOnly:
            """A mailbox without ``replay_since`` — the optional-protocol fallback."""

            async def read_since(
                self, *, principal: str, since: HlcTimestamp | None
            ) -> list[MailboxEntry]:
                del principal, since
                return [MailboxEntry(event_id="a", hlc=_hlc(1), event="e", payload={})]

        got = [e.event_id async for e in iter_replay(_BufferedOnly(), principal="p1", since=None)]  # type: ignore[arg-type]
        assert got == ["a"]


# ----------------------- #


class TestAcknowledgeUpTo:
    async def test_advances_and_trims_the_all_device_floor(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        cursors = InMemoryMailboxCursors()
        ids = await _seed(mailbox)

        position = await acknowledge_up_to(
            mailbox, cursors, principal="p1", client_key="d1", event_id=ids[1]
        )

        assert position == _hlc(2)
        # the single known device acked through entry 2, so 1..2 were trimmed
        remaining = await mailbox.read_since(principal="p1", since=None)
        assert [e.event_id for e in remaining] == [ids[2]]

    async def test_floor_is_the_minimum_across_known_devices(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        cursors = InMemoryMailboxCursors()
        ids = await _seed(mailbox)

        # d2 registers first (a device only becomes "known" once it has a cursor row —
        # before that the TTL/cap is its backstop, and the floor ignores it)
        await acknowledge_up_to(mailbox, cursors, principal="p1", client_key="d2", event_id=ids[0])
        await acknowledge_up_to(mailbox, cursors, principal="p1", client_key="d1", event_id=ids[2])

        # d2 lags at entry 1: only entry 1 may be trimmed, 2..3 stay retained for d2
        remaining = await mailbox.read_since(principal="p1", since=None)
        assert [e.event_id for e in remaining] == ids[1:]

    async def test_delivered_floor_clamps_an_ack_past_the_delivered_prefix(self) -> None:
        # a live frame acked mid-replay: the cumulative claim stops at the transport's
        # contiguously-delivered floor, so undelivered entries are neither skipped nor
        # trimmed out from under the still-running replay
        mailbox = InMemoryRealtimeMailbox()
        cursors = InMemoryMailboxCursors()
        ids = await _seed(mailbox)

        position = await acknowledge_up_to(
            mailbox,
            cursors,
            principal="p1",
            client_key="d1",
            event_id=ids[2],
            delivered_floor=_hlc(1),
        )

        assert position == _hlc(1)  # clamped, not the acked entry's own position
        assert await cursors.get(principal="p1", client_key="d1") == _hlc(1)
        remaining = await mailbox.read_since(principal="p1", since=None)
        assert [e.event_id for e in remaining] == ids[1:]  # 2..3 stay retained

    async def test_delivered_floor_at_or_past_the_ack_does_not_clamp(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        cursors = InMemoryMailboxCursors()
        ids = await _seed(mailbox)

        position = await acknowledge_up_to(
            mailbox,
            cursors,
            principal="p1",
            client_key="d1",
            event_id=ids[1],
            delivered_floor=_hlc(5),
        )

        assert position == _hlc(2)  # within the delivered prefix: the ack stands as-is

    async def test_unknown_event_id_is_a_noop(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        cursors = InMemoryMailboxCursors()
        await _seed(mailbox)

        position = await acknowledge_up_to(
            mailbox, cursors, principal="p1", client_key="d1", event_id="missing"
        )

        assert position is None
        assert await cursors.get(principal="p1", client_key="d1") is None
        assert len(await mailbox.read_since(principal="p1", since=None)) == 3


# ----------------------- #


class TestProtocolNegotiation:
    def test_missing_means_current(self) -> None:
        assert negotiate_realtime_protocol(None) == REALTIME_PROTOCOL_VERSION

    @pytest.mark.parametrize("raw", [1, "1", " 1 "])
    def test_supported_forms_accepted(self, raw: object) -> None:
        assert negotiate_realtime_protocol(raw) == 1

    @pytest.mark.parametrize(
        # "²"/"①" pass str.isdigit but int() rejects them — must refuse, not crash
        "raw",
        [2, "2", "x", "", True, False, 1.5, {"v": 1}, [1], "²", "①"],
    )
    def test_unsupported_or_garbage_refused(self, raw: object) -> None:
        with pytest.raises(CoreException) as caught:
            negotiate_realtime_protocol(raw)

        assert caught.value.code == "realtime_protocol_unsupported"
        assert caught.value.details == {"supported": sorted(SUPPORTED_REALTIME_PROTOCOLS)}


class TestFrameEncoding:
    """The one serialization boundary every transport frame crosses — never raises."""

    def test_plain_payload_passes_through(self) -> None:
        from forze.application.integrations.realtime import encode_frame

        assert encode_frame({"type": "ack", "cid": 1, "data": {"ok": True}}) == (
            '{"type":"ack","cid":1,"data":{"ok":true}}'
        )

    @pytest.mark.parametrize(
        # the leaves that idiomatically leak into payloads — plus a hostile object
        "poison",
        [
            __import__("datetime").datetime(2026, 1, 1),
            __import__("datetime").date(2026, 1, 1),
            __import__("decimal").Decimal("1.5"),
            object(),
            {"nested": {object()}},
        ],
    )
    def test_never_raises_and_preserves_correlation(self, poison: object) -> None:
        import json

        from forze.application.integrations.realtime import (
            FRAME_UNSERIALIZABLE_CODE,
            encode_frame,
        )

        frame = json.loads(encode_frame({"type": "ack", "cid": 7, "data": poison}))

        assert frame["type"] == "ack"
        assert frame["cid"] == 7  # any JSON scalar cid survives, ints included
        assert frame["error"]["code"] == FRAME_UNSERIALIZABLE_CODE
        assert frame["error"]["kind"] == "internal"
        assert "data" not in frame  # the poisoned value never reaches the wire

    @pytest.mark.parametrize(
        ("payload", "wire"),
        [
            # orjson's form: UTF-8 text, an escaped line break (an SSE data line stays one
            # line), a UUID or an Enum as its JSON value, a non-finite float as null
            ({"s": "héllo ✓"}, '{"s":"héllo ✓"}'),
            ({"s": "a\nb\r\nc"}, '{"s":"a\\nb\\r\\nc"}'),
            ({"u": UUID(int=1)}, '{"u":"00000000-0000-0000-0000-000000000001"}'),
            ({"k": Enum("K", {"A": "a"}).A}, '{"k":"a"}'),
            ({"x": float("nan"), "y": float("inf")}, '{"x":null,"y":null}'),
            # what orjson refuses and the stdlib encoded keeps the stdlib's form
            ({1: "é"}, '{"1":"\\u00e9"}'),
            ({"n": 2**64}, '{"n":18446744073709551616}'),
            ({"s": "\ud800"}, '{"s":"\\ud800"}'),
            # ... with a non-finite float still written as null
            (
                {"n": 2**64, "x": float("nan"), "l": [float("-inf")]},
                '{"n":18446744073709551616,"x":null,"l":[null]}',
            ),
        ],
    )
    def test_wire_form(self, payload: dict[Any, Any], wire: str) -> None:
        from forze.application.integrations.realtime import encode_frame

        assert encode_frame(payload) == wire

    def test_jsonable_frame_reads_back_either_encoding(self) -> None:
        from forze.application.integrations.realtime import jsonable_frame

        assert jsonable_frame({"x": float("nan"), "s": "é"}) == {"x": None, "s": "é"}
        assert jsonable_frame({"n": 2**64, "s": "\ud800"}) == {"n": 2**64, "s": "\ud800"}

    def test_nesting_too_deep_for_the_stdlib_falls_back(self) -> None:
        import json

        from forze.application.integrations.realtime import (
            FRAME_UNSERIALIZABLE_CODE,
            encode_frame,
        )

        deep: Any = []
        for _ in range(100_000):
            deep = [deep]

        frame = json.loads(encode_frame({"type": "ack", "cid": "c1", "data": deep}))

        assert frame["cid"] == "c1"
        assert frame["error"]["code"] == FRAME_UNSERIALIZABLE_CODE

    def test_fallback_code_is_overridable(self) -> None:
        import json

        from forze.application.integrations.realtime import encode_frame

        frame = json.loads(
            encode_frame(
                {"type": "ack", "cid": "c1", "data": object()},
                fallback_code="realtime_ack_unserializable",
            )
        )

        assert frame["error"]["code"] == "realtime_ack_unserializable"

    def test_jsonable_frame_is_the_dict_twin(self) -> None:
        from forze.application.integrations.realtime import jsonable_frame

        ok = jsonable_frame({"error": {"detail": "bad", "code": "x", "kind": "validation"}})
        assert ok == {"error": {"detail": "bad", "code": "x", "kind": "validation"}}

        broken = jsonable_frame({"error": {"context": object()}})
        assert broken["error"]["kind"] == "internal"  # replaced, never raised

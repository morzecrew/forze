"""The transport-neutral realtime kernel — client-key ladder, replay, ack, protocol.

# covers: forze.application.integrations.realtime (resolve_client_key, iter_replay,
#         acknowledge_up_to, negotiate_realtime_protocol, in-memory mailbox/cursors)

The kernel is what makes a second transport cheap: the Socket.IO connection layer and
the SSE route both consume these helpers, so their delivery semantics cannot drift.
"""

from __future__ import annotations

import pytest

from forze.application.contracts.authn import ClientIdentity
from forze.application.contracts.realtime import Audience, MailboxEntry, RealtimeSignal
from forze.application.integrations.realtime import (
    REALTIME_PROTOCOL_VERSION,
    SUPPORTED_REALTIME_PROTOCOLS,
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    acknowledge_up_to,
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

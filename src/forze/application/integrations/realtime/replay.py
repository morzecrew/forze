"""Replay and cumulative-ack discipline over the mailbox seam — shared by every transport.

These helpers are the transport-neutral half of a realtime connection: given an
already-resolved mailbox + cursors pair and an already-authenticated principal,
they implement the client-key ladder, the backlog drain, and the cumulative ack.
The transport edge (Socket.IO connection layer, SSE route) owns only what is
genuinely transport-specific — sessions, framing, and how the handshake arrives.
"""

from collections.abc import AsyncGenerator

from forze.application.contracts.authn import ClientIdentity
from forze.application.contracts.realtime import MailboxEntry
from forze.base.primitives import HlcTimestamp

from .mailbox import MailboxCursors, RealtimeMailbox

# ----------------------- #

__all__ = [
    "resolve_client_key",
    "iter_replay",
    "has_entries_after",
    "acknowledge_up_to",
]


def resolve_client_key(client: ClientIdentity | None, *, fallback: str) -> str:
    """The stable cursor key ladder: ``device_id`` → ``session_id`` → *fallback*.

    The fallback is the transport's per-connection identifier (the Socket.IO ``sid``,
    an SSE per-principal default) — always namespaced under the principal downstream,
    so a spoofed value can only ever address that principal's own cursor.
    """

    key = client.key if client is not None else None

    return key or fallback


# ....................... #


async def iter_replay(
    mailbox: RealtimeMailbox,
    *,
    principal: str,
    since: HlcTimestamp | None,
) -> AsyncGenerator[MailboxEntry]:
    """Stream a mailbox's backlog, preferring the paged ``replay_since`` when present.

    ``replay_since`` is optional on the :class:`RealtimeMailbox` protocol, so a mailbox
    that only implements the buffered :meth:`~RealtimeMailbox.read_since` still works —
    it just materializes the page at once instead of streaming.
    """

    stream = getattr(mailbox, "replay_since", None)

    if stream is not None:
        async for entry in stream(principal=principal, since=since):
            yield entry

        return

    for entry in await mailbox.read_since(principal=principal, since=since):
        yield entry


# ....................... #


async def has_entries_after(
    mailbox: RealtimeMailbox,
    *,
    principal: str,
    since: HlcTimestamp | None,
) -> bool:
    """Whether any mailbox entry remains past *since* — a single-entry probe.

    Lets a transport tell a replay that merely **filled** its cap from one that
    drained the backlog at exactly the cap: the delivered count alone cannot
    (``replay_since`` exits identically either way), and misreading an
    exactly-drained replay as truncated keeps the ack clamp engaged (Socket.IO) or
    ends the stream before its live tail (SSE) for no reason. Only the first entry
    is pulled, so the drained case costs one empty query.

    Granularity matches the cursor/trim semantics: entries sharing the *since* HLC
    count as delivered — a cumulative ack at that position claims the whole
    equal-HLC run, and the trim deletes it.
    """

    stream = iter_replay(mailbox, principal=principal, since=since)

    try:
        await anext(stream)

    except StopAsyncIteration:
        return False

    else:
        return True

    finally:
        await stream.aclose()


# ....................... #


async def acknowledge_up_to(
    mailbox: RealtimeMailbox,
    cursors: MailboxCursors,
    *,
    principal: str,
    client_key: str,
    event_id: str,
    delivered_floor: HlcTimestamp | None = None,
) -> HlcTimestamp | None:
    """Cumulative ack: advance the device cursor to *event_id*, trim the all-device floor.

    Returns the acked position, or ``None`` when the event id is no longer retained
    (already trimmed, or never durable) — a no-op then, since the cursor only ever
    moves forward past entries that still exist.

    A cumulative ack asserts "this device has everything at or before this position" —
    which is only true when the transport has delivered that prefix **contiguously**.
    A transport that interleaves live delivery with an in-progress replay (Socket.IO
    room emits race the connect-time drain) passes *delivered_floor*: the highest
    position it has delivered in order so far. An ack past the floor (a live frame
    received mid-replay) is clamped to it, so the cursor never advances over mailbox
    entries the replay has not delivered yet — jumping would skip them forever and let
    the all-device trim delete them. ``None`` means the transport's delivery is
    strictly ordered (replay fully drains before the live tail starts), so any acked
    id implies its whole prefix.
    """

    position = await mailbox.position_of(principal=principal, event_id=event_id)

    if position is None:
        return None

    if delivered_floor is not None and position > delivered_floor:
        position = delivered_floor

    await cursors.advance(principal=principal, client_key=client_key, up_to=position)

    # trim what every known device has now acked (the retention sweep is the backstop)
    floor = await cursors.min_cursor(principal=principal)

    if floor is not None:
        await mailbox.trim(principal=principal, before=floor)

    return position

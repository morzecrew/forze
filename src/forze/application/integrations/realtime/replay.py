"""Replay and cumulative-ack discipline over the mailbox seam — shared by every transport.

These helpers are the transport-neutral half of a realtime connection: given an
already-resolved mailbox + cursors pair and an already-authenticated principal,
they implement the client-key ladder, the backlog drain, and the cumulative ack.
The transport edge (Socket.IO connection layer, SSE route) owns only what is
genuinely transport-specific — sessions, framing, and how the handshake arrives.
"""

from collections.abc import AsyncGenerator
from contextlib import aclosing
from typing import final

import attrs

from forze.application.contracts.authn import ClientIdentity
from forze.application.contracts.realtime import MailboxEntry
from forze.base.primitives import HlcTimestamp

from .mailbox import MailboxCursors, RealtimeMailbox

# ----------------------- #

__all__ = [
    "resolve_client_key",
    "iter_replay",
    "BacklogDrain",
    "iter_backlog",
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
        # ``async for`` does not close its iterator on early exit: when THIS generator
        # is aclosed (a torn SSE response, an ``iter_backlog`` pass boundary), the
        # nested ``replay_since`` would stay suspended until GC finalizes it
        # asynchronously. ``aclosing`` propagates the closure deterministically.
        async with aclosing(stream(principal=principal, since=since)) as entries:
            async for entry in entries:
                yield entry

        return

    for entry in await mailbox.read_since(principal=principal, since=since):
        yield entry


# ....................... #


_MAX_BACKLOG_ROUNDS = 8
"""Backlog-drain round budget: a cap-filled pass is followed by another pass from the
last fully delivered position, so a mid-drain live arrival or a window-bound remainder
is drained rather than misread as truncation. Bounded so a producer writing faster
than the drain cannot hold a connection in replay forever."""


@final
@attrs.define(slots=True)
class BacklogDrain:
    """Out-box for :func:`iter_backlog` (a generator cannot return a value)."""

    claim_floor: HlcTimestamp | None = None
    """The highest position a cumulative ack may safely claim: every entry at or
    before it was delivered. A claimed position claims its **whole** equal-HLC run
    (the trim deletes ``<= floor``), so this only ever advances across runs proven
    complete — never onto a run the drain may have left partially delivered."""

    complete: bool = False
    """The retained backlog confirmably drained — nothing undelivered remains, so
    cumulative acks past *claim_floor* are safe without any clamp."""


@final
@attrs.define(slots=True)
class _RunTracker:
    """Per-pass entry classification for :func:`iter_backlog` — tracks the trailing
    equal-HLC run so a re-fetch can skip what an earlier pass already yielded, and
    advances the claimable floor only across runs proven complete."""

    outcome: BacklogDrain
    resume: HlcTimestamp | None
    """Where the next pass re-fetches from: the last proven-complete run."""

    run_hlc: HlcTimestamp | None = None
    """The trailing run: the highest HLC yielded so far."""

    run_ids: set[str] = attrs.field(factory=set)
    """Entries already yielded at ``run_hlc``."""

    # ....................... #

    def admit(self, entry: MailboxEntry) -> bool:
        """Whether *entry* is new (yield it) or a re-fetched duplicate (skip it)."""

        if self.run_hlc is None or entry.hlc > self.run_hlc:
            if self.run_hlc is not None:
                # a strictly greater entry proves the previous run complete
                self.outcome.claim_floor = self.run_hlc
                self.resume = self.run_hlc
                self.run_ids.clear()

            self.run_hlc = entry.hlc

        elif entry.event_id in self.run_ids:
            return False  # yielded before the previous pass's boundary

        self.run_ids.add(entry.event_id)

        return True

    # ....................... #

    def seal(self) -> None:
        """A pass reached the retained end: the trailing run is complete, drained."""

        if self.run_hlc is not None:
            self.outcome.claim_floor = self.run_hlc

        self.outcome.complete = True


async def iter_backlog(
    mailbox: RealtimeMailbox,
    *,
    principal: str,
    since: HlcTimestamp | None,
    outcome: BacklogDrain,
    max_rounds: int = _MAX_BACKLOG_ROUNDS,
) -> AsyncGenerator[MailboxEntry]:
    """Drain the backlog past *since*, yielding each entry once; report what's claimable.

    A mailbox whose replay window is bounded (``cap``) exits identically
    drained-vs-capped, and the strict-greater ``since`` cannot resume **inside** an
    equal-HLC run — resuming from the last delivered HLC would silently skip its
    undelivered siblings, and a later ack would let the all-device trim hard-delete
    them. So a cap-filled pass re-fetches from the last position whose run is proven
    complete (a strictly greater entry was seen past it), skipping already-yielded
    entries by id, until a pass underfills the cap (the retained end: drained) or
    yields nothing new (the trailing run is at least ``cap`` long — unreachable
    through a strict-greater window, so it stays unclaimed) or the round budget runs
    out. *outcome* is only meaningful once the generator finishes on its own; a
    consumer that stops early must treat the drain as incomplete — though
    ``outcome.claim_floor`` is safe to read at any point (it trails delivery by the
    unproven run, which is exactly what makes it claimable mid-drain).
    """

    cap = getattr(mailbox, "cap", None)
    tracker = _RunTracker(outcome=outcome, resume=since)

    for _ in range(max_rounds):
        fetched = 0
        progressed = False

        async with aclosing(
            iter_replay(mailbox, principal=principal, since=tracker.resume)
        ) as entries:
            async for entry in entries:
                fetched += 1

                if not tracker.admit(entry):
                    continue

                progressed = True

                yield entry

        if cap is None or fetched < int(cap):
            tracker.seal()  # the window undershot the cap: the retained end, drained
            return

        if not progressed:
            return  # a full window of already-yielded entries: no way further

    # round budget exhausted while still cap-filled: not complete


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

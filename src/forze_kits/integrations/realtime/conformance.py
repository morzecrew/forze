"""The capped-replay boundary — a reconnect replay interleaved with a live cumulative ack.

The offline mailbox makes two promises that only interact under load, and their interaction
is where the sharpest realtime finding of the fifth-edition audit lived. Separately they are
easy: a replay is bounded by a retention cap, and a client acks *cumulatively* ("I have
everything up to this id"). Together they are a data-loss hazard, because a cumulative ack
is only true if the delivered prefix was **complete**.

Consider a backlog larger than the cap. An oldest-first read limited to ``cap`` delivers an
incomplete prefix — the newest entries are missing — while live frames keep flowing to the
same connection. The client acks a *live* frame, which is newer than everything replayed;
the cursor jumps the gap; the trim floor follows the cursor; and the entries that were never
delivered are deleted. Nothing errors. The client is simply missing signals it will never be
sent again, and the store agrees they were acked.

:meth:`~forze_kits.integrations.realtime.DocumentRealtimeMailbox._window_filters` is the
defence: the cap moves the window *start* instead of truncating the read, so whatever is
replayed is a complete newest-``cap`` suffix and the entries below the floor are a declared,
counted retention loss rather than a silent one. This module is what checks that the defence
holds — and holds identically on every store behind it.

Like the portability family, it lives beside the code it validates rather than under
``forze_dst.conformance``: the mailbox is ``forze_kits`` code, and ``forze_dst`` is not a
sanctioned importer of ``forze_kits``. The trust harness ships with the feature.

The scenario is deliberately *not* expressed as "the mock and the real store returned the
same rows". It reduces the run to a :class:`CursorReplayOutcome` whose fields are the
properties a caller actually depends on — was the delivered window complete, did the cursor
outrun what was delivered, did the trim delete something undelivered, do two tenants sharing
a principal and a device key keep separate cursors — because those are comparable across
stores whose ids, orderings and page boundaries are not.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from typing import Final, Protocol, runtime_checkable
from uuid import UUID

import attrs

from forze.application.contracts.realtime import Audience, MailboxEntry, RealtimeSignal
from forze.application.integrations.realtime import MailboxCursors, RealtimeMailbox
from forze.base.exceptions import CoreException
from forze.base.primitives import HlcTimestamp

from .mailbox import MailboxStats

# ----------------------- #

REPLAY_CAP: Final = 5
"""The retention cap the scenario wires. Small enough to overflow deliberately."""

BACKLOG_OVERSHOOT: Final = 3
"""How far past the cap the seeded backlog goes, so the window start has to move."""

ACK_AFTER: Final = 2
"""Replayed entries consumed before the live ack is interleaved.

Any value strictly inside the replay works; two puts the ack in the middle of the stream
rather than at either edge, where an off-by-one in the window logic could hide.
"""

CURSOR_STALLED_CODE: Final = "realtime_cursor_advance_stalled"
"""Raised when a cursor row is invisible to the scope yet its derived id conflicts.

Precisely what a tenant-blind cursor id produces for a principal present in two tenants, so
the tenant probe catches it and reports a lost independence rather than exploding.
"""

PRINCIPAL: Final = "u1"
CLIENT_KEY: Final = "device-1"

TENANT_PROBE_PRINCIPAL: Final = "u-tenant-probe"
TENANT_PROBE_CLIENT_KEY: Final = "device-tenant-probe"
"""The tenant probe uses its own principal and device, not the replay's.

Cursors are monotonic, so a probe sharing the replay's device would be setting up behind a
cursor the mid-replay ack had already pushed to :data:`LIVE_HLC` — its own ``advance`` would
be a silent no-op and the probe would report a collision that is really its own setup
failing to take effect.
"""
TENANT_A: Final = UUID("aaaaaaaa-0000-4000-8000-000000000001")
TENANT_B: Final = UUID("bbbbbbbb-0000-4000-8000-000000000002")


# ....................... #


@runtime_checkable
class CountsOverflows(Protocol):
    """A mailbox that reports how often a replay overran the retention cap.

    Runtime-checkable because the scenario accepts mailboxes that do not keep counters at
    all; ``isinstance`` verifies the method is there, and the annotation is what makes the
    subsequent ``.overflowed`` read a checked one rather than an attribute lookup on
    ``object``.
    """

    def stats(self) -> MailboxStats: ...  # pragma: no cover


@attrs.frozen(kw_only=True)
class MailboxScope:
    """One tenant's mailbox, cursors and an uncapped view of the same rows.

    Typed against the core ``RealtimeMailbox`` / ``MailboxCursors`` Protocols rather than a
    local restatement of them: a hand-rolled copy is free to drift from the contract it is
    supposed to be validating, which is the one kind of drift this whole family exists to
    prevent.
    """

    mailbox: RealtimeMailbox
    """The mailbox under test, wired at :data:`REPLAY_CAP`."""

    cursors: MailboxCursors

    observer: RealtimeMailbox
    """The same collection read through an effectively uncapped mailbox.

    Purely an instrument. ``read_since`` on the capped mailbox returns the *window*, not the
    store, so it cannot answer "what did the trim actually delete" — and a scenario that
    measured deletion through the same cap it is testing would be grading its own homework.
    """


Scoped = Callable[[UUID], AbstractContextManager[MailboxScope]]
"""Enter *tenant*'s scope and yield its ports.

A callable rather than pre-built ports because tenancy is ambient: the document adapter
reads the bound tenant at call time, so the scenario has to run *inside* the binding.
"""


# ----------------------- #


@attrs.frozen(kw_only=True)
class CursorReplayOutcome:
    """The comparable result of one capped-replay-with-live-ack run."""

    replayed_complete_suffix: bool
    """Whether the replay delivered a complete, in-order newest-``cap`` suffix.

    The whole defence in one field. A truncated replay — an oldest-first prefix — is what
    makes every other failure here possible.
    """

    replayed_count: int
    overflowed: int
    """Replays that reported losing oldest backlog. Expected to be 1: the loss is real and
    declared, and a run reporting 0 would mean the cap never engaged and the scenario
    proved nothing."""

    undelivered_deleted: int
    """Entries at or above the replay window that the trim deleted without delivering.

    The data-loss count, and the reason this leg exists. Anything but 0 means a client was
    told it had everything up to a point it had not been sent.
    """

    cursor_crossed_undelivered: bool
    """Whether the cursor came to rest at or beyond an undelivered entry.

    The same fault seen one step earlier — before the trim acts on it — so a store that
    loses the guarantee is caught even if nothing has been deleted yet.
    """

    tenant_cursors_independent: bool
    """Whether one principal's device cursor stays separate across two tenants.

    The cursor id is derived deterministically so concurrent first-acks converge on one
    row; if the tenant were left out of that derivation, the same principal in two tenants
    would share a row — one tenant reading another's read position, and an ack in one
    silently advancing the other.
    """


EXPECTED_CURSOR_REPLAY = CursorReplayOutcome(
    replayed_complete_suffix=True,
    replayed_count=REPLAY_CAP,
    overflowed=1,
    undelivered_deleted=0,
    cursor_crossed_undelivered=False,
    tenant_cursors_independent=True,
)
"""What every store must answer. The counts are pinned too: a run that quietly stopped
overflowing, or replayed a different number of entries, is not the run this asserts about."""


# ----------------------- #


def _hlc(physical_ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=physical_ms, logical=0)


def _event_id(n: int) -> str:
    return str(UUID(int=n))


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal(PRINCIPAL), "order.shipped", {"text": text})


LIVE_HLC: Final = _hlc(10_000)
"""A live frame's position, far beyond every seeded entry.

Beyond on purpose: a cumulative ack at a *live* frame is the one that jumps the gap a
truncated replay leaves behind. An ack inside the replayed range could not.
"""

LIVE_EVENT_ID: Final = _event_id(9_999)


async def _live_frame_arrives(scope: MailboxScope) -> None:
    """A live durable signal lands and the client acks it cumulatively.

    The ack is the dangerous half: it asserts the client holds everything up to this
    position — including anything the replay has not sent and now never will.
    """

    await scope.mailbox.store(
        principal=PRINCIPAL,
        event_id=LIVE_EVENT_ID,
        hlc=LIVE_HLC,
        signal=_signal("live"),
    )
    await scope.cursors.advance(principal=PRINCIPAL, client_key=CLIENT_KEY, up_to=LIVE_HLC)


async def _seed_backlog(mailbox: RealtimeMailbox) -> None:
    """Store ``cap + 3`` entries at ascending positions."""

    for n in range(1, REPLAY_CAP + BACKLOG_OVERSHOOT + 1):
        await mailbox.store(
            principal=PRINCIPAL,
            event_id=_event_id(n),
            hlc=_hlc(n),
            signal=_signal(f"s{n}"),
        )


def _overflowed(mailbox: RealtimeMailbox) -> int:
    """How many replays reported losing oldest backlog, for mailboxes that count.

    Optional rather than part of ``RealtimeMailbox``: the counters are the document-backed
    store's observability, and a mailbox that keeps none still has to satisfy every other
    field of the outcome. Narrowed through :class:`CountsOverflows` rather than ``getattr``
    so the counter read is type-checked — an untyped probe here would silently accept a
    ``stats()`` that returned something else entirely and report 0.
    """

    if not isinstance(mailbox, CountsOverflows):
        return 0

    return mailbox.stats().overflowed


def _is_complete_suffix(replayed: Sequence[MailboxEntry], stored: Sequence[MailboxEntry]) -> bool:
    """Whether *replayed* is exactly the tail of *stored*, in order and with no gaps."""

    delivered = [entry.event_id for entry in replayed]
    newest = [entry.event_id for entry in stored][-len(delivered) :] if delivered else []

    return delivered == newest


# ....................... #


async def run_capped_replay_boundary(scoped: Scoped) -> CursorReplayOutcome:
    """Drive the boundary: overflow the cap, replay, ack live mid-stream, then trim.

    The ordering is the point. The ack lands *while the replay is still streaming*, which is
    what a reconnecting client actually does — it does not wait for the backlog before
    acknowledging live traffic — and it is the interleaving no simulation schedule reaches,
    because the race lives in document-port code rather than in stream code.
    """

    with scoped(TENANT_A) as scope:
        await _seed_backlog(scope.mailbox)

        replayed: list[MailboxEntry] = []
        acked = False

        async for entry in scope.mailbox.replay_since(principal=PRINCIPAL, since=None):
            replayed.append(entry)

            if len(replayed) == ACK_AFTER:
                await _live_frame_arrives(scope)
                acked = True

        if not acked:
            # The replay ended before reaching the interleave point, which a *badly*
            # truncating store does. The live frame still arrives — a client acks live
            # traffic regardless of how the backlog is going — and skipping the ack here
            # would let the worst truncations escape: the shorter the replay, the less
            # likely the fault would be observed at all.
            await _live_frame_arrives(scope)

        delivered = {entry.event_id for entry in replayed} | {LIVE_EVENT_ID}
        window_floor = min((entry.hlc for entry in replayed), default=LIVE_HLC)

        before = list(await scope.observer.read_since(principal=PRINCIPAL, since=None))
        cursor = await scope.cursors.min_cursor(principal=PRINCIPAL)

        crossed = any(
            entry.hlc >= window_floor
            and cursor is not None
            and entry.hlc <= cursor
            and entry.event_id not in delivered
            for entry in before
        )

        if cursor is not None:
            await scope.mailbox.trim(principal=PRINCIPAL, before=cursor)

        after = {
            entry.event_id
            for entry in await scope.observer.read_since(principal=PRINCIPAL, since=None)
        }
        deleted = [entry for entry in before if entry.event_id not in after]

        # Entries *below* the window floor were already a declared retention loss — the
        # replay reported it via `overflowed` and the client knows to refetch. Only a
        # deletion at or above the floor, of something never sent, is the fault.
        undelivered_deleted = sum(
            1 for entry in deleted if entry.hlc >= window_floor and entry.event_id not in delivered
        )

        overflowed = _overflowed(scope.mailbox)

        stored_view = [entry for entry in before if entry.event_id != LIVE_EVENT_ID]
        complete = _is_complete_suffix(replayed, stored_view)

    return CursorReplayOutcome(
        replayed_complete_suffix=complete,
        replayed_count=len(replayed),
        overflowed=overflowed,
        undelivered_deleted=undelivered_deleted,
        cursor_crossed_undelivered=crossed,
        tenant_cursors_independent=await _tenant_cursors_independent(scoped),
    )


async def _tenant_cursors_independent(scoped: Scoped) -> bool:
    """Whether the same principal + device key keeps separate cursors in two tenants.

    A tenant-blind cursor id does not merely leak a read position: on the tagged-tenancy
    table shape this kit recommends, the derived primary key collides, so the second
    tenant's lookup misses while its insert hits the first tenant's row. That surfaces as
    the advance loop exhausting its retry budget, which is caught here and reported as the
    lost guarantee it is rather than as an unrelated crash.
    """

    first, second = _hlc(4_000), _hlc(5_000)
    device = {"principal": TENANT_PROBE_PRINCIPAL, "client_key": TENANT_PROBE_CLIENT_KEY}

    with scoped(TENANT_A) as a:
        await a.cursors.advance(**device, up_to=first)

        if await a.cursors.get(**device) != first:
            # The setup itself did not take, so nothing below would mean anything.
            return False

    try:
        with scoped(TENANT_B) as b:
            leaked = await b.cursors.get(**device)

            if leaked is not None:
                return False

            # Deliberately *later* than the first tenant's: if the two share a row, this
            # advance drags the first tenant's cursor forward and the check below sees it.
            await b.cursors.advance(**device, up_to=second)
    except CoreException as error:
        if error.code == CURSOR_STALLED_CODE:
            return False

        raise

    with scoped(TENANT_A) as a:
        return await a.cursors.get(**device) == first

"""Realtime gateway delivery semantics under a crash — the ack-stream twin of the outbox family.

The realtime gateway promises, for a **durable** signal: at-least-once delivery off the stream's
consumer group (an unacked entry is reclaimed and redelivered), exactly-once *emit* via the
inbox dedup mark, and — for a mailboxed principal signal — **store-then-forward atomicity** (the
mark and the mailbox store commit in one transaction, so a crash can never mark a signal seen
without persisting it for reconnect replay). The classic failure windows are a crash *before*
the bridge ever ran (entry pending, nothing marked) and a crash *after* the bridge committed but
*before* the entry was acknowledged (mark stands, entry pending — redelivery must dedup).

:func:`run_gateway_crash_delivery` drives those windows through the real ports (append → group
read → **crash** → reclaim → re-bridge → ack), so the same scenario runs against the in-memory
mock and, via the differential leg, a real Redis stream. The *bridge* — the dedup + mailbox +
emit core — is **injected**: layering keeps this package from importing the Socket.IO edge, and
injecting it is what lets the tests pass the *actual* gateway bridge rather than a re-implementation
this scenario would then be tautologically checking against itself. The read/reclaim/ack
choreography is inlined here the same way the outbox family inlines the relay's claim/publish/mark:
it is the documented consumer discipline (ack only after the bridge succeeds), driven step by step
so the crash lands deterministically between two named steps.

Like the outbox family, the scenario has a built-in control: run it with a bridge that does **no**
dedup and the post-commit crash double-emits — proving the redelivery is real and the dedup mark is
doing the work, not that the crash was a no-op.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from datetime import timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID

import attrs

from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.stream import (
    AckStreamGroupAdminDepKey,
    AckStreamGroupQueryDepKey,
    StreamCommandDepKey,
    StreamSpec,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import HlcTimestamp, utcnow

# ----------------------- #

RealtimeBridge = Callable[[RealtimeSignal, UUID | None, str | None, HlcTimestamp], Awaitable[None]]
"""The injected bridge under test: (signal, tenant, dedup id, hlc) → emitted/stored.

Shape-compatible with the Socket.IO gateway's per-signal handler, so a test can pass the real
``RealtimeGateway`` bridge closure; declared here from core types only.
"""

REALTIME_DELIVERY_GROUP = "dst-realtime"
"""The consumer group the scenario creates and drives."""

REALTIME_DELIVERY_PRINCIPAL = "dst-user"
"""The principal every canonical scenario signal addresses (one recipient, one mailbox)."""

REALTIME_DELIVERY_SIGNALS: tuple[RealtimeSignal, ...] = tuple(
    RealtimeSignal.of(
        Audience.principal(REALTIME_DELIVERY_PRINCIPAL),
        "conformance.realtime",
        {"seq": i},
    )
    for i in range(5)
)
"""Five durable principal-addressed signals appended in round one (fresh event id per append)."""


# ....................... #


class GatewayCrashPoint(StrEnum):
    """Where the consumer crashes, relative to the bridge and the acknowledgement."""

    BEFORE_BRIDGE = "before_bridge"
    """Entries were delivered (read) but the bridge never ran — nothing marked, nothing
    emitted. Recovery must deliver every signal exactly once."""

    AFTER_BRIDGE_BEFORE_ACK = "after_bridge_before_ack"
    """The bridge committed (mark + store) and emitted, then the consumer died before acking.
    Recovery redelivers; a deduping bridge must collapse the duplicate (exactly-once emit,
    exactly one mailbox row), a non-deduping one double-emits (the control)."""

    BRIDGE_FAILS_ONCE = "bridge_fails_once"
    """The bridge itself fails on every round-one invocation (the emit raised inside the
    transaction, rolling the mark back). Nothing was acked; recovery re-bridges and succeeds."""


# ....................... #


@attrs.frozen(kw_only=True)
class GatewayDeliveryOutcome:
    """The observable result of the gateway crash-delivery scenario, compared across backends."""

    appended: int
    """Signals appended to the stream (the durable obligations)."""

    deliveries: int
    """Bridge invocations across both rounds — redelivery makes this exceed ``appended``."""

    emitted: int
    """Frames the transport accepted (from the injected observer)."""

    distinct_emitted: int
    """Distinct envelope ids emitted — exactly-once means ``emitted`` equals this."""

    mailboxed: int
    """Rows in the recipient's mailbox after recovery (``0`` when no counter was injected)."""

    pending_after: int
    """Entries still pending in the group after recovery — a clean run leaves none."""


# ....................... #


async def run_gateway_crash_delivery(
    ctx: ExecutionContext,
    *,
    stream_spec: StreamSpec[RealtimeSignal],
    bridge: RealtimeBridge,
    crash: GatewayCrashPoint,
    emitted_ids: Callable[[], Sequence[str | None]],
    mailbox_rows: Callable[[], Awaitable[int]] | None = None,
    signals: Sequence[RealtimeSignal] = REALTIME_DELIVERY_SIGNALS,
    group: str = REALTIME_DELIVERY_GROUP,
) -> GatewayDeliveryOutcome:
    """Append → read → **crash** → reclaim → re-bridge → ack, over *ctx*'s real stream ports.

    *ctx* is a live context whose stream ports resolve to the backend under test (mock or
    Redis); *bridge* is the dedup/mailbox/emit core (pass the real gateway's); *emitted_ids*
    reads the envelope ids the test's transport observer recorded; *mailbox_rows* counts the
    recipient's mailbox after recovery. Each signal is appended durable (a fresh
    ``forze_event_id`` header per append). Returns the observable
    :class:`GatewayDeliveryOutcome` — identical on every backend that honors the ack-stream
    delivery contract.
    """

    stream = str(stream_spec.name)
    admin = ctx.deps.resolve_configurable(
        ctx, AckStreamGroupAdminDepKey, stream_spec, route=stream_spec.name
    )
    command = ctx.deps.resolve_configurable(
        ctx, StreamCommandDepKey, stream_spec, route=stream_spec.name
    )
    query = ctx.deps.resolve_configurable(
        ctx, AckStreamGroupQueryDepKey, stream_spec, route=stream_spec.name
    )

    # 1. Provision the group at the head, then append every signal durable — the same order the
    #    ensure-group lifecycle step enforces at startup so "$" semantics can't lose the batch.
    await admin.ensure_group(group, stream, start_id="0")

    for i, signal in enumerate(signals):
        # The durable id is a UUID in production (the outbox event id); deterministic
        # fixed UUIDs keep the scenario replayable without an entropy source.
        await command.append(
            stream, signal, type=signal.event, headers={HEADER_EVENT_ID: str(UUID(int=i + 1))}
        )

    deliveries = 0

    async def _bridge(message: Any, *, failure_expected: bool) -> bool:
        """One bridge invocation; ``True`` when it succeeded (the consumer's ack condition)."""

        nonlocal deliveries
        deliveries += 1
        dedup_id = message.headers.get(HEADER_EVENT_ID)

        try:
            await bridge(message.payload, None, dedup_id, _hlc())

        except Exception:
            if not failure_expected:
                raise

            return False

        else:
            if failure_expected:
                # The test primed the transport to fail and it did not — the scenario would
                # silently degenerate into the no-crash baseline. Refuse instead.
                raise exc.precondition(
                    "GatewayCrashPoint.BRIDGE_FAILS_ONCE requires a bridge that fails in "
                    "round one; it succeeded — prime the transport to fail first"
                )

            return True

    # 2. Round one: the doomed consumer reads its batch, then crashes at the declared point.
    #    Nothing is ever acknowledged in this round — that is what makes the entries pending.
    first = await query.read(group, "dst-node-1", {stream: ">"}, limit=len(signals) + 1)

    if crash is not GatewayCrashPoint.BEFORE_BRIDGE:
        for message in first:
            await _bridge(message, failure_expected=crash is GatewayCrashPoint.BRIDGE_FAILS_ONCE)
    # ---- crash: the consumer dies here; no entry from round one is ever acked. ----

    # 3. Restart: a fresh consumer reclaims the stranded entries (``idle=0`` forces the reclaim
    #    deterministically, standing in for "pending longer than the stale threshold") and runs
    #    the consumer discipline for real: bridge, then ack only on success.
    reclaimed = await query.claim(
        group, "dst-node-2", stream, idle=timedelta(0), limit=len(signals) + 1
    )

    for message in reclaimed:
        if await _bridge(message, failure_expected=False):
            await query.ack(group=group, stream=stream, ids=[message.id])

    # 4. The observable outcome.
    emitted = list(emitted_ids())
    pending_after = await query.pending(group, stream)

    return GatewayDeliveryOutcome(
        appended=len(signals),
        deliveries=deliveries,
        emitted=len(emitted),
        distinct_emitted=len({one for one in emitted if one is not None}),
        mailboxed=await mailbox_rows() if mailbox_rows is not None else 0,
        pending_after=len(pending_after),
    )


# ....................... #


def _hlc() -> HlcTimestamp:
    """A wall-clock HLC stamp (deterministic under a simulated ``TimeSource``)."""

    return HlcTimestamp(physical_ms=int(utcnow().timestamp() * 1000), logical=0)

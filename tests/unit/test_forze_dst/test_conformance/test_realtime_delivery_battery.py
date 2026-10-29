"""The gateway crash-delivery scenario, run against the mock — the mock-only first leg.

Asserts the in-memory ack-stream + inbox + document mailbox honor the realtime delivery
contract across the crash windows: at-least-once redelivery of an unacked entry, exactly-once
emit via the dedup mark, and store-then-forward atomicity (exactly one mailbox row per signal,
no matter how many times the bridge ran). The bridge under test is the **real**
``RealtimeGateway`` core — this file may import the Socket.IO edge, the scenario package may
not. The same scenario, pointed at a real Redis stream over testcontainers, is the
differential (``test_redis_realtime_dst_conformance.py``).

The transport-neutral ``RealtimeMailboxWriter`` runs the same delivery core with no live leg,
so it is driven through the same scenario and held to the same outcome, emits aside.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

import attrs
import pytest

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution import ExecutionContext
from forze.application.execution.realtime import RealtimeMailboxWriter
from forze.application.integrations.realtime import RealtimeMailbox
from forze.base.primitives import HlcTimestamp
from forze.testing import context_from_modules
from forze_dst.conformance import (
    REALTIME_DELIVERY_PRINCIPAL,
    REALTIME_DELIVERY_SIGNALS,
    GatewayCrashPoint,
    GatewayDeliveryOutcome,
    run_gateway_crash_delivery,
)
from forze_kits.integrations.realtime import (
    build_realtime_mailbox,
    realtime_inbox_spec,
    realtime_stream_spec,
)
from forze_mock import MockDepsModule, MockState
from forze_socketio import GatewayDedup, RealtimeGateway

# ----------------------- #

_N = len(REALTIME_DELIVERY_SIGNALS)

_TOPIC_SIGNALS = tuple(
    RealtimeSignal.of(Audience.topic("dst-room"), "conformance.realtime", {"seq": i})
    for i in range(_N)
)
from tests.support.realtime_retention import UNSWEPT

"""Topic-addressed durable signals — the non-mailboxed (emit-inside-tx) lane."""


class _RecordingSio:
    """Records envelope ids; optionally fails the first N emits."""

    def __init__(self, fail_times: int = 0) -> None:
        self.ids: list[str | None] = []
        self.fail_times = fail_times

    async def emit(self, event: str, data: Any = None, **_: Any) -> None:
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("transport down")

        self.ids.append(data["id"])


class _NullSource:
    async def run(self, ctx: Any, handler: Any, *, stop: Any = None) -> None:  # pragma: no cover
        raise NotImplementedError  # the scenario drives the bridge directly


# ....................... #


def _context() -> ExecutionContext:
    # One fresh MockState = one shared store surviving the (logical) crash — the pending
    # entries, the dedup marks, and the mailbox rows all persist across the "restart".
    return context_from_modules(MockDepsModule(state=MockState()))


class _FlakyMailbox:
    """Fails the first *fail_times* stores, then delegates — a store that rolls the mark back."""

    def __init__(self, inner: RealtimeMailbox, fail_times: int) -> None:
        self.inner = inner
        self.fail_times = fail_times

    async def store(self, **kw: Any) -> None:
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("mailbox down")

        await self.inner.store(**kw)


def _bridge(gateway: RealtimeGateway, ctx: ExecutionContext, mailbox: Any):  # type: ignore[no-untyped-def]
    """The real gateway core as the scenario's injected bridge (mirrors ``run``'s closure)."""

    async def bridge(
        signal: RealtimeSignal, tenant: UUID | None, dedup_id: str | None, hlc: HlcTimestamp
    ) -> None:
        await gateway._handle(ctx, mailbox, signal, tenant, dedup_id, hlc)  # pyright: ignore[reportPrivateUsage]

    return bridge


def _writer_bridge(writer: RealtimeMailboxWriter, ctx: ExecutionContext, mailbox: Any):  # type: ignore[no-untyped-def]
    """The writer's per-signal core as the injected bridge (mirrors its ``run`` closure)."""

    async def bridge(
        signal: RealtimeSignal, tenant: UUID | None, dedup_id: str | None, hlc: HlcTimestamp
    ) -> None:
        await writer._handle(ctx, mailbox, signal, tenant, dedup_id, hlc)  # pyright: ignore[reportPrivateUsage]

    return bridge


Consumer = Literal["gateway", "writer"]


async def _run_store_half(
    consumer: Consumer, *, crash: GatewayCrashPoint, failing_stores: int = 0
) -> GatewayDeliveryOutcome:
    """Run *consumer* — the gateway or the mailbox writer — over the mailboxed signals."""

    ctx = _context()
    sio = _RecordingSio()
    dedup = GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock")
    mailbox = build_realtime_mailbox(ctx, retention=UNSWEPT)
    stored: Any = _FlakyMailbox(mailbox, failing_stores) if failing_stores else mailbox

    if consumer == "gateway":
        gateway = RealtimeGateway(sio=sio, source=_NullSource(), dedup=dedup)  # pyright: ignore[reportArgumentType]
        bridge = _bridge(gateway, ctx, stored)

    else:
        writer = RealtimeMailboxWriter(
            source=_NullSource(), dedup=dedup, mailbox_factory=lambda _ctx: mailbox
        )
        bridge = _writer_bridge(writer, ctx, stored)

    async def _rows() -> int:
        return len(await mailbox.read_since(principal=REALTIME_DELIVERY_PRINCIPAL, since=None))

    return await run_gateway_crash_delivery(
        ctx,
        stream_spec=realtime_stream_spec(),
        bridge=bridge,
        crash=crash,
        emitted_ids=lambda: sio.ids,
        mailbox_rows=_rows,
    )


def _store_half_expected(consumer: Consumer, *, deliveries: int) -> GatewayDeliveryOutcome:
    """One outcome for both consumers; only the gateway emits."""

    expected = GatewayDeliveryOutcome(
        appended=_N,
        deliveries=deliveries,
        emitted=_N,
        distinct_emitted=_N,
        mailboxed=_N,
        pending_after=0,
    )

    if consumer == "writer":
        return attrs.evolve(expected, emitted=0, distinct_emitted=0)

    return expected


async def _run(
    *,
    crash: GatewayCrashPoint,
    dedup: bool = True,
    mailboxed: bool = True,
    fail_times: int = 0,
) -> GatewayDeliveryOutcome:
    ctx = _context()
    spec = realtime_stream_spec()
    sio = _RecordingSio(fail_times=fail_times)

    gateway = RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=_NullSource(),
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock") if dedup else None,
    )
    mailbox = build_realtime_mailbox(ctx, retention=UNSWEPT) if (dedup and mailboxed) else None

    async def _rows() -> int:
        if mailbox is None:
            return 0

        return len(
            await mailbox.read_since(principal=REALTIME_DELIVERY_PRINCIPAL, since=None)
        )

    return await run_gateway_crash_delivery(
        ctx,
        stream_spec=spec,
        bridge=_bridge(gateway, ctx, mailbox),
        crash=crash,
        emitted_ids=lambda: sio.ids,
        mailbox_rows=_rows,
        signals=REALTIME_DELIVERY_SIGNALS if mailboxed else _TOPIC_SIGNALS,
    )


# ----------------------- #


@pytest.mark.conformance(plane="realtime_delivery", engine="mock")
class TestMockGatewayCrashDelivery:
    async def test_crash_before_bridge_recovers_every_signal_once(self) -> None:
        outcome = await _run(crash=GatewayCrashPoint.BEFORE_BRIDGE)
        # Round one delivered nothing (the bridge never ran); recovery reclaimed every entry
        # and delivered each exactly once — emitted, marked, and mailboxed atomically.
        assert outcome == GatewayDeliveryOutcome(
            appended=_N,
            deliveries=_N,
            emitted=_N,
            distinct_emitted=_N,
            mailboxed=_N,
            pending_after=0,
        )

    async def test_crash_between_commit_and_ack_dedups_the_redelivery(self) -> None:
        outcome = await _run(crash=GatewayCrashPoint.AFTER_BRIDGE_BEFORE_ACK)
        # The mark + store committed before the crash, so the redelivery collapsed: one emit
        # and one mailbox row per signal across two full delivery rounds.
        assert outcome == GatewayDeliveryOutcome(
            appended=_N,
            deliveries=2 * _N,
            emitted=_N,
            distinct_emitted=_N,
            mailboxed=_N,
            pending_after=0,
        )

    async def test_duplicate_is_real_without_dedup(self) -> None:
        outcome = await _run(crash=GatewayCrashPoint.AFTER_BRIDGE_BEFORE_ACK, dedup=False)
        # The control: with no dedup mark the same crash double-emits every signal — the
        # redelivery is real, and in the dedup case the mark is doing the work.
        assert outcome == GatewayDeliveryOutcome(
            appended=_N,
            deliveries=2 * _N,
            emitted=2 * _N,
            distinct_emitted=_N,
            mailboxed=0,
            pending_after=0,
        )

    async def test_failed_emit_rolls_the_mark_back_and_recovery_emits_once(self) -> None:
        # Topic-durable signals emit inside the dedup transaction: a transport failure rolls
        # the mark back, so recovery re-bridges and emits exactly once.
        outcome = await _run(
            crash=GatewayCrashPoint.BRIDGE_FAILS_ONCE, mailboxed=False, fail_times=_N
        )
        assert outcome == GatewayDeliveryOutcome(
            appended=_N,
            deliveries=2 * _N,
            emitted=_N,
            distinct_emitted=_N,
            mailboxed=0,
            pending_after=0,
        )

    @pytest.mark.parametrize(
        "crash",
        [GatewayCrashPoint.BEFORE_BRIDGE, GatewayCrashPoint.AFTER_BRIDGE_BEFORE_ACK],
    )
    async def test_no_signal_lost_or_conjured(self, crash: GatewayCrashPoint) -> None:
        outcome = await _run(crash=crash)
        assert outcome.distinct_emitted == _N  # none lost (at-least-once held)...
        assert outcome.emitted == _N  # ...and none double-emitted (dedup held)
        assert outcome.pending_after == 0  # nothing left stranded


@pytest.mark.conformance(plane="realtime_delivery", engine="mock")
@pytest.mark.parametrize("consumer", ["gateway", "writer"])
class TestMockMailboxStoreHalf:
    """The gateway and the mailbox writer, one scenario, one expected mailbox outcome."""

    async def test_crash_before_bridge_stores_every_signal_once(self, consumer: Consumer) -> None:
        outcome = await _run_store_half(consumer, crash=GatewayCrashPoint.BEFORE_BRIDGE)
        assert outcome == _store_half_expected(consumer, deliveries=_N)

    async def test_crash_between_commit_and_ack_dedups_the_redelivery(
        self, consumer: Consumer
    ) -> None:
        outcome = await _run_store_half(consumer, crash=GatewayCrashPoint.AFTER_BRIDGE_BEFORE_ACK)
        assert outcome == _store_half_expected(consumer, deliveries=2 * _N)

    async def test_failed_store_rolls_the_mark_back_and_recovery_stores_once(
        self, consumer: Consumer
    ) -> None:
        # The mark and the store share one transaction: a store that fails leaves no mark
        # standing, so the redelivery stores (and, on the gateway, emits) exactly once.
        outcome = await _run_store_half(
            consumer, crash=GatewayCrashPoint.BRIDGE_FAILS_ONCE, failing_stores=_N
        )
        assert outcome == _store_half_expected(consumer, deliveries=2 * _N)


async def test_bridge_fails_once_refuses_a_bridge_that_did_not_fail() -> None:
    """A mis-primed transport must not silently degenerate into the no-crash baseline."""

    from forze.base.exceptions import CoreException

    ctx = _context()
    spec = realtime_stream_spec()
    sio = _RecordingSio()  # healthy — but the crash point promised round-one failures
    gateway = RealtimeGateway(sio=sio, source=_NullSource())  # pyright: ignore[reportArgumentType]

    with pytest.raises(CoreException) as caught:
        await run_gateway_crash_delivery(
            ctx,
            stream_spec=spec,
            bridge=_bridge(gateway, ctx, None),
            crash=GatewayCrashPoint.BRIDGE_FAILS_ONCE,
            emitted_ids=lambda: sio.ids,
        )

    assert "expected" in str(caught.value).lower() or caught.value.kind is not None


async def test_unexpected_recovery_failure_propagates() -> None:
    """A bridge that fails in the RECOVERY round is a scenario failure, not a swallow."""

    ctx = _context()
    spec = realtime_stream_spec()

    async def _always_broken(
        signal: RealtimeSignal, tenant: UUID | None, dedup_id: str | None, hlc: HlcTimestamp
    ) -> None:
        raise RuntimeError("still broken in recovery")

    with pytest.raises(RuntimeError):
        await run_gateway_crash_delivery(
            ctx,
            stream_spec=spec,
            bridge=_always_broken,
            crash=GatewayCrashPoint.BEFORE_BRIDGE,
            emitted_ids=list,
        )

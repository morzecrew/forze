"""HLC outbox wiring: enrichment stamp, relay header, inbox causal merge.

The clock is node-local (one per ``ExecutionContext``), so these unit tests thread
an explicit clock through the producer (enricher) and consumer (merge) — the same
clock a single runtime's context would hand both.
"""

from __future__ import annotations

from datetime import timedelta

from pydantic import BaseModel

from forze.application.contracts.envelope import HEADER_HLC
from forze.application.contracts.outbox import OutboxClaim
from forze.application.execution.context.invocation import InvocationContext
from forze.application.execution.outbox import InvocationOutboxEnricher
from forze.base.primitives import HlcTimestamp, HybridLogicalClock
from forze_kits.integrations.inbox.consumer import _merge_inbound_hlc
from forze_kits.integrations.outbox.relay import _claim_envelope_headers

# ----------------------- #


class _Payload(BaseModel):
    value: str


def _enricher(clock: HybridLogicalClock) -> InvocationOutboxEnricher:
    return InvocationOutboxEnricher(inv=InvocationContext(), clock=clock)


def _claim(hlc: HlcTimestamp | None) -> OutboxClaim:
    from uuid import uuid4

    return OutboxClaim(
        id=uuid4(),
        outbox_route="r",
        event_id=uuid4(),
        event_type="demo.created",
        payload={},
        hlc=hlc,
    )


# ----------------------- #


class TestEnrichmentStamp:
    def test_event_is_stamped_with_hlc(self) -> None:
        event = _enricher(HybridLogicalClock()).enrich(
            "demo.created", _Payload(value="a")
        )

        assert isinstance(event.hlc, HlcTimestamp)

    def test_successive_events_are_monotonic(self) -> None:
        enricher = _enricher(HybridLogicalClock())
        first = enricher.enrich("demo.created", _Payload(value="a"))
        second = enricher.enrich("demo.created", _Payload(value="b"))

        assert second.hlc is not None and first.hlc is not None
        assert second.hlc > first.hlc


class TestRelayHeader:
    def test_hlc_forwarded_when_present(self) -> None:
        ts = HlcTimestamp(physical_ms=1_700_000_000_000, logical=3)
        headers = _claim_envelope_headers(_claim(ts))

        assert headers[HEADER_HLC] == ts.encode()

    def test_absent_when_not_persisted(self) -> None:
        headers = _claim_envelope_headers(_claim(None))

        assert HEADER_HLC not in headers


class TestInboxMerge:
    def test_reaction_causally_follows_consumed_event(self) -> None:
        # One node's clock: consume, then produce in reaction.
        clock = HybridLogicalClock()

        # Consume an event from a "future" replica clock.
        remote = HlcTimestamp(physical_ms=9_000_000_000_000, logical=0)
        _merge_inbound_hlc({HEADER_HLC: remote.encode()}, clock)

        # An event produced in reaction sorts after its cause.
        reaction = _enricher(clock).enrich("demo.reacted", _Payload(value="r"))

        assert reaction.hlc is not None and reaction.hlc > remote

    def test_malformed_header_is_ignored(self) -> None:
        clock = HybridLogicalClock()
        before = clock.last
        _merge_inbound_hlc({HEADER_HLC: "not-an-hlc"}, clock)

        assert clock.last == before

    def test_drift_guard_ignores_forged_far_future_inbound(self) -> None:
        # HEADER_HLC is untrusted: a drift-guarded clock must not let a forged
        # far-future timestamp skew the node clock.
        clock = HybridLogicalClock(max_drift=timedelta(seconds=1))
        before = clock.last

        far_future = HlcTimestamp(physical_ms=9_000_000_000_000, logical=0)
        _merge_inbound_hlc({HEADER_HLC: far_future.encode()}, clock)

        assert clock.last == before  # rejected, not skewed

    def test_absent_header_is_noop(self) -> None:
        clock = HybridLogicalClock()
        before = clock.last
        _merge_inbound_hlc({}, clock)

        assert clock.last == before

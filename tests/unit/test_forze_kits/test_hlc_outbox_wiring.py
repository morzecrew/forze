"""HLC outbox wiring: enrichment stamp, relay header, inbox causal merge."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.envelope import HEADER_HLC
from forze.application.contracts.outbox import OutboxClaim
from forze.application.execution.context.invocation import InvocationContext
from forze.application.execution.outbox import InvocationOutboxEnricher
from forze.application.execution.outbox.clock import outbox_clock, set_outbox_clock
from forze.base.primitives import HlcTimestamp, HybridLogicalClock
from forze_kits.integrations.inbox.consumer import _merge_inbound_hlc
from forze_kits.integrations.outbox.relay import _claim_envelope_headers

# ----------------------- #


class _Payload(BaseModel):
    value: str


@pytest.fixture(autouse=True)
def _fresh_clock():
    saved = outbox_clock()
    set_outbox_clock(HybridLogicalClock())

    yield

    set_outbox_clock(saved)


def _enricher() -> InvocationOutboxEnricher:
    return InvocationOutboxEnricher(inv=InvocationContext())


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
        event = _enricher().enrich("demo.created", _Payload(value="a"))

        assert isinstance(event.hlc, HlcTimestamp)

    def test_successive_events_are_monotonic(self) -> None:
        enricher = _enricher()
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
        # Consume an event from a "future" replica clock.
        remote = HlcTimestamp(physical_ms=9_000_000_000_000, logical=0)
        _merge_inbound_hlc({HEADER_HLC: remote.encode()})

        # An event produced in reaction sorts after its cause.
        reaction = _enricher().enrich("demo.reacted", _Payload(value="r"))

        assert reaction.hlc is not None and reaction.hlc > remote

    def test_malformed_header_is_ignored(self) -> None:
        before = outbox_clock().last
        _merge_inbound_hlc({HEADER_HLC: "not-an-hlc"})

        assert outbox_clock().last == before

    def test_absent_header_is_noop(self) -> None:
        before = outbox_clock().last
        _merge_inbound_hlc({})

        assert outbox_clock().last == before

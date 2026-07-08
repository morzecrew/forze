"""The crash-recovery delivery scenario, run against the mock — the mock-only first leg.

Asserts the in-memory outbox/inbox honor the delivery contract across a publish-then-crash window:
at-least-once delivery (the reclaimed row is re-published) and, with the inbox on, exactly-once
effect (the duplicate collapses to one). The same scenario, pointed at real Postgres over
testcontainers, is the differential (`test_pg_delivery_conformance.py`).
"""

from __future__ import annotations

import pytest

from forze.testing import context_from_modules
from forze_dst.conformance import (
    DELIVERY_EVENTS,
    DeliveryOutcome,
    observe_uncommitted_outbox_visibility,
    run_crash_recovery_delivery,
)
from forze_mock import MockDepsModule, MockState

# ----------------------- #

_N = len(DELIVERY_EVENTS)
_TX_SCOPE = "mock"


async def _run(*, dedup: bool) -> DeliveryOutcome:
    # One fresh MockState = one shared store surviving the (logical) crash — the mock counterpart of
    # a Postgres table persisting the un-marked rows across the restart.
    ctx = context_from_modules(MockDepsModule(state=MockState()))
    return await run_crash_recovery_delivery(ctx, tx_scope=_TX_SCOPE, dedup=dedup)


# ....................... #


class TestMockCrashRecoveryDelivery:
    async def test_exactly_once_effect_with_inbox(self) -> None:
        outcome = await _run(dedup=True)
        # The crash re-published every event (at-least-once: delivered twice), the restart reclaimed
        # the crashed round's rows, and the inbox collapsed the duplicate to a single effect.
        assert outcome == DeliveryOutcome(
            staged=_N,
            delivered=2 * _N,
            reclaimed=_N,
            applied=_N,
            distinct_applied=_N,
        )

    async def test_duplicate_is_real_without_inbox(self) -> None:
        outcome = await _run(dedup=False)
        # Without dedup the redelivery applies twice — proving the crash genuinely re-published (the
        # inbox in the dedup case is doing real work, not masking a no-op).
        assert outcome == DeliveryOutcome(
            staged=_N,
            delivered=2 * _N,
            reclaimed=_N,
            applied=2 * _N,
            distinct_applied=_N,
        )

    @pytest.mark.parametrize("dedup", [True, False])
    async def test_no_event_lost_or_conjured(self, dedup: bool) -> None:
        # Whatever the dedup setting, exactly the staged event set reaches the consumer — none lost
        # (at-least-once holds), none invented.
        outcome = await _run(dedup=dedup)
        assert outcome.distinct_applied == _N
        assert outcome.staged == _N


class TestMockOutboxOverVisibility:
    """Pins the ``outbox-inbox-write-through`` divergence: the mock's write-through outbox lets a
    concurrent relay claim a producer's *uncommitted* row. Real Postgres does not (the differential
    leg asserts the other side) — a documented, expected disagreement, checked from both ends."""

    async def test_relay_sees_uncommitted_row_on_the_mock(self) -> None:
        state = MockState()
        producer = context_from_modules(MockDepsModule(state=state))
        relay = context_from_modules(MockDepsModule(state=state))
        over_visible = await observe_uncommitted_outbox_visibility(
            producer, relay, tx_scope=_TX_SCOPE
        )
        # The mock over-permits: the relay claimed a not-yet-committed row (a phantom event if the
        # producer rolled back). This is why a premature-event finding on the outbox path must be
        # confirmed against a real store before it is trusted as a real bug.
        assert over_visible is True

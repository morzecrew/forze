"""The isolation anomaly battery, run against the mock — the mock-only first leg of RFC 0004 §4.A.

Asserts the mock produces the verdict a correct Forze adapter should at each level, and — the
integrity guard — that any deviation from the textbook contract is a registered, justified
strengthening (no silent divergence). The same battery, pointed at a real backend over
testcontainers, is the differential conformance step.
"""

from __future__ import annotations

from collections.abc import Sequence

import attrs
import pytest

from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import ExecutionContext
from forze.testing import context_from_modules
from forze_dst.conformance import (
    BATTERY,
    CONTRACT_STRENGTHENINGS,
    Verdict,
    expected_verdict,
)
from forze_mock import MockDepsModule, MockState

# ----------------------- #

_LEVELS = (
    IsolationLevel.READ_COMMITTED,
    IsolationLevel.SNAPSHOT,
    IsolationLevel.SERIALIZABLE,
)


@attrs.define
class MockConformanceBackend:
    """N independent mock sessions over one fresh shared ``MockState`` per anomaly run."""

    scope_name: str = "mock"

    def contexts(self, n: int) -> Sequence[ExecutionContext]:
        state = MockState()
        return [context_from_modules(MockDepsModule(state=state)) for _ in range(n)]


# ....................... #


@pytest.mark.parametrize("case", BATTERY, ids=lambda c: c.name)
@pytest.mark.parametrize("level", _LEVELS, ids=lambda level: level.name)
class TestIsolationBattery:
    async def test_mock_matches_expected_verdict(
        self, case, level: IsolationLevel
    ) -> None:
        observed = await case.run(MockConformanceBackend(), level)
        assert observed == expected_verdict(case, level)

    async def test_any_deviation_from_contract_is_registered(
        self, case, level: IsolationLevel
    ) -> None:
        # The no-silent-divergence guard: the mock may differ from the textbook contract ONLY
        # where a ContractStrengthening justifies it.
        observed = await case.run(MockConformanceBackend(), level)

        if observed != case.contract[level]:
            registered = {(s.anomaly, s.level) for s in CONTRACT_STRENGTHENINGS}
            assert (case.name, level) in registered, (
                f"unregistered divergence: {case.name}@{level.name} "
                f"observed={observed.value} contract={case.contract[level].value}"
            )


# ....................... #


class TestLevelDiscrimination:
    """The battery as a whole distinguishes all three levels (it is not SI masquerading as SSI)."""

    async def test_write_skew_separates_snapshot_from_serializable(self) -> None:
        write_skew = next(c for c in BATTERY if c.name == "write_skew")
        backend = MockConformanceBackend()
        assert await write_skew.run(backend, IsolationLevel.SNAPSHOT) == Verdict.PERMITTED
        assert await write_skew.run(backend, IsolationLevel.SERIALIZABLE) == Verdict.PREVENTED

    async def test_read_skew_separates_read_committed_from_snapshot(self) -> None:
        read_skew = next(c for c in BATTERY if c.name == "read_skew")
        backend = MockConformanceBackend()
        assert await read_skew.run(backend, IsolationLevel.READ_COMMITTED) == Verdict.PERMITTED
        assert await read_skew.run(backend, IsolationLevel.SNAPSHOT) == Verdict.PREVENTED

    async def test_lost_update_strengthened_at_every_level(self) -> None:
        # Forze's rev-OCC prevents lost update even at READ_COMMITTED (the registered strengthening).
        lost_update = next(c for c in BATTERY if c.name == "lost_update")
        backend = MockConformanceBackend()
        for level in _LEVELS:
            assert await lost_update.run(backend, level) == Verdict.PREVENTED

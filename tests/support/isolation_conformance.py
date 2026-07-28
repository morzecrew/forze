"""The mock leg of the isolation conformance battery — shared across unit and differential tiers.

One :class:`MockConformanceBackend` definition, imported by the mock-only unit battery and by the
real-backend differential modules (whose fidelity-matrix tests collect the mock half alongside the
real half in the same process, so a matrix always pairs verdicts produced by identical code).
"""

from __future__ import annotations

from collections.abc import Sequence

import attrs

from forze.application.execution import ExecutionContext
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockState

# ----------------------- #


@attrs.define
class MockConformanceBackend:
    """N independent mock sessions over one fresh shared ``MockState`` per anomaly run."""

    scope_name: str = "mock"

    def contexts(self, n: int) -> Sequence[ExecutionContext]:
        state = MockState()
        return [context_from_modules(MockDepsModule(state=state)) for _ in range(n)]

"""The shared saga coordinator: pivot tracking, compensation order, error builders."""

from __future__ import annotations

import pytest

from forze.application.contracts.saga import SagaProgress, SagaStepKind
from forze.base.exceptions import ExceptionKind

# ----------------------- #

_KINDS = [SagaStepKind.COMPENSATABLE, SagaStepKind.PIVOT, SagaStepKind.RETRYABLE]
_NAMES = ["reserve", "charge", "ship"]


def _progress() -> SagaProgress:
    p = SagaProgress(saga_name="checkout")
    for name, kind in zip(_NAMES, _KINDS, strict=True):
        p.register(name, kind)
    return p


class TestSagaProgress:
    def test_register_returns_sequential_indices(self) -> None:
        p = SagaProgress(saga_name="checkout")
        assert p.register("reserve", SagaStepKind.COMPENSATABLE) == 0
        assert p.register("charge", SagaStepKind.PIVOT) == 1
        assert p.register("ship", SagaStepKind.RETRYABLE) == 2

    def test_register_validates_order(self) -> None:
        from forze.base.exceptions import CoreException

        p = SagaProgress(saga_name="checkout")
        with pytest.raises(CoreException):
            p.register("ship", SagaStepKind.RETRYABLE)  # retryable before any pivot

    def test_committed_flips_at_the_pivot(self) -> None:
        p = _progress()
        assert p.committed is False

        p.record_success(0)  # compensatable
        assert p.committed is False

        p.record_success(1)  # pivot
        assert p.committed is True

        p.record_success(2)  # retryable
        assert p.committed is True

    def test_steps_to_compensate_is_reverse_completion_order(self) -> None:
        p = _progress()
        p.record_success(0)
        p.record_success(1)

        assert p.steps_to_compensate() == [1, 0]

    def test_step_failed_error_is_domain_when_compensation_succeeds(self) -> None:
        """DOMAIN by decision: compensations succeeded, so the system is consistent
        and the rolled-back saga is a modeled business outcome — the kind encodes
        the saga outcome, not the failing step's cause (chained as ``__cause__``).
        """
        p = _progress()
        err = p.step_failed_error(2, RuntimeError("boom"), comp_errors=[])

        assert err.kind is ExceptionKind.DOMAIN
        assert "step_failed" in (err.code or "")

    def test_step_failed_error_is_infrastructure_when_compensation_fails(self) -> None:
        p = _progress()
        err = p.step_failed_error(
            2, RuntimeError("boom"), comp_errors=[RuntimeError("undo failed")]
        )

        assert err.kind is ExceptionKind.INFRASTRUCTURE
        assert "compensation_failed" in (err.code or "")

    def test_forward_incomplete_error(self) -> None:
        p = _progress()
        err = p.forward_incomplete_error(2, RuntimeError("ship failed"))

        assert err.kind is ExceptionKind.INFRASTRUCTURE
        assert "forward_incomplete" in (err.code or "")

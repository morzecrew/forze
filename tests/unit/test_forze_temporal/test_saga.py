"""TemporalSaga maps a saga ``CoreException`` to a temporalio ``ApplicationError`` so the
workflow *fails* (reaches FAILED) instead of failing the workflow task — which Temporal would
retry forever, hanging the run. ``non_retryable`` follows the framework's per-kind policy."""

import pytest
from temporalio.exceptions import ApplicationError

from forze.application.contracts.saga import SagaStepKind
from forze.base.exceptions import exc
from forze_temporal.saga import TemporalSaga

pytestmark = pytest.mark.unit


async def _ok[T](value: T) -> T:
    return value


async def test_step_failure_before_pivot_raises_non_retryable_application_error() -> None:
    saga = TemporalSaga(name="checkout")
    compensated: list[str] = []

    async def _comp() -> None:
        compensated.append("reserve")

    await saga.step("reserve", lambda: _ok("r"), compensation=_comp)

    async def _boom() -> str:
        raise exc.validation("bad charge", code="charge.invalid")

    with pytest.raises(ApplicationError) as ei:
        await saga.step("charge", _boom)

    err = ei.value
    assert err.type == "saga.step_failed"
    assert err.non_retryable is True  # a domain/deterministic failure must not retry
    assert compensated == ["reserve"]  # pre-pivot failure ran the compensation
    assert isinstance(err.__cause__, Exception)  # original error chained


async def test_pre_pivot_compensation_failure_raises_compensation_failed() -> None:
    saga = TemporalSaga(name="checkout")

    async def _bad_comp() -> None:
        # The rollback itself fails — the system may be inconsistent.
        raise exc.infrastructure("rollback down", code="reserve.rollback_failed")

    await saga.step("reserve", lambda: _ok("r"), compensation=_bad_comp)

    async def _boom() -> str:
        raise exc.validation("bad charge", code="charge.invalid")

    with pytest.raises(ApplicationError) as ei:
        await saga.step("charge", _boom)

    err = ei.value
    assert err.type == "saga.compensation_failed"  # rollback failed, not a clean step_failed
    assert err.non_retryable is False  # infrastructure outcome stays retryable per policy
    assert isinstance(err.__cause__, Exception)  # original step error chained


async def test_forward_failure_after_pivot_raises_retryable_application_error() -> None:
    saga = TemporalSaga(name="checkout")

    await saga.step("commit", lambda: _ok("c"), kind=SagaStepKind.PIVOT)
    assert saga.committed is True

    async def _boom() -> str:
        raise exc.infrastructure("downstream down", code="notify.down")

    with pytest.raises(ApplicationError) as ei:
        await saga.step("notify", _boom, kind=SagaStepKind.RETRYABLE)

    err = ei.value
    assert err.type == "saga.forward_incomplete"
    assert err.non_retryable is False  # an infrastructure failure stays retryable


async def test_happy_path_returns_step_result() -> None:
    saga = TemporalSaga(name="checkout")
    assert await saga.step("s", lambda: _ok(42)) == 42

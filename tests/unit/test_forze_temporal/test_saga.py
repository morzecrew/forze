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


async def test_ambiguous_step_commit_is_non_retryable_and_compensates_nothing() -> None:
    # ``saga.step_ambiguous`` is infrastructure-kind, whose per-kind policy is
    # retryable — but the interrupted step may have committed, so the mapping pins it
    # non-retryable: a Temporal retry would re-run the saga into a possible
    # double-execution. Reconcile before re-running.
    from forze.application.contracts.transaction import COMMIT_AMBIGUOUS_CODE

    saga = TemporalSaga(name="checkout")
    compensated: list[str] = []

    async def _comp() -> None:
        compensated.append("reserve")

    await saga.step("reserve", lambda: _ok("r"), compensation=_comp)

    async def _ambiguous() -> str:
        raise exc.internal(
            "Cancelled at or after the transaction commit",
            code=COMMIT_AMBIGUOUS_CODE,
        )

    with pytest.raises(ApplicationError) as ei:
        await saga.step("charge", _ambiguous)

    err = ei.value
    assert err.type == "saga.step_ambiguous"
    assert err.non_retryable is True  # pinned, despite the retryable infrastructure kind
    assert compensated == []  # nothing rolled back around the unknown outcome


async def test_wrapped_activity_commit_ambiguity_is_detected() -> None:
    # An activity that dies at its transaction commit raises the commit_ambiguous
    # CoreException inside the activity; the WORKFLOW receives it wrapped — an
    # ActivityError whose cause is the ApplicationError the default failure converter
    # built (type = the class name, the code surviving only inside the message). The
    # saga must still classify it as indeterminate, not compensate around it.
    from temporalio.exceptions import ActivityError

    from forze.application.contracts.transaction import COMMIT_AMBIGUOUS_CODE

    saga = TemporalSaga(name="checkout")
    compensated: list[str] = []

    async def _comp() -> None:
        compensated.append("reserve")

    await saga.step("reserve", lambda: _ok("r"), compensation=_comp)

    core = exc.internal(
        "Cancelled at or after the transaction commit", code=COMMIT_AMBIGUOUS_CODE
    )
    wrapped_cause = ApplicationError(str(core), type="CoreException")
    activity_error = ActivityError(
        "activity task failed",
        scheduled_event_id=1,
        started_event_id=2,
        identity="worker",
        activity_type="charge",
        activity_id="1",
        retry_state=None,
    )
    activity_error.__cause__ = wrapped_cause

    async def _boom() -> str:
        raise activity_error

    with pytest.raises(ApplicationError) as ei:
        await saga.step("charge", _boom)

    assert ei.value.type == "saga.step_ambiguous"
    assert ei.value.non_retryable is True
    assert compensated == []  # nothing rolled back around the unknown outcome


async def test_converter_typed_commit_ambiguity_is_detected() -> None:
    # A failure converter that maps the CoreException code into the ApplicationError
    # ``type`` (the shape _as_application_error itself produces) is detected too.
    from forze.application.contracts.transaction import COMMIT_AMBIGUOUS_CODE

    saga = TemporalSaga(name="checkout")
    compensated: list[str] = []

    async def _comp() -> None:
        compensated.append("reserve")

    await saga.step("reserve", lambda: _ok("r"), compensation=_comp)

    async def _boom() -> str:
        raise ApplicationError("commit torn", type=COMMIT_AMBIGUOUS_CODE)

    with pytest.raises(ApplicationError) as ei:
        await saga.step("charge", _boom)

    assert ei.value.type == "saga.step_ambiguous"
    assert compensated == []


async def test_outcome_probe_survives_a_cyclic_cause_chain() -> None:
    # The cause walk is bounded: a pathological __cause__ cycle must terminate as
    # "not ambiguous" instead of spinning the workflow task.
    from forze_temporal.saga import _step_outcome_unknown

    first = RuntimeError("first")
    second = RuntimeError("second")
    first.__cause__ = second
    second.__cause__ = first

    assert _step_outcome_unknown(first) is False


async def test_step_without_compensation_is_skipped_during_rollback() -> None:
    # A completed step that registered no compensation is simply skipped by the
    # rollback pass — not an error, and the steps that did register still run.
    saga = TemporalSaga(name="checkout")
    compensated: list[str] = []

    await saga.step("audit", lambda: _ok("a"))  # nothing to undo

    async def _comp() -> None:
        compensated.append("reserve")

    await saga.step("reserve", lambda: _ok("r"), compensation=_comp)

    async def _boom() -> str:
        raise exc.validation("bad charge", code="charge.invalid")

    with pytest.raises(ApplicationError) as ei:
        await saga.step("charge", _boom)

    assert ei.value.type == "saga.step_failed"
    assert compensated == ["reserve"]  # the compensation-less step was skipped


async def test_happy_path_returns_step_result() -> None:
    saga = TemporalSaga(name="checkout")
    assert await saga.step("s", lambda: _ok(42)) == 42

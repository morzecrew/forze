"""Validation tests for resilience strategy value objects and policy ordering."""

from __future__ import annotations

from datetime import timedelta

import pytest

from forze.application.contracts.resilience import (
    BackoffStrategy,
    BulkheadStrategy,
    CircuitBreakerStrategy,
    FallbackStrategy,
    ResiliencePolicy,
    RetryBudget,
    RetryStrategy,
    TimeoutStrategy,
)
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


def _backoff() -> BackoffStrategy:
    return BackoffStrategy(base=timedelta(milliseconds=10), max=timedelta(seconds=1))


def _retry(**kw: object) -> RetryStrategy:
    params: dict[str, object] = {
        "max_attempts": 3,
        "backoff": _backoff(),
        "retry_on": frozenset({ExceptionKind.INFRASTRUCTURE}),
    }
    params.update(kw)
    return RetryStrategy(**params)  # type: ignore[arg-type]


# ....................... #


class TestStrategyValidation:
    def test_backoff_base_must_be_positive(self) -> None:
        with pytest.raises(CoreException, match="base must be positive"):
            BackoffStrategy(base=timedelta(0), max=timedelta(seconds=1))

    def test_backoff_max_must_exceed_base(self) -> None:
        with pytest.raises(CoreException, match="max must be >= base"):
            BackoffStrategy(base=timedelta(seconds=2), max=timedelta(seconds=1))

    def test_retry_budget_ratio_bounds(self) -> None:
        with pytest.raises(CoreException, match="ratio must be in"):
            RetryBudget(ratio=0.0)

        with pytest.raises(CoreException, match="ratio must be in"):
            RetryBudget(ratio=1.5)

    def test_bulkhead_concurrency_floor(self) -> None:
        with pytest.raises(CoreException, match="max_concurrency must be >= 1"):
            BulkheadStrategy(max_concurrency=0)

    def test_circuit_breaker_ratio_bounds(self) -> None:
        with pytest.raises(CoreException, match="failure_ratio must be in"):
            CircuitBreakerStrategy(
                failure_ratio=0.0,
                sampling_window=timedelta(seconds=10),
                min_throughput=5,
                break_duration=timedelta(seconds=5),
            )

    def test_timeout_must_be_positive(self) -> None:
        with pytest.raises(CoreException, match="Timeout must be positive"):
            TimeoutStrategy(timeout=timedelta(0))

    def test_retry_rejects_non_retryable_kind(self) -> None:
        with pytest.raises(CoreException, match="non-retryable kinds"):
            _retry(retry_on=frozenset({ExceptionKind.VALIDATION}))

    def test_retry_accepts_retryable_kinds(self) -> None:
        strat = _retry(
            retry_on=frozenset(
                {ExceptionKind.CONCURRENCY, ExceptionKind.INFRASTRUCTURE}
            ),
        )
        assert ExceptionKind.CONCURRENCY in strat.retry_on

    def test_retry_empty_retry_on_rejected(self) -> None:
        with pytest.raises(CoreException, match="must not be empty"):
            _retry(retry_on=frozenset())


# ....................... #


class TestPolicyComposition:
    def test_rejects_empty(self) -> None:
        with pytest.raises(CoreException, match="must declare a strategy"):
            ResiliencePolicy(name="p", strategies=())

    def test_rejects_duplicate_strategy_types(self) -> None:
        with pytest.raises(CoreException, match="duplicate strategy types"):
            ResiliencePolicy(name="p", strategies=(_retry(), _retry()))

    def test_rejects_out_of_order(self) -> None:
        with pytest.raises(CoreException, match="must be ordered"):
            ResiliencePolicy(
                name="p",
                strategies=(TimeoutStrategy(timeout=timedelta(seconds=1)), _retry()),
            )

    def test_accepts_canonical_order(self) -> None:
        policy = ResiliencePolicy(
            name="p",
            strategies=(
                BulkheadStrategy(max_concurrency=5),
                CircuitBreakerStrategy(
                    failure_ratio=0.5,
                    sampling_window=timedelta(seconds=10),
                    min_throughput=5,
                    break_duration=timedelta(seconds=5),
                ),
                _retry(),
                TimeoutStrategy(timeout=timedelta(seconds=1)),
            ),
        )
        assert policy.bulkhead is not None
        assert policy.circuit_breaker is not None
        assert policy.retry is not None
        assert policy.timeout is not None
        assert policy.has_fallback is False

    def test_fallback_marker_allowed_anywhere(self) -> None:
        policy = ResiliencePolicy(
            name="p",
            strategies=(FallbackStrategy(), _retry()),
        )
        assert policy.has_fallback is True
        assert policy.retry is not None

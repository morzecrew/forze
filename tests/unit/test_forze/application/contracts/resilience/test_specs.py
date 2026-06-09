"""Validation tests for the resilience spec catalog."""

from __future__ import annotations

from datetime import timedelta

import pytest

from forze.application.contracts.resilience import (
    BackoffStrategy,
    ResiliencePolicy,
    ResilienceSpec,
    RetryStrategy,
)
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


def _policy(name: str) -> ResiliencePolicy:
    return ResiliencePolicy(
        name=name,
        strategies=(
            RetryStrategy(
                max_attempts=2,
                backoff=BackoffStrategy(
                    base=timedelta(milliseconds=10),
                    max=timedelta(seconds=1),
                ),
                retry_on=frozenset({ExceptionKind.INFRASTRUCTURE}),
            ),
        ),
    )


# ....................... #


def test_spec_requires_policies() -> None:
    with pytest.raises(CoreException, match="at least one policy"):
        ResilienceSpec(name="catalog", policies={})


def test_spec_rejects_key_name_mismatch() -> None:
    with pytest.raises(CoreException, match="does not match"):
        ResilienceSpec(name="catalog", policies={"a": _policy("b")})


def test_spec_accepts_matching_catalog() -> None:
    spec = ResilienceSpec(name="catalog", policies={"transient": _policy("transient")})
    assert "transient" in spec.policies

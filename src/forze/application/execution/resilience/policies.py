"""Built-in default resilience policies."""

from datetime import timedelta

from forze.application.contracts.resilience import (
    BackoffStrategy,
    ResiliencePolicy,
    RetryStrategy,
    TimeoutStrategy,
)
from forze.base.exceptions import ExceptionKind
from forze.base.primitives import StrKey

# ----------------------- #


def builtin_default_policies() -> dict[StrKey, ResiliencePolicy]:
    """Return the default named policies used when no spec is provided.

    - ``occ``: retry optimistic-concurrency conflicts (read-modify-write boundary).
    - ``transient``: retry transient infrastructure faults with a per-attempt timeout.
    """

    occ = ResiliencePolicy(
        name="occ",
        strategies=(
            RetryStrategy(
                max_attempts=3,
                backoff=BackoffStrategy(
                    base=timedelta(milliseconds=50),
                    max=timedelta(seconds=2),
                    jitter="decorrelated",
                ),
                retry_on=frozenset({ExceptionKind.CONCURRENCY}),
            ),
        ),
    )

    transient = ResiliencePolicy(
        name="transient",
        strategies=(
            RetryStrategy(
                max_attempts=3,
                backoff=BackoffStrategy(
                    base=timedelta(milliseconds=100),
                    max=timedelta(seconds=5),
                    jitter="decorrelated",
                ),
                retry_on=frozenset({ExceptionKind.INFRASTRUCTURE}),
            ),
            TimeoutStrategy(timeout=timedelta(seconds=30)),
        ),
    )

    return {"occ": occ, "transient": transient}

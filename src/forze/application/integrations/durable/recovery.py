"""How long durable recovery holds back a claimed run it could not open, and when it
quarantines the run instead."""

from datetime import timedelta
from typing import Final

from forze.base.exceptions import CoreException, ExceptionKind, exception_egress_policy

# ----------------------- #

UNOPENABLE_HOLD_CAP: Final[timedelta] = timedelta(days=1)
"""The longest a run that could not be opened is kept out of the recovery scan, unless its
lease is longer still."""


def is_passing_fault(error: BaseException) -> bool:
    """Whether *error* passes on its own: a throttled, briefly unreachable or timed-out KMS.

    By the kinds the KMS adapters raise: a key that is gone, disabled or pending deletion is
    a ``configuration`` error, and a fault that clears by itself is retryable.
    """

    if isinstance(error, CoreException):
        return error.kind is ExceptionKind.TIMEOUT or exception_egress_policy(error.kind).retryable

    return isinstance(error, (TimeoutError, ConnectionError))


def _doubled(lease_for: timedelta, attempts: int) -> timedelta:
    """Two leases on a run's first claim, doubling with each claim after it."""

    return lease_for * 2 ** min(max(attempts, 1), 20)


def unopenable_hold(
    error: BaseException,
    *,
    lease_for: timedelta,
    attempts: int,
) -> timedelta | None:
    """How long to keep a claimed run that could not be opened out of the recovery scan.

    The scan claims oldest first, so a run left under an ordinary lease is back at the head
    of the next scan once that lease lapses. Enough of them, from one offboarded tenant,
    would fill every batch and leave each newer run behind them unclaimed. So a run whose
    key is gone (a configuration error, or any fault that is not passing) is held from two
    leases, doubling with each claim, up to a day or two leases, whichever is longer. Once
    the doubling reaches that day the run is quarantined instead (see
    :func:`quarantines_unopenable`), so the held runs at any time are only those still
    doubling.

    A passing fault (a throttled, briefly unreachable or timed-out KMS) gets no hold: the
    run is retried once its ordinary lease lapses, like a run claimed and not finished.

    :param error: Why the run could not be opened.
    :param lease_for: The lease the scan claimed the run with.
    :param attempts: The run's attempt count after the claim.
    :returns: The hold, or ``None`` to leave the run under its lease.
    """

    if is_passing_fault(error):
        return None

    return max(2 * lease_for, min(UNOPENABLE_HOLD_CAP, _doubled(lease_for, attempts)))


def quarantines_unopenable(
    error: BaseException,
    *,
    lease_for: timedelta,
    attempts: int,
) -> bool:
    """Whether recovery quarantines a claimed run that could not be opened, rather than hold it.

    True once the hold's doubling has reached :data:`UNOPENABLE_HOLD_CAP`: at a 5-minute
    lease, the ninth claim, after about 42 hours of holds. Never on a run's first claim,
    though: a lease of half a day or more doubles to the cap at once, and is still held once.
    The count is the run's ``attempts``, as for the hold, so earlier crash reclaims count
    too.

    A passing fault never quarantines: it clears by itself, and the run is retried under its
    lease.

    :param error: Why the run could not be opened.
    :param lease_for: The lease the scan claimed the run with.
    :param attempts: The run's attempt count after the claim.
    :returns: Whether to quarantine the run.
    """

    if is_passing_fault(error) or attempts < 2:
        return False

    return _doubled(lease_for, attempts) >= UNOPENABLE_HOLD_CAP

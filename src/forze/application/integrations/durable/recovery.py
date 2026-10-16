"""How long durable recovery holds back a claimed run it could not open."""

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
    leases, doubling with each claim, up to a day or two leases, whichever is longer.

    A passing fault (a throttled, briefly unreachable or timed-out KMS) gets no hold: the
    run is retried once its ordinary lease lapses, like a run claimed and not finished.

    :param error: Why the run could not be opened.
    :param lease_for: The lease the scan claimed the run with.
    :param attempts: The run's attempt count after the claim.
    :returns: The hold, or ``None`` to leave the run under its lease.
    """

    if is_passing_fault(error):
        return None

    # ponytail: past roughly limit x cap / interval such runs (about 29k at the recovery
    # step's defaults) they crowd the scan again; a status they leave the scan by lifts it.
    doubled = lease_for * 2 ** min(max(attempts, 1), 20)

    return max(2 * lease_for, min(UNOPENABLE_HOLD_CAP, doubled))

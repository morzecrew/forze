"""How long durable recovery holds back a claimed run it could not open."""

from datetime import timedelta
from typing import Final

# ----------------------- #

UNOPENABLE_HOLD_CAP: Final[timedelta] = timedelta(days=1)
"""The longest a run that could not be opened is kept out of the recovery scan."""


def unopenable_hold(lease_for: timedelta, attempts: int) -> timedelta:
    """How long to keep a claimed run that could not be opened out of the recovery scan.

    The scan claims oldest first, so a run left under an ordinary lease is back at the head
    of the next scan once that lease lapses. Enough of them, from one offboarded tenant,
    would fill every batch and leave each newer run behind them unclaimed. The hold doubles
    with each claim, so a run that opens after a passing outage is retried soon, and a run
    that never will comes back about once a day.

    :param lease_for: The lease the scan claimed the run with.
    :param attempts: The run's attempt count after the claim.
    """

    # ponytail: past roughly limit x cap / interval such runs (about 29k at the recovery
    # step's defaults) they crowd the scan again; a status they leave the scan by lifts it.
    return min(UNOPENABLE_HOLD_CAP, lease_for * 2 ** min(max(attempts, 0), 20))

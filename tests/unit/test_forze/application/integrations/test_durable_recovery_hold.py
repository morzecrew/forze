"""How long durable recovery holds back a claimed run it could not open."""

from datetime import timedelta

from forze.application.integrations.durable import UNOPENABLE_HOLD_CAP, unopenable_hold
from forze.base.exceptions import exc

# ----------------------- #

LEASE = timedelta(minutes=5)
KEY_GONE = exc.configuration("Key is pending deletion")


def test_a_lost_key_holds_the_run_from_two_leases_doubling() -> None:
    assert [unopenable_hold(KEY_GONE, lease_for=LEASE, attempts=n) for n in (1, 2, 3)] == [
        timedelta(minutes=10),
        timedelta(minutes=20),
        timedelta(minutes=40),
    ]


def test_the_hold_stops_at_its_cap_however_many_attempts_came_before() -> None:
    assert unopenable_hold(KEY_GONE, lease_for=LEASE, attempts=9) == UNOPENABLE_HOLD_CAP
    assert unopenable_hold(KEY_GONE, lease_for=LEASE, attempts=10**9) == UNOPENABLE_HOLD_CAP


def test_the_hold_outlasts_a_lease_longer_than_its_cap() -> None:
    lease = timedelta(days=2)

    assert unopenable_hold(KEY_GONE, lease_for=lease, attempts=10**9) == 2 * lease


def test_a_passing_fault_adds_no_hold_to_the_lease() -> None:
    for error in (
        exc.infrastructure("KMS internal error"),
        exc.throttled("KMS request throttled"),
        TimeoutError(),
    ):
        assert unopenable_hold(error, lease_for=LEASE, attempts=1) is None

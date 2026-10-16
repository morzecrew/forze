"""How long durable recovery holds back a claimed run it could not open, and when it stops
holding it and quarantines it instead."""

from datetime import timedelta

from forze.application.integrations.durable import (
    UNOPENABLE_HOLD_CAP,
    quarantines_unopenable,
    unopenable_hold,
)
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


def test_a_lost_key_is_quarantined_on_the_claim_whose_hold_reaches_the_cap() -> None:
    # The eighth claim is held 21h20m, short of the day; the ninth would be held the cap.
    assert unopenable_hold(KEY_GONE, lease_for=LEASE, attempts=8) < UNOPENABLE_HOLD_CAP
    assert unopenable_hold(KEY_GONE, lease_for=LEASE, attempts=9) == UNOPENABLE_HOLD_CAP

    assert [
        quarantines_unopenable(KEY_GONE, lease_for=LEASE, attempts=n) for n in (1, 8, 9, 10**9)
    ] == [False, False, True, True]


def test_a_lease_that_doubles_past_the_cap_at_once_is_still_held_on_its_first_claim() -> None:
    for lease in (timedelta(hours=12), timedelta(days=2)):
        assert not quarantines_unopenable(KEY_GONE, lease_for=lease, attempts=1)
        assert quarantines_unopenable(KEY_GONE, lease_for=lease, attempts=2)


def test_a_passing_fault_is_never_quarantined() -> None:
    for error in (
        exc.infrastructure("KMS internal error"),
        exc.throttled("KMS request throttled"),
        TimeoutError(),
    ):
        assert not quarantines_unopenable(error, lease_for=LEASE, attempts=10**9)


def test_a_hold_that_lands_exactly_on_the_cap_has_reached_it() -> None:
    lease = timedelta(minutes=45)  # 45 minutes x 2**5 is one day

    assert unopenable_hold(KEY_GONE, lease_for=lease, attempts=5) == UNOPENABLE_HOLD_CAP
    assert not quarantines_unopenable(KEY_GONE, lease_for=lease, attempts=4)
    assert quarantines_unopenable(KEY_GONE, lease_for=lease, attempts=5)

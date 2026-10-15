"""How long durable recovery holds back a claimed run it could not open."""

from datetime import timedelta

from forze.application.integrations.durable import UNOPENABLE_HOLD_CAP, unopenable_hold

# ----------------------- #

LEASE = timedelta(minutes=5)


def test_the_hold_starts_past_one_lease_and_doubles() -> None:
    assert [unopenable_hold(LEASE, attempts) for attempts in (1, 2, 3)] == [
        timedelta(minutes=10),
        timedelta(minutes=20),
        timedelta(minutes=40),
    ]


def test_the_hold_stops_at_its_cap_however_many_attempts_came_before() -> None:
    assert unopenable_hold(LEASE, 9) == UNOPENABLE_HOLD_CAP
    assert unopenable_hold(LEASE, 10**9) == UNOPENABLE_HOLD_CAP
    assert unopenable_hold(timedelta(days=365), 10**9) == UNOPENABLE_HOLD_CAP

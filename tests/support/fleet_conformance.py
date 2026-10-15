"""Shared ``FleetPort`` conformance battery: the membership promises, on every backend.

The policies over a view are pure and tested once; what can differ between backends is the
view itself. Redis builds it in a Lua script on the server's clock, and the mock builds it
in memory on an injected one, so reading either proves nothing about the other.

What each check pins:

1. One beat writes this member's entry and returns the whole view, this member included.
2. An expired member is gone from the next view, and the next beat removes it.
3. ``since`` survives beats in the same state and resets on a change of state.
4. ``leave`` removes the member at once.
5. A short final beat (a linger) expires on its own and shortens no other member's life.
6. An invalid beat is refused before anything is written.
7. Report numbers read back exactly, with their types.
8. Two fleets never see each other's members.

Used by:

- ``tests/unit/test_forze_mock/test_mock_fleet_conformance.py`` (the oracle)
- ``tests/integration/test_forze_redis/test_redis_fleet_conformance.py``
"""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.fleet import FleetPort, FleetSpec
from forze.base.exceptions import CoreException

# ----------------------- #

_SHORT = timedelta(milliseconds=150)
"""A lifetime the checks outwait; long enough that a slow beat cannot outlive its own."""

_PAST_SHORT = timedelta(milliseconds=400)

_PAST_KEY_SLACK = timedelta(milliseconds=1500)
"""Past ``_SHORT`` plus the one second Redis keeps a fleet's keys beyond their last expiry, so
a backend that expired the keys from a short beat's own ttl has lost every member by then."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class FleetHarness:
    """One backend's seam for the membership battery."""

    backend: str
    """Label used in assertion messages."""

    fleet: Callable[[FleetSpec], FleetPort]
    """A port over the fleet *spec* names; each check names a fresh one."""

    wait: Callable[[timedelta], Awaitable[None]]
    """Let at least this much of the backend's time pass: a sleep on a real server, a clock
    step on the mock."""


Check = Callable[[FleetHarness], Any]
"""One battery check."""


def _spec(**kwargs: Any) -> FleetSpec:
    return FleetSpec(name=f"battery-{uuid4().hex[:12]}", **kwargs)


# ....................... #


async def check_a_beat_returns_the_view_with_this_member(h: FleetHarness) -> None:
    fleet = h.fleet(_spec())

    first = await fleet.beat("b", "starting", {"load": 1})
    view = await fleet.beat("a", "serving", {"load": 2})

    assert [m.id for m in first.members] == ["b"], h.backend
    assert [(m.id, m.state, dict(m.report)) for m in view.members] == [
        ("a", "serving", {"load": 2}),
        ("b", "starting", {"load": 1}),
    ], h.backend
    assert all(m.since <= view.at for m in view.members), h.backend
    assert (await fleet.view()).members == view.members, h.backend


async def check_an_expired_member_is_gone_from_the_next_view(h: FleetHarness) -> None:
    fleet = h.fleet(_spec())

    await fleet.beat("short", "serving", {}, ttl=_SHORT)
    await fleet.beat("long", "serving", {})
    await h.wait(_PAST_SHORT)

    assert [m.id for m in (await fleet.view()).members] == ["long"], h.backend
    assert [m.id for m in (await fleet.beat("new", "starting", {})).members] == [
        "long",
        "new",
    ], h.backend

    # The expired member coming back is a new member, not the old one resumed.
    back = await fleet.beat("short", "serving", {})
    member = next(m for m in back.members if m.id == "short")

    assert member.since == back.at, h.backend


async def check_since_survives_the_same_state_and_resets_on_a_change(
    h: FleetHarness,
) -> None:
    fleet = h.fleet(_spec())

    first = await fleet.beat("a", "serving", {"n": 1})
    since = first.members[0].since

    assert since == first.at, h.backend

    await h.wait(timedelta(milliseconds=20))
    same = await fleet.beat("a", "serving", {"n": 2})

    assert same.members[0].since == since, h.backend
    assert same.members[0].report == {"n": 2}, h.backend

    await h.wait(timedelta(milliseconds=20))
    changed = await fleet.beat("a", "draining", {"n": 2})

    assert changed.members[0].since == changed.at, h.backend
    assert changed.members[0].since > since, h.backend


async def check_leave_removes_the_member_at_once(h: FleetHarness) -> None:
    fleet = h.fleet(_spec())

    await fleet.beat("a", "serving", {})
    await fleet.beat("b", "serving", {})
    await fleet.leave("a")
    await fleet.leave("never-joined")

    assert [m.id for m in (await fleet.view()).members] == ["b"], h.backend


async def check_a_linger_expires_without_shortening_other_lives(h: FleetHarness) -> None:
    fleet = h.fleet(_spec(ttl=timedelta(seconds=30)))

    await fleet.beat("stays", "serving", {})
    await fleet.beat("lingers", "draining", {}, ttl=_SHORT)
    await h.wait(_PAST_KEY_SLACK)

    view = await fleet.view()

    assert [m.id for m in view.members] == ["stays"], h.backend


_REFUSED: tuple[tuple[str, str, Any, timedelta | None], ...] = (
    ("a", "serving", {"pad": "x" * 64}, None),
    ("a", "serving", {"flag": True}, None),
    ("a", "serving", {"nan": math.nan}, None),
    ("a", "serving", {"inf": math.inf}, None),
    ("a", "serving", {"nested": {"x": 1}}, None),
    ("a\tb", "serving", {}, None),
    ("a\nb", "serving", {}, None),
    ("{a}", "serving", {}, None),
    ("a", "paused", {}, None),
    ("a", "serving", {}, timedelta(0)),
)


async def check_an_invalid_beat_is_refused_before_anything_is_written(
    h: FleetHarness,
) -> None:
    fleet = h.fleet(_spec(report_max_bytes=64))

    for member_id, state, report, ttl in _REFUSED:
        with pytest.raises(CoreException):
            await fleet.beat(member_id, state, report, ttl=ttl)  # type: ignore[arg-type]

    with pytest.raises(CoreException):
        await fleet.leave("a\tb")

    assert (await fleet.view()).members == (), h.backend


async def check_report_numbers_read_back_exactly(h: FleetHarness) -> None:
    fleet = h.fleet(_spec())
    report = {
        "int": 7,
        "big": 2**70,
        "float": 0.1,
        "whole": 2.0,
        "tiny": 5e-324,
        "text": "a\tb\nc|d{e}ё",
    }

    beat = await fleet.beat("a", "serving", report)
    read = await fleet.view()

    for view in (beat, read):
        got = dict(view.members[0].report)

        assert got == report, h.backend
        assert {k: type(v) for k, v in got.items()} == {k: type(v) for k, v in report.items()}, (
            h.backend
        )


async def check_two_fleets_never_see_each_other(h: FleetHarness) -> None:
    one, two = h.fleet(_spec()), h.fleet(_spec())

    await one.beat("a", "serving", {})

    assert (await two.beat("b", "serving", {})).members[0].id == "b", h.backend
    assert [m.id for m in (await one.view()).members] == ["a"], h.backend


# ....................... #


FLEET_BATTERY: tuple[Check, ...] = (
    check_a_beat_returns_the_view_with_this_member,
    check_an_expired_member_is_gone_from_the_next_view,
    check_since_survives_the_same_state_and_resets_on_a_change,
    check_leave_removes_the_member_at_once,
    check_a_linger_expires_without_shortening_other_lives,
    check_an_invalid_beat_is_refused_before_anything_is_written,
    check_report_numbers_read_back_exactly,
    check_two_fleets_never_see_each_other,
)

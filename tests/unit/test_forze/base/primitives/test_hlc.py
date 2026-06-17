"""Unit tests for the Hybrid Logical Clock primitive."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st

from forze.base.exceptions import CoreException
from forze.base.primitives import HlcTimestamp, HybridLogicalClock
from forze.base.primitives.hlc import _MAX_LOGICAL
from forze.base.primitives.time_source import FrozenTimeSource, bind_time_source

# ----------------------- #


def _at(millis: int) -> FrozenTimeSource:
    """A frozen source whose ``now`` is ``millis`` since the epoch."""

    return FrozenTimeSource(instant=datetime.fromtimestamp(millis / 1000, tz=UTC))


# ----------------------- #
# HlcTimestamp


def test_pack_unpack_roundtrip() -> None:
    ts = HlcTimestamp(physical_ms=1_700_000_000_000, logical=42)

    assert HlcTimestamp.unpack(ts.pack()) == ts


def test_encode_parse_roundtrip() -> None:
    ts = HlcTimestamp(physical_ms=1_700_000_000_000, logical=42)

    assert HlcTimestamp.parse(ts.encode()) == ts


def test_pack_preserves_sort_order() -> None:
    ordered = [
        HlcTimestamp(10, 0),
        HlcTimestamp(10, 1),
        HlcTimestamp(10, _MAX_LOGICAL),
        HlcTimestamp(11, 0),
        HlcTimestamp(12, 5),
    ]

    assert [t.pack() for t in ordered] == sorted(t.pack() for t in ordered)


def test_encode_is_lexicographically_sortable() -> None:
    ordered = [
        HlcTimestamp(9, 999),
        HlcTimestamp(10, 0),
        HlcTimestamp(10, 1),
        HlcTimestamp(100, 0),
    ]
    encoded = [t.encode() for t in ordered]

    assert encoded == sorted(encoded)


def test_timestamp_validation() -> None:
    with pytest.raises(CoreException):
        HlcTimestamp(physical_ms=-1, logical=0)

    with pytest.raises(CoreException):
        HlcTimestamp(physical_ms=0, logical=_MAX_LOGICAL + 1)

    with pytest.raises(CoreException):
        HlcTimestamp(physical_ms=0, logical=-1)


def test_parse_rejects_malformed() -> None:
    for bad in ("", "123", "abc.0", "10.xyz"):
        with pytest.raises(CoreException):
            HlcTimestamp.parse(bad)


# ----------------------- #
# now()


def test_now_advances_with_wall_clock() -> None:
    with bind_time_source(_at(1000)):
        clock = HybridLogicalClock()
        first = clock.now()

    assert first == HlcTimestamp(1000, 0)

    with bind_time_source(_at(2000)):
        second = clock.now()

    assert second == HlcTimestamp(2000, 0)


def test_now_increments_logical_within_same_millisecond() -> None:
    with bind_time_source(_at(1000)):
        clock = HybridLogicalClock()

        assert clock.now() == HlcTimestamp(1000, 0)
        assert clock.now() == HlcTimestamp(1000, 1)
        assert clock.now() == HlcTimestamp(1000, 2)


def test_now_never_goes_backwards_when_wall_clock_regresses() -> None:
    clock = HybridLogicalClock()

    with bind_time_source(_at(5000)):
        ahead = clock.now()

    # Wall clock jumps backwards; the HLC must still move forward.
    with bind_time_source(_at(3000)):
        after = clock.now()

    assert after > ahead
    assert after == HlcTimestamp(5000, 1)


def test_now_is_strictly_monotonic() -> None:
    with bind_time_source(_at(1000)):
        clock = HybridLogicalClock()
        stamps = [clock.now() for _ in range(1000)]

    assert stamps == sorted(stamps)
    assert len(set(stamps)) == len(stamps)


def test_logical_overflow_carries_into_physical() -> None:
    clock = HybridLogicalClock()
    # Seed _last at the logical cap so the next same-ms tick must overflow.
    clock._last = HlcTimestamp(1000, _MAX_LOGICAL)

    with bind_time_source(_at(1000)):
        issued = clock.now()

    assert issued == HlcTimestamp(1001, 0)
    assert issued > HlcTimestamp(1000, _MAX_LOGICAL)


# ----------------------- #
# update()


def test_update_exceeds_remote_when_remote_is_ahead() -> None:
    clock = HybridLogicalClock()
    remote = HlcTimestamp(9000, 7)

    with bind_time_source(_at(1000)):
        issued = clock.update(remote)

    # Remote physical is ahead of both local last and wall, so adopt it +1 logical.
    assert issued == HlcTimestamp(9000, 8)
    assert issued > remote


def test_update_uses_wall_clock_when_it_leads() -> None:
    clock = HybridLogicalClock()
    remote = HlcTimestamp(1000, 3)

    with bind_time_source(_at(5000)):
        issued = clock.update(remote)

    assert issued == HlcTimestamp(5000, 0)
    assert issued > remote


def test_update_breaks_tie_on_equal_physical() -> None:
    clock = HybridLogicalClock()
    clock._last = HlcTimestamp(2000, 4)
    remote = HlcTimestamp(2000, 9)

    with bind_time_source(_at(2000)):
        issued = clock.update(remote)

    # All three share physical 2000 → max(last, remote) logical + 1.
    assert issued == HlcTimestamp(2000, 10)
    assert issued > remote
    assert issued > HlcTimestamp(2000, 4)


def test_update_preserves_causality_chain() -> None:
    """A relayed timestamp always sorts after its cause across two clocks."""

    a = HybridLogicalClock()
    b = HybridLogicalClock()

    # a and b run on skewed clocks: b's wall clock lags a's by 4 seconds.
    with bind_time_source(_at(10_000)):
        e1 = a.now()

    with bind_time_source(_at(6000)):
        # b receives e1 (from the "future" relative to its lagging clock).
        e2 = b.update(e1)
        # b then produces a causal successor.
        e3 = b.now()

    assert e1 < e2 < e3


def test_update_skew_guard_rejects_far_future_remote() -> None:
    clock = HybridLogicalClock(max_drift=timedelta(seconds=1))
    remote = HlcTimestamp(10_000, 0)  # 9s ahead of a 1000ms wall clock

    with bind_time_source(_at(1000)):
        with pytest.raises(CoreException):
            clock.update(remote)


def test_update_skew_guard_allows_within_drift() -> None:
    clock = HybridLogicalClock(max_drift=timedelta(seconds=5))
    remote = HlcTimestamp(4000, 0)  # 3s ahead of a 1000ms wall clock — within 5s

    with bind_time_source(_at(1000)):
        issued = clock.update(remote)

    assert issued == HlcTimestamp(4000, 1)


# ----------------------- #
# Property-based


@given(
    ticks=st.lists(
        st.tuples(
            st.sampled_from(["now", "update"]),
            st.integers(min_value=0, max_value=10_000),  # wall ms
            st.integers(min_value=0, max_value=10_000),  # remote physical ms
            st.integers(min_value=0, max_value=_MAX_LOGICAL),  # remote logical
        ),
        min_size=1,
        max_size=200,
    )
)
def test_clock_is_monotonic_under_arbitrary_interleavings(
    ticks: list[tuple[str, int, int, int]],
) -> None:
    """Across any mix of now()/update() and any clock movement, output is monotonic."""

    clock = HybridLogicalClock()
    issued: list[HlcTimestamp] = []

    for op, wall_ms, remote_phys, remote_log in ticks:
        with bind_time_source(_at(wall_ms)):
            if op == "now":
                issued.append(clock.now())

            else:
                issued.append(clock.update(HlcTimestamp(remote_phys, remote_log)))

    # Every issued timestamp strictly exceeds its predecessor.
    for earlier, later in zip(issued, issued[1:]):
        assert later > earlier


@given(
    physical_ms=st.integers(min_value=0, max_value=(1 << 48) - 1),
    logical=st.integers(min_value=0, max_value=_MAX_LOGICAL),
)
def test_pack_and_encode_roundtrips_property(physical_ms: int, logical: int) -> None:
    ts = HlcTimestamp(physical_ms=physical_ms, logical=logical)

    assert HlcTimestamp.unpack(ts.pack()) == ts
    assert HlcTimestamp.parse(ts.encode()) == ts

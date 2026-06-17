"""Task-scoped request criticality: default, binding, ordering."""

from __future__ import annotations

import pytest

from forze.application.execution.context import (
    Criticality,
    bind_criticality,
    current_criticality,
)
from forze.application.execution.context.criticality import (
    reset_criticality,
    set_criticality,
)

# ----------------------- #


def test_default_is_normal() -> None:
    assert current_criticality() is Criticality.NORMAL


def test_tier_ordering() -> None:
    assert (
        Criticality.BEST_EFFORT
        < Criticality.DEGRADED
        < Criticality.NORMAL
        < Criticality.CRITICAL
    )


def test_bind_overrides_within_block_and_restores() -> None:
    assert current_criticality() is Criticality.NORMAL

    with bind_criticality(Criticality.CRITICAL):
        assert current_criticality() is Criticality.CRITICAL

    assert current_criticality() is Criticality.NORMAL


def test_bind_nests() -> None:
    with bind_criticality(Criticality.DEGRADED):
        assert current_criticality() is Criticality.DEGRADED

        with bind_criticality(Criticality.CRITICAL):
            assert current_criticality() is Criticality.CRITICAL

        assert current_criticality() is Criticality.DEGRADED


def test_bind_none_is_passthrough() -> None:
    with bind_criticality(Criticality.CRITICAL):
        with bind_criticality(None):
            assert current_criticality() is Criticality.CRITICAL


def test_set_reset_fast_path() -> None:
    token = set_criticality(Criticality.BEST_EFFORT)

    try:
        assert current_criticality() is Criticality.BEST_EFFORT

    finally:
        reset_criticality(token)

    assert current_criticality() is Criticality.NORMAL


@pytest.mark.parametrize(
    "tier",
    [
        Criticality.BEST_EFFORT,
        Criticality.DEGRADED,
        Criticality.NORMAL,
        Criticality.CRITICAL,
    ],
)
def test_round_trips_every_tier(tier: Criticality) -> None:
    with bind_criticality(tier):
        assert current_criticality() is tier

"""TierLattice: the shared fail-closed strength-floor primitive."""

from __future__ import annotations

from typing import Literal

import pytest

from forze.application.contracts.tiers import TierLattice
from forze.base.exceptions import CoreException

# ----------------------- #

_Tier = Literal["none", "low", "high"]


def _lattice() -> TierLattice[_Tier]:
    return TierLattice[_Tier](
        field="thing",
        validation_label="thing",
        wired_noun="level",
        ceiling_noun="thing",
        floor_remediation="Wire something stronger or lower the requirement.",
        ranks={"none": 0, "low": 1, "high": 2},
    )


# ....................... #


def test_satisfies_is_at_least_as_strong() -> None:
    lat = _lattice()
    assert lat.satisfies(derived="high", required="low")
    assert lat.satisfies(derived="low", required="low")
    assert not lat.satisfies(derived="low", required="high")


def test_none_required_opts_out() -> None:
    # No declared floor → never raises, whatever is wired.
    _lattice().validate(
        integration="svc", derived="none", required=None, code="x.floor"
    )


def test_floor_failure_carries_required_and_derived_details() -> None:
    with pytest.raises(CoreException) as ei:
        _lattice().validate(
            integration="svc", derived="low", required="high", code="x.floor"
        )

    assert ei.value.details["required_thing"] == "high"
    assert ei.value.details["derived_thing"] == "low"
    assert "validation failed" in str(ei.value)


def test_ceiling_failure_reports_capability_mismatch() -> None:
    # required exceeds what the backend can ever provide → ceiling mismatch, not a wiring gap.
    with pytest.raises(CoreException) as ei:
        _lattice().validate(
            integration="svc",
            derived="none",
            required="high",
            code="x.floor",
            max_supported="low",
        )

    assert ei.value.details["required_thing"] == "high"
    assert ei.value.details["max_supported_thing"] == "low"
    assert "supports at most" in str(ei.value)


def test_within_ceiling_and_above_floor_passes() -> None:
    _lattice().validate(
        integration="svc",
        derived="high",
        required="low",
        code="x.floor",
        max_supported="high",
    )

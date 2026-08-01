"""The divergence catalog's own guards.

Every row in the catalog is a claim two engines behaved differently and a pointer to the
test that proves it still true. Two things make a row worthless: fewer than two engines (a
"divergence" from nothing) and a missing probe (a claim nothing checks). Both are refused at
construction, which is only useful if the refusal works — an unexercised validator is a
comment with a syntax error waiting to happen.
"""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_dst.conformance.catalog import (
    PLANE_DIVERGENCES,
    DivergenceResolution,
    EngineBehaviour,
    PlaneDivergence,
)

# ----------------------- #


def _row(**overrides: object) -> PlaneDivergence:
    fields: dict[str, object] = {
        "plane": "counter",
        "name": "example",
        "observed": (
            EngineBehaviour(engine="mock", behaviour="did one thing"),
            EngineBehaviour(engine="postgres", behaviour="did another"),
        ),
        "resolution": DivergenceResolution.UNIFIED,
        "reason": "because",
        "probe": "tests/x.py::test_y",
    }
    fields.update(overrides)

    return PlaneDivergence(**fields)  # type: ignore[arg-type]


def test_a_well_formed_row_is_accepted() -> None:
    """The positive control — otherwise the refusals below could be refusing everything."""

    assert _row().probe == "tests/x.py::test_y"


@pytest.mark.parametrize(
    ("observed", "why"),
    [
        ((), "no engines at all"),
        ((EngineBehaviour(engine="mock", behaviour="alone"),), "a single engine"),
    ],
)
def test_a_row_needs_two_engines_to_diverge_between(observed: tuple, why: str) -> None:
    with pytest.raises(CoreException) as refused:
        _row(observed=observed)

    assert refused.value.kind is ExceptionKind.CONFIGURATION, why
    assert "two engines" in str(refused.value)


def test_a_row_without_a_probe_is_refused() -> None:
    """A catalogued divergence with no probe is exactly the prose this catalog replaces."""

    with pytest.raises(CoreException) as refused:
        _row(probe="")

    assert refused.value.kind is ExceptionKind.CONFIGURATION
    assert "probe" in str(refused.value)


def test_every_shipped_row_satisfies_its_own_guards() -> None:
    """The guards run at import, so this is really a statement about coverage of the table.

    It also pins the shape a reader relies on: each row is filed under the plane it names,
    which the manifest checker assumes when it resolves probes per plane.
    """

    for plane, rows in PLANE_DIVERGENCES.items():
        assert rows, f"{plane}: an empty tuple records nothing"

        for row in rows:
            assert row.plane == plane
            assert len(row.observed) >= 2
            assert row.probe

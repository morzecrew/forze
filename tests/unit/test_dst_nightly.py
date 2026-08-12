"""Tests for the nightly DST matrix (.github/scripts/dst_nightly.py) and its fault profiles.

The nightly's whole value is that it fails when the night was bad, and a gate nobody has watched
fail is a gate nobody knows works. So these pin each refusal separately: a cell that never
reported, a band that ran nothing, a seed that violated, a declared target the band never drove,
and a result for a cell nobody declared.

The one worth naming is the empty band. Every other rule reads "no bad thing happened", which a
run of zero seeds satisfies perfectly — the run count is the only thing separating "searched and
found nothing" from "did not search", and without it a broken shard reports a clean night.

The profile half is pinned for the same reason from the other direction: a profile declaring no
reachability targets would make its whole band vacuously green, so the declaration refuses it.
"""

from __future__ import annotations

import importlib.util
import json
import pickle
import sys
from pathlib import Path
from types import ModuleType

import pytest

from forze.base.exceptions import CoreException
from tests.support.dst_flagship import (
    DLOCK_PROFILES,
    DLOCK_TARGETS,
    HLC_TARGETS,
    FaultProfile,
    dlock_config,
    dlock_target,
)

# ----------------------- #

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / ".github" / "scripts" / "dst_nightly.py"
_WORKFLOW = _REPO / ".github" / "workflows" / "nightly.yml"


def _load_runner() -> ModuleType:
    spec = importlib.util.spec_from_file_location("dst_nightly", _SCRIPT)

    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load the nightly runner at {_SCRIPT}")

    module = importlib.util.module_from_spec(spec)
    # The dataclasses resolve their defining module through sys.modules, so register before exec.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    return module


runner = _load_runner()


# ....................... #


def _result(cell: str, **overrides: object) -> dict[str, object]:
    """A clean cell result, before a test spoils one field of it."""

    return {
        "cell": cell,
        "seeds": 512,
        "violations": [],
        "reached": dict.fromkeys(DLOCK_TARGETS, 512),
        "targets": sorted(DLOCK_TARGETS),
        "behaviors": 5,
        "wall_seconds": 1.0,
    } | overrides


@pytest.fixture
def results(tmp_path: Path):  # type: ignore[no-untyped-def]
    """Write cell results to a directory and read them back the way the verdict does."""

    def write(*payloads: dict[str, object]) -> dict[str, object]:
        for payload in payloads:
            (tmp_path / f"{payload['cell']}.json").write_text(json.dumps(payload), encoding="utf-8")

        return runner.load_results(tmp_path)

    return write


# ----------------------- #


class TestFaultProfiles:
    """The environment declarations the matrix is derived from."""

    def test_a_profile_must_declare_what_it_drives(self) -> None:
        """An empty target set satisfies every reachability check while proving nothing.

        A profile like this would report a green band forever, including on the night the
        scenario stopped racing altogether.
        """

        with pytest.raises(CoreException, match="no reachability targets"):
            FaultProfile(name="vacuous", targets=frozenset())

    def test_a_partition_needs_both_halves(self) -> None:
        """A window with nobody isolated cuts nothing; isolated nodes with no window never cut."""

        with pytest.raises(CoreException, match="window and isolated nodes together"):
            FaultProfile(name="half", targets=DLOCK_TARGETS, window=(0.5, 1.5))

        with pytest.raises(CoreException, match="window and isolated nodes together"):
            FaultProfile(name="other-half", targets=DLOCK_TARGETS, isolated=frozenset({1}))

    def test_an_empty_window_is_refused(self) -> None:
        """``start >= end`` is a partition that never opens — silently no faults at all."""

        with pytest.raises(CoreException, match="empty partition window"):
            FaultProfile(
                name="closed",
                targets=DLOCK_TARGETS,
                isolated=frozenset({1}),
                window=(1.5, 1.5),
            )

    @pytest.mark.parametrize("error", [-0.1, 1.5])
    def test_an_error_rate_outside_the_unit_interval_is_refused(self, error: float) -> None:
        with pytest.raises(CoreException, match="error rate outside"):
            FaultProfile(name="impossible", targets=DLOCK_TARGETS, error=error)

    # ....................... #

    def test_the_declared_profiles_reach_what_they_claim(self) -> None:
        """Each profile's targets must be drivable *by that profile*, not by some other one.

        This is the check that keeps the target sets honest as the environments drift: shrink a
        partition window far enough, or drop an error rate to zero, and the profile stops
        provoking the state it says it provokes — which the nightly would then report as a
        failure at 4am instead of here.
        """

        from forze_dst.artifacts.sweep import sweep

        for name, profile in DLOCK_PROFILES.items():
            result = sweep(dlock_target(name), tuple(range(24)))
            report = result.reachability(profile.targets)

            assert result.violations == (), f"{name} violated at seeds {result.violations}"
            assert report.satisfied, f"{name}: {report.format()}"

    def test_contention_cannot_reach_the_partition_target(self) -> None:
        """The control for the test above: targets are per-profile because reachability is.

        ``contention`` cuts no link and injects no error, so no call can fail and the retry
        branch is dead code under it. If this ever starts reaching ``write-retried`` something
        is injecting faults the profile did not ask for.
        """

        from forze_dst.artifacts.sweep import sweep

        result = sweep(dlock_target("contention"), tuple(range(24)))

        assert "write-retried" not in result.reached
        assert "write-retried" not in DLOCK_PROFILES["contention"].targets

    def test_a_bound_target_survives_pickling(self) -> None:
        """``parallel_sweep`` ships the target to a worker process; a closure would not arrive."""

        assert pickle.loads(pickle.dumps(dlock_target("storm"))) is not None

    def test_an_unknown_profile_is_refused_at_the_seam(self) -> None:
        with pytest.raises(CoreException, match="Unknown dlock fault profile"):
            dlock_target("no-such-profile")

    def test_the_config_carries_the_profile_through(self) -> None:
        """The knobs must reach ``SimulationConfig``, not stop at the declaration."""

        storm = dlock_config(range(2), profile="storm")
        quiet = dlock_config(range(2), profile="contention")

        assert storm.cluster is not None
        assert quiet.cluster is not None
        assert storm.faults is not None
        assert quiet.faults is not None

        partitions = storm.cluster.partitions

        assert partitions is not None
        assert partitions.windows[0].isolated == frozenset({0, 2})
        assert storm.faults.rules[0].error == pytest.approx(0.7)

        # No partition and no rules at all, rather than a rule with a zero rate — a zero-rate
        # rule reads in a trace as "faults were configured here" when none can fire.
        assert quiet.cluster.partitions is None
        assert quiet.faults.rules == ()


# ....................... #


class TestMatrixDeclaration:
    """Where the cell list comes from, and what keeps it from drifting."""

    def test_every_profile_becomes_a_cell(self) -> None:
        """Derived, not hand-listed: adding a profile must add a night's worth of work.

        A hand-written list is how a new environment ships covered-in-name-only — the same
        failure the conformance ratchet was built to stop one level up.
        """

        cells = {cell.name for cell in runner.declared_cells()}

        assert cells == {f"dlock-{name}" for name in DLOCK_PROFILES} | {runner.HLC_CELL}

    def test_each_cell_carries_its_own_targets(self) -> None:
        by_name = {cell.name: cell for cell in runner.declared_cells()}

        assert set(by_name["dlock-contention"].targets) == {"lock-contended"}
        assert set(by_name["dlock-storm"].targets) == DLOCK_TARGETS
        assert set(by_name[runner.HLC_CELL].targets) == HLC_TARGETS

    def test_the_hlc_cell_has_no_fault_profile(self) -> None:
        """It is register-based — no ports, so no surface a partition or fault could act on."""

        by_name = {cell.name: cell for cell in runner.declared_cells()}

        assert by_name[runner.HLC_CELL].profile is None

    def test_an_unknown_cell_is_refused(self) -> None:
        with pytest.raises(SystemExit, match="unknown cell"):
            runner.run_cell("dlock-nonexistent", seeds=1)

    def test_an_empty_band_is_refused_at_the_source(self) -> None:
        """The verdict catches a zero-seed result; this stops one being produced at all."""

        with pytest.raises(SystemExit, match="must not be empty"):
            runner.run_cell("hlc", seeds=0)

    def test_the_workflow_takes_its_matrix_from_the_declaration(self) -> None:
        """A literal matrix in the YAML would be a second list, free to drift from the first."""

        workflow = _WORKFLOW.read_text(encoding="utf-8")

        assert "fromJSON(needs.matrix.outputs.cells)" in workflow
        assert "--matrix" in workflow

        # The verdict must require exactly the cells the matrix declared. Hardcoding the
        # expectation here would let a dropped profile pass unnoticed at both ends.
        assert "needs.matrix.outputs.names" in workflow


# ....................... #


class TestVerdict:
    """The gate, one refusal at a time."""

    def test_a_clean_night_passes(self) -> None:
        loaded = {"dlock-storm": runner.CellResult(**_result("dlock-storm"))}

        assert runner.check_verdict(("dlock-storm",), loaded) == []

    def test_a_cell_that_never_reported_fails(self, results) -> None:  # type: ignore[no-untyped-def]
        """A shard that died leaves silence, and silence must not read as success."""

        loaded = results(_result("dlock-storm"))
        violations = runner.check_verdict(("dlock-storm", "hlc"), loaded)

        assert len(violations) == 1
        assert "hlc: no result" in violations[0]

    def test_an_empty_band_fails(self, results) -> None:  # type: ignore[no-untyped-def]
        """Zero seeds pass every other rule — no violations, and nothing to be unreachable."""

        loaded = results(_result("dlock-storm", seeds=0, reached={}))
        violations = runner.check_verdict(("dlock-storm",), loaded)

        assert len(violations) == 1
        assert "ran 0 seeds" in violations[0]

    def test_a_violating_seed_fails_and_is_named(self, results) -> None:  # type: ignore[no-untyped-def]
        """The seed is the reproduction, so the message has to carry it."""

        loaded = results(_result("dlock-storm", violations=[7, 19]))
        violations = runner.check_verdict(("dlock-storm",), loaded)

        assert len(violations) == 1
        assert "7, 19" in violations[0]
        assert "REGRESSION_SEEDS" in violations[0]

    def test_a_long_violation_list_is_truncated_but_counted(self, results) -> None:  # type: ignore[no-untyped-def]
        """Truncating the list must not truncate the count — the total is the severity."""

        loaded = results(_result("dlock-storm", violations=list(range(50))))
        violations = runner.check_verdict(("dlock-storm",), loaded)

        assert "50 seed(s)" in violations[0]
        assert "+42 more" in violations[0]

    def test_an_undriven_target_fails(self, results) -> None:  # type: ignore[no-untyped-def]
        """A green band that never raced is not evidence; this is the rule that says so."""

        loaded = results(_result("dlock-storm", reached={"lock-contended": 512}))
        violations = runner.check_verdict(("dlock-storm",), loaded)

        assert len(violations) == 1
        assert "never reached write-retried" in violations[0]

    def test_a_target_reached_zero_times_counts_as_undriven(self, results) -> None:  # type: ignore[no-untyped-def]
        """Present-with-a-zero and absent are the same fact and must be treated alike."""

        loaded = results(
            _result("dlock-storm", reached={"lock-contended": 512, "write-retried": 0}),
        )

        assert "never reached write-retried" in runner.check_verdict(("dlock-storm",), loaded)[0]

    def test_a_result_for_an_undeclared_cell_fails(self, results) -> None:  # type: ignore[no-untyped-def]
        """The matrix and the declaration disagreeing is itself the finding."""

        loaded = results(_result("dlock-storm"), _result("dlock-ghost"))
        violations = runner.check_verdict(("dlock-storm",), loaded)

        assert len(violations) == 1
        assert "dlock-ghost" in violations[0]

    def test_every_failing_cell_is_reported_not_just_the_first(self, results) -> None:  # type: ignore[no-untyped-def]
        """A night with two bad cells must name both; stopping at the first hides work."""

        loaded = results(
            _result("dlock-storm", violations=[3]),
            _result("dlock-baseline", reached={"lock-contended": 512}),
        )
        violations = runner.check_verdict(("dlock-storm", "dlock-baseline"), loaded)

        assert len(violations) == 2

    def test_an_empty_expectation_is_a_failure_not_a_pass(self) -> None:
        """Requiring nothing is how a matrix that produced nothing reports a clean night."""

        assert runner.main(["--verdict", str(_REPO), "--expect", ""]) == 1


# ....................... #


class TestRoundTrip:
    """The result written by a cell must be the result the verdict reads."""

    def test_a_real_cell_run_passes_its_own_gate(self, tmp_path: Path) -> None:
        """The two halves are separated by a JSON file and an artifact upload — pin the seam.

        A field renamed on one side and not the other would leave the gate reading defaults:
        zero seeds, no targets, nothing unreached. Green, and blind.
        """

        assert runner.main(["--cell", "hlc", "--seeds", "16", "--out", str(tmp_path / "hlc.json")]) == 0

        loaded = runner.load_results(tmp_path)

        assert loaded["hlc"].seeds == 16
        assert loaded["hlc"].targets == tuple(sorted(HLC_TARGETS))
        assert runner.check_verdict((runner.HLC_CELL,), loaded) == []

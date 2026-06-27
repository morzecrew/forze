"""Parallel timelines (E5.1) — fan a seed sweep across processes, fold the results.

Inter-seed parallelism is safe because each seed is fully deterministic in its own process (the
seam ContextVars bind per process), so a parallel sweep must fold to *exactly* the same coverage
and violations as a sequential one — only wall time differs. These tests pin that equivalence, the
aggregation (violations, behaviour union, throughput), and the picklable ``SimulationSeedRunner``
that resolves a ``module:attr`` target inside each worker.

The ``run`` callable must be importable by qualified name for a process pool to pickle it, so the
toy runner here is a module-level function (a closure would not pickle).
"""

from __future__ import annotations

from forze_dst.artifacts import SeedOutcome, SimulationSeedRunner, parallel_sweep, sweep

# ----------------------- #

_PAYMENTS = "examples.recipes.dst_payments.app:simulation"


def _toy_run(seed: int) -> SeedOutcome:
    """A deterministic, picklable per-seed run — coverage + violations are a function of seed."""

    reached = {"even" if seed % 2 == 0 else "odd"}
    if seed % 3 == 0:
        reached.add("third")  # reached by seeds 0, 3, 6, 9, …

    return SeedOutcome(
        seed=seed,
        violated=(seed % 5 == 0),  # seeds 0, 5, 10, … "violate"
        behaviors=frozenset({("op", f"shape-{seed % 3}", "ok")}),
        reached=frozenset(reached),
        sim_seconds=float(seed % 4),
    )


# ....................... #


class TestAggregation:
    def test_folds_violations_behaviors_and_throughput(self) -> None:
        result = sweep(_toy_run, range(12))

        assert result.runs == 12
        assert result.violations == (0, 5, 10)  # ascending
        assert result.first_violation == 0
        # Three distinct behaviour shapes across the seeds (seed % 3).
        assert len(result.behaviors) == 3
        assert result.wall_seconds >= 0.0
        assert result.runs_per_second > 0.0
        # sim_seconds summed → a positive time-dilation metric.
        assert result.simulated_seconds == sum(s % 4 for s in range(12))
        assert result.time_dilation > 0.0

    def test_no_violation_leaves_first_violation_none(self) -> None:
        result = sweep(_toy_run, [1, 2, 3])  # none divisible by 5
        assert result.violations == ()
        assert result.first_violation is None

    def test_format_is_human_readable(self) -> None:
        rendered = sweep(_toy_run, range(10)).format()
        assert "seeds run:" in rendered
        assert "violations" in rendered
        assert "reached:" in rendered  # the folded reachability surfaces in the summary


class TestReachabilityFold:
    def test_folds_reached_labels_with_run_counts(self) -> None:
        result = sweep(_toy_run, range(12))

        # The union (parallels behaviours); per-label run counts kept for the trend.
        assert result.reached == {"even", "odd", "third"}
        assert result.reached_runs["even"] == 6  # 0,2,4,6,8,10
        assert result.reached_runs["third"] == 4  # 0,3,6,9

    def test_reachability_report_satisfied_when_targets_fire(self) -> None:
        report = sweep(_toy_run, range(12)).reachability({"even", "odd"})

        assert report.satisfied
        assert report.unreached == frozenset()
        assert report.runs == 12

    def test_reachability_report_flags_a_target_no_run_reached(self) -> None:
        # The false-confidence guard: a declared target the band never drove is a failure, even
        # though no invariant tripped — a green sweep that never exercised the state proves nothing.
        report = sweep(_toy_run, range(12)).reachability({"even", "never-fires"})

        assert not report.satisfied
        assert report.unreached == frozenset({"never-fires"})

    def test_empty_reachability_is_inert(self) -> None:
        # A runner that tracks no reachability (the SimulationSeedRunner path) folds an empty map;
        # a report against no targets is vacuously satisfied.
        runner = SimulationSeedRunner(target=_PAYMENTS, concurrency=2, act_count=4)
        result = sweep(runner, range(2))

        assert result.reached == frozenset()
        assert result.reachability(frozenset()).satisfied


class TestParallelMatchesSequential:
    def test_parallel_folds_identically_to_sequential(self) -> None:
        seeds = range(16)
        seq = sweep(_toy_run, seeds)
        par = parallel_sweep(_toy_run, seeds, workers=2)

        # Each seed is deterministic in its own process → identical fold, modulo wall time.
        assert par.runs == seq.runs
        assert par.violations == seq.violations
        assert par.behaviors == seq.behaviors
        assert par.reached == seq.reached
        assert dict(par.reached_runs) == dict(seq.reached_runs)
        assert par.simulated_seconds == seq.simulated_seconds

    def test_parallel_handles_chunking(self) -> None:
        seeds = range(20)
        par = parallel_sweep(_toy_run, seeds, workers=2, chunk=4)
        assert par.runs == 20
        assert par.violations == (0, 5, 10, 15)


class TestSimulationSeedRunner:
    def test_runs_a_real_simulation_per_seed(self) -> None:
        # Sequential (in-process) — proves the runner resolves the import target and reports a
        # real run's coverage; the process-pool path is exercised by the toy-run tests above.
        runner = SimulationSeedRunner(target=_PAYMENTS, concurrency=2, act_count=4)
        outcome = runner(0)

        assert outcome.seed == 0
        assert isinstance(outcome.violated, bool)
        assert outcome.behaviors  # a real run exercises behaviours

    def test_aggregates_over_a_seed_range(self) -> None:
        runner = SimulationSeedRunner(target=_PAYMENTS, concurrency=2, act_count=4)
        result = sweep(runner, range(3))

        assert result.runs == 3
        assert result.behaviors  # union across the seeds

    def test_rejects_a_non_simulation_target(self) -> None:
        import pytest

        runner = SimulationSeedRunner(target="forze_dst:Simulation")  # a class, not an instance
        with pytest.raises(TypeError):
            runner(0)

    def test_rejects_a_malformed_target(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            SimulationSeedRunner(target="no-colon-here")(0)

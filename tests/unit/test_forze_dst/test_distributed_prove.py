"""Flagship DST: Forze passes its own simulation (E2 — "prove it") + the continuous sweep (P5).

The strongest claim a DST harness can make is not "I explored N seeds" but "I drove my own
distributed machinery through the dangerous interleavings and it held". A green invariant over a
fault that never bit is *false confidence*; so each scenario pairs a safety invariant (must *always*
hold) with a **reachability target** (must *sometimes* be reached) — and asserts both.

Two of Forze's own distributed primitives, under simulation: a **distributed lock** (mutual
exclusion + no lost update under a partition and write faults) and a **hybrid logical clock**
(causal monotonicity under perturbed interleaving). The scenario bodies live in
``tests.support.dst_flagship`` so they are reusable: here as fast point-in-time smoke tests, as a
**corpus** run every build (the merge guard — a small band plus any seed that ever found a bug), and
as a **fuzz-marked wide sweep** (the nightly, fanned across processes by ``parallel_sweep``).
"""

from __future__ import annotations

import asyncio

import pytest

from forze.application.execution import ExecutionContext
from forze.base.primitives import monotonic
from forze_dst import Cluster, SimulationConfig
from forze_dst.artifacts.sweep import parallel_sweep, sweep
from forze_dst.cluster import ClusterConfig
from forze_dst.invariants import assess_reachability, check, reached_labels, sometimes
from forze_dst.markers import record_event
from forze_dst.oracle import run_recorded
from forze_mock.state import MockState

from tests.support.dst_flagship import (
    DLOCK_INVARIANTS as _DLOCK_INVARIANTS,
    DLOCK_TARGETS as _DLOCK_TARGETS,
    DLOCK_WIDE,
    HLC_INVARIANTS as _HLC_INVARIANTS,
    HLC_WIDE,
    _HLC_CAUSAL,
    _deps,
    _observe,
    _reset,
    dlock_config as _dlock_config,
    dlock_corpus_seeds,
    guarded_cluster as _guarded_cluster,
    hlc_corpus_seeds,
    hlc_scenario as _hlc_scenario,
    run_dlock_seed,
    run_hlc_seed,
    shared_counter as _shared_counter,
)

# ----------------------- #


class TestDistributedLockProven:
    def test_holds_AND_the_dangerous_interleaving_actually_fired(self) -> None:
        counter = _shared_counter()
        cluster = _guarded_cluster(counter)
        config = _dlock_config(range(8))

        histories = cluster.histories(config)

        # Safety: no run violated mutual exclusion or lost an update.
        assert all(not check(history, _DLOCK_INVARIANTS) for history in histories)

        # Liveness of the *test*: the hard states were actually reached across the sweep — so the
        # green safety result was tested against contention + a mid-section partition.
        reachability = assess_reachability(histories, _DLOCK_TARGETS)
        assert reachability.satisfied, reachability.format()

        # And the partition genuinely isolated a node mid-run (recorded on the timeline).
        assert sometimes(histories, lambda h: bool(h.of_kind("partition")))

    def test_unguarded_variant_is_caught_minimized_and_reproduced(self) -> None:
        counter = _shared_counter()

        async def node(node_id: int, _ctx: ExecutionContext) -> None:
            counter["attempts"].append(node_id)  # type: ignore[attr-defined]
            start = monotonic()
            current = counter["value"]  # type: ignore[assignment]
            await asyncio.sleep(1.0)  # hold across virtual time → critical sections overlap
            counter["value"] = current + 1  # type: ignore[operator]
            record_event(
                "hold", resource="counter", holder=str(node_id), start=start, end=monotonic()
            )

        cluster = Cluster(
            deps=_deps,
            state_factory=MockState,
            node=node,
            setup=_reset(counter),
            observe=_observe(counter),
            invariants=_DLOCK_INVARIANTS,
        )
        config = SimulationConfig(seeds=range(3), cluster=ClusterConfig(nodes=4))

        report = cluster.run(config)
        assert report is not None
        assert {v.invariant for v in report.violations} & {"mutual_exclusion", "expect"}
        assert 2 <= len(report.workload) < 4  # two contenders are the minimal counterexample

        # Reproducible: same seed, same minimal counterexample.
        again = cluster.run(config)
        assert again is not None and again.seed == report.seed

    def test_single_node_cannot_reach_contention(self) -> None:
        # A one-node cluster can never contend — the reachability check must report it as a
        # *failure* (false confidence: a passing safety result that never exercised the race).
        counter = _shared_counter()
        cluster = _guarded_cluster(counter)
        config = SimulationConfig(
            seeds=range(3),
            cluster=ClusterConfig(nodes=1),
            reachability_targets=_DLOCK_TARGETS,
        )

        reachability = assess_reachability(cluster.histories(config), _DLOCK_TARGETS)
        assert not reachability.satisfied
        assert "lock-contended" in reachability.unreached


# ....................... #


class TestHybridLogicalClockProven:
    def test_causal_monotonicity_holds_AND_a_merge_ahead_actually_fired(self) -> None:
        histories = [
            run_recorded(_hlc_scenario(causal=True), seed=seed, schedule_seed=seed)
            for seed in range(12)
        ]

        # Safety: every replica's clock is monotonic and every merge exceeds its cause.
        assert all(not check(history, _HLC_INVARIANTS) for history in histories)

        # The causality path was actually exercised (not vacuous).
        assert sometimes(histories, lambda h: "hlc-merged-ahead" in reached_labels(h))

    def test_broken_clock_that_drops_the_remote_stamp_is_caught(self) -> None:
        histories = [
            run_recorded(_hlc_scenario(causal=False), seed=seed, schedule_seed=seed)
            for seed in range(12)
        ]

        violating = [history for history in histories if check(history, (_HLC_CAUSAL,))]
        assert violating, "the broken clock should violate causality under some interleaving"


# ....................... #
# P5 / B.2 — the regression corpus: a fast band (plus any seed that ever found a bug) run EVERY
# build as a merge guard. Sequential (no process pool) so it stays fast and reliable in `just test`.


class TestFlagshipCorpus:
    def test_dlock_corpus_is_clean(self) -> None:
        seeds = dlock_corpus_seeds()
        result = sweep(run_dlock_seed, seeds)
        assert result.violations == (), f"dlock regressed at seeds {result.violations}"
        assert result.runs == len(seeds)  # the whole band ran (not a vacuous pass)
        assert result.behaviors  # dlock is port-based → it exercised behaviours

    def test_hlc_corpus_is_clean(self) -> None:
        # The HLC scenario is register/event-based (no ports), so behavioral_coverage is empty by
        # design — its non-vacuity signal is reachability ("hlc-merged-ahead"), asserted by the
        # smoke test above; the corpus guards against a causal-monotonicity regression over the band.
        seeds = hlc_corpus_seeds()
        result = sweep(run_hlc_seed, seeds)
        assert result.violations == (), f"HLC regressed at seeds {result.violations}"
        assert result.runs == len(seeds)


# ....................... #
# P5 / B.1 — the wide continuous sweep: fuzz-marked (excluded from default `just test`, run by
# `just fuzz` / nightly), fanned across processes. A violation prints the seed to add to the corpus.


class TestFlagshipFuzz:
    @pytest.mark.fuzz
    def test_dlock_wide_sweep(self) -> None:
        result = parallel_sweep(run_dlock_seed, tuple(DLOCK_WIDE))
        assert result.violations == (), result.format()

    @pytest.mark.fuzz
    def test_hlc_wide_sweep(self) -> None:
        result = parallel_sweep(run_hlc_seed, tuple(HLC_WIDE))
        assert result.violations == (), result.format()

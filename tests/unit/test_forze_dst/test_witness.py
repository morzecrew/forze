"""The falsifiability-witness machinery — named invariants, horizon analysis, accounting,
positional faults, and the countable clean-verdict clause.

The doctrine under test: a clean verdict may cover an invariant only if the harness has
demonstrated it could catch that invariant failing (a witness), or an audited declaration
excludes it by name — and every surface that prints the verdict states the same countable claim.
"""

from __future__ import annotations

import asyncio
import random

import pytest

from forze.application.contracts.interception import PortCall
from forze.base.exceptions import CoreException
from forze_dst import SimulationConfig
from forze_dst.faults import (
    CrashPolicy,
    FaultPolicy,
    FaultRule,
    SimulatedCrash,
    compile_fault_policy,
)
from forze_dst.invariants import Violation, expect, name_of, named, no_duplicate_effect
from forze_dst.misuse import MisuseCase
from forze_dst.oracle.confidence import ConfidenceProbe
from forze_dst.oracle.horizon import HorizonProbe, read_kinds
from forze_dst.oracle.recorder import History, Recorder
from forze_dst.oracle.witness import (
    HorizonClass,
    HorizonDeclaration,
    InvariantAccountingError,
    InvariantStatus,
    InvariantWitness,
    Perturbation,
    account_invariants,
    load_witnesses,
    mine_witnesses,
    replay_witnesses,
    save_witnesses,
    witnesses_from_mutants,
)
from forze_dst.stats import format_clean_verdict
from tests.support.misuse import CORPUS

# ----------------------- #


def _history(*events: tuple[str, float, dict]) -> History:  # type: ignore[type-arg]
    recorder = Recorder(seed=0)
    for kind, at, fields in events:
        recorder.record(kind, at=at, **fields)
    return recorder.history


def _witness(name: str, *, fingerprint: str | None = "fp", seed: int = 1) -> InvariantWitness:
    from forze_dst.artifacts.corpus import RegressionEntry

    return InvariantWitness(
        invariant=name, entry=RegressionEntry(seed=seed, registry_fingerprint=fingerprint)
    )


# ....................... #


class TestNamedInvariants:
    def test_named_renames_fired_violations_and_sets_the_name(self) -> None:
        inner = expect("k", lambda _e: False, message="always fires")
        wrapped = named("mine", inner)

        assert name_of(wrapped) == "mine"

        violations = wrapped(_history(("k", 0.0, {})))
        assert violations and all(v.invariant == "mine" for v in violations)

    def test_builtins_carry_their_canonical_names(self) -> None:
        assert name_of(expect("k", lambda _e: True, message="m")) == "expect"
        assert name_of(no_duplicate_effect("paid", by="order")) == "no_duplicate_effect"

    def test_name_of_falls_back_to_the_callable_name(self) -> None:
        def my_invariant(_history: History) -> list[Violation]:
            return []

        assert name_of(my_invariant) == "my_invariant"


# ....................... #


class TestReadKinds:
    def test_footprint_of_a_kind_reading_invariant(self) -> None:
        assert read_kinds(expect("payments", lambda _e: True, message="m")) == {"payments"}

    def test_events_access_makes_the_footprint_opaque(self) -> None:
        def opaque(history: History) -> list[Violation]:
            _ = list(history.events)  # a wholesale scan — the footprint is undecidable
            return []

        assert read_kinds(opaque) is None

    def test_kinds_read_before_a_raise_still_count(self) -> None:
        def brittle(history: History) -> list[Violation]:
            history.of_kind("audit_marks")
            raise RuntimeError("predicate bug")

        assert read_kinds(brittle) == {"audit_marks"}

    def test_reading_nothing_is_unknown_not_vacuous(self) -> None:
        def inert(_history: History) -> list[Violation]:
            return []

        assert read_kinds(inert) is None


# ....................... #


class TestHorizonProbe:
    def test_vacuous_when_no_run_records_the_read_kinds(self) -> None:
        probe = HorizonProbe()
        probe.observe(_history(("operation", 0.0, {"op": "pay", "outcome": "ok"})))

        analysis = probe.analyze([expect("payments", lambda _e: True, message="m")])

        assert analysis.vacuous == (("expect", ("payments",)),)

    def test_not_vacuous_once_any_run_records_the_kind(self) -> None:
        probe = HorizonProbe()
        probe.observe(_history(("operation", 0.0, {"op": "pay", "outcome": "ok"})))
        probe.observe(_history(("payments", 0.0, {"total": 1})))

        assert probe.analyze([expect("payments", lambda _e: True, message="m")]).vacuous == ()

    def test_marker_inside_a_rolled_back_tx_window_flags_the_folding_invariant(self) -> None:
        probe = HorizonProbe()
        probe.observe(
            _history(
                ("trace", 1.0, {"trace_domain": "tx", "op": "enter", "tx_id": 7}),
                ("charged", 1.5, {"order": "o1"}),
                ("trace", 2.0, {"trace_domain": "tx", "op": "exit", "tx_id": 7, "outcome": "rollback"}),
            )
        )

        analysis = probe.analyze([no_duplicate_effect("charged", by="order")])
        assert analysis.marker_blind == (("no_duplicate_effect", ("charged",)),)

    def test_committed_tx_does_not_flag(self) -> None:
        probe = HorizonProbe()
        probe.observe(
            _history(
                ("trace", 1.0, {"trace_domain": "tx", "op": "enter", "tx_id": 7}),
                ("charged", 1.5, {"order": "o1"}),
                ("trace", 2.0, {"trace_domain": "tx", "op": "exit", "tx_id": 7, "outcome": "commit"}),
            )
        )

        assert probe.analyze([no_duplicate_effect("charged", by="order")]).marker_blind == ()

    def test_observe_emitted_markers_are_exempt(self) -> None:
        # The marker lands inside the rolled-back window by timestamp, but it was recorded by
        # the observe hook (after the phase boundary) over settled state — no rollback hazard.
        probe = HorizonProbe()
        probe.observe(
            _history(
                ("trace", 1.0, {"trace_domain": "tx", "op": "enter", "tx_id": 7}),
                ("phase", 1.2, {"phase": "observe"}),
                ("charged", 1.5, {"order": "o1"}),
                ("trace", 2.0, {"trace_domain": "tx", "op": "exit", "tx_id": 7, "outcome": "rollback"}),
            )
        )

        assert probe.analyze([no_duplicate_effect("charged", by="order")]).marker_blind == ()


# ....................... #


class TestAccounting:
    def test_statuses_partition_witnessed_declared_unaccounted(self) -> None:
        accounting = account_invariants(
            ["a", "b", "c"],
            witnesses=[_witness("a")],
            declarations=[
                HorizonDeclaration(
                    invariant="b", horizon=HorizonClass.BELOW_PORT, covered_by="itest::b"
                )
            ],
            fingerprint="fp",
        )

        assert dict(accounting.statuses) == {
            "a": InvariantStatus.WITNESSED,
            "b": InvariantStatus.DECLARED,
            "c": InvariantStatus.UNACCOUNTED,
        }
        assert accounting.witnessed == ("a",)
        assert accounting.declared == ("b",)
        assert accounting.unaccounted == ("c",)

        with pytest.raises(InvariantAccountingError, match="unaccounted"):
            accounting.require_accounted()

    def test_fingerprint_drift_demotes_the_witness_loudly(self) -> None:
        accounting = account_invariants(
            ["a"], witnesses=[_witness("a", fingerprint="old")], declarations=[], fingerprint="fp"
        )

        assert accounting.unaccounted == ("a",)
        assert accounting.drifted == ("a",)

        with pytest.raises(InvariantAccountingError, match="re-mine"):
            accounting.require_accounted()

    def test_declared_yet_witnessed_is_a_wrong_declaration(self) -> None:
        accounting = account_invariants(
            ["a"],
            witnesses=[_witness("a")],
            declarations=[
                HorizonDeclaration(
                    invariant="a", horizon=HorizonClass.EXTERNAL_EFFECT, covered_by="itest::a"
                )
            ],
            fingerprint="fp",
        )

        assert accounting.conflicts == ("a",)

        with pytest.raises(InvariantAccountingError, match="wrong declaration"):
            accounting.require_accounted()

    def test_fully_accounted_passes(self) -> None:
        accounting = account_invariants(
            ["a"], witnesses=[_witness("a")], declarations=[], fingerprint="fp"
        )

        assert accounting.problems == ()
        accounting.require_accounted()  # does not raise

    def test_declaration_requires_a_covering_check(self) -> None:
        with pytest.raises(ValueError, match="covered_by"):
            HorizonDeclaration(invariant="a", horizon=HorizonClass.REAL_TIME, covered_by="   ")


# ....................... #
# Config-scoped accounting: WITNESSED is a claim about the *citing* sweep, so a witness mined
# under a perturbation the sweep's config does not enable must not license its clean verdict.


def _mined_witness(
    name: str, *, config: SimulationConfig, fingerprint: str = "fp", seed: int = 1
) -> InvariantWitness:
    """A miner-shaped witness: the entry embeds the find config (what makes replay exact)."""

    from forze_dst.artifacts.corpus import RegressionEntry
    from forze_dst.artifacts.serialize import config_to_dict

    return InvariantWitness(
        invariant=name,
        entry=RegressionEntry(
            seed=seed,
            registry_fingerprint=fingerprint,
            explore={"config": config_to_dict(config)},
        ),
    )


class TestSweepScopedAccounting:
    def test_crash_mined_witness_is_unexercisable_without_a_crash_policy(self) -> None:
        crash_config = SimulationConfig(crash=CrashPolicy(probability=0.25))

        accounting = account_invariants(
            ["a"],
            witnesses=[_mined_witness("a", config=crash_config)],
            declarations=[],
            fingerprint="fp",
            config=SimulationConfig(seeds=range(1000)),  # the issue's exact citing config
        )

        assert dict(accounting.statuses) == {"a": InvariantStatus.UNEXERCISABLE}
        assert accounting.witnessed == ()  # the detection bound must not cover it
        assert accounting.unexercisable == ("a",)
        assert accounting.missing_capabilities == (("a", ("crash",)),)

        with pytest.raises(InvariantAccountingError, match=r"unexercisable.*needs crash"):
            accounting.require_accounted()

    def test_the_same_witness_is_witnessed_once_the_config_enables_crash(self) -> None:
        crash_config = SimulationConfig(crash=CrashPolicy(probability=0.25))

        accounting = account_invariants(
            ["a"],
            witnesses=[_mined_witness("a", config=crash_config)],
            declarations=[],
            fingerprint="fp",
            config=SimulationConfig(seeds=range(4), crash=CrashPolicy(probability=0.05)),
        )

        assert accounting.witnessed == ("a",)
        assert accounting.problems == ()
        accounting.require_accounted()  # does not raise

    def test_fault_kinds_match_individually(self) -> None:
        timeout_config = SimulationConfig(faults=FaultPolicy(rules=(FaultRule(timeout=0.1),)))
        witness = _mined_witness("a", config=timeout_config)

        error_only = SimulationConfig(faults=FaultPolicy(rules=(FaultRule(error=0.1),)))
        scoped = account_invariants(
            ["a"], witnesses=[witness], declarations=[], fingerprint="fp", config=error_only
        )
        assert scoped.unexercisable == ("a",)
        assert scoped.missing_capabilities == (("a", ("fault:timeout",)),)

        timeout_too = SimulationConfig(faults=FaultPolicy(rules=(FaultRule(timeout=0.05),)))
        assert (
            account_invariants(
                ["a"], witnesses=[witness], declarations=[], fingerprint="fp", config=timeout_too
            ).witnessed
            == ("a",)
        )

    def test_any_exercisable_witness_licenses_the_invariant(self) -> None:
        crash_mined = _mined_witness("a", config=SimulationConfig(crash=CrashPolicy()))
        schedule_mined = _mined_witness("a", config=SimulationConfig(), seed=2)

        accounting = account_invariants(
            ["a"],
            witnesses=[crash_mined, schedule_mined],
            declarations=[],
            fingerprint="fp",
            config=SimulationConfig(),  # default perturbing scheduler, no crash
        )

        assert accounting.witnessed == ("a",)
        assert accounting.problems == ()

    def test_registry_only_accounting_stays_unscoped(self) -> None:
        crash_config = SimulationConfig(crash=CrashPolicy(probability=0.25))

        accounting = account_invariants(
            ["a"],
            witnesses=[_mined_witness("a", config=crash_config)],
            declarations=[],
            fingerprint="fp",
        )

        assert accounting.witnessed == ("a",)
        assert accounting.missing_capabilities == ()

    def test_bare_crash_knobs_still_carry_the_requirement(self) -> None:
        # Corpus-style killing entry: bare knobs, no config snapshot.
        from forze_dst.artifacts.corpus import RegressionEntry

        witness = InvariantWitness(
            invariant="a",
            entry=RegressionEntry(
                seed=0,
                registry_fingerprint="fp",
                explore={"crash_surface": "document_command", "crash_probability": 0.25},
            ),
        )

        scoped = account_invariants(
            ["a"],
            witnesses=[witness],
            declarations=[],
            fingerprint="fp",
            config=SimulationConfig(),
        )
        assert scoped.unexercisable == ("a",)

        # The bare knob's surface is preserved: a crash policy on a different surface still
        # cannot exercise it; a broad one can.
        wrong_surface = SimulationConfig(crash=CrashPolicy(surface="mailbox"))
        assert (
            account_invariants(
                ["a"], witnesses=[witness], declarations=[], fingerprint="fp", config=wrong_surface
            ).unexercisable
            == ("a",)
        )
        broad = SimulationConfig(crash=CrashPolicy())
        assert (
            account_invariants(
                ["a"], witnesses=[witness], declarations=[], fingerprint="fp", config=broad
            ).witnessed
            == ("a",)
        )

    def test_config_capabilities_enumeration(self) -> None:
        from forze_dst.oracle.witness import PerturbationCapability, config_capabilities
        from forze_dst.scheduler import FIFOScheduler

        schedule = PerturbationCapability(kind="schedule")
        assert config_capabilities(SimulationConfig()) == {schedule}
        assert config_capabilities(SimulationConfig(scheduler=FIFOScheduler())) == frozenset()
        assert config_capabilities(SimulationConfig(crash=CrashPolicy(surface="mailbox"))) == {
            schedule,
            PerturbationCapability(kind="crash", surface="mailbox"),
        }
        assert config_capabilities(
            SimulationConfig(
                scheduler=FIFOScheduler(),
                faults=FaultPolicy(rules=(FaultRule(error=0.1, crash=0.01, op="update"),)),
            )
        ) == {
            PerturbationCapability(kind="crash", op="update"),
            PerturbationCapability(kind="fault:error", op="update"),
        }

    def test_crash_selector_mismatch_is_unexercisable(self) -> None:
        # Both configs "have crash", but on disjoint surfaces — the sweep's crash policy can
        # never select the calls the witness's did, so kind-level matching would over-claim.
        witness = _mined_witness(
            "a",
            config=SimulationConfig(
                crash=CrashPolicy(probability=0.25, surface="document_command")
            ),
        )

        scoped = account_invariants(
            ["a"],
            witnesses=[witness],
            declarations=[],
            fingerprint="fp",
            config=SimulationConfig(crash=CrashPolicy(probability=0.25, surface="mailbox")),
        )

        assert scoped.unexercisable == ("a",)
        assert scoped.missing_capabilities == (("a", ("crash[surface=document_command]",)),)

    def test_broad_and_pinned_selectors_overlap_both_ways(self) -> None:
        pinned = _mined_witness(
            "a", config=SimulationConfig(crash=CrashPolicy(surface="document_command"))
        )
        broad = _mined_witness("a", config=SimulationConfig(crash=CrashPolicy()))

        # A sweep-wide crash policy covers a surface-pinned witness…
        assert (
            account_invariants(
                ["a"],
                witnesses=[pinned],
                declarations=[],
                fingerprint="fp",
                config=SimulationConfig(crash=CrashPolicy()),
            ).witnessed
            == ("a",)
        )

        # …and a pinned sweep overlaps a global witness (both can crash at that surface).
        assert (
            account_invariants(
                ["a"],
                witnesses=[broad],
                declarations=[],
                fingerprint="fp",
                config=SimulationConfig(crash=CrashPolicy(surface="mailbox")),
            ).witnessed
            == ("a",)
        )

    def test_fault_rule_selector_mismatch_is_unexercisable(self) -> None:
        witness = _mined_witness(
            "a",
            config=SimulationConfig(faults=FaultPolicy(rules=(FaultRule(error=0.1, op="update"),))),
        )

        wrong_op = SimulationConfig(faults=FaultPolicy(rules=(FaultRule(error=0.1, op="get"),)))
        scoped = account_invariants(
            ["a"], witnesses=[witness], declarations=[], fingerprint="fp", config=wrong_op
        )
        assert scoped.unexercisable == ("a",)
        assert scoped.missing_capabilities == (("a", ("fault:error[op=update]",)),)

        same_op = SimulationConfig(faults=FaultPolicy(rules=(FaultRule(error=0.05, op="update"),)))
        assert (
            account_invariants(
                ["a"], witnesses=[witness], declarations=[], fingerprint="fp", config=same_op
            ).witnessed
            == ("a",)
        )


# ....................... #


class TestWitnessRegistryIO:
    def test_jsonl_roundtrip(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        witnesses = (_witness("a", seed=3), _witness("b", seed=9))
        path = tmp_path / "witnesses.jsonl"

        save_witnesses(path, witnesses)
        assert load_witnesses(path) == witnesses

    def test_absent_registry_is_empty(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        assert load_witnesses(tmp_path / "missing.jsonl") == ()


# ....................... #


class TestAtCall:
    def _interceptor(self, rule: FaultRule) -> object:
        return compile_fault_policy(FaultPolicy(rules=(rule,)), random.Random(0))

    def _drive(self, rule: FaultRule, calls: int) -> list[int | None]:
        """Run *calls* matched calls; per call record None (clean) or the 1-based index the
        fault fired at."""

        interceptor = self._interceptor(rule)
        outcomes: list[int | None] = []

        async def go() -> None:
            for index in range(1, calls + 1):
                call = PortCall(surface="document_command", route="r", op="update", args=())

                async def nxt(_call: PortCall) -> str:
                    return "ok"

                try:
                    await interceptor.around(call, nxt)  # type: ignore[attr-defined]
                    outcomes.append(None)
                except (SimulatedCrash, CoreException):
                    outcomes.append(index)

        asyncio.run(go())
        return outcomes

    def test_crash_fires_at_exactly_the_targeted_call(self) -> None:
        outcomes = self._drive(FaultRule(crash=1.0, at_call=3), calls=5)
        assert outcomes == [None, None, 3, None, None]

    def test_error_kind_selector_under_at_call(self) -> None:
        outcomes = self._drive(FaultRule(error=0.2, at_call=2), calls=3)
        # Under at_call the rate is a kind selector: 0.2 > 0 fires with certainty at call 2.
        assert outcomes == [None, 2, None]

    def test_placement_is_seed_independent(self) -> None:
        # No RNG is drawn for a positioned rule, so the placement is exact under any seed.
        for seed in (0, 1, 42):
            interceptor = compile_fault_policy(
                FaultPolicy(rules=(FaultRule(crash=1.0, at_call=2),)), random.Random(seed)
            )

            async def go(icpt=interceptor) -> list[bool]:  # type: ignore[no-untyped-def]
                fired = []
                for _ in range(3):
                    call = PortCall(surface="s", route="r", op="o", args=())

                    async def nxt(_call: PortCall) -> None:
                        return None

                    try:
                        await icpt.around(call, nxt)
                        fired.append(False)
                    except SimulatedCrash:
                        fired.append(True)
                return fired

            assert asyncio.run(go()) == [False, True, False]

    def test_at_call_validation(self) -> None:
        with pytest.raises(ValueError, match="at_call"):
            FaultRule(crash=1.0, at_call=0)


# ....................... #


class TestCountableVerdict:
    def test_locked_default_scope_is_unchanged(self) -> None:
        assert "for this scenario × strategy × oracle set" in format_clean_verdict(10)

    def test_accounting_scope_is_countable(self) -> None:
        verdict = format_clean_verdict(10, witnessed=3, declared=("ext_charge",))

        assert "the 3 witnessed invariants" in verdict
        assert "1 declared out-of-horizon: ext_charge" in verdict
        assert "oracle set" not in verdict

    def test_unaccounted_never_hides(self) -> None:
        verdict = format_clean_verdict(10, witnessed=1, unaccounted=("mystery",))
        assert "1 UNACCOUNTED: mystery" in verdict

    def test_unexercisable_never_hides(self) -> None:
        verdict = format_clean_verdict(10, witnessed=1, unexercisable=("lost_effect",))
        assert "the 1 witnessed invariant" in verdict  # the bound's K excludes it
        assert "1 witnessed but UNEXERCISABLE under this config: lost_effect" in verdict


# ....................... #


class TestConfidenceIntegration:
    def test_vacuous_invariant_is_a_confidence_gap(self) -> None:
        probe = ConfidenceProbe()
        probe.observe(_history(("operation", 0.0, {"op": "pay", "outcome": "ok"})))

        report = probe.report(invariants=[expect("payments", lambda _e: True, message="m")])

        assert any("vacuous invariant" in warning for warning in report.warnings)
        assert not report.clean

    def test_accounting_threads_into_format_and_verdict(self) -> None:
        accounting = account_invariants(
            ["a", "b"],
            witnesses=[_witness("a")],
            declarations=[
                HorizonDeclaration(
                    invariant="b", horizon=HorizonClass.CROSS_RUN, covered_by="itest::b"
                )
            ],
            fingerprint="fp",
        )

        probe = ConfidenceProbe()
        probe.observe(_history(("payments", 0.0, {"total": 1})))
        report = probe.report(accounting=accounting)

        rendered = report.format()
        assert "invariants:   1 witnessed / 1 declared out-of-horizon / 0 unaccounted" in rendered
        assert "the 1 witnessed invariant" in report.verdict()
        assert "1 declared out-of-horizon: b" in report.verdict()


# ....................... #
# The corpus is the reference implementation: its killing entries are witnesses, so the full
# loop — mine → account → gate → replay — runs against a real corpus case.


def _t1() -> tuple[MisuseCase, object]:
    mutant = next(m for m in CORPUS if m.mutant_id == "T1-blind-write-payment")

    import importlib

    module_name, _, attr = mutant.base.partition(":")
    case = getattr(importlib.import_module(module_name), attr)()
    return case, mutant


class TestFullWitnessLoop:
    def test_mine_account_gate_and_replay_on_a_corpus_case(self) -> None:
        case, mutant = _t1()
        sim = case.simulation
        assert mutant.killing.explore is not None

        base = SimulationConfig(
            seeds=[mutant.killing.seed],
            act_count=int(mutant.killing.explore["act_count"]),
            concurrency=int(mutant.killing.explore["concurrency"]),
        )

        # Mine: the killing seed under the smoke knobs is a guaranteed find, so the miner is
        # deterministic here (one probe, one witness).
        mining = mine_witnesses(
            sim,
            base,
            scenario=case.scenario,
            targets=mutant.expected_invariants,
            repertoire=(Perturbation(label="schedule:random", config=base),),
        )

        assert mining.unwitnessed == ()
        assert mining.wrong_declarations == ()
        assert {w.invariant for w in mining.witnesses} == set(mutant.expected_invariants)

        # Account: the mined witness makes the invariant WITNESSED against the live catalog.
        sim.witnesses = mining.witnesses
        accounting = sim.invariant_accounting()
        assert accounting is not None
        assert accounting.witnessed == mutant.expected_invariants
        accounting.require_accounted()

        # Replay tier: the witness still fires (the embedded config makes the replay exact).
        replay_witnesses(sim, scenario=case.scenario)

    def test_audit_gates_on_a_drifted_witness_before_spending_compute(self) -> None:
        case, _ = _t1()
        sim = case.simulation
        sim.witnesses = (_witness("expect", fingerprint="not-this-catalog"),)

        with pytest.raises(InvariantAccountingError, match="re-mine"):
            sim.audit(SimulationConfig(seeds=range(4)))

    def test_audit_gates_on_a_witness_this_config_could_not_exercise(self) -> None:
        # The issue scenario: the witness was mined under a crash policy, the citing audit runs
        # no crash — its clean sweep structurally could not catch the invariant, so the gate
        # fails instead of printing a detection bound scoped to it.
        case, _ = _t1()
        sim = case.simulation
        sim.witnesses = (
            _mined_witness(
                "expect",
                config=SimulationConfig(crash=CrashPolicy(probability=0.25)),
                fingerprint=sim.fingerprint(),
            ),
        )

        with pytest.raises(InvariantAccountingError, match="unexercisable"):
            sim.audit(SimulationConfig(seeds=range(1000)))

        # A crash-capable citing config passes the same gate (checked without the sweep).
        accounting = sim.invariant_accounting(
            SimulationConfig(seeds=range(4), crash=CrashPolicy(probability=0.05))
        )
        assert accounting is not None
        accounting.require_accounted()
        assert accounting.witnessed == ("expect",)

    def test_run_warns_but_does_not_fail(self) -> None:
        case, mutant = _t1()
        sim = case.simulation
        sim.witnesses = (_witness("expect", fingerprint="not-this-catalog"),)
        assert mutant.killing.explore is not None

        with pytest.warns(UserWarning, match="invariant accounting"):
            sim.run(
                SimulationConfig(
                    seeds=[mutant.killing.seed],
                    act_count=int(mutant.killing.explore["act_count"]),
                    concurrency=int(mutant.killing.explore["concurrency"]),
                ),
                scenario=case.scenario,
            )

    def test_duplicate_invariant_names_fail_loud(self) -> None:
        case, _ = _t1()
        sim = case.simulation
        sim.invariants = [
            expect("payments", lambda _e: True, message="one"),
            expect("payments", lambda _e: True, message="two"),
        ]
        sim.witnesses = (_witness("expect"),)

        with pytest.raises(InvariantAccountingError, match="duplicate"):
            sim.invariant_accounting()


# ....................... #


class TestCorpusAsWitnessRegistry:
    def test_every_expected_invariant_converts_to_a_witness(self) -> None:
        witnesses = witnesses_from_mutants(CORPUS)

        expected = {name for mutant in CORPUS for name in mutant.expected_invariants}
        assert {witness.invariant for witness in witnesses} == expected

    def test_each_mutants_killing_entry_accounts_for_its_invariants(self) -> None:
        # Per mutant: its killing entry is a live witness against its own (current) catalog —
        # the corpus smoke tier's fingerprint gate, restated in accounting terms.
        import importlib

        for mutant in CORPUS:
            module_name, _, attr = mutant.base.partition(":")
            case = getattr(importlib.import_module(module_name), attr)()

            accounting = account_invariants(
                mutant.expected_invariants,
                witnesses=witnesses_from_mutants([mutant]),
                declarations=(),
                fingerprint=case.simulation.fingerprint(),
            )

            assert accounting.problems == (), mutant.mutant_id
            assert set(accounting.witnessed) == set(mutant.expected_invariants), mutant.mutant_id

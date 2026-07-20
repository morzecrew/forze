"""The find → reproduce → minimize → regression loop (S5): unified report + seed corpus.

Two halves:

* the counterexample report now carries an **injected-environment timeline** — the seeded
  faults (error / timeout / crash / drop / duplicate / delay) and latency the simulator applied,
  in virtual-time order, kept separate from the app's observed domain facts;
* a **regression corpus** (JSON Lines) turns a found seed into a permanent, replayable entry.
"""

from __future__ import annotations

import attrs

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig, Strategy
from forze_dst.artifacts import (
    RegressionEntry,
    append_regression,
    entry_from_report,
    load_regressions,
)
from forze_dst.faults import FaultPolicy, FaultRule
from forze_dst.invariants import operation_succeeds
from forze_dst.oracle import ViolationReport, behavioral_fingerprint
from forze_dst.oracle.invariants import Violation
from forze_dst.oracle.recorder import Event, History, Recorder


def test_recorded_event_fields_are_immutable() -> None:
    # Recorded history claims to be immutable; the recorder must store a read-only fields view
    # so a recorded event can't be mutated after the fact (which would corrupt invariants).
    import pytest

    recorder = Recorder(seed=0)
    recorder.record("balance", at=0.0, final=1)
    (event,) = recorder.history.events

    with pytest.raises(TypeError):
        event.fields["final"] = 2  # type: ignore[index]
from forze_dst.oracle.report import CausalGraph
from forze_mock import MockDepsModule

# ----------------------- #


def _ev(seq: int, kind: str, **fields: object) -> Event:
    return Event(seq=seq, kind=kind, at=float(seq), fields=fields)


def _history_with_injection() -> History:
    return History(
        seed=7,
        events=(
            _ev(0, "op_start", call_id=0, op="pay"),
            _ev(
                1,
                "fault",
                fault="error",
                surface="document_command",
                route="orders",
                op="update",
            ),
            _ev(
                2,
                "latency",
                surface="document_query",
                route="orders",
                op="get",
                seconds=1.5,
            ),
            _ev(
                3,
                "operation",
                call_id=0,
                op="pay",
                outcome="error",
                error="InjectedFault",
                invoked_at=0.0,
                returned_at=2.0,
                start_seq=0,
                end_seq=2,
            ),
            _ev(4, "balance", final=1, expected=2),  # an observed domain fact
        ),
    )


# ....................... #


class TestInjectedEnvironmentTimeline:
    def test_faults_and_latency_are_split_into_the_timeline(self) -> None:
        graph = CausalGraph.from_history(_history_with_injection())

        kinds = {event.kind for event in graph.timeline}
        assert kinds == {"fault", "latency"}
        # The timeline is sorted by virtual time.
        assert [event.at for event in graph.timeline] == sorted(
            event.at for event in graph.timeline
        )
        # Injected events are NOT mixed into the app's observed facts.
        assert all(event.kind not in ("fault", "latency") for event in graph.facts)
        assert any(event.kind == "balance" for event in graph.facts)

    def test_report_renders_the_injected_environment_section(self) -> None:
        report = ViolationReport(
            seed=7,
            schedule_seed=None,
            violations=(Violation(invariant="expect", message="lost"),),
            workload=(("pay", None),),
            history=_history_with_injection(),
        )
        rendered = report.format()

        assert "injected environment" in rendered
        assert "error → document_command[orders].update" in rendered
        assert "latency 1.500s → document_query[orders].get" in rendered


# ....................... #


class TestRegressionCorpus:
    def test_append_and_load_round_trip(self, tmp_path: object) -> None:
        path = tmp_path / "corpus.jsonl"  # type: ignore[operator]

        assert load_regressions(path) == []  # missing file → empty corpus

        entry = RegressionEntry(
            seed=42,
            schedule_seed=99,
            target="app:sim",
            registry_fingerprint="abc123",
            invariants=("no_duplicate_effect",),
            found_at="2026-06-18T00:00:00+00:00",
        )
        append_regression(path, entry)

        loaded = load_regressions(path)
        assert loaded == [entry]

    def test_append_is_idempotent_on_seed_and_target(self, tmp_path: object) -> None:
        path = tmp_path / "corpus.jsonl"  # type: ignore[operator]
        entry = RegressionEntry(seed=1, target="app:sim")

        append_regression(path, entry)
        append_regression(path, entry)  # same seed+target → not duplicated
        append_regression(path, RegressionEntry(seed=2, target="app:sim"))

        assert [e.seed for e in load_regressions(path)] == [1, 2]

    def test_entry_from_report_captures_seed_and_invariants(self) -> None:
        report = ViolationReport(
            seed=5,
            schedule_seed=11,
            violations=(
                Violation(invariant="no_unexpected_error", message="boom"),
                Violation(invariant="expect", message="x"),
            ),
            workload=(),
            history=History(seed=5, events=()),
            registry_fingerprint="fp",
        )
        entry = entry_from_report(report, target="m:sim", found_at="t")

        assert entry.seed == 5 and entry.schedule_seed == 11
        assert entry.target == "m:sim" and entry.registry_fingerprint == "fp"
        assert entry.invariants == ("expect", "no_unexpected_error")  # sorted, de-duped
        assert entry.behavioral_fingerprint is None  # default: structural-only posture


def _history(*ops: tuple[str, str]) -> History:
    """A history whose operation events have the given ``(op, outcome)`` shape."""

    events = tuple(
        Event(seq=i, kind="operation", at=float(i), fields={"op": op, "outcome": outcome})
        for i, (op, outcome) in enumerate(ops)
    )
    return History(seed=0, events=events)


class TestStrictBehavioralFingerprint:
    def test_fingerprint_is_stable_and_shape_sensitive(self) -> None:
        a = _history(("pay", "ok"), ("ship", "ok"))
        same = _history(("pay", "ok"), ("ship", "ok"))
        reordered = _history(("ship", "ok"), ("pay", "ok"))
        different_outcome = _history(("pay", "ok"), ("ship", "error"))

        # Same shape → same digest; order and outcome are part of the signature.
        assert behavioral_fingerprint(a) == behavioral_fingerprint(same)
        assert behavioral_fingerprint(a) != behavioral_fingerprint(reordered)
        assert behavioral_fingerprint(a) != behavioral_fingerprint(different_outcome)

    def test_id_independent(self) -> None:
        # Entity ids/keys are not part of the trace shape, so they don't move the fingerprint.
        with_key = History(
            seed=0,
            events=(
                Event(seq=0, kind="trace", at=0.0,
                      fields={"trace_domain": "document", "surface": "document_command",
                              "route": "orders", "op": "create", "phase": "command",
                              "key": "abc", "outcome": None}),
            ),
        )
        other_key = History(
            seed=0,
            events=(
                Event(seq=0, kind="trace", at=0.0,
                      fields={"trace_domain": "document", "surface": "document_command",
                              "route": "orders", "op": "create", "phase": "command",
                              "key": "zzz", "outcome": None}),
            ),
        )
        assert behavioral_fingerprint(with_key) == behavioral_fingerprint(other_key)

    def test_strict_entry_flags_drift_but_default_does_not(self) -> None:
        found = _history(("pay", "ok"), ("ship", "ok"))
        report = ViolationReport(
            seed=5,
            schedule_seed=None,
            violations=(Violation(invariant="expect", message="x"),),
            workload=(),
            history=found,
            registry_fingerprint="fp",
        )

        strict = entry_from_report(report, strict_behavior=True)
        assert strict.behavioral_fingerprint == behavioral_fingerprint(found)

        # Replaying the same logic → no drift; replaying drifted logic (a changed outcome) → drift.
        assert not strict.behavior_drifted(_history(("pay", "ok"), ("ship", "ok")))
        assert strict.behavior_drifted(_history(("pay", "ok"), ("ship", "error")))

        # The default (structural) entry never claims drift — it can't tell.
        lenient = entry_from_report(report)
        assert lenient.behavioral_fingerprint is None
        assert not lenient.behavior_drifted(_history(("totally", "different")))

    def test_strict_fingerprint_survives_corpus_round_trip(self, tmp_path: object) -> None:
        path = tmp_path / "corpus.jsonl"  # type: ignore[operator]
        entry = RegressionEntry(seed=7, target="app:sim", behavioral_fingerprint="deadbeef")
        append_regression(path, entry)
        assert load_regressions(path) == [entry]


# ....................... #
# End-to-end: a real run's injected faults are recorded by the interceptor and rendered.


class Thing(Document):
    pass


class ThingCreate(CreateDocumentCmd):
    pass


class ThingRead(ReadDocument):
    pass


THING_SPEC = DocumentSpec(
    name="things",
    read=ThingRead,
    write=DocumentWriteTypes(domain=Thing, create_cmd=ThingCreate),
)


@attrs.define(slots=True, kw_only=True)
class _Make(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        await self.ctx.document.command(THING_SPEC).create(ThingCreate())


class TestEndToEndRecording:
    def test_injected_fault_is_recorded_and_rendered(self) -> None:
        registry = OperationRegistry(
            handlers={"make": lambda ctx: _Make(ctx=ctx)},
            descriptors={
                "make": OperationDescriptor(
                    input_type=None, output_type=None, description="x"
                )
            },
        ).freeze()

        sim = Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            # The injected error makes the op a declared failure → operation_succeeds catches it.
            invariants=[operation_succeeds("make")],
        )
        report = sim.run(
            SimulationConfig(
                strategy=Strategy.SCENARIO,
                seeds=range(2),
                act_count=1,
                concurrency=1,
                faults=FaultPolicy(
                    rules=(FaultRule(surface="document_command", error=1.0),)
                ),
            ),
            scenario=Scenario(state=ModelState, act=(Rule(op="make"),)),
        )

        assert report is not None
        rendered = report.format()
        assert "injected environment" in rendered
        assert "error → document_command" in rendered

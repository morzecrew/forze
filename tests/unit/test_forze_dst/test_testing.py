"""Pytest-native DST — ``assert_no_violation`` fails a test with the counterexample, and the
opt-in plugin scales sweeps via ``--dst-seeds`` and registers the ``dst`` marker.
"""

from __future__ import annotations

import attrs
import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig
from forze_dst.invariants import expect, operation_succeeds
from forze_dst.markers import record_event
from forze_dst.testing import assert_no_regressions, assert_no_violation, plugin
from forze_dst.testing._options import (
    CleanSweep,
    DstOptions,
    active,
    drain_clean_sweeps,
    record_clean_sweep,
    set_active,
)
from forze_dst.testing.assertions import _resolve_config
from forze_mock import MockDepsModule
from tests.unit.test_forze_dst._racy_ledger import DepositDTO, racy_sim

# ----------------------- #
# A clean sim (one document-creating op) and a racy sim (lost update under concurrency).


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


def _clean_sim() -> Simulation:
    registry = OperationRegistry(
        handlers={"make": lambda ctx: _Make(ctx=ctx)},
        descriptors={
            "make": OperationDescriptor(input_type=None, output_type=None, description="x")
        },
    ).freeze()
    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        invariants=[operation_succeeds("make")],
    )


_MAKE_SCENARIO = Scenario(state=ModelState, act=(Rule(op="make"),))


@attrs.define(slots=True, kw_only=True)
class _AtomicDeposit(Handler[DepositDTO, None]):
    ledger: dict[str, int]

    async def __call__(self, args: DepositDTO) -> None:
        # No await between read and write → no lost update (the fixed version).
        self.ledger["expected"] += args.amount
        self.ledger["balance"] += args.amount


def _fixed_sim() -> Simulation:
    ledger = {"balance": 0, "expected": 0}
    registry = OperationRegistry(
        handlers={"deposit": lambda _c: _AtomicDeposit(ledger=ledger)},
        descriptors={
            "deposit": OperationDescriptor(
                input_type=DepositDTO, output_type=None, description="x"
            )
        },
    ).freeze()

    async def reset(_ctx: ExecutionContext) -> None:
        ledger["balance"] = ledger["expected"] = 0

    async def observe(_ctx: ExecutionContext) -> None:
        record_event("balance", final=ledger["balance"], expected=ledger["expected"])

    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        setup=reset,
        observe=observe,
        invariants=[
            expect("balance", lambda e: e.fields["final"] == e.fields["expected"],
                   message="lost deposit")
        ],
    )


_DEPOSIT_SCENARIO = Scenario(
    state=ModelState,
    act=(Rule(op="deposit", arg=lambda _state, _rng: DepositDTO(amount=1)),),
)


# ....................... #


class TestAssertNoViolation:
    def test_passes_on_a_clean_simulation(self) -> None:
        # No raise → the test passes, like any other assertion.
        assert_no_violation(
            _clean_sim(),
            SimulationConfig.quick(),
            scenario=_MAKE_SCENARIO,
        )

    def test_fails_with_the_counterexample_on_a_bug(self) -> None:
        with pytest.raises(AssertionError) as excinfo:
            assert_no_violation(
                racy_sim(),
                SimulationConfig(seeds=range(40), act_count=6, concurrency=6),
                scenario=_DEPOSIT_SCENARIO,
            )
        message = str(excinfo.value)
        assert "lost deposit" in message  # the minimized, reproducible report is the message

    def test_defaults_to_thorough_when_no_config(self) -> None:
        # No config → SimulationConfig.thorough() (256 seeds); the racy sim is still caught.
        with pytest.raises(AssertionError, match="lost deposit"):
            assert_no_violation(racy_sim(), scenario=_DEPOSIT_SCENARIO)


class TestSeedOverride:
    def test_resolve_applies_dst_seeds(self) -> None:
        cfg = _resolve_config(SimulationConfig.thorough(), DstOptions(seeds=5))
        assert list(cfg.seeds) == list(range(5))

    def test_resolve_without_options_is_untouched(self) -> None:
        base = SimulationConfig(seeds=range(123))
        assert list(_resolve_config(base, None).seeds) == list(range(123))

    def test_resolve_defaults_to_thorough(self) -> None:
        assert len(list(_resolve_config(None, None).seeds)) == 256

    def test_active_override_flows_through_the_helper(self) -> None:
        # With the plugin's options stashed, the helper honors --dst-seeds (1 clean seed here).
        set_active(DstOptions(seeds=1))
        try:
            assert_no_violation(_clean_sim(), scenario=_MAKE_SCENARIO)
        finally:
            set_active(None)


class TestCleanRunVerdicts:
    """A clean scenario sweep under the plugin records its exclusion bound for the summary."""

    def test_clean_scenario_sweep_records_under_the_plugin(self) -> None:
        set_active(DstOptions())
        try:
            assert_no_violation(_clean_sim(), SimulationConfig.quick(), scenario=_MAKE_SCENARIO)
            records = drain_clean_sweeps()
        finally:
            set_active(None)

        (record,) = records
        assert record.runs == len(SimulationConfig.quick().seeds)
        # The label is this very test (pytest exports PYTEST_CURRENT_TEST), stage suffix stripped.
        assert record.label.endswith("test_clean_scenario_sweep_records_under_the_plugin")
        assert "(call)" not in record.label

    def test_no_recording_without_the_plugin(self) -> None:
        set_active(None)  # plugin not enabled → the helper stays a plain assertion

        assert_no_violation(_clean_sim(), SimulationConfig.quick(), scenario=_MAKE_SCENARIO)

        assert drain_clean_sweeps() == ()

    def test_violation_records_nothing(self) -> None:
        set_active(DstOptions())
        try:
            with pytest.raises(AssertionError, match="lost deposit"):
                assert_no_violation(
                    racy_sim(),
                    SimulationConfig(seeds=range(40), act_count=6, concurrency=6),
                    scenario=_DEPOSIT_SCENARIO,
                )
            assert drain_clean_sweeps() == ()
        finally:
            set_active(None)

    def test_zero_seed_sweep_records_nothing(self) -> None:
        # ``--dst-seeds=0`` runs an empty sweep: it establishes nothing, and a runs=0 record
        # would make the summary's bound undefined — so it must not be recorded at all.
        set_active(DstOptions(seeds=0))
        try:
            assert_no_violation(_clean_sim(), scenario=_MAKE_SCENARIO)
            assert drain_clean_sweeps() == ()
        finally:
            set_active(None)

    def test_set_active_resets_stale_records(self) -> None:
        # A new session (configure) must not inherit a previous in-process session's verdicts.
        record_clean_sweep(CleanSweep(label="stale", runs=10))


        set_active(DstOptions())
        try:
            assert drain_clean_sweeps() == ()
        finally:
            set_active(None)

    def test_terminal_summary_prints_one_scoped_line_per_sweep(self) -> None:
        class _Reporter:
            def __init__(self) -> None:
                self.lines: list[str] = []

            def write_sep(self, sep: str, title: str) -> None:
                self.lines.append(title)

            def write_line(self, line: str) -> None:
                self.lines.append(line)

        record_clean_sweep(CleanSweep(label="test_a", runs=1000))
        record_clean_sweep(CleanSweep(label="test_b", runs=20))
        reporter = _Reporter()

        plugin.pytest_terminal_summary(reporter)

        assert reporter.lines[0] == "DST clean-run verdicts"
        assert reporter.lines[1].startswith("test_a: 0 violations in 1000 seeds")
        assert "< 0.30%" in reporter.lines[1]
        assert reporter.lines[2].startswith("test_b: 0 violations in 20 seeds")
        # Drained: a second summary (or the next session) starts empty.
        assert drain_clean_sweeps() == ()

    @staticmethod
    def _summarize(options: DstOptions, *records: CleanSweep) -> list[str]:
        """Run the terminal summary for *records* under *options* (which resets the buffer, so it
        is installed first), leaving no session state behind."""

        class _Reporter:
            def __init__(self) -> None:
                self.lines: list[str] = []

            def write_sep(self, sep: str, title: str) -> None:
                self.lines.append(title)

            def write_line(self, line: str) -> None:
                self.lines.append(line)

        reporter = _Reporter()
        set_active(options)
        try:
            for record in records:
                record_clean_sweep(record)
            plugin.pytest_terminal_summary(reporter)
        finally:
            set_active(None)

        return reporter.lines

    def test_no_family_verdict_unless_opted_in(self) -> None:
        # Per-sweep non-aggregation is the default and stays: a combined number claims more than
        # any single sweep established, and a family claim over unrelated scenarios is rarely the
        # question anyone is asking.
        lines = self._summarize(
            DstOptions(),
            CleanSweep(label="test_a", runs=1000),
            CleanSweep(label="test_b", runs=1000),
        )

        assert any(line.startswith("test_a:") for line in lines)
        assert not any("simultaneously" in line for line in lines)

    def test_family_verdict_is_wider_than_any_per_sweep_line(self) -> None:
        lines = self._summarize(
            DstOptions(family_verdict=True),
            *(CleanSweep(label=f"test_{i}", runs=1000) for i in range(10)),
        )

        family = next(line for line in lines if "simultaneously" in line)
        assert "0 violations across 10 sweeps" in family
        assert "Bonferroni over 10" in family
        # 0.53% simultaneously against 0.30% per sweep — the price of the joint claim, stated.
        assert "< 0.53%" in family
        assert all("< 0.30%" in line for line in lines if line.startswith("test_"))

    def test_family_verdict_refuses_a_mixed_confidence_session(self) -> None:
        # A simultaneous claim needs one level to state; sweeps bounded at different levels have
        # none, so the line is withheld rather than quietly picking one.
        lines = self._summarize(
            DstOptions(family_verdict=True),
            CleanSweep(label="test_a", runs=1000),
            CleanSweep(label="test_b", runs=1000, confidence=0.99),
        )

        assert any(line.startswith("test_a:") for line in lines)
        assert not any("simultaneously" in line for line in lines)

    def test_terminal_summary_is_silent_with_no_records(self) -> None:
        class _Reporter:
            def __init__(self) -> None:
                self.lines: list[str] = []

            def write_sep(self, sep: str, title: str) -> None:
                self.lines.append(title)

            def write_line(self, line: str) -> None:
                self.lines.append(line)

        reporter = _Reporter()
        plugin.pytest_terminal_summary(reporter)

        assert reporter.lines == []


class TestPluginHooks:
    def test_addoption_registers_dst_seeds(self) -> None:
        recorded: dict[str, object] = {}

        class _Group:
            def addoption(self, name: str, **kwargs: object) -> None:
                recorded[name] = kwargs

        class _Parser:
            def getgroup(self, *_a: object, **_k: object) -> _Group:
                return _Group()

            def addini(self, name: str, *_a: object, **_k: object) -> None:
                recorded[name] = True

        plugin.pytest_addoption(_Parser())
        assert "--dst-seeds" in recorded
        assert "dst_seeds" in recorded

    def test_configure_registers_marker_and_stashes_seeds(self) -> None:
        markers: list[str] = []

        class _Config:
            def addinivalue_line(self, _kind: str, line: str) -> None:
                markers.append(line)

            def getoption(self, _name: str) -> int:
                return 7

            def getini(self, _name: str) -> None:
                return None

        try:
            plugin.pytest_configure(_Config())
            assert any(line.startswith("dst:") for line in markers)
            opts = active()
            assert opts is not None and opts.seeds == 7
        finally:
            set_active(None)

    def test_ini_default_used_when_no_cli_flag(self) -> None:
        class _Config:
            def addinivalue_line(self, _kind: str, _line: str) -> None:
                pass

            def getoption(self, _name: str) -> None:
                return None

            def getini(self, name: str) -> str | None:
                return "12" if name == "dst_seeds" else None

        try:
            plugin.pytest_configure(_Config())
            opts = active()
            assert opts is not None and opts.seeds == 12
        finally:
            set_active(None)

    @staticmethod
    def _family_verdict(cli: bool | None, ini: bool) -> bool:
        """Resolve ``family_verdict`` for one (CLI, ini) pair through the real configure hook."""

        class _Config:
            def addinivalue_line(self, _kind: str, _line: str) -> None:
                pass

            def getoption(self, name: str) -> object:
                return cli if name == "dst_family_verdict" else None

            def getini(self, name: str) -> object:
                return ini if name == "dst_family_verdict" else None

        try:
            plugin.pytest_configure(_Config())
            opts = active()
            assert opts is not None
            return opts.family_verdict
        finally:
            set_active(None)

    def test_the_cli_can_turn_the_family_verdict_off_against_the_ini(self) -> None:
        # A bare store_true flag makes the ini un-overridable: `dst_family_verdict = true` would
        # stick for every run with no way to say *off* for one of them. The flag is tri-state, so
        # an explicit False must not fall through to the ini.
        assert self._family_verdict(cli=False, ini=True) is False

    def test_an_absent_flag_leaves_the_ini_standing(self) -> None:
        assert self._family_verdict(cli=None, ini=True) is True

    def test_the_cli_can_turn_it_on_against_a_silent_ini(self) -> None:
        assert self._family_verdict(cli=True, ini=False) is True

    def test_off_by_default(self) -> None:
        assert self._family_verdict(cli=None, ini=False) is False

    def test_both_flags_are_registered_on_one_dest(self) -> None:
        recorded: dict[str, dict[str, object]] = {}

        class _Group:
            def addoption(self, name: str, **kwargs: object) -> None:
                recorded[name] = kwargs

        class _Parser:
            def getgroup(self, *_a: object, **_k: object) -> _Group:
                return _Group()

            def addini(self, *_a: object, **_k: object) -> None:
                pass

        plugin.pytest_addoption(_Parser())

        on, off = recorded["--dst-family-verdict"], recorded["--no-dst-family-verdict"]
        assert on["dest"] == off["dest"] == "dst_family_verdict"
        # Both default to None, which is what makes "the CLI said nothing" distinguishable from
        # "the CLI said no".
        assert on["default"] is None and off["default"] is None


# ....................... #


class TestBundles:
    def test_save_bundle_writes_a_file_on_violation(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        set_active(DstOptions(save_bundle=str(tmp_path)))
        try:
            with pytest.raises(AssertionError):
                assert_no_violation(
                    racy_sim(),
                    SimulationConfig(seeds=range(40), act_count=6, concurrency=6),
                    scenario=_DEPOSIT_SCENARIO,
                )
        finally:
            set_active(None)

        bundles = list(tmp_path.glob("*.json"))
        assert bundles, "a failing sweep saved no bundle"

    def test_round_trip_replay_refinds_the_bug(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # Save a bundle from the buggy sim, then replay it against the (still buggy) sim.
        set_active(DstOptions(save_bundle=str(tmp_path)))
        try:
            with pytest.raises(AssertionError):
                assert_no_violation(
                    racy_sim(),
                    SimulationConfig(seeds=range(40), act_count=6, concurrency=6),
                    scenario=_DEPOSIT_SCENARIO,
                )
        finally:
            set_active(None)

        with pytest.raises(AssertionError, match="still violates"):
            assert_no_regressions(racy_sim(), bundles=tmp_path)

    def test_replay_passes_against_the_fixed_sim(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # The bundle was found against the racy sim; the fixed sim reproduces it clean.
        set_active(DstOptions(save_bundle=str(tmp_path)))
        try:
            with pytest.raises(AssertionError):
                assert_no_violation(
                    racy_sim(),
                    SimulationConfig(seeds=range(40), act_count=6, concurrency=6),
                    scenario=_DEPOSIT_SCENARIO,
                )
        finally:
            set_active(None)

        # No raise → the regression is fixed.
        assert_no_regressions(_fixed_sim(), bundles=tmp_path)

    def test_empty_dir_is_a_no_op(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        assert_no_regressions(_fixed_sim(), bundles=tmp_path)  # nothing to replay → passes

    @staticmethod
    def _op_case_bundle():  # type: ignore[no-untyped-def]
        # A bundle whose workload is the caller's cases= — a bundle never stores it, so replay
        # cannot reproduce it from seed + config alone.
        from forze_dst.artifacts import FailureBundle
        from forze_dst.artifacts.serialize import config_to_dict
        from forze_dst.config import Strategy

        return FailureBundle(
            seed=0,
            schedule_seed=None,
            target="tests:unused",
            config=config_to_dict(SimulationConfig(strategy=Strategy.OP_CASE, seeds=[0])),
            registry_fingerprint=None,
        )

    def test_op_case_bundle_is_reported_not_crashed(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # Previously an OP_CASE bundle raised a raw ValueError out of dispatch, aborting the whole
        # regression check. Now it is a clear per-bundle failure — never a crash, never a silent pass.
        self._op_case_bundle().save(tmp_path / "opcase.json")

        with pytest.raises(AssertionError, match="not a self-contained"):
            assert_no_regressions(_fixed_sim(), bundles=tmp_path)

    def test_op_case_bundle_does_not_abort_other_bundles(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # An OP_CASE bundle (sorted first) must not short-circuit a real reproducing bundle behind it.
        set_active(DstOptions(save_bundle=str(tmp_path)))
        try:
            with pytest.raises(AssertionError):
                assert_no_violation(
                    racy_sim(),
                    SimulationConfig(seeds=range(40), act_count=6, concurrency=6),
                    scenario=_DEPOSIT_SCENARIO,
                )
        finally:
            set_active(None)

        self._op_case_bundle().save(tmp_path / "aaa_opcase.json")  # sorts before the real bundle

        with pytest.raises(AssertionError) as excinfo:
            assert_no_regressions(racy_sim(), bundles=tmp_path)

        message = str(excinfo.value)
        assert "not a self-contained" in message  # the OP_CASE bundle reported, not crashed
        assert "still violates" in message  # the real bundle behind it was still replayed

    def test_replay_bundle_rejects_op_case_with_clear_error(self) -> None:
        from forze_dst.artifacts import replay_bundle

        with pytest.raises(ValueError, match="not self-contained"):
            replay_bundle(self._op_case_bundle(), load=lambda _t: _fixed_sim())

    def test_bundle_whose_replay_raises_is_reported_not_crashed(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # A self-contained bundle whose replay raises is reported per-bundle (never aborts the
        # batch); a single bundle *file* (not a directory) is also a valid input.
        from forze_dst.artifacts import FailureBundle
        from forze_dst.artifacts.serialize import config_to_dict

        bundle_file = tmp_path / "b.json"
        FailureBundle(
            seed=0,
            schedule_seed=None,
            target="tests:unused",
            config=config_to_dict(SimulationConfig(seeds=[0])),  # SCENARIO → self-replayable
            registry_fingerprint="fp",
        ).save(bundle_file)

        class _RaisingSim:
            def fingerprint(self) -> str:
                return "fp"

            def run(self, _config: object) -> object:
                raise RuntimeError("replay blew up")

        with pytest.raises(AssertionError, match="could not be replayed"):
            assert_no_regressions(_RaisingSim(), bundles=bundle_file)  # type: ignore[arg-type]

    def test_non_reproducing_bundle_with_fingerprint_drift_is_not_a_pass(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # A bundle that no longer reproduces but whose registry fingerprint drifted is not a
        # trustworthy pass — reported as a failure.
        from forze_dst.artifacts import FailureBundle
        from forze_dst.artifacts.serialize import config_to_dict

        bundle_file = tmp_path / "b.json"
        FailureBundle(
            seed=0,
            schedule_seed=None,
            target="tests:unused",
            config=config_to_dict(SimulationConfig(seeds=[0])),
            registry_fingerprint="stale-fingerprint",  # != the fixed sim's
        ).save(bundle_file)

        with pytest.raises(AssertionError, match="fingerprint drifted"):
            assert_no_regressions(_fixed_sim(), bundles=bundle_file)

    def test_plugin_registers_save_bundle_option(self) -> None:
        class _Config:
            def addinivalue_line(self, _kind: str, _line: str) -> None:
                pass

            def getoption(self, name: str) -> str | None:
                return "/tmp/bundles" if name == "--dst-save-bundle" else None

            def getini(self, _name: str) -> None:
                return None

        try:
            plugin.pytest_configure(_Config())
            opts = active()
            assert opts is not None and opts.save_bundle == "/tmp/bundles"
        finally:
            set_active(None)

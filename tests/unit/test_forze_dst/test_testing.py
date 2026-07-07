"""Pytest-native DST — ``assert_no_violation`` fails a test with the counterexample, and the
opt-in plugin scales sweeps via ``--dst-seeds`` and registers the ``dst`` marker.
"""

from __future__ import annotations

import asyncio

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig
from forze_dst.invariants import expect, operation_succeeds
from forze_dst.markers import record_event
from forze_dst.testing import assert_no_regressions, assert_no_violation
from forze_dst.testing._options import DstOptions, active, set_active
from forze_dst.testing.assertions import _resolve_config
from forze_dst.testing import plugin
from forze_mock import MockDepsModule

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


class DepositDTO(BaseModel):
    amount: int


@attrs.define(slots=True, kw_only=True)
class _Deposit(Handler[DepositDTO, None]):
    ledger: dict[str, int]

    async def __call__(self, args: DepositDTO) -> None:
        self.ledger["expected"] += args.amount
        current = self.ledger["balance"]
        await asyncio.sleep(0)  # yield: concurrent deposits race here
        self.ledger["balance"] = current + args.amount


def _racy_sim() -> Simulation:
    ledger = {"balance": 0, "expected": 0}
    registry = OperationRegistry(
        handlers={"deposit": lambda _c: _Deposit(ledger=ledger)},
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
                _racy_sim(),
                SimulationConfig(seeds=range(40), act_count=6, concurrency=6),
                scenario=_DEPOSIT_SCENARIO,
            )
        message = str(excinfo.value)
        assert "lost deposit" in message  # the minimized, reproducible report is the message

    def test_defaults_to_thorough_when_no_config(self) -> None:
        # No config → SimulationConfig.thorough() (256 seeds); the racy sim is still caught.
        with pytest.raises(AssertionError, match="lost deposit"):
            assert_no_violation(_racy_sim(), scenario=_DEPOSIT_SCENARIO)


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


# ....................... #


class TestBundles:
    def test_save_bundle_writes_a_file_on_violation(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        set_active(DstOptions(save_bundle=str(tmp_path)))
        try:
            with pytest.raises(AssertionError):
                assert_no_violation(
                    _racy_sim(),
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
                    _racy_sim(),
                    SimulationConfig(seeds=range(40), act_count=6, concurrency=6),
                    scenario=_DEPOSIT_SCENARIO,
                )
        finally:
            set_active(None)

        with pytest.raises(AssertionError, match="still violates"):
            assert_no_regressions(_racy_sim(), bundles=tmp_path)

    def test_replay_passes_against_the_fixed_sim(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # The bundle was found against the racy sim; the fixed sim reproduces it clean.
        set_active(DstOptions(save_bundle=str(tmp_path)))
        try:
            with pytest.raises(AssertionError):
                assert_no_violation(
                    _racy_sim(),
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
                    _racy_sim(),
                    SimulationConfig(seeds=range(40), act_count=6, concurrency=6),
                    scenario=_DEPOSIT_SCENARIO,
                )
        finally:
            set_active(None)

        self._op_case_bundle().save(tmp_path / "aaa_opcase.json")  # sorts before the real bundle

        with pytest.raises(AssertionError) as excinfo:
            assert_no_regressions(_racy_sim(), bundles=tmp_path)

        message = str(excinfo.value)
        assert "not a self-contained" in message  # the OP_CASE bundle reported, not crashed
        assert "still violates" in message  # the real bundle behind it was still replayed

    def test_replay_bundle_rejects_op_case_with_clear_error(self) -> None:
        from forze_dst.artifacts import replay_bundle

        with pytest.raises(ValueError, match="not self-contained"):
            replay_bundle(self._op_case_bundle(), load=lambda _t: _fixed_sim())

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

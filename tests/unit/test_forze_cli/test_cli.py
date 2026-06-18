"""The ``forze`` CLI — drive and inspect DST from an import string, no driver script.

Commands load a Simulation via ``module:attr`` (here the test module's own ``__name__``, so
the fixtures resolve without temp files) and exercise the turnkey surface: run exploration
(exit 1 on a found bug, 0 when clean), print the reactive topology, and print the derived
scenario.
"""

from __future__ import annotations

import asyncio

import attrs
import pytest
import typer
from pydantic import BaseModel
from typer.testing import CliRunner

from forze.application.contracts.execution import Handler
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry

import forze_cli
from forze_cli._compat import require_dst
from forze_cli.app import app
from forze_cli.dst import _parse_seeds
from forze_cli.loader import load_object, load_simulation
from forze_dst import Simulation, no_duplicate_effect, record_event
from forze_mock import MockDepsModule

# ----------------------- #

runner = CliRunner()


class PayDTO(BaseModel):
    order_id: str


@attrs.define(slots=True, kw_only=True)
class _CreateOrder(Handler[None, str]):
    orders: dict[str, dict]

    async def __call__(self, _args: None) -> str:
        order_id = str(len(self.orders))
        self.orders[order_id] = {"paid": False}
        return order_id


@attrs.define(slots=True, kw_only=True)
class _PayOrder(Handler[PayDTO, None]):
    orders: dict[str, dict]
    atomic: bool

    async def __call__(self, args: PayDTO) -> None:
        order = self.orders[args.order_id]
        if order["paid"]:
            return
        if not self.atomic:
            await asyncio.sleep(0)
        order["paid"] = True
        record_event("charge", order_id=args.order_id)


def _build(*, atomic: bool) -> Simulation:
    orders: dict[str, dict] = {}

    registry = OperationRegistry(
        handlers={
            "create_order": lambda _c: _CreateOrder(orders=orders),
            "pay_order": lambda _c: _PayOrder(orders=orders, atomic=atomic),
        },
        descriptors={
            "create_order": OperationDescriptor(
                input_type=None, output_type=None, description="Create."
            ),
            "pay_order": OperationDescriptor(
                input_type=PayDTO, output_type=None, description="Pay."
            ),
        },
    ).freeze()

    async def reset(_ctx: object) -> None:
        orders.clear()

    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        setup=reset,
        invariants=[no_duplicate_effect("charge", by="order_id")],
    )


# Module-level fixtures resolved by import string (this module is already in sys.modules).
RACY = _build(atomic=False)
CLEAN = _build(atomic=True)

# A producer with no consumer — derives an arrange rule but an empty act phase.
_PRODUCER_ONLY = OperationRegistry(
    handlers={"create_order": lambda _c: _CreateOrder(orders={})},
    descriptors={
        "create_order": OperationDescriptor(
            input_type=None, output_type=None, description="Create."
        )
    },
).freeze()
PRODUCER_ONLY = Simulation(operations=_PRODUCER_ONLY, deps=lambda: MockDepsModule())


def make_racy() -> Simulation:
    return _build(atomic=False)


def _ref(name: str) -> str:
    return f"{__name__}:{name}"


# ....................... #


class TestLoader:
    def test_load_object_resolves_module_attr(self) -> None:
        assert load_object("forze_dst:Simulation") is Simulation

    def test_load_object_rejects_bad_string(self) -> None:
        with pytest.raises(ValueError):
            load_object("no-colon-here")

    def test_load_simulation_accepts_instance_and_callable(self) -> None:
        assert load_simulation(_ref("RACY")) is RACY
        assert isinstance(load_simulation(_ref("make_racy")), Simulation)

    def test_load_simulation_rejects_non_simulation(self) -> None:
        # A non-Simulation, non-callable object → the explicit TypeError.
        with pytest.raises(TypeError):
            load_simulation("forze_dst:DEFAULT_CREATE_VERBS")  # a frozenset


class TestDiscovery:
    def test_callable_returning_non_simulation_raises(self) -> None:
        with pytest.raises(TypeError):
            load_simulation("builtins:dict")  # dict() is neither Simulation nor registry

    def test_discover_ambiguous_simulations(self) -> None:
        # This very module exposes several Simulations (RACY/CLEAN/PRODUCER_ONLY).
        with pytest.raises(ValueError):
            load_simulation(__name__)

    def test_discover_registry_and_none_and_ambiguous(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import types

        from forze_cli import loader

        registry = PRODUCER_ONLY.operations
        one_registry = types.ModuleType("m_one")
        one_registry.r = registry  # type: ignore[attr-defined]
        two_registries = types.ModuleType("m_two")
        two_registries.a = registry  # type: ignore[attr-defined]
        two_registries.b = registry  # type: ignore[attr-defined]
        empty = types.ModuleType("m_empty")

        modules = {"m_one": one_registry, "m_two": two_registries, "m_empty": empty}
        monkeypatch.setattr(loader.importlib, "import_module", lambda name: modules[name])

        assert isinstance(load_simulation("m_one"), Simulation)  # single registry → wrapped
        with pytest.raises(ValueError):
            load_simulation("m_two")  # ambiguous registries
        with pytest.raises(ValueError):
            load_simulation("m_empty")  # nothing to drive

    def test_cwd_is_put_on_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from forze_cli import loader

        sentinel = "/tmp/forze-cli-sentinel-dir"  # nosec B108 - test path, never created
        monkeypatch.setattr(loader.os, "getcwd", lambda: sentinel)
        monkeypatch.setattr(loader.sys, "path", [p for p in __import__("sys").path])
        loader._ensure_cwd_importable()
        assert sentinel in loader.sys.path


class TestParseSeeds:
    def test_forms(self) -> None:
        assert _parse_seeds("5") == [0, 1, 2, 3, 4]
        assert _parse_seeds("3-6") == [3, 4, 5, 6]
        assert _parse_seeds("1,4,9") == [1, 4, 9]

    def test_range_within_comma_list(self) -> None:
        assert _parse_seeds("1-3,5") == [1, 2, 3, 5]

    @pytest.mark.parametrize("spec", ["7-3", "-5", "abc", "", "1,x", "3-"])
    def test_malformed_specs_raise_bad_parameter(self, spec: str) -> None:
        # Reversed range / non-numeric / empty must fail loudly, not crash or
        # silently produce an empty seed set (a false-clean DST run).
        with pytest.raises(typer.BadParameter):
            _parse_seeds(spec)


class TestRun:
    def test_finds_violation_exits_one(self) -> None:
        result = runner.invoke(
            app,
            ["dst", "run", _ref("RACY"), "--act-count", "3", "--concurrency", "3"],
        )
        assert result.exit_code == 1
        assert "DST counterexample" in result.stdout
        assert "no_duplicate_effect" in result.stdout

    def test_clean_exits_zero(self) -> None:
        result = runner.invoke(
            app,
            ["dst", "run", _ref("CLEAN"), "--act-count", "3", "--concurrency", "3"],
        )
        assert result.exit_code == 0
        assert "no violation" in result.stdout

    def test_no_invariants_is_not_a_silent_pass(self) -> None:
        # A Simulation with no invariants must not read as "✓ no violation found".
        result = runner.invoke(app, ["dst", "run", _ref("PRODUCER_ONLY")])
        assert result.exit_code == 0
        assert "no invariants" in result.stdout
        assert "no violation" not in result.stdout

    @pytest.mark.parametrize("strategy", ["scenario", "hypothesis", "dpor"])
    def test_strategies_find_the_bug(self, strategy: str) -> None:
        result = runner.invoke(
            app,
            [
                "dst", "run", _ref("RACY"),
                "--strategy", strategy,
                "--act-count", "3", "--concurrency", "3",
                "--max-examples", "50", "--max-runs", "200",
            ],
        )
        assert result.exit_code == 1
        assert "DST counterexample" in result.stdout

    def test_scenario_with_pct(self) -> None:
        result = runner.invoke(
            app,
            ["dst", "run", _ref("RACY"), "--pct", "--act-count", "3", "--concurrency", "3"],
        )
        assert result.exit_code == 1


class TestRegressionLoop:
    def test_save_and_replay_round_trip(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        from forze_dst import load_regressions

        corpus = str(tmp_path / "regressions.jsonl")

        # Find the bug and save the seed to the corpus.
        found = runner.invoke(
            app,
            [
                "dst", "run", _ref("RACY"),
                "--act-count", "3", "--concurrency", "3",
                "--save-regression", "--regression-file", corpus,
            ],
        )
        assert found.exit_code == 1
        assert "saved seed" in found.stdout

        entries = load_regressions(corpus)
        assert len(entries) == 1
        assert entries[0].target == _ref("RACY")
        assert "no_duplicate_effect" in entries[0].invariants

        # Replaying against the (still buggy) app reproduces the violation → exit 1.
        replay_buggy = runner.invoke(
            app,
            ["dst", "replay", "--regression-file", corpus, "--act-count", "3", "--concurrency", "3"],
        )
        assert replay_buggy.exit_code == 1
        assert "still violate" in replay_buggy.stdout

        # Replaying the same seed against the FIXED app (CLEAN) is clean → exit 0.
        replay_fixed = runner.invoke(
            app,
            [
                "dst", "replay", "--target", _ref("CLEAN"),
                "--regression-file", corpus, "--act-count", "3", "--concurrency", "3",
            ],
        )
        assert replay_fixed.exit_code == 0
        assert "clean" in replay_fixed.stdout

    def test_replay_empty_corpus_is_clean(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        result = runner.invoke(
            app, ["dst", "replay", "--regression-file", str(tmp_path / "absent.jsonl")]
        )
        assert result.exit_code == 0
        assert "no regression seeds" in result.stdout


class TestCoverage:
    def test_clean_app_reports_coverage_and_exits_zero(self) -> None:
        result = runner.invoke(
            app,
            ["dst", "coverage", _ref("CLEAN"), "--seeds", "8", "--plateau", "2",
             "--act-count", "3", "--concurrency", "2"],
        )
        assert result.exit_code == 0
        assert "coverage report" in result.stdout
        assert "behaviors covered" in result.stdout

    def test_violation_exits_one_with_counterexample(self) -> None:
        result = runner.invoke(
            app,
            ["dst", "coverage", _ref("RACY"), "--seeds", "10",
             "--act-count", "3", "--concurrency", "3"],
        )
        assert result.exit_code == 1
        assert "coverage report" in result.stdout
        assert "DST counterexample" in result.stdout


class TestInspect:
    def test_topology(self) -> None:
        result = runner.invoke(app, ["dst", "topology", _ref("RACY")])
        assert result.exit_code == 0
        assert "reactive topology" in result.stdout
        assert "entry points" in result.stdout

    def test_derive(self) -> None:
        result = runner.invoke(app, ["dst", "derive", _ref("RACY")])
        assert result.exit_code == 0
        assert "derived scenario" in result.stdout
        assert "create_order" in result.stdout  # arrange producer
        assert "pay_order" in result.stdout  # act consumer

    def test_derive_empty_act(self) -> None:
        result = runner.invoke(app, ["dst", "derive", _ref("PRODUCER_ONLY")])
        assert result.exit_code == 0
        assert "create_order" in result.stdout  # arrange
        assert "(none)" in result.stdout  # no act rules

    def test_help_lists_the_dst_group(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "dst" in result.output

    def test_version(self) -> None:
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert result.stdout.strip().startswith("forze ")

    def test_dst_help_does_not_require_the_extra(self) -> None:
        # --help is eager: it must render without triggering the require_dst guard.
        result = runner.invoke(app, ["dst", "--help"])
        assert result.exit_code == 0
        assert "run" in result.output and "topology" in result.output


class TestExtras:
    def test_require_dst_passes_when_installed(self) -> None:
        require_dst()  # dst extra present in the dev environment → no raise

    def test_require_dst_exits_when_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "forze_cli._compat.find_spec",
            lambda name: None if name == "hypothesis" else object(),
        )
        with pytest.raises(typer.Exit):
            require_dst()

    def test_main_without_typer_exits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(forze_cli, "find_spec", lambda name: None)
        with pytest.raises(SystemExit):
            forze_cli.main()

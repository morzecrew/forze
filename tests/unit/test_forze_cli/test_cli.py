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

import forze_cli
from forze.application.contracts.execution import Handler
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze_cli._compat import require_dst
from forze_cli.app import app
from forze_cli.dst import _parse_seeds
from forze_cli.loader import load_object, load_simulation
from forze_dst import Simulation
from forze_dst.invariants import no_duplicate_effect
from forze_dst.markers import record_event
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
            load_simulation("forze_dst.derive:DEFAULT_CREATE_VERBS")  # a frozenset


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

    @pytest.mark.parametrize(
        "spec",
        ["7-3", "-5", "abc", "", "1,x", "3-", "1,", ",1", "1,,2", ","],
    )
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
        from forze_dst.artifacts import load_regressions

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

    def test_replay_bad_target_is_reported_not_a_traceback(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # A corpus entry whose target no longer imports (renamed/moved app) must not abort the
        # whole replay with a raw traceback — it is reported and counted, others still run.
        from forze_dst.artifacts import RegressionEntry, append_regression

        corpus = tmp_path / "regressions.jsonl"
        append_regression(corpus, RegressionEntry(seed=0, target="no.such.module:nope"))
        append_regression(corpus, RegressionEntry(seed=1, target=_ref("CLEAN")))

        result = runner.invoke(
            app, ["dst", "replay", "--regression-file", str(corpus)]
        )

        # A clean typer exit (SystemExit), NOT a propagated loader traceback (ImportError/…).
        assert result.exception is None or isinstance(result.exception, SystemExit)
        assert result.exit_code == 1  # the bad target counts as a failure
        assert "could not be loaded" in result.stdout
        assert "1 seed(s) skipped" in result.stdout  # the good CLEAN target still replayed
        # An unloadable target is an unverified ERROR, not a confirmed violation.
        assert "could not be replayed" in result.stdout
        assert "still violate" not in result.stdout

    def test_replay_entry_without_target_is_skipped(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        from forze_dst.artifacts import RegressionEntry, append_regression

        corpus = tmp_path / "regressions.jsonl"
        append_regression(corpus, RegressionEntry(seed=0, target=None))

        result = runner.invoke(app, ["dst", "replay", "--regression-file", str(corpus)])

        assert result.exit_code == 0  # nothing verifiable → clean
        assert "has no saved target" in result.stdout

    def test_replay_seed_run_error_is_an_error_not_a_violation(self, tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        from forze_dst.artifacts import RegressionEntry, append_regression

        corpus = tmp_path / "regressions.jsonl"
        append_regression(corpus, RegressionEntry(seed=0, target="mod:sim"))

        class _RaisingSim:
            def fingerprint(self) -> str:
                return "fp"

            def derive_scenario(self) -> object:
                return object()

            def run(self, _cfg: object, scenario: object = None) -> object:
                raise RuntimeError("kaboom")

        monkeypatch.setattr("forze_cli.dst.load_simulation", lambda _app: _RaisingSim())

        result = runner.invoke(
            app, ["dst", "replay", "--regression-file", str(corpus), "--act-count", "3"]
        )

        assert result.exit_code == 1
        assert "replay raised" in result.stdout
        assert "could not be replayed" in result.stdout
        assert "still violate" not in result.stdout

    def test_replay_warns_on_registry_fingerprint_drift(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        from forze_dst.artifacts import RegressionEntry, append_regression

        corpus = tmp_path / "regressions.jsonl"
        append_regression(
            corpus,
            RegressionEntry(
                seed=1, target=_ref("CLEAN"), registry_fingerprint="stale-fingerprint"
            ),
        )

        result = runner.invoke(
            app,
            ["dst", "replay", "--regression-file", str(corpus), "--act-count", "3"],
        )

        assert "registry changed since saved" in result.stdout


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


def test_pretty_exceptions_never_render_locals() -> None:
    # Typer's default pretty exceptions print a rich locals panel for every frame
    # of an unhandled error — raw values, straight past the framework's log
    # scrubbing. CLI commands drive application code whose frames hold live
    # credentials (OAuth secrets, DSNs, tokens); one crash would print them to the
    # terminal and to whatever captures it. Pretty tracebacks stay; locals never.
    from forze_cli.app import app
    from forze_cli.dst import dst_app

    assert app.pretty_exceptions_show_locals is False
    assert dst_app.pretty_exceptions_show_locals is False


# ....................... #
# `dst campaign` — the detection-time protocol entrypoint over a misuse-corpus registry.

from tests.support.misuse import CONTROLS, CORPUS  # noqa: E402

MINI_CORPUS = tuple(m for m in CORPUS if m.mutant_id == "I1-retry-without-key")
MINI_CONTROLS = tuple(c for c in CONTROLS if c.control_id == "ctrl-retry-with-key")
EMPTY_CORPUS: tuple = ()
NO_KNOBS_CORPUS = (
    attrs.evolve(MINI_CORPUS[0], killing=attrs.evolve(MINI_CORPUS[0].killing, explore=None)),
)


class TestDstCampaignCommand:
    def test_writes_jsonl_and_summary_file(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        out = tmp_path / "records.jsonl"
        summary = tmp_path / "summary.md"

        result = runner.invoke(
            app,
            [
                "dst", "campaign", _ref("MINI_CORPUS"),
                "--controls", _ref("MINI_CONTROLS"),
                "--campaigns", "2", "--ceiling", "4", "--fp-runs", "2",
                "--out", str(out), "--summary", str(summary),
            ],
        )

        assert result.exit_code == 0, result.output
        assert "I1-retry-without-key" in result.output
        assert "ctrl-retry-with-key" in result.output
        lines = out.read_text().splitlines()
        assert '"kind": "meta"' in lines[0]
        assert any('"kind": "campaign"' in line for line in lines)
        assert any('"kind": "false_positive"' in line for line in lines)
        assert "Detection-time campaigns" in summary.read_text()

    def test_without_summary_echoes_the_tables(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        result = runner.invoke(
            app,
            [
                "dst", "campaign", _ref("MINI_CORPUS"),
                "--campaigns", "1", "--ceiling", "3",
                "--out", str(tmp_path / "records.jsonl"),
            ],
        )

        assert result.exit_code == 0, result.output
        assert "Detection-time campaigns" in result.output  # echoed, no controls section

    def test_controls_with_an_empty_corpus_fail_friendly(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        result = runner.invoke(
            app,
            [
                "dst", "campaign", _ref("EMPTY_CORPUS"),
                "--controls", _ref("MINI_CONTROLS"),
                "--out", str(tmp_path / "records.jsonl"),
            ],
        )

        assert result.exit_code != 0
        assert "non-empty corpus" in result.output

    def test_controls_without_recorded_knobs_fail_friendly(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        result = runner.invoke(
            app,
            [
                "dst", "campaign", _ref("NO_KNOBS_CORPUS"),
                "--controls", _ref("MINI_CONTROLS"),
                "--campaigns", "1", "--ceiling", "2",
                "--out", str(tmp_path / "records.jsonl"),
            ],
        )

        assert result.exit_code != 0
        assert "no explore knobs" in result.output


class TestRunSideOutputs:
    def test_fault_and_latency_knobs_build_their_policies(self) -> None:
        # Non-zero knobs exercise the policy builders (the zero default returns None).
        result = runner.invoke(
            app,
            [
                "dst", "run", _ref("CLEAN"),
                "--act-count", "2", "--concurrency", "2", "--seeds", "0-2",
                "--fault-error", "0.2", "--latency", "0.01",
            ],
        )

        assert result.exit_code == 0, result.output

    def test_violation_writes_html_viewer_and_regression_seed(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        html = tmp_path / "viewer.html"
        corpus = tmp_path / "regressions.jsonl"

        result = runner.invoke(
            app,
            [
                "dst", "run", _ref("RACY"),
                "--act-count", "3", "--concurrency", "3",
                "--html", str(html),
                "--save-regression", "--regression-file", str(corpus),
            ],
        )

        assert result.exit_code == 1
        assert "time-travel viewer" in result.stdout
        assert html.exists() and html.stat().st_size > 0
        assert "saved seed" in result.stdout
        assert '"seed"' in corpus.read_text()

    def test_no_confidence_clean_run_still_prints_the_exact_bound(self) -> None:
        # With the confidence report opted out, a clean scenario sweep must still quantify
        # itself — the locked verdict line, never a bare "no violation found".
        result = runner.invoke(
            app,
            [
                "dst", "run", _ref("CLEAN"),
                "--act-count", "2", "--concurrency", "2", "--seeds", "0-4",
                "--no-confidence",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "per-seed detection probability" in result.stdout

"""The DST payments example is a real forze app, and DST finds its double-charge.

Backs ``examples/recipes/dst_payments/app.py`` so it can't silently rot. The handlers use
real document ports (no DST calls); only the Simulation's ``observe`` + invariant are
test-side. Also exercises the ``forze`` CLI end-to-end against it.
"""

from __future__ import annotations

from typer.testing import CliRunner

from forze_cli.app import app
from forze_cli.loader import load_simulation
from forze_dst import no_unexpected_error

from examples.recipes.dst_payments.app import registry, simulation

# ----------------------- #

runner = CliRunner()

_TARGET = "examples.recipes.dst_payments.app:simulation"
_REGISTRY = "examples.recipes.dst_payments.app:registry"
_MODULE = "examples.recipes.dst_payments.app"


class TestExampleDirectly:
    def test_dst_finds_the_double_charge(self) -> None:
        report = simulation.explore_scenario(
            simulation.derive_scenario(), act_count=4, concurrency=4, seeds=range(5)
        )
        assert report is not None
        assert "charged more than once" in report.violations[0].message
        # Minimized to the two racing payments.
        assert [op for op, _ in report.workload] == ["pay_order", "pay_order"]

    def test_topology_recovers_the_event_cascade(self) -> None:
        rmap = simulation.reactive_map()
        assert "notify" in rmap.cascades["pay_order"]  # the cascade
        assert "OrderPaid" in rmap.events["pay_order"]  # the event that carried it
        assert rmap.entry_points() == frozenset({"pay_order"})

    def test_derive_drops_the_reactive_op(self) -> None:
        scenario = simulation.derive_scenario()
        assert {rule.op for rule in scenario.arrange} == {"create_order"}
        assert {rule.op for rule in scenario.act} == {"pay_order"}

    def test_handlers_carry_no_dst_instrumentation(self) -> None:
        # The whole point: DST observes via the engine trace + a test-side observe hook,
        # never via record_event in the production handlers.
        import inspect

        from examples.recipes.dst_payments import app

        source = inspect.getsource(app._PayOrder)
        assert "record_event" not in source
        # And no artificial yield crutch — the real port awaits are the interleaving points.
        assert "sleep(" not in source


class TestViaCLI:
    def test_run_finds_violation(self) -> None:
        result = runner.invoke(
            app,
            ["dst", "run", _TARGET, "--strategy", "dpor", "--act-count", "4", "--concurrency", "4"],
        )
        assert result.exit_code == 1
        assert "DST counterexample" in result.stdout

    def test_topology(self) -> None:
        result = runner.invoke(app, ["dst", "topology", _TARGET])
        assert result.exit_code == 0
        assert "pay_order" in result.stdout and "OrderPaid" in result.stdout

    def test_module_discovery_resolves_simulation(self) -> None:
        assert load_simulation(_MODULE) is simulation

    def test_ad_hoc_registry_gets_the_builtin_safety_net(self) -> None:
        # A bare registry auto-mocks deps AND applies the built-in no-unexpected-error
        # invariant (so it's a real check, not "nothing to check").
        built = load_simulation(_REGISTRY)
        assert built.operations is registry
        names = {getattr(inv, "__qualname__", "") for inv in built.invariants}
        assert any("no_unexpected_error" in n for n in names)

        # The double-charge is a *domain* bug (not a crash), so the safety net alone is clean.
        result = runner.invoke(app, ["dst", "run", _REGISTRY])
        assert result.exit_code == 0
        assert "no violation" in result.stdout


def test_no_unexpected_error_is_exported() -> None:
    assert callable(no_unexpected_error())

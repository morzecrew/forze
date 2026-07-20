"""The DST payments example: faithful transactions make DST's findings trustworthy.

Backs ``examples/recipes/dst_payments/app.py``. The handlers are ordinary forze code (real
document ports, a domain event, a transaction); only the Simulation's ``observe`` + invariant
are test-side. The headline: under the default faithful (journal) transaction manager the
rev-guarded operation is correct (the losing payment rolls back) so DST reports *no*
violation — while the legacy no-op manager would report a *false* double-charge.
"""

from __future__ import annotations

from typer.testing import CliRunner

from examples.recipes.dst_payments.app import (
    _EVENTS,
    _observe,
    registry,
    simulation,
)
from forze_cli.app import app
from forze_cli.loader import load_simulation
from forze_dst import Simulation, SimulationConfig, Strategy
from forze_dst.invariants import expect
from forze_mock import MockDepsModule

# ----------------------- #

runner = CliRunner()

_TARGET = "examples.recipes.dst_payments.app:simulation"
_REGISTRY = "examples.recipes.dst_payments.app:registry"
_MODULE = "examples.recipes.dst_payments.app"

_INVARIANT = [
    expect(
        "payments", lambda e: e.fields["total"] <= 1, message="charged more than once"
    )
]


class TestFaithfulTransactions:
    def test_faithful_default_is_correct_no_false_positive(self) -> None:
        # Default (journal) tx manager: the loser's rev-conflict rolls back its whole
        # transaction (including its payment), so the app is correct → no violation.
        report = simulation.run(
            SimulationConfig(
                strategy=Strategy.SCENARIO, act_count=4, concurrency=4, seeds=range(8)
            ),
            scenario=simulation.derive_scenario(),
        )
        assert report is None

    def test_no_op_tx_surfaces_the_false_positive(self) -> None:
        # The legacy no-op manager does not roll back: the aborted payment persists, so DST
        # reports a (false) double-charge. This is exactly what faithful tx fixes.
        unfaithful = Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(domain_events=_EVENTS, transactions="none"),
            observe=_observe,
            invariants=_INVARIANT,
        )
        report = unfaithful.run(
            SimulationConfig(
                strategy=Strategy.SCENARIO, act_count=4, concurrency=4, seeds=range(5)
            ),
            scenario=unfaithful.derive_scenario(),
        )
        assert report is not None
        assert "charged more than once" in report.violations[0].message


class TestExampleShape:
    def test_topology_recovers_the_event_cascade(self) -> None:
        rmap = simulation.reactive_map()
        assert "notify" in rmap.cascades["pay_order"]
        assert "OrderPaid" in rmap.events["pay_order"]
        assert rmap.entry_points() == frozenset({"pay_order"})

    def test_derive_drops_the_reactive_op(self) -> None:
        scenario = simulation.derive_scenario()
        assert {rule.op for rule in scenario.arrange} == {"create_order"}
        assert {rule.op for rule in scenario.act} == {"pay_order"}

    def test_handlers_carry_no_dst_instrumentation(self) -> None:
        import inspect

        from examples.recipes.dst_payments import app as example

        source = inspect.getsource(example._PayOrder)
        assert "record_event" not in source  # observation is a test-side observe hook
        assert (
            "sleep(" not in source
        )  # the real port awaits are the interleaving points


class TestViaCLI:
    def test_run_is_clean_under_faithful_tx(self) -> None:
        result = runner.invoke(
            app,
            [
                "dst",
                "run",
                _TARGET,
                "--strategy",
                "dpor",
                "--act-count",
                "4",
                "--concurrency",
                "4",
            ],
        )
        assert result.exit_code == 0
        assert "no violation" in result.stdout

    def test_topology(self) -> None:
        result = runner.invoke(app, ["dst", "topology", _TARGET])
        assert result.exit_code == 0
        assert "pay_order" in result.stdout and "OrderPaid" in result.stdout

    def test_module_discovery_resolves_simulation(self) -> None:
        assert load_simulation(_MODULE) is simulation

    def test_ad_hoc_registry_gets_the_builtin_safety_net(self) -> None:
        built = load_simulation(_REGISTRY)
        assert built.operations is registry
        names = {getattr(inv, "__qualname__", "") for inv in built.invariants}
        assert any("no_unexpected_error" in n for n in names)

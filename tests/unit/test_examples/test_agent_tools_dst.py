"""Battery 7: an agent's tool calls hold the same law under the same interleavings.

The contrast is the proof, as in the direct-call DST example. Under one schedule and one
compiled oracle, a palette over a **bare** registry double-books the sprint's capacity —
so the oracle can fire through the bridge — while the same turns against the
**kit-composed** registry hold. A green run without that control would only show that the
simulation reached the store.

What is deliberately *not* asserted here is that `dispatch_tool_use` returns a
`ToolResult`: single-threaded unit tests already prove that, and a simulation that checked
it would be measuring the bridge's plumbing rather than the governance.
"""

from __future__ import annotations

import random

import attrs
import pytest

from examples.recipes.agent_tools_dst.app import (
    CREATE_OP,
    LIST_OP,
    SPRINT_CAPACITY,
    TICKET_SPEC,
    TICKETS,
    TURN_OP,
    AgentTurnInput,
    _turn,
    agent_scenario,
    simulation,
    turn_registry,
)
from forze_dst import Simulation, SimulationConfig, Strategy
from forze_dst.oracle import compile_oracle
from forze_dst.scenario import ModelState
from forze_kits.aggregates.document import build_document_registry
from forze_kits.integrations.agent_tools import operation_tools
from forze_mock import MockDepsModule

pytestmark = pytest.mark.unit

# ----------------------- #

_CONFIG = SimulationConfig(strategy=Strategy.SCENARIO, act_count=8, concurrency=4, seeds=range(10))


def _over(operations) -> Simulation:
    """The example's own simulation with a different registry under the palette.

    `attrs.evolve` rather than a fresh `Simulation`: both halves of the contrast then carry
    the *example's* oracle objects, so a tautological law in the example cannot hide behind
    a correct one built here — the bare half would stop reporting anything.
    """

    return attrs.evolve(simulation, operations=operations)


# ....................... #


class TestTheGovernanceSurvivesToolCalls:
    def test_a_bare_registry_double_books_through_the_bridge(self) -> None:
        # The falsifiability half: with no enforcement composed into the write op, concurrent
        # turns push the sprint past its capacity and DST reports it. Without this, the green
        # run below would be indistinguishable from a simulation that never reached the cap.
        bare = _over(turn_registry(build_document_registry(TICKET_SPEC).freeze()))
        report = bare.run(_CONFIG, scenario=agent_scenario())

        assert report is not None
        # And it fires for *this* law, through the example's own oracle. The violation
        # carries the declared name even though the compiled callable does not, so this is
        # where the oracle's identity is pinned — swap the example's law for a tautology and
        # this stops reporting.
        assert {violation.invariant for violation in report.violations} == {SPRINT_CAPACITY.name}

    def test_the_kit_composed_slice_holds_under_interleaved_turns(self) -> None:
        # Same schedule, same oracle, same palette — the enforcement the kit folded into the
        # write op composes into a tool call because a tool call is an operation invocation.
        # This is the example's own simulation, which is the run the docs point a reader at.
        assert simulation.run(_CONFIG, scenario=agent_scenario()) is None


class TestTheExamplesOwnWiring:
    """The green run above is only evidence while the example's simulation is wired.

    Both halves of the contrast are built by `_simulation_over`, so they share an oracle by
    construction — but the run that the docs tell a reader to reproduce is the example's own
    `simulation`, and an unwired one reports no violation for the most boring reason there
    is. Dropping `observe` or `invariants` from the example fails here rather than passing
    green over there.
    """

    def test_the_oracle_is_attached(self) -> None:
        assert simulation.observe is not None
        assert len(simulation.invariants) > 0

    def test_it_carries_as_many_checks_as_the_law_compiles_to(self) -> None:
        # `compile_oracle` does not wrap its checks with `named`, so a compiled invariant's
        # callable name is the ambiguous `_check` and identity cannot be read off it. Which
        # law it watches is established by the violation the bare half reports; this only
        # pins that nothing was added or dropped.
        assert len(simulation.invariants) == len(compile_oracle(SPRINT_CAPACITY).invariants)

    def test_it_drives_the_governed_registrys_tools(self) -> None:
        # The turn registry the example builds, over the kit's composed registry — not a
        # hand-assembled one that might differ from what `forze dst run` would execute.
        expected = turn_registry(TICKETS.registry(tx_route="mock"))

        assert simulation.operations is not None
        assert simulation.operations.fingerprint() == expected.fingerprint()


class TestTheWorkloadReallyGoesThroughTheBridge:
    def test_the_simulation_drives_only_turns(self) -> None:
        # The governed operations stay out of the driven registry on purpose. Were they in it,
        # the workload could exercise `tickets.create` directly and this file would be
        # measuring the direct path while claiming to measure the tool path.
        assert [str(op) for op in simulation.operations.catalog()] == [TURN_OP]

    def test_every_act_in_the_scenario_is_a_turn(self) -> None:
        assert [rule.op for rule in agent_scenario().act] == [TURN_OP]

    def test_the_palette_can_write(self) -> None:
        # A read-only palette cannot race anything, so the capacity cap would never be
        # approached and the green run would be vacuous.
        palette = operation_tools(
            TICKETS.registry(tx_route="mock"), include=[CREATE_OP, LIST_OP], read_only=False
        )
        create = palette.entry_for(CREATE_OP)

        assert set(palette.names) == {CREATE_OP, LIST_OP}
        assert create is not None
        assert create.is_read_only is False

    def test_the_model_chooses_both_tools_across_seeds(self) -> None:
        # The RNG stands in for the model, so both tools must actually be reachable — a
        # generator that only ever created would leave the read path unexercised, and one
        # that only ever listed would never contend.
        chosen = {_turn(ModelState(), random.Random(seed)).tool for seed in range(40)}

        assert chosen == {CREATE_OP, LIST_OP}


class TestATurnAnswersRatherThanRaises:
    async def test_a_tool_outside_the_palette_comes_back_as_an_error(self) -> None:
        # The handler returns the dispatch's verdict, so a refusal is data the next turn can
        # act on. A raise would abort the simulated workload and report a crash where the
        # agent simply asked for something it was not given.
        from forze.application.execution import ExecutionRuntime
        from forze.application.execution.deps import DepsRegistry
        from forze.application.execution.operations import run_operation

        runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

        async with runtime.scope():
            ctx = runtime.get_context()
            result = await run_operation(
                simulation.operations,
                TURN_OP,
                AgentTurnInput(tool="tickets.kill", input={}),
                ctx,
            )

        assert result == {"is_error": True}

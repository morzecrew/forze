"""A model call inside a governed operation, simulated — and the contrast that proves it.

The green run is only evidence if the law could have broken. So the same workload runs twice
under one schedule and one compiled oracle: over an **ungoverned** aggregate the model's own
weights overload the queue and the oracle fires, while over the **kit-composed** one they do
not. Without the first half, a clean run would be indistinguishable from a simulation whose
model call never happened.

The seam itself is pinned here too: with no predictor registered the operation must fail,
because a handler that quietly computed the answer locally would pass every test above while
proving nothing about the inference plane.
"""

from __future__ import annotations

import random
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import attrs
import pytest

from examples.recipes.llm_triage_dst.app import (
    CREATE_OP,
    HTTP_ROUTE_CAPABILITIES,
    INVARIANT_VIOLATED_CODE,
    ITEM_SPEC,
    ITEMS,
    QUEUE_CAPACITY,
    TRIAGE_OP,
    TRIAGE_SPEC,
    ItemCreate,
    TicketText,
    TriageInput,
    _ticket,
    production_route,
    simulation,
    triage,
    triage_registry,
    triage_scenario,
)
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze.application.execution.deps import DepsRegistry
from forze.application.execution.operations import (
    OperationDescriptor,
    OperationRegistry,
    run_operation,
)
from forze.base.exceptions import CoreException
from forze_dst import Simulation, SimulationConfig, Strategy
from forze_dst.oracle import compile_oracle
from forze_dst.scenario import ModelState
from forze_kits.aggregates import AggregateKit
from forze_mock import MockDepsModule, MockInferenceRegistry

pytestmark = pytest.mark.unit

# ----------------------- #

_CONFIG = SimulationConfig(strategy=Strategy.SCENARIO, act_count=8, concurrency=4, seeds=range(10))


def _over(operations) -> Simulation:  # type: ignore[no-untyped-def]
    """The example's own simulation with a different registry underneath.

    `attrs.evolve` rather than a fresh `Simulation`: both halves then carry the *example's*
    oracle objects, so a tautological law in the example could not hide behind a correct one
    assembled here — the ungoverned half would simply stop reporting.
    """

    return attrs.evolve(simulation, operations=operations)


def _ungoverned() -> Simulation:
    return _over(
        triage_registry(AggregateKit(spec=ITEM_SPEC, invariants=()).registry(tx_route="mock"))
    )


@asynccontextmanager
async def _scope(registry: MockInferenceRegistry | None = None) -> AsyncIterator[ExecutionContext]:
    """One runtime scope over the mock plane, with *registry* answering the model route."""

    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule(inference=registry)).freeze()
    )

    async with runtime.scope():
        yield runtime.get_context()


def _fixed(weight: int) -> MockInferenceRegistry:
    """A model that always answers *weight* — the assertion is about the handler."""

    return MockInferenceRegistry().on(
        TRIAGE_SPEC.name,
        lambda instances: [{"queue_id": "support", "weight": weight} for _ in instances],
    )


# ....................... #


class TestTheModelsAnswerIsGovernedEitherWay:
    def test_an_ungoverned_queue_is_overloaded_by_the_models_own_weights(self) -> None:
        report = _ungoverned().run(_CONFIG, scenario=triage_scenario())

        assert report is not None
        # And it fires for *this* law, through the example's own oracle: swap the example's
        # invariant for a tautology and this stops reporting.
        assert {violation.invariant for violation in report.violations} == {QUEUE_CAPACITY.name}

    def test_the_governed_queue_holds_whatever_the_model_asks_for(self) -> None:
        # Same schedule, same oracle, same model — the aggregate's preventive enforcement is
        # what makes the difference, and this is the run the recipe's docstring points at.
        assert simulation.run(_CONFIG, scenario=triage_scenario()) is None

    def test_the_run_reproduces_from_its_seed(self) -> None:
        # The acceptance the whole recipe exists for: a model call inside a simulation that
        # replays. Two runs of the failing half from the same seeds must produce the same
        # counterexample, or "reproduces" is a claim rather than a property.
        one_seed = attrs.evolve(_CONFIG, seeds=range(4))
        first = _ungoverned().run(one_seed, scenario=triage_scenario())
        second = _ungoverned().run(one_seed, scenario=triage_scenario())

        assert first is not None and second is not None
        assert str(first) == str(second)


class TestTheHandlerReallyCallsTheSeam:
    async def test_an_unprogrammed_route_fails_the_operation(self) -> None:
        """No predictor, no triage — a handler computing the answer itself would pass anyway."""

        async with _scope() as ctx:
            with pytest.raises(CoreException) as ei:
                await run_operation(
                    simulation.operations,
                    TRIAGE_OP,
                    TriageInput(text="printer offline"),
                    ctx,
                )

        assert ei.value.code == "mock.inference.unprogrammed"

    async def test_the_models_answer_is_what_gets_filed(self) -> None:
        async with _scope(_fixed(3)) as ctx:
            filed = await run_operation(
                simulation.operations, TRIAGE_OP, TriageInput(text="anything"), ctx
            )

        assert filed == {"filed": True, "weight": 3}

    async def test_a_queue_that_cannot_take_the_weight_answers_rather_than_raises(self) -> None:
        """The plane's refusal is the caller's verdict to act on, not a crash."""

        async with _scope(_fixed(9)) as ctx:
            results = [
                await run_operation(simulation.operations, TRIAGE_OP, TriageInput(text="x"), ctx)
                for _ in range(2)
            ]

        # 9 fits an empty queue; 18 does not, and the second call says so instead of raising.
        assert results[0]["filed"] is True
        assert results[1] == {"filed": False, "weight": 9, "refused": INVARIANT_VIOLATED_CODE}

    async def test_any_other_failure_propagates(self) -> None:
        """Only the declared law's refusal is data. A catch-all would read a broken store
        as "the queue is full" — absorbing exactly what a simulation exists to surface."""

        class _Broken(Handler[ItemCreate, None]):
            async def __call__(self, args: ItemCreate) -> None:
                raise RuntimeError("the store is gone")

        def _build(ctx: ExecutionContext) -> Handler[ItemCreate, None]:
            _ = ctx

            return _Broken()

        broken = (
            OperationRegistry(handlers={CREATE_OP: _build})
            .set_descriptor(CREATE_OP, OperationDescriptor(input_type=ItemCreate))
            .freeze()
        )
        async with _scope(_fixed(1)) as ctx:
            with pytest.raises(Exception, match="store is gone"):
                await run_operation(triage_registry(broken), TRIAGE_OP, TriageInput(text="x"), ctx)


class TestTheOracleCannotOutCapableTheDeployedRoute:
    def test_the_pinned_capabilities_match_a_real_http_adapter(self) -> None:
        """The mirror in the recipe is asserted against the adapter's own declaration.

        The mock otherwise advertises the full surface, which is more than the chat dialect
        can serve: a batch gate would then pass against the oracle and refuse only in
        production. Written as an assertion rather than a comment so the two cannot drift.
        """

        from forze_inference.http import (
            HttpInferenceAdapter,
            HttpInferenceConfig,
            InferenceHttpClient,
            PromptTemplate,
        )

        config = HttpInferenceConfig(
            protocol="openai_chat",
            model_name="gpt-5",
            prompt=PromptTemplate(template="Triage this ticket:\n\n{text}"),
            temperature=0.0,
            acknowledge_data_egress=True,
        )
        adapter = HttpInferenceAdapter(
            spec=TRIAGE_SPEC,
            # Never called: only the declaration is read.
            client=InferenceHttpClient(),
            config=config,
            protocol=config.wire_protocol(),
        )

        assert adapter.inference_capabilities == HTTP_ROUTE_CAPABILITIES

    def test_the_simulated_route_declares_that_surface(self) -> None:
        assert HTTP_ROUTE_CAPABILITIES.native_batch is False


class TestTheDeployedWiringIsValidForThisSpec:
    def test_the_production_route_builds(self) -> None:
        assert production_route() is not None

    def test_its_prompt_and_output_model_pass_the_routes_own_checks(self) -> None:
        """The recipe's wiring is checked against the recipe's spec, not against a stand-in.

        This is what makes the example's production half more than decoration: a slot the
        input model does not declare, or an output model the structured constraint cannot
        express, fails here.
        """

        from forze_inference.http import HttpInferenceConfig, PromptTemplate

        config = HttpInferenceConfig(
            protocol="openai_chat",
            model_name="gpt-5",
            prompt=PromptTemplate(template="Triage this ticket:\n\n{text}"),
            acknowledge_data_egress=True,
        )

        config.validate_against_spec(TRIAGE_SPEC)

        with pytest.raises(CoreException):
            attrs.evolve(
                config, prompt=PromptTemplate(template="Triage this:\n\n{body}")
            ).validate_against_spec(TRIAGE_SPEC)


class TestTheWorkloadCanActuallyReachTheCap:
    def test_the_simulation_drives_only_triage(self) -> None:
        assert [str(op) for op in simulation.operations.catalog()] == [TRIAGE_OP]

    def test_every_act_in_the_scenario_is_a_triage(self) -> None:
        assert [rule.op for rule in triage_scenario().act] == [TRIAGE_OP]

    def test_it_drives_the_governed_registry(self) -> None:
        expected = triage_registry(ITEMS.registry(tx_route="mock"))

        assert simulation.operations.fingerprint() == expected.fingerprint()

    def test_the_oracle_is_attached_and_whole(self) -> None:
        assert simulation.observe is not None
        assert len(simulation.invariants) == len(compile_oracle(QUEUE_CAPACITY).invariants)

    def test_the_model_answers_with_more_than_one_weight(self) -> None:
        # A triage function that always answered 1 would keep the queue safe by never
        # testing it, and the ungoverned half above would report nothing.
        weights = {answer.weight for answer in triage([TicketText(text=text) for text in _texts()])}

        assert len(weights) > 1
        assert max(weights) > 1

    def test_the_model_is_a_pure_function_of_its_input(self) -> None:
        # The registry's contract, and what makes the replay above exact: re-asking must
        # give the same answer.
        instances = [TicketText(text=text) for text in _texts()]

        assert [a.weight for a in triage(instances)] == [a.weight for a in triage(instances)]

    def test_arriving_tickets_vary_across_seeds(self) -> None:
        arriving = {_ticket(ModelState(), random.Random(seed)).text for seed in range(40)}

        assert len(arriving) > 1


def _texts() -> list[str]:
    return [_ticket(ModelState(), random.Random(seed)).text for seed in range(40)]

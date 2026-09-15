"""A model call inside a governed operation — and simulated, which is the whole point.

`agent_tools_dst` proved an agent's *tool calls* hold a declared law under interleaving. The
model call in the same loop was the hole in that proof: called from a vendor client it is
outside the plane, so it has no deadline, no tenant route, no declared egress — and no way
to be simulated, which is exactly where an agentic flow needs proof most.

Here the model call is an inference route. The handler asks the model to triage a support
ticket and writes the result through a governed aggregate; the invariant is a queue's
capacity, and the model's answer is what feeds it. That ordering is deliberate: the model's
output is **untrusted input to an invariant**, so a run explores whether the plane's
enforcement survives whatever the model says.

In production the route is one wire dialect (:func:`production_route`). Under simulation it
is `MockInferenceAdapter` answering from a pure function, with the *same capabilities the
HTTP route declares* — so a batch the deployed dialect would refuse is refused here too,
instead of passing against an oracle that can do more than the real thing.

Run it (from the repo root)::

    python -m examples.recipes.llm_triage_dst.app   # ✓ no violation

The derived driver works here too, and is worth running — a triage function answers any
string, so nothing has to be scripted for the law to be at risk::

    forze dst run examples.recipes.llm_triage_dst.app:simulation --act-count 6 --concurrency 2
    # ✓ no violation · raced 2/2 operations · at risk 3–3 of 3 runs per invariant

:func:`triage_scenario` exists for a different reason: derived text is arbitrary, and a queue
of recognisable tickets is what makes the weights the model answers with mean anything to a
reader. Both drivers exercise the same invariant.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from typing import TYPE_CHECKING, final

import attrs
import structlog
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.contracts.inference import InferenceCapabilities, InferenceSpec
from forze.application.contracts.invariants import ReadSet, SumOf, SystemInvariant
from forze.application.execution.context import ExecutionContext
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationDescriptor,
    OperationRegistry,
    run_operation,
)
from forze.base.exceptions import CoreException
from forze.base.logging import LogLevel, configure_logging
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import Simulation, SimulationConfig, Strategy
from forze_dst.oracle import compile_oracle
from forze_dst.scenario import ModelState, Rule, Scenario
from forze_kits.aggregates import AggregateKit
from forze_mock import MockDepsModule, MockInferenceRegistry

if TYPE_CHECKING:
    # Imported for the annotation only: the recipe runs (and simulates) without the
    # `inference-http` extra, and the deployed half is what needs it.
    from forze_inference.http import HttpInferenceDepsModule

_LOGGER_NAME = "llm_triage_dst"


def _setup_logging(level: LogLevel) -> None:
    # Render this run's narration and any framework logs cleanly, **only when run as a
    # script** — leaving global logging untouched so imports and tests are unaffected.
    configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze"])


# ----------------------- #
# The inference route: one typed task, no prompt in sight.


# --8<-- [start:spec]
class TicketText(BaseModel):
    """What the model is given: the customer's words, and nothing else."""

    text: str


class Triage(BaseModel):
    """What the model must answer with. No defaults, deliberately: under a structured
    constraint a field cannot be absent, so a default would be unreachable — and the route
    refuses such a model at wiring rather than letting one look meaningful."""

    queue_id: str
    weight: int


TRIAGE_SPEC = InferenceSpec(name="ticket_triage", input=TicketText, output=Triage)
# --8<-- [end:spec]


# ----------------------- #
# Domain — a queue with finite capacity, loaded by whatever the model decides.


class Item(Document):
    queue_id: str = "support"
    weight: int = 1


class ItemCreate(CreateDocumentCmd):
    queue_id: str = "support"
    weight: int = 1


class ItemUpdate(BaseDTO):
    weight: int | None = None


class ItemRead(ReadDocument):
    queue_id: str = "support"
    weight: int = 1


ITEM_SPEC = DocumentSpec(
    name="queue_items",
    read=ItemRead,
    write=DocumentWriteTypes(domain=Item, create_cmd=ItemCreate, update_cmd=ItemUpdate),
)

QUEUE_CAPACITY = SystemInvariant(
    name="queue_capacity",
    read_set=ReadSet(spec=ITEM_SPEC, scope_keys=("queue_id",)),
    aggregate=SumOf("weight"),
    holds=lambda total: total <= 10,
)

ITEMS = AggregateKit(spec=ITEM_SPEC, invariants=(QUEUE_CAPACITY,))

CREATE_OP = str(ITEM_SPEC.default_namespace.key("create"))

INVARIANT_VIOLATED_CODE = "system_invariant_violated"
"""The code the aggregate's preventive enforcement refuses with — the one refusal this
handler answers rather than raises."""

TRIAGE_OP = "support.triage"
"""One ticket, triaged by the model and filed by the plane — see :class:`TriageTicket`."""


# --8<-- [start:handler]
class TriageInput(BaseDTO):
    """A ticket as it arrives: free text, from a channel nobody controls."""

    text: str


@final
@attrs.define(slots=True, kw_only=True)
class TriageTicket(Handler[TriageInput, dict[str, object]]):
    """Ask the model, then file what it said — under the invariant either way.

    Two things are worth reading closely. The handler resolves the route off the live
    context, so the call carries the invocation's deadline, tenant and declared egress;
    swapping the mock for a served model is a wiring change and this code does not move.

    And the model's answer is never trusted: it is a *validated* ``Triage`` (a response that
    does not fit the type fails at the port boundary, not three layers later), and the
    aggregate's preventive enforcement is what keeps a queue inside its capacity when the
    model asks for more.
    """

    ctx: ExecutionContext
    governed: FrozenOperationRegistry

    async def __call__(self, args: TriageInput) -> dict[str, object]:
        triage = await self.ctx.inference.model(TRIAGE_SPEC).predict(TicketText(text=args.text))

        # The declared law's refusal is the caller's to act on: the model decided the
        # weight, the plane decided whether the queue can take it, and returning that
        # verdict is what an agent loop — or an API caller — needs in order to do something
        # else. Only *that* refusal, though. A catch-all here would read a broken store or
        # a wiring fault as "the queue is full", which is the failure a simulation is
        # supposed to surface rather than absorb.
        try:
            await run_operation(
                self.governed,
                CREATE_OP,
                ItemCreate(queue_id=triage.queue_id, weight=triage.weight),
                self.ctx,
            )

        except CoreException as e:
            if e.code != INVARIANT_VIOLATED_CODE:
                raise

            return {"filed": False, "weight": triage.weight, "refused": e.code}

        return {"filed": True, "weight": triage.weight}


# --8<-- [end:handler]


def triage_registry(governed: FrozenOperationRegistry) -> FrozenOperationRegistry:
    """A registry holding one operation: the triage a ticket goes through.

    The governed create stays out of it, so the simulation can only reach the aggregate the
    way production does — through a handler that asked the model first.
    """

    return (
        OperationRegistry(
            handlers={TRIAGE_OP: lambda ctx: TriageTicket(ctx=ctx, governed=governed)}
        )
        .set_descriptor(TRIAGE_OP, OperationDescriptor(input_type=TriageInput))
        .freeze()
    )


# ----------------------- #
# The two halves of the seam: the deployed dialect, and the oracle that stands in for it.


# --8<-- [start:wiring]
def production_route() -> HttpInferenceDepsModule:
    """The deployed route. Not used by the simulation — this is what it stands in for.

    Everything prompt-shaped is wiring: what the model is asked, how the answer is
    constrained, and what sampling it runs at. The handler above passes a typed instance and
    receives a typed one, so none of this reaches it.
    """

    from forze_inference.http import (
        HttpInferenceConfig,
        HttpInferenceDepsModule,
        InferenceHttpClient,
        PromptTemplate,
    )

    return HttpInferenceDepsModule(
        client=InferenceHttpClient(),
        models={
            "ticket_triage": HttpInferenceConfig(
                protocol="openai_chat",
                model_name="gpt-5",
                prompt=PromptTemplate(
                    system=(
                        "You triage support tickets. Answer with the queue that should own "
                        "the ticket and a weight from 1 (trivial) to 4 (urgent)."
                    ),
                    template="Triage this ticket:\n\n{text}",
                ),
                temperature=0.0,
                # A model endpoint is the most egress-shaped call an app makes, and saying so
                # is a reviewed wiring fact rather than a default.
                acknowledge_data_egress=True,
            )
        },
    )


# --8<-- [end:wiring]

HTTP_ROUTE_CAPABILITIES = InferenceCapabilities(
    # What the openai_chat dialect declares: a chat completion answers one prompt, so a
    # batch is N sequential requests rather than one vectorized call.
    native_batch=False,
    supports_stream=True,
    deterministic=False,
)
"""The deployed route's surface, pinned here so the oracle cannot out-capable it.

The mock otherwise advertises everything it can genuinely serve, which is more than any real
adapter — and a capability gate that passes against the oracle then refuses only in
production. ``test_llm_triage_dst.py`` asserts this mirror against a real adapter's own
declaration, so the two cannot drift apart silently.
"""


# --8<-- [start:model]
def triage(instances: Sequence[BaseModel]) -> Sequence[Triage]:
    """The oracle's model: a pure function of the ticket text.

    A real model is not a pure function, but a *simulated* one has to be — the registry's
    contract is purity, because a replay that re-asks a sampling model is not a replay. The
    exploration therefore lives in the workload (which tickets arrive, in what order, racing
    each other), not in this function, which is where a deterministic search can actually
    reason about it.

    The weights it returns are deliberately capable of overloading the queue: an oracle that
    only ever answered ``1`` would keep the invariant safe by never testing it.
    """

    answers: list[Triage] = []

    for instance in instances:
        text = str(getattr(instance, "text", ""))
        answers.append(Triage(queue_id="support", weight=1 + len(text) % 4))

    return answers


# --8<-- [end:model]


def _ticket(state: ModelState, rng: random.Random) -> TriageInput:
    """Which ticket arrives, from the run's seeded RNG.

    The texts differ in length, so they triage to different weights — the model's answers
    vary across a run while staying reproducible from the master seed.
    """

    _ = state

    return TriageInput(
        text=rng.choice(
            [
                "printer offline",
                "cannot log in at all",
                "billing looks wrong on my invoice",
                "urgent: production is down and customers cannot check out",
            ]
        )
    )


def triage_scenario() -> Scenario:
    """Triage only: every act in this workload goes through a model call."""

    return Scenario(state=ModelState, act=(Rule(op=TRIAGE_OP, arg=_ticket),))


# --8<-- [start:simulation]
_ORACLE = compile_oracle(QUEUE_CAPACITY)

simulation = Simulation(
    operations=triage_registry(ITEMS.registry(tx_route="mock")),
    deps=lambda: MockDepsModule(
        inference=MockInferenceRegistry().on(
            TRIAGE_SPEC.name,
            triage,
            capabilities=HTTP_ROUTE_CAPABILITIES,
        )
    ),
    observe=_ORACLE.observe,
    invariants=[*_ORACLE.invariants],
)
# --8<-- [end:simulation]


# ....................... #

if __name__ == "__main__":
    log = structlog.get_logger(_LOGGER_NAME)
    _setup_logging("info")

    # Tickets arriving four at a time, eight acts deep, across ten seeds — each one a model
    # call whose answer the invariant then has to survive.
    violation = simulation.run(
        SimulationConfig(strategy=Strategy.SCENARIO, act_count=8, concurrency=4, seeds=range(10)),
        scenario=triage_scenario(),
    )

    if violation is None:
        log.info("no violation: the queue held its capacity whatever the model asked for")
    else:
        log.error("violation", report=str(violation))
        raise SystemExit(1)  # a violation is a failed verification; `forze dst run` exits 1 too

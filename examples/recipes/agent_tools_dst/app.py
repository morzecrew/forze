"""An agent's tool calls, simulated: the same law, proved to survive the same interleavings.

`aggregate_kit_dst` shows a declared invariant holding under concurrency when the caller is
the operation itself. This example changes exactly one thing — the caller is an **agent**,
choosing a tool and its arguments — and asks whether the governance still holds.

It does, and not because the bridge is careful: because a tool call *is* an operation
invocation. `dispatch_tool_use` goes through `run_operation` on the live context, so the
kit's preventive enforcement composes into it the way it composes into an HTTP route. The
simulation is the proof rather than the claim: concurrent turns race the capacity cap, and
the same compiled oracle that watches the direct-call example watches this one.

What stands in for the model is the run's seeded RNG. It picks which tool to call and what
to pass, so an agent's *choice* is part of what the search explores and reproduces from the
master seed — where a scripted stub would freeze one choice per run and call it coverage.

Run it (from the repo root)::

    python -m examples.recipes.agent_tools_dst.app   # ✓ no violation

``forze dst run`` is the wrong driver here, and instructively so: it derives inputs from the
declared type, which for a turn means tool names no palette holds. Every dispatch is then
refused before it reaches an operation, so nothing races and the law is never at risk — the
run says as much (``vacuous invariant``) rather than reporting a clean bill. A turn's
arguments have to come from the palette, which is what :func:`agent_scenario` is for.

    forze dst topology examples.recipes.agent_tools_dst.app:simulation
"""

from __future__ import annotations

import random
from typing import final

import attrs
import structlog
from pydantic import Field

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.contracts.invariants import ReadSet, SumOf, SystemInvariant
from forze.application.execution.context import ExecutionContext
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationDescriptor,
    OperationRegistry,
)
from forze.base.logging import LogLevel, configure_logging
from forze.base.primitives import JsonDict, current_entropy_source
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import Simulation, SimulationConfig, Strategy
from forze_dst.oracle import compile_oracle
from forze_dst.scenario import ModelState, Rule, Scenario
from forze_kits.aggregates import AggregateKit
from forze_kits.integrations.agent_tools import (
    OperationToolset,
    ToolUse,
    dispatch_tool_use,
    operation_tools,
)
from forze_mock import MockDepsModule

_LOGGER_NAME = "agent_tools_dst"


def _setup_logging(level: LogLevel) -> None:
    # Render this run's narration and any framework logs cleanly, **only when run as a
    # script** — leaving global logging untouched so imports and tests are unaffected.
    configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze"])


# ----------------------- #
# Domain — a sprint's tickets, deliberately the same shape as the direct-call DST example so
# the two differ in the caller and nothing else.


class Ticket(Document):
    sprint_id: str = "S"
    points: int = 1


class TicketCreate(CreateDocumentCmd):
    sprint_id: str = "S"
    points: int = 1


class TicketUpdate(BaseDTO):
    points: int | None = None


class TicketRead(ReadDocument):
    sprint_id: str = "S"
    points: int = 1


TICKET_SPEC = DocumentSpec(
    name="tickets",
    read=TicketRead,
    write=DocumentWriteTypes(domain=Ticket, create_cmd=TicketCreate, update_cmd=TicketUpdate),
)

SPRINT_CAPACITY = SystemInvariant(
    name="sprint_capacity",
    read_set=ReadSet(spec=TICKET_SPEC, scope_keys=("sprint_id",)),
    aggregate=SumOf("points"),
    holds=lambda total: total <= 10,
)

TICKETS = AggregateKit(spec=TICKET_SPEC, invariants=(SPRINT_CAPACITY,))

CREATE_OP = str(TICKET_SPEC.default_namespace.key("create"))
LIST_OP = str(TICKET_SPEC.default_namespace.key("list"))

TURN_OP = "agent.turn"
"""One turn of an agent's loop, as an operation — see :class:`AgentTurn`."""


# --8<-- [start:turn]
class AgentTurnInput(BaseDTO):
    """What the model decided this turn: a tool from the palette, and its arguments."""

    tool: str
    input: JsonDict = Field(default_factory=dict)


@final
@attrs.define(slots=True, kw_only=True)
class AgentTurn(Handler[AgentTurnInput, JsonDict]):
    """Dispatches one tool call through the bridge, on the context it was built with.

    This is the whole adapter between an agent loop and a simulation. It exists because DST
    drives *operations* — both its engines pick an operation by key — and a tool dispatch is
    not one until something makes it one. Wrapping it means the unit the scheduler
    interleaves is a turn, which is what an agent loop is made of.

    The nested ``run_operation`` inside ``dispatch_tool_use`` is a supported shape, not a
    trick: the engine records which task entered the enclosing operation precisely so an
    in-await nested call is recognised and rides the outer admitted slot.
    """

    ctx: ExecutionContext
    tools: OperationToolset

    async def __call__(self, args: AgentTurnInput) -> JsonDict:
        # One id per call, from the replayable entropy seam: a result block is correlated to
        # its call by this id, so reusing one across turns would make two results
        # indistinguishable to the loop. Drawn from the seam, it still reproduces from the seed.
        use_id = f"turn-{current_entropy_source().uuid4()}"

        result = await dispatch_tool_use(
            ToolUse(id=use_id, name=args.tool, input=dict(args.input)),
            ctx=self.ctx,
            tools=self.tools,
        )

        # The whole result, not just its verdict: content is what an agent loop feeds back to
        # the model, and a turn that dropped it would leave the model unable to read what it
        # just did. Returned rather than raised, because a governed refusal is the agent's to
        # act on — a simulation that treated one as a crash would measure the wrong thing.
        return {
            "tool_use_id": result.tool_use_id,
            "is_error": result.is_error,
            "content": result.content,
        }


# --8<-- [end:turn]


# --8<-- [start:registry]
def turn_registry(governed: FrozenOperationRegistry) -> FrozenOperationRegistry:
    """A registry holding one operation: the turn an agent takes.

    The governed operations deliberately stay out of it. A toolset carries the registry it
    dispatches into, so the tools reach *governed* while the simulation drives only turns —
    which means every act in the workload goes through the bridge, with no way to
    accidentally exercise the operation directly and call that a tool call.

    The palette is command-capable on purpose: an agent that cannot write cannot race
    anything, and the capacity cap is only interesting under contention.
    """

    tools = operation_tools(governed, include=[CREATE_OP, LIST_OP], read_only=False)

    return (
        OperationRegistry(handlers={TURN_OP: lambda ctx: AgentTurn(ctx=ctx, tools=tools)})
        .set_descriptor(TURN_OP, OperationDescriptor(input_type=AgentTurnInput))
        .freeze()
    )


# --8<-- [end:registry]


# --8<-- [start:scenario]
def _turn(state: ModelState, rng: random.Random) -> AgentTurnInput:
    """The model's choice, drawn from the run's seeded RNG.

    A create carries points that may or may not fit what the sprint has left, which is what
    makes concurrent turns race the cap; a list is the read an agent takes to decide. Both
    are tools the palette actually holds — an invented name would be refused by the bridge
    and would explore nothing.
    """

    if rng.random() < 0.75:
        return AgentTurnInput(tool=CREATE_OP, input={"points": rng.randint(1, 4)})

    return AgentTurnInput(tool=LIST_OP, input={})


def agent_scenario() -> Scenario:
    """Turns only: every act in this workload goes through the bridge.

    Written out rather than derived, because a derived scenario invents inputs from the
    declared type — which for a turn means tool names no palette contains.
    """

    return Scenario(state=ModelState, act=(Rule(op=TURN_OP, arg=_turn),))


# --8<-- [end:scenario]


# --8<-- [start:simulation]
_ORACLE = compile_oracle(SPRINT_CAPACITY)

simulation = Simulation(
    operations=turn_registry(TICKETS.registry(tx_route="mock")),
    deps=lambda: MockDepsModule(),
    observe=_ORACLE.observe,
    invariants=[*_ORACLE.invariants],
)
# --8<-- [end:simulation]


# ....................... #

if __name__ == "__main__":
    log = structlog.get_logger(_LOGGER_NAME)
    _setup_logging("info")

    # The scenario is the point: turns, with arguments the palette accepts, interleaved four
    # deep across ten seeds. `run` returns the counterexample, or `None` when the law held.
    violation = simulation.run(
        SimulationConfig(strategy=Strategy.SCENARIO, act_count=8, concurrency=4, seeds=range(10)),
        scenario=agent_scenario(),
    )

    if violation is None:
        log.info("no violation: the declared law survived every agent turn")
    else:
        log.error("violation", report=str(violation))

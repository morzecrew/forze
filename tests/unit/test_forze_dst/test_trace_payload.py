"""Production-grade trace enrichment (WS3) — entity key + operation outcome in the trace.

The engine runtime trace now carries, beyond bare metadata, the **entity key** a port call
targets (a document primary key, id-only so no redaction is needed) and the terminal
**outcome / error** of an operation. The harness folds these into the recorded history, so a
DST invariant can assert per-entity properties straight from the engine trace — no handler
instrumentation — and counterexample reports name the entity each step touched.
"""

from __future__ import annotations

from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import (
    BaseDTO,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig, Strategy
from forze_dst.oracle.recorder import History
from forze_mock import MockDepsModule

# ----------------------- #


class Thing(Document):
    value: int = 0


class ThingCreate(CreateDocumentCmd):
    value: int = 0


class ThingUpdate(BaseDTO):
    value: int | None = None


class ThingRead(ReadDocument):
    value: int


THING_SPEC = DocumentSpec(
    name="things",
    read=ThingRead,
    write=DocumentWriteTypes(
        domain=Thing, create_cmd=ThingCreate, update_cmd=ThingUpdate
    ),
)


class BumpCmd(BaseModel):
    target: UUID


@attrs.define(slots=True, kw_only=True)
class _CreateThing(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        thing = await self.ctx.document.command(THING_SPEC).create(ThingCreate())
        return thing.id


@attrs.define(slots=True, kw_only=True)
class _Bump(Handler[BumpCmd, None]):
    ctx: ExecutionContext

    async def __call__(self, args: BumpCmd) -> None:
        thing = await self.ctx.document.query(THING_SPEC).get(args.target)
        await self.ctx.document.command(THING_SPEC).update(
            args.target, thing.rev, ThingUpdate(value=thing.value + 1)
        )


@attrs.define(slots=True, kw_only=True)
class _Boom(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        raise KeyError("boom")  # an unexpected (non-CoreException) failure


_REGISTRY = OperationRegistry(
    handlers={
        "create_thing": lambda ctx: _CreateThing(ctx=ctx),
        "bump": lambda ctx: _Bump(ctx=ctx),
        "boom": lambda ctx: _Boom(ctx=ctx),
    },
    plans={},
    descriptors={
        "create_thing": OperationDescriptor(
            input_type=None, output_type=None, description="x"
        ),
        "bump": OperationDescriptor(
            input_type=BumpCmd, output_type=None, description="x"
        ),
        "boom": OperationDescriptor(input_type=None, output_type=None, description="x"),
    },
).freeze()


def _capture() -> tuple[list[History], object]:
    seen: list[History] = []

    def invariant(history: History) -> list:  # type: ignore[type-arg]
        seen.append(history)
        return []

    return seen, invariant


# ....................... #


def test_trace_carries_entity_key_and_ok_outcome() -> None:
    seen, invariant = _capture()
    scenario = Scenario(
        state=ModelState,
        arrange=(Rule(op="create_thing", produces="thing"),),
        act=(
            Rule(
                op="bump",
                requires=("thing",),
                arg=lambda state, rng: BumpCmd(target=state.pick("thing", rng)),
            ),
        ),
    )
    Simulation(
        operations=_REGISTRY, deps=lambda: MockDepsModule(), invariants=[invariant]
    ).run(
        SimulationConfig(
            strategy=Strategy.SCENARIO, act_count=1, concurrency=1, seeds=range(1)
        ),
        scenario=scenario,
    )

    trace = [event for event in seen[-1].events if event.kind == "trace"]

    # The get and update on the thing both carry the same entity key (its primary key).
    keyed = {
        event.fields["op"]: event.fields.get("key")
        for event in trace
        if event.fields.get("route") == "things"
        and event.fields.get("op") in ("get", "update")
    }
    assert keyed.get("get") is not None
    assert keyed["get"] == keyed["update"]
    UUID(keyed["get"])  # the captured key is the document id

    # The bump operation completed: its engine-trace boundary records outcome=ok.
    completed = [
        event
        for event in trace
        if event.fields.get("trace_domain") == "operation"
        and event.fields.get("op") == "bump"
        and event.fields.get("outcome") == "ok"
    ]
    assert completed


def test_trace_records_operation_error_outcome() -> None:
    seen, invariant = _capture()
    scenario = Scenario(
        state=ModelState,
        arrange=(),
        act=(Rule(op="boom"),),
    )
    Simulation(
        operations=_REGISTRY, deps=lambda: MockDepsModule(), invariants=[invariant]
    ).run(
        SimulationConfig(
            strategy=Strategy.SCENARIO, act_count=1, concurrency=1, seeds=range(1)
        ),
        scenario=scenario,
    )

    trace = [event for event in seen[-1].events if event.kind == "trace"]
    errored = [
        event
        for event in trace
        if event.fields.get("trace_domain") == "operation"
        and event.fields.get("op") == "boom"
        and event.fields.get("outcome") == "error"
    ]
    assert errored
    assert errored[0].fields.get("error") == "KeyError"

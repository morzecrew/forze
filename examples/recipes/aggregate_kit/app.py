"""Recipe: a governed aggregate from one `AggregateKit` declaration (in-process, mock).

A `Task` aggregate that is — from a single declaration — persisted, soft-deletable, kept in an
external search index on every write, guarded by a cross-record invariant (a project's task points
stay within budget), and event-relaying (completing a task publishes `task.completed`). The kit
composes the *wiring*; you still write the four models. It emits the pieces as **separate** artifacts
(`registry()` / `facade()` / `domain_events()` / `lifecycle_steps()`), so the app and backend layers
stay decoupled — here everything runs on the in-memory mock.

Run it:  ``python -m examples.recipes.aggregate_kit.app``
Exercised by ``tests/unit/test_examples/test_aggregate_kit.py``.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Self, cast
from uuid import UUID

import structlog
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.invariants import ReadSet, SumOf, SystemInvariant
from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.search import SearchSpec
from forze.application.execution import ExecutionRuntime, build_runtime
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    run_operation,
)
from forze.application.execution.operations.facade import OperationFacadeFactory
from forze.base.exceptions import CoreException
from forze.base.logging import configure_logging
from forze.base.logging.constants import LogLevel
from forze.base.primitives import JsonDict, StrKey
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import (
    AggregateRoot,
    CreateDocumentCmd,
    DomainEvent,
    ReadDocument,
    event_emitter,
)
from forze_kits.aggregates import AggregateKit
from forze_kits.aggregates.document import (
    DocumentFacade,
    DocumentIdRevDTO,
    DocumentUpdateDTO,
    ListRequestDTO,
)
from forze_kits.aggregates.soft_deletion import SoftDeletionKernelOp
from forze_kits.domain.soft_deletion import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)
from forze_kits.integrations.outbox import (
    EmitMapping,
    OutboxEmit,
    OutboxRelay,
    RelayBinding,
)
from forze_mock import MockDepsModule

_LOGGER_NAME = "aggregate_kit"
log = structlog.get_logger(_LOGGER_NAME)


def _setup_logging(level: LogLevel) -> None:
    configure_logging(level=level, logger_names=[_LOGGER_NAME])


if __name__ == "__main__":
    _setup_logging("info")

_TX = "mock"


# --8<-- [start:domain]
# The four models — the author's, and the only thing the kit cannot invent. `Task` extends the
# soft-delete mixin (the type-level precondition for `soft_delete=True`) and emits an event when it
# is completed; the emitter fires as the governed update persists.
class TaskCompleted(DomainEvent):
    aggregate_id: UUID


class Task(DocWithSoftDeletion, AggregateRoot):
    project_id: str
    title: str
    points: int = 1
    done: bool = False

    @event_emitter(fields={"done"})
    def _on_complete(before, after: Self, diff: JsonDict) -> DomainEvent | None:  # type: ignore[no-untyped-def]
        if after.done and not before.done:
            return TaskCompleted(aggregate_id=after.id)

        return None


class TaskCreate(CreateDocumentCmd):
    project_id: str
    title: str
    points: int = 1


class TaskUpdate(UpdateCmdWithSoftDeletion):
    title: str | None = None
    points: int | None = None
    done: bool | None = None


class TaskRead(ReadDocument):
    project_id: str
    title: str
    points: int = 1
    done: bool = False
    is_deleted: bool = False


# --8<-- [end:domain]


TASK_SPEC = DocumentSpec(
    name="tasks",
    read=TaskRead,
    write=DocumentWriteTypes(domain=Task, create_cmd=TaskCreate, update_cmd=TaskUpdate),
)


class TaskEventPayload(BaseModel):
    task_id: str


# soft_delete + search together: the kit's search reads exclude soft-deleted rows, so the
# index must be able to filter `is_deleted` — declaring it facetable provisions it as a
# filterable attribute on external-index backends (ensure_index).
TASK_INDEX = SearchSpec(
    name="tasks_index",
    model_type=TaskRead,
    fields=["title"],
    facetable_fields={"is_deleted"},
)
TASKS_QUEUE = QueueSpec(name="task-events", codec=PydanticModelCodec(TaskEventPayload))
TASK_EVENTS = OutboxSpec(
    name="task-events",
    codec=PydanticModelCodec(TaskEventPayload),
    destination=OutboxDestination.queue(route="task-events", channel="task-events"),
)

# A cross-record law: a project's total task points stay within budget.
PROJECT_BUDGET = SystemInvariant(
    name="project_budget",
    read_set=ReadSet(spec=TASK_SPEC, scope_keys=("project_id",)),
    aggregate=SumOf("points"),
    holds=lambda total: total <= 10,
)


# --8<-- [start:kit]
# One declaration → the governed slice: persisted + soft-deletable + searchable-and-synced +
# invariant-guarded + event-relaying. The kit composes the wiring; the models stay yours.
TASKS = AggregateKit(
    spec=TASK_SPEC,
    soft_delete=True,
    search=TASK_INDEX,
    invariants=(PROJECT_BUDGET,),
    outbox=OutboxEmit(
        spec=TASK_EVENTS,
        emits=(
            EmitMapping(
                event=TaskCompleted,
                event_type="task.completed",
                to_payload=lambda e: TaskEventPayload(task_id=str(e.aggregate_id)),
            ),
        ),
        relay=RelayBinding(queue_spec=TASKS_QUEUE),
    ),
)
# --8<-- [end:kit]


# --8<-- [start:wiring]
# The kit emits its pieces separately: the staging bridges go on the deps module, the typed facade
# and the frozen registry stay in the app layer. Which store backs it (backend config) stays yours.
def build_stack() -> tuple[
    ExecutionRuntime,
    OperationFacadeFactory[DocumentFacade[TaskRead, TaskCreate, TaskUpdate]],
]:
    module = MockDepsModule(domain_events=TASKS.domain_events())
    runtime = build_runtime(module)
    tasks = TASKS.facade(runtime, tx_route=_TX)
    return runtime, tasks


# --8<-- [end:wiring]


def _key(op: StrKey) -> str:
    return TASK_SPEC.default_namespace.key(op)


async def _live_titles(
    tasks: DocumentFacade[TaskRead, TaskCreate, TaskUpdate],
) -> list[str]:
    page = await tasks.list(ListRequestDTO())
    return sorted(hit.title for hit in page.hits)


async def _demo() -> None:
    runtime, factory = build_stack()
    reg: FrozenOperationRegistry = TASKS.registry(tx_route=_TX)

    async with runtime.scope():
        ctx = runtime.get_context()
        tasks = factory()

        # Create within the project budget (2 + 3 = 5 <= 10).
        design = await tasks.create(
            TaskCreate(project_id="P", title="design", points=2)
        )
        await tasks.create(TaskCreate(project_id="P", title="build", points=3))
        log.info("created two tasks within budget", project="P")

        # The invariant rejects a write that would blow the budget — rolled back, never durable.
        try:
            await tasks.create(TaskCreate(project_id="P", title="oversize", points=9))
        except CoreException as error:
            log.info("over-budget task rejected by the invariant", code=error.code)

        # Complete the task through the governed update: the emitter fires, the event is staged to
        # the outbox in the same transaction, and a relay carries it to the queue.
        completed = await tasks.update(
            DocumentUpdateDTO(id=design.id, rev=design.rev, dto=TaskUpdate(done=True))
        )
        relayed = await OutboxRelay(outbox_spec=TASK_EVENTS).to_queue(ctx, TASKS_QUEUE)
        log.info("task completed and event relayed", published=relayed.published)

        # Soft-delete a task → the generated LIST excludes it (read-side exclusion).
        await run_operation(
            reg,
            _key(SoftDeletionKernelOp.DELETE),
            DocumentIdRevDTO(id=design.id, rev=completed.data.rev),
            ctx,
        )
        log.info("live tasks after soft-delete", titles=await _live_titles(tasks))


async def main(level: LogLevel = "info") -> None:
    _setup_logging(level)
    await _demo()


if __name__ == "__main__":
    chosen = cast(LogLevel, sys.argv[1]) if len(sys.argv) > 1 else "info"
    asyncio.run(main(chosen))

"""Runs the aggregate-kit recipe end to end — proves one `AggregateKit` declaration composes.

Invariant guard + external-search sync + soft-delete read exclusion + outbox relay, all wired by the
single `TASKS = AggregateKit(...)` declaration, exercised over `forze_mock`.
"""

from __future__ import annotations

import pytest

from examples.recipes.aggregate_kit.app import (
    TASK_EVENTS,
    TASK_SPEC,
    TASKS,
    TASKS_QUEUE,
    TaskCreate,
    TaskUpdate,
    build_stack,
)
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kits.aggregates.document.dto import (
    DocumentIdRevDTO,
    DocumentUpdateDTO,
    ListRequestDTO,
)
from forze_kits.aggregates.soft_deletion import SoftDeletionKernelOp
from forze_kits.integrations.outbox import OutboxRelay
from forze_mock import MockStateDepKey

# ----------------------- #

_TX = "mock"


def _key(op) -> str:
    return TASK_SPEC.default_namespace.key(op)


class TestAggregateKitRecipe:
    async def test_invariant_rejects_an_over_budget_create(self) -> None:
        runtime, factory = build_stack()

        async with runtime.scope():
            tasks = factory()
            await tasks.create(TaskCreate(project_id="P", title="a", points=6))

            with pytest.raises(CoreException) as ei:
                await tasks.create(TaskCreate(project_id="P", title="b", points=6))
            assert ei.value.kind is ExceptionKind.DOMAIN  # 12 > 10 budget

    async def test_create_syncs_the_external_search_index(self) -> None:
        runtime, factory = build_stack()

        async with runtime.scope():
            ctx = runtime.get_context()
            task = await factory().create(TaskCreate(project_id="P", title="a", points=1))

            index = ctx.deps.provide(MockStateDepKey).documents.get("tasks_index", {})
            assert task.id in index

    async def test_completion_stages_and_relays_the_event(self) -> None:
        runtime, factory = build_stack()

        async with runtime.scope():
            ctx = runtime.get_context()
            tasks = factory()
            task = await tasks.create(TaskCreate(project_id="P", title="a", points=1))

            await tasks.update(
                DocumentUpdateDTO(id=task.id, rev=task.rev, dto=TaskUpdate(done=True))
            )
            relayed = await OutboxRelay(outbox_spec=TASK_EVENTS).to_queue(ctx, TASKS_QUEUE)
            assert relayed.published == 1  # task.completed reached the queue

    async def test_soft_delete_excludes_from_list(self) -> None:
        runtime, factory = build_stack()
        reg = TASKS.registry(tx_route=_TX)

        async with runtime.scope():
            ctx = runtime.get_context()
            tasks = factory()
            keep = await tasks.create(TaskCreate(project_id="P", title="keep", points=1))
            drop = await tasks.create(TaskCreate(project_id="P", title="drop", points=1))

            await run_operation(
                reg,
                _key(SoftDeletionKernelOp.DELETE),
                DocumentIdRevDTO(id=drop.id, rev=drop.rev),
                ctx,
            )

            page = await tasks.list(ListRequestDTO())
            assert [hit.id for hit in page.hits] == [keep.id]

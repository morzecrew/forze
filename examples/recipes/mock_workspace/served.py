"""The workspace's operation catalog, and the `MockApp` that serves it.

The catalog is deliberately broad — documents, soft deletion, search and storage all
contribute operations — because it is what the reachability gate sweeps. The seed fills
every plane the catalog can read from, so "answers against seeded data" means something.
"""

from __future__ import annotations

from fastapi import APIRouter, FastAPI

from examples.recipes.mock_workspace.app import (
    attachment_spec,
    job_queue,
    programmed_http,
    programmed_inference,
    programmed_procedures,
    project_spec,
    task_index,
    task_spec,
)
from forze.application.execution import ExecutionRuntime
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.lifespan import runtime_lifespan
from forze_fastapi.routes import attach_document_routes
from forze_kits.aggregates.document import build_document_registry
from forze_kits.aggregates.search import build_search_registry
from forze_kits.aggregates.storage import build_storage_registry
from forze_mock import MockDepsModule, MockState
from forze_mock.seeding import QueueSeed, SearchSeed, SeedPlan, StorageSeed, spec_seed
from forze_mock.server import MockApp

# ----------------------- #


# --8<-- [start:catalog]
def build_registry() -> FrozenOperationRegistry:
    """Every plane's operations in one catalog — what the gate sweeps."""

    return (
        build_document_registry(project_spec)
        .merge(build_document_registry(task_spec))
        .merge(build_search_registry(task_index))
        .merge(build_storage_registry(attachment_spec))
        .freeze()
    )


# --8<-- [end:catalog]


registry = build_registry()


def build_app(runtime: ExecutionRuntime) -> FastAPI:
    """The app's own factory — HTTP is a thin projection of the same catalog."""

    router = APIRouter(prefix="/tasks")
    attach_document_routes(
        router,
        registry=registry,
        ns=task_spec.default_namespace,
        ctx_dep=runtime.get_context,
        style="rest",
    )

    app = FastAPI(title="Workspace (mock)", lifespan=runtime_lifespan(runtime))
    app.include_router(router)
    register_exception_handlers(app)

    return app


# --8<-- [start:seed]
def build_seed() -> SeedPlan:
    """One plan, five planes — and the cross-plane links that keep it coherent."""

    return SeedPlan(
        specs=(
            spec_seed(project_spec, count=2, fixtures=({"name": "Apollo"},)),
            # `project_id` is inferred to the `projects` spec from the field name.
            spec_seed(task_spec, count=6, fixtures=({"title": "Write the brief", "points": 3},)),
        ),
        # ...and the index takes its ids from the seeded tasks, so a hit names a real row.
        search=(SearchSeed(spec=task_index, count=4, ids_from="tasks"),),
        storage=(
            StorageSeed(
                spec=attachment_spec,
                objects=({"filename": "brief.txt", "data": "the brief"},),
                count=2,
            ),
        ),
        queues=(QueueSeed(spec=job_queue, channel="jobs", count=3),),
        rng_seed=20260803,
    )


# --8<-- [end:seed]


# --8<-- [start:declaration]
def build_mock() -> MockDepsModule:
    """The mock, with every programmable plane programmed.

    Left unprogrammed, HTTP / inference / procedures raise ``mock.*.unprogrammed`` — which
    is the mock being honest, and exactly what stops a broad app from being answerable
    in-memory. Programming them is this call, not a fake adapter.
    """

    return MockDepsModule(
        state=MockState(),
        http=programmed_http(),
        inference=programmed_inference(),
        procedures=programmed_procedures(),
    )


mock_app = MockApp(build_app=build_app, mock=build_mock(), seed=build_seed())
# --8<-- [end:declaration]

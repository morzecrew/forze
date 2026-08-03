"""The catalog gate: every operation the example registers must be reachable and answer.

RFC 0055 §9. The failure this exists to prevent is a *new plane shipping green* — a spec or
a port the seeder cannot fill, which nobody notices because no test drove it. So the sweep is
over the whole catalog rather than a chosen subset, and the exemption table is read from
`pyproject.toml` and checked **both ways**: an exemption whose operation starts passing must
be deleted, so the table can only shrink.

Two halves, per the RFC's decision 15:

* **reachability** — every operation resolves against the served context (no exemptions; a
  resolution failure is always a wiring bug);
* **invocation** — every non-exempt operation runs against seeded data without raising.

Plus a thin HTTP smoke, so the gate still proves the *server* serves rather than only that
operations resolve.
"""

from __future__ import annotations

import tomllib
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import attrs
import pytest
from pydantic import BaseModel

pytest.importorskip("fastapi")
pytest.importorskip("polyfactory")

from fastapi.testclient import TestClient

from examples.recipes.mock_workspace.served import build_mock, build_seed, mock_app, registry
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import check_wiring, run_operation
from forze.base.exceptions import CoreException
from forze_mock.seeding import SeedResult, apply_seed
from forze_mock.server import build_mock_server

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[3]

# ....................... #


def _manifest() -> dict[str, Any]:
    with (_REPO_ROOT / "pyproject.toml").open("rb") as handle:
        return tomllib.load(handle)["tool"]["mock_gate"]


def _exemptions() -> dict[str, str]:
    return {entry["op"]: entry["reason"] for entry in _manifest().get("exempt", ())}


@pytest.fixture
def seeded() -> Iterator[_World]:
    """A served world: the example's mock, its seed, and a context over both."""

    from forze.application.execution import DepsRegistry, ExecutionRuntime

    mock = build_mock()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(mock).freeze())

    import anyio

    with anyio.from_thread.start_blocking_portal() as portal:

        async def _scoped() -> None:
            async with runtime.scope():
                nonlocal world
                ctx = runtime.get_context()
                world = _World(
                    ctx=ctx, state=mock.state, result=await apply_seed(ctx, build_seed())
                )
                await stop.wait()

        world: _World | None = None
        stop = anyio.Event()
        portal.start_task_soon(_scoped)

        while world is None:  # the scope task sets it before waiting
            portal.call(anyio.sleep, 0)

        yield world

        portal.call(stop.set)


@attrs.define(slots=True)
class _World:
    """The seeded world, re-seedable between operations."""

    ctx: ExecutionContext
    state: Any
    result: SeedResult

    async def reseed(self) -> SeedResult:
        """Back to the pristine seed.

        Between operations, not once for the sweep: `delete` and `update` both address the
        first seeded row, so without this the sweep measures its own ordering — one operation
        removing the row the next one needs.
        """

        self.state.clear()
        self.result = await apply_seed(self.ctx, build_seed())

        return self.result


def _substituted(field: str, annotation: Any, result: SeedResult, namespace: str) -> Any | None:
    """A seeded value for an input field, when the field names something the seed created.

    Blind generation gives a random UUID, and a random UUID is a 404 — so an invocation
    sweep over generated inputs would measure nothing but the miss rate. Ids come from the
    seed instead, which is what "answers against **seeded** data" has to mean.
    """

    if field in {"id", "document_id"}:
        pool = result.ids.get(namespace) or next(iter(result.ids.values()), ())

        return pool[0] if pool else None

    if field == "rev":
        return 1

    if field in {"key", "object_key", "filename"}:
        keys = next(iter(result.stored.values()), ())

        return keys[0] if keys else None

    if field == "return_fields":
        # A projection needs real column names, and "id" is on every read model — the
        # smallest request that is still a legitimate one.
        return ("id",)

    _ = annotation

    return None


def _build_input(op: str, descriptor: Any, result: SeedResult) -> Any:
    """An input for *op*: the natural request, with seeded values where a field names a row.

    Deliberately **not** a fully generated payload. Generating every optional field produces
    a filter on a column that does not exist and a sort on a random string — which the app
    correctly refuses, so the sweep would measure the generator rather than the app. The
    natural request is the one with defaults left alone: "list everything", "get this id".
    """

    input_type = descriptor.input_type if descriptor else None

    if input_type is None:
        return None

    namespace = op.split(".", 1)[0]
    payload: dict[str, Any] = {}

    for field, info in input_type.model_fields.items():
        seeded = _substituted(field, info.annotation, result, namespace)

        if seeded is not None:
            payload[field] = seeded

        elif info.is_required():
            payload[field] = _required_value(info, result)

    return input_type.model_validate(payload)


def _required_value(info: Any, result: SeedResult) -> Any:
    """A plausible value for a required field the seed cannot name."""

    from polyfactory.factories.pydantic_factory import ModelFactory

    annotation = info.annotation

    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        factory = ModelFactory.create_factory(annotation)
        factory.seed_random(0)

        return factory.build()

    _ = result

    return _SCALARS.get(annotation, "seeded")


_SCALARS: dict[Any, Any] = {
    int: 1,
    float: 1.0,
    bool: True,
    str: "seeded",
    UUID: UUID(int=1),
    timedelta: timedelta(minutes=5),
}


# ....................... #


class TestEveryOperationIsReachable:
    def test_the_whole_catalog_resolves(self, seeded: _World) -> None:
        # No exemptions here on purpose: an operation that cannot even be *built* is a
        # wiring bug, never a seeding gap.
        report = check_wiring(registry, lambda: seeded.ctx)

        assert report.ok, report.raise_if_failed()
        assert len(report.checked) == len(registry.catalog())

    def test_the_catalog_spans_more_than_one_plane(self, seeded: _World) -> None:
        # The gate is only worth running over an example broad enough to catch something.
        planes = {str(op).split(".", 1)[0] for op in registry.catalog()}

        assert planes >= {"projects", "tasks", "tasks_index", "attachments"}


class TestEveryPlaneIsActuallySeeded:
    """The assertion that gives §9 its teeth, and the one "answers non-error" does not.

    Deleting the storage seed entirely left the sweep **green**: `upload` makes its own
    object, `list` returns an empty page, and an empty page is a successful answer. So a
    plane nobody seeds ships green under a non-error sweep — exactly the failure the gate
    exists to prevent. Coverage has to be asserted directly: every plane the catalog serves
    must be one the plan fills, and filling it must produce rows.
    """

    def test_every_plane_the_catalog_serves_is_covered_by_the_plan(self) -> None:
        plan = build_seed()
        planes = {str(op).split(".", 1)[0] for op in registry.catalog()}
        covered = {
            str(seed.spec.name)
            for group in (plan.specs, plan.search, plan.storage, plan.queues)
            for seed in group
        }
        # A plane is excused only when *every* one of its operations is — exempting four
        # multipart operations must not quietly excuse the storage plane itself.
        exempt = set(_exemptions())
        required = {
            plane
            for plane in planes
            if any(
                str(op) not in exempt
                for op in registry.catalog()
                if str(op).startswith(f"{plane}.")
            )
        }

        missing = required - covered

        assert not missing, (
            "the catalog serves planes the seed does not fill, so a request against them "
            f"answers with nothing and no test notices: {sorted(missing)}"
        )

    @pytest.mark.asyncio
    async def test_filling_a_plane_actually_produces_rows(self, seeded: _World) -> None:
        # A plan entry with nothing in it covers a plane on paper only.
        result = seeded.result

        assert result.total, "no documents were seeded"
        assert all(ids for ids in result.indexed.values()), "a search index was left empty"
        assert all(keys for keys in result.stored.values()), "a storage spec was left empty"
        assert all(count for count in result.queued.values()), "a queue was left empty"


class TestEveryOperationAnswers:
    @pytest.mark.asyncio
    async def test_non_exempt_operations_run_against_seeded_data(self, seeded: _World) -> None:
        exempt = _exemptions()
        failures: dict[str, str] = {}

        for op, entry in registry.catalog().items():
            key = str(op)

            if key in exempt:
                continue

            result = await seeded.reseed()

            try:
                await run_operation(
                    registry, key, _build_input(key, entry.descriptor, result), seeded.ctx
                )

            except CoreException as error:
                failures[key] = f"[{error.kind}] {error.summary}"

            except Exception as error:
                failures[key] = f"[{type(error).__name__}] {error}"

        assert not failures, (
            "operations did not answer against seeded data — either the seeder cannot fill "
            f"the plane, or the operation needs an exemption with a reason:\n{failures}"
        )

    @pytest.mark.asyncio
    async def test_the_exemption_table_can_only_shrink(self, seeded: _World) -> None:
        # The other direction: an exemption that has started passing is debt already repaid,
        # and leaving it in the table hides the next real gap behind it.
        catalog = {str(op): entry for op, entry in registry.catalog().items()}
        now_passing: list[str] = []

        for op, reason in _exemptions().items():
            assert op in catalog, f"exemption names an operation that no longer exists: {op}"
            assert reason.strip(), f"exemption for {op} has no reason"

            result = await seeded.reseed()

            try:
                await run_operation(
                    registry, op, _build_input(op, catalog[op].descriptor, result), seeded.ctx
                )

            except Exception:
                continue

            now_passing.append(op)

        assert not now_passing, (
            f"these operations now answer and must be removed from [tool.mock_gate]: {now_passing}"
        )


class TestTheServerServesIt:
    def test_the_app_boots_and_answers_over_http(self) -> None:
        # The registry sweep proves operations run; this proves the *server* runs them.
        with TestClient(build_mock_server(mock_app)) as client:
            listed = client.post("/tasks/list", json={})

            assert listed.status_code == 200, listed.text
            assert listed.json()["hits"], "the served app answered with no seeded data"
            assert client.get("/_mock/health").json()["mock"] is True


class TestTheProgrammedPlanesAnswer:
    """Decision 12's other half: programming a plane has to mean it *works*.

    The catalog gate never touches these — HTTP, inference and procedures have no registered
    operations, so a handler with the wrong shape sits there looking programmed. All three of
    these were wrong when first written (a single-instance scorer for a batch port, reversed
    procedure arguments, a raw-request HTTP handler) and only the type checker objected. So
    they are called here, for real.
    """

    @pytest.mark.asyncio
    async def test_the_http_operation_answers_with_its_declared_return_type(
        self, seeded: _World
    ) -> None:
        from examples.recipes.mock_workspace.app import ChargeArgs, ChargeResult, billing_service

        response = await seeded.ctx.http.service(billing_service).invoke(
            "charge", ChargeArgs(amount=250)
        )

        assert isinstance(response, ChargeResult)
        assert (response.status, response.amount) == ("charged", 250)

    @pytest.mark.asyncio
    async def test_the_inference_route_scores_a_batch_in_order(self, seeded: _World) -> None:
        from examples.recipes.mock_workspace.app import TaskFeatures, TaskPriority, priority_spec

        port = seeded.ctx.inference.model(priority_spec)

        single = await port.predict(TaskFeatures(points=5))
        assert isinstance(single, TaskPriority)
        assert single.score == 0.5

        # The port is batch-shaped; a handler taking one instance passes the call above by
        # accident and breaks here.
        batch = await port.predict_many([TaskFeatures(points=2), TaskFeatures(points=30)])
        assert [item.score for item in batch] == [0.2, 1.0]

    @pytest.mark.asyncio
    async def test_the_procedure_reports_its_effect(self, seeded: _World) -> None:
        from examples.recipes.mock_workspace.app import RecalculateParams, recalculate_spec

        result = await seeded.ctx.procedure.command(recalculate_spec).run(
            RecalculateParams(project_id="any")
        )

        # Side-effect-only (`result=None` on the spec), so the count is what comes back —
        # and it counts the tasks the seed actually created.
        assert result.affected_count == len(seeded.result["tasks"])

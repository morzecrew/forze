"""Recipe: a broad app on the mock — five planes, every programmable one programmed.

The `mock_server` recipe shows the *swap*; this one shows the *reach*. A small workspace
domain spans documents, search, storage, a queue and outbound HTTP, and the planes the mock
deliberately leaves unprogrammed — HTTP, inference, procedures — are programmed here rather
than left to raise. That is what makes an app fully answerable on in-memory backends, and it
is what the catalog-reachability gate in `tests/unit/test_examples/test_mock_workspace.py`
checks: every registered operation must resolve and answer against seeded data.

Programming a plane is a small typed handler, not a fake adapter — and typed is the point:
the inference port is **batch**-shaped, so a handler written to take one instance is wrong
in a way only a real call reveals.

    MockInferenceRegistry().on("priority", lambda batch: [score(item) for item in batch])

Run it:  ``just serve``  (from examples/recipes/mock_workspace/)
Exercised by ``tests/unit/test_examples/test_mock_workspace.py``.
"""

from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.http import HttpOperationSpec, HttpServiceSpec
from forze.application.contracts.inference import InferenceSpec
from forze.application.contracts.procedure import ExecResult, ProcedureSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import StorageSpec
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockState
from forze_mock.adapters import (
    MockHttpRegistry,
    MockInferenceRegistry,
    MockProcedureRegistry,
)

# ----------------------- #


# --8<-- [start:domain]
class Project(Document):
    name: str = ""


class ProjectCreate(CreateDocumentCmd):
    name: str = ""


class ProjectUpdate(BaseDTO):
    name: str | None = None


class ProjectRead(ReadDocument):
    name: str = ""


class Task(Document):
    title: str = ""
    project_id: UUID | None = None
    points: int = 0


class TaskCreate(CreateDocumentCmd):
    title: str = ""
    project_id: UUID | None = None
    points: int = 0


class TaskUpdate(BaseDTO):
    title: str | None = None
    points: int | None = None


class TaskRead(ReadDocument):
    title: str = ""
    project_id: UUID | None = None
    points: int = 0


project_spec = DocumentSpec(
    name="projects",
    read=ProjectRead,
    write=DocumentWriteTypes(domain=Project, create_cmd=ProjectCreate, update_cmd=ProjectUpdate),
)

task_spec = DocumentSpec(
    name="tasks",
    read=TaskRead,
    write=DocumentWriteTypes(domain=Task, create_cmd=TaskCreate, update_cmd=TaskUpdate),
)
# --8<-- [end:domain]


# --8<-- [start:planes]
class IndexedTask(BaseModel):
    id: str = ""
    title: str = ""


class JobMessage(BaseModel):
    task_id: str = ""
    kind: str = "reindex"


class TaskFeatures(BaseModel):
    points: int = 0


class TaskPriority(BaseModel):
    score: float = 0.0


task_index = SearchSpec(name="tasks_index", model_type=IndexedTask, fields=["title"])
attachment_spec = StorageSpec(name="attachments")
job_queue = QueueSpec(name="jobs", codec=PydanticModelCodec(model_type=JobMessage))
priority_spec = InferenceSpec(name="priority", input=TaskFeatures, output=TaskPriority)


class ChargeArgs(BaseModel):
    amount: int = 0


class ChargeResult(BaseModel):
    status: str = ""
    amount: int = 0


class RecalculateParams(BaseModel):
    project_id: str = ""


billing_service = HttpServiceSpec(
    name="billing",
    operations={
        "charge": HttpOperationSpec(
            name="charge",
            method="POST",
            path="/charges",
            args_type=ChargeArgs,
            return_type=ChargeResult,
        ),
    },
)

recalculate_spec: ProcedureSpec[RecalculateParams, int] = ProcedureSpec(
    name="recalculate",
    params=RecalculateParams,
)
# --8<-- [end:planes]


# --8<-- [start:programmed]
def _charge(args: BaseModel | None) -> ChargeResult:
    """One outbound HTTP operation, answered in-process.

    The handler receives the **validated args model** (or ``None`` for an operation with no
    body), not a raw request — so this is ordinary typed code, and a wrong field name is a
    type error rather than a mystery at call time.
    """

    amount = args.amount if isinstance(args, ChargeArgs) else 0

    return ChargeResult(status="charged", amount=amount)


def programmed_http() -> MockHttpRegistry:
    """Outbound HTTP, answered in-process.

    Unprogrammed, an `HttpServicePort` call raises ``mock.http.unprogrammed`` — correct, and
    the reason a broad app is not answerable on the mock until someone writes these.
    """

    return MockHttpRegistry().on(billing_service.name, "charge", _charge)


def _score(instances: Sequence[BaseModel]) -> Sequence[TaskPriority]:
    """Score a **batch** — one prediction per instance, in order.

    The port is batch-shaped even when a caller predicts a single row, so a handler written
    to take one instance is wrong in a way only a real call reveals.
    """

    return [
        TaskPriority(score=min(1.0, item.points / 10))
        for item in instances
        if isinstance(item, TaskFeatures)
    ]


def programmed_inference() -> MockInferenceRegistry:
    """A pure scoring function per route — purity is what keeps a replay exact."""

    return MockInferenceRegistry().on(priority_spec.name, _score)


def _recalculate(params: BaseModel, state: MockState) -> ExecResult[int]:
    """A governed procedure, modelled as its effect on the mock's state.

    Params first, state second, and an ``ExecResult`` back: this spec declares
    ``result=None``, so it is side-effect-only and reports an affected count.
    """

    _ = params

    return ExecResult(affected_count=len(state.documents.get("tasks", {})))


def programmed_procedures() -> MockProcedureRegistry:
    """Program the procedure plane; unprogrammed it raises ``mock.procedures.unprogrammed``."""

    return MockProcedureRegistry().on(recalculate_spec.name, _recalculate)


# --8<-- [end:programmed]

"""Recipe: a broad app on the mock — five planes, every programmable one programmed.

The `mock_server` recipe shows the *swap*; this one shows the *reach*. A small workspace
domain spans documents, search, storage, a queue and outbound HTTP, and the planes the mock
deliberately leaves unprogrammed — HTTP, inference, procedures — are programmed here rather
than left to raise. That is what makes an app fully answerable on in-memory backends, and it
is what the catalog-reachability gate in `tests/unit/test_examples/test_mock_workspace.py`
checks: every registered operation must resolve and answer against seeded data.

Programming a plane is a two-line registry, not a fake adapter:

    MockInferenceRegistry().on("priority", lambda features: {"score": ...})

Run it:  ``just serve``  (from examples/recipes/mock_workspace/)
Exercised by ``tests/unit/test_examples/test_mock_workspace.py``.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.http import HttpOperationSpec, HttpServiceSpec
from forze.application.contracts.inference import InferenceSpec
from forze.application.contracts.procedure import ProcedureSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import StorageSpec
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

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

recalculate_spec = ProcedureSpec(name="recalculate", params=RecalculateParams)
# --8<-- [end:planes]


# --8<-- [start:programmed]
def programmed_http() -> Any:
    """Outbound HTTP, answered in-process.

    Unprogrammed, an `HttpServicePort` call raises ``mock.http.unprogrammed`` — correct, and
    the reason a broad app is not answerable on the mock until someone writes these.
    """

    from forze_mock.adapters import MockHttpRegistry

    return MockHttpRegistry().on(
        billing_service.name,
        "charge",
        lambda request: {"status": "charged", "amount": request.json.get("amount", 0)},
    )


def programmed_inference() -> Any:
    """A pure scoring function per route — purity is what keeps a replay exact."""

    from forze_mock.adapters import MockInferenceRegistry

    return MockInferenceRegistry().on(
        priority_spec.name,
        lambda features: TaskPriority(score=min(1.0, features.points / 10)),
    )


def programmed_procedures() -> Any:
    """A governed procedure, modelled as its effect on the mock's state."""

    from forze_mock.adapters import MockProcedureRegistry

    return MockProcedureRegistry().on(
        recalculate_spec.name,
        lambda state, params: {"recalculated": len(state.documents.get("tasks", {}))},
    )


# --8<-- [end:programmed]

"""Quickstart: a minimal in-memory CRUD service for a ``User`` aggregate.

Run it:   uv run uvicorn examples.quickstart.app:app --reload
Exercised by tests/unit/test_examples/test_quickstart.py (FastAPI TestClient, no Docker).
"""

from __future__ import annotations

from uuid import UUID

from fastapi import FastAPI
from pydantic import computed_field

from forze.application.contracts.document import DocumentSpec
from forze.application.execution import build_runtime
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi import runtime_lifespan
from forze_fastapi.exceptions import register_exception_handlers
from forze_kits.aggregates.document import (
    DocumentDTOs,
    DocumentFacade,
    DocumentIdDTO,
    ListRequestDTO,
    build_document_registry,
)
from forze_kits.dto import Paginated
from forze_mock import MockDepsModule


# --8<-- [start:domain]
class User(Document):
    name: str
    email: str | None = None


class CreateUserCmd(CreateDocumentCmd):
    name: str
    email: str | None = None


class ReadUser(ReadDocument):
    name: str
    email: str | None = None

    @computed_field
    @property
    def email_provided(self) -> bool:
        return self.email is not None


# --8<-- [end:domain]


# --8<-- [start:spec]
user_spec = DocumentSpec(
    name="users",
    read=ReadUser,
    write={
        "domain": User,
        "create_cmd": CreateUserCmd,
    },
)
# --8<-- [end:spec]


# --8<-- [start:registry]
registry = build_document_registry(
    user_spec, DocumentDTOs(read=ReadUser, create=CreateUserCmd)
).freeze()
# --8<-- [end:registry]


# --8<-- [start:runtime]
runtime = build_runtime(MockDepsModule())
# --8<-- [end:runtime]


def users() -> DocumentFacade[ReadUser, CreateUserCmd, BaseDTO]:
    return DocumentFacade(
        ctx=runtime.get_context(),
        registry=registry,
        namespace=user_spec.default_namespace,
    )


# --8<-- [start:routes]
app = FastAPI(title="Users API", lifespan=runtime_lifespan(runtime))
register_exception_handlers(app)  # CoreException → HTTP (e.g. not_found → 404)


@app.post("/users")
async def create_user(cmd: CreateUserCmd) -> ReadUser:
    return await users().create(cmd)


@app.get("/users/{user_id}")
async def get_user(user_id: UUID) -> ReadUser:
    return await users().get(DocumentIdDTO(id=user_id))


@app.get("/users")
async def list_users(page: int = 1, size: int = 10) -> Paginated[ReadUser]:
    return await users().list(ListRequestDTO(page=page, size=size))


@app.delete("/users/{user_id}", status_code=204)
async def delete_user(user_id: UUID) -> None:
    await users().kill(DocumentIdDTO(id=user_id))


# --8<-- [end:routes]

"""Quickstart: a minimal in-memory CRUD service for a ``User`` aggregate.

Run it:   uv run uvicorn examples.quickstart.app:app --reload
Exercised by tests/unit/test_examples/test_quickstart.py (FastAPI TestClient, no Docker).
"""

from __future__ import annotations

# --8<-- [start:imports]
from uuid import UUID

from fastapi import FastAPI
from pydantic import computed_field

from forze import (
    CreateDocumentCmd,
    Document,
    DocumentSpec,
    DocumentWriteTypes,
    ReadDocument,
    build_runtime,
)
from forze_fastapi import runtime_lifespan
from forze_fastapi.exceptions import register_exception_handlers
from forze_kits import Paginated, build_document_registry, document_facade
from forze_kits.aggregates.document import DocumentIdDTO, ListRequestDTO
from forze_mock import MockDepsModule

# --8<-- [end:imports]


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
    write=DocumentWriteTypes(domain=User, create_cmd=CreateUserCmd),
)
# --8<-- [end:spec]


# --8<-- [start:registry]
# DTOs are derived from the spec; pass an explicit DocumentDTOs only to override.
registry = build_document_registry(user_spec).freeze()
# --8<-- [end:registry]


# --8<-- [start:runtime]
runtime = build_runtime(MockDepsModule())

# A per-call, fully-typed facade factory bound to the runtime's current context.
users = document_facade(runtime, registry, user_spec)
# --8<-- [end:runtime]


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

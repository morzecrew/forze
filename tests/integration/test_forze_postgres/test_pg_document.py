import pytest
from uuid import UUID

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


# Domain Definitions
class MyDoc(Document):
    name: str


class MyCreateDoc(CreateDocumentCmd):
    name: str


class MyUpdateDoc(BaseDTO):
    name: str | None = None


class MyReadDoc(ReadDocument):
    name: str


@pytest.fixture
def execution_context(pg_client: PostgresClient):
    configurable = ConfigurablePostgresDocument(
        config={
            "read": ("public", "my_docs"),
            "write": ("public", "my_docs"),
            "bookkeeping_strategy": "application",
        }
    )
    deps = Deps.plain(
        {
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            DocumentQueryDepKey: configurable,
            DocumentCommandDepKey: configurable,
        }
    )
    return ExecutionContext(deps=deps)


@pytest.mark.asyncio
async def test_postgres_document_adapter(
    pg_client: PostgresClient, execution_context: ExecutionContext
):
    await pg_client.execute(
        """
        CREATE TABLE my_docs (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )

    spec = DocumentSpec(
        name="my_docs_ns",
        read=MyReadDoc,
        write={
            "domain": MyDoc,
            "create_cmd": MyCreateDoc,
            "update_cmd": MyUpdateDoc,
        },
    )

    adapter = execution_context.doc_command(spec)

    # CREATE
    create_dto = MyCreateDoc(name="test item")
    doc = await adapter.create(create_dto)

    assert doc.name == "test item"
    assert doc.rev == 1
    assert isinstance(doc.id, UUID)

    # READ
    read_doc = await adapter.get(doc.id)
    assert read_doc.id == doc.id
    assert read_doc.name == "test item"
    assert read_doc.rev == 1

    # UPDATE
    update_dto = MyUpdateDoc(name="updated item")
    updated_doc = await adapter.update(doc.id, doc.rev, update_dto)
    assert updated_doc.name == "updated item"
    assert updated_doc.rev == 2

    # TOUCH
    touched_doc = await adapter.touch(doc.id)
    assert touched_doc.name == "updated item"
    assert touched_doc.rev == 3

    # DELETE
    # Note: Using adapter.kill() for hard delete. Soft delete requires deleted_at in MyDoc
    await adapter.kill(doc.id)

    res = await pg_client.fetch_all("SELECT * FROM my_docs")
    assert len(res) == 0

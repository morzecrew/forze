import pytest
from uuid import UUID

from forze.application.contracts.document.specs import DocumentSpec
from forze.application.execution import ExecutionContext, Deps
from forze.domain.models import Document, CreateDocumentCmd, ReadDocument, BaseDTO

from forze_postgres.kernel.platform.client import PostgresClient
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.deps import postgres_document_configurable


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
    deps = Deps(
        {
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
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
        namespace="my_docs_ns",
        read={"source": "my_docs", "model": MyReadDoc},
        write={
            "source": "my_docs",
            "models": {
                "domain": MyDoc,
                "create_cmd": MyCreateDoc,
                "update_cmd": MyUpdateDoc,
            },
        },
    )

    factory = postgres_document_configurable(rev_bump_strategy="application")
    adapter = factory(execution_context, spec)

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
    updated_doc = await adapter.update(doc.id, update_dto, rev=doc.rev)
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

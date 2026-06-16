"""Integration test: equality search over a deterministically-encrypted Mongo field."""

from uuid import uuid4

import pytest

from forze.application.contracts.crypto import (
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import CryptoDepsModule, Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockKeyManagement
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _Person(Document):
    name: str
    email: str


class _PersonCreate(CreateDocumentCmd):
    name: str
    email: str


class _PersonUpdate(BaseDTO):
    name: str | None = None
    email: str | None = None


class _PersonRead(ReadDocument):
    name: str
    email: str


def _ctx(mongo_client: MongoClient, db: str, collection: str) -> ExecutionContext:
    fac = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db, collection), write=(db, collection))
    )
    deps = Deps.merge(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="people-cmk")),
            deterministic_root=b"searchable-root-secret-32-bytes!",
        )(),
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            }
        ),
    )
    return context_from_deps(deps)


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_equality_search_on_encrypted_field(
    mongo_client: MongoClient,
) -> None:
    db = (await mongo_client.db()).name
    collection = f"people_{uuid4().hex[:8]}"
    spec = DocumentSpec(
        name="people_ns",
        read=_PersonRead,
        write={  # type: ignore[arg-type]
            "domain": _Person,
            "create_cmd": _PersonCreate,
            "update_cmd": _PersonUpdate,
        },
        encryption=FieldEncryption(searchable=frozenset({"email"})),
    )

    ctx = _ctx(mongo_client, db, collection)
    await ctx.document.command(spec).create(
        _PersonCreate(name="Alice", email="alice@example.com")
    )
    await ctx.document.command(spec).create(
        _PersonCreate(name="Bob", email="bob@example.com")
    )

    page = await ctx.document.query(spec).find_page(
        filters={"$values": {"email": "alice@example.com"}},
    )
    assert page.count == 1
    assert page.hits[0].name == "Alice"
    assert page.hits[0].email == "alice@example.com"

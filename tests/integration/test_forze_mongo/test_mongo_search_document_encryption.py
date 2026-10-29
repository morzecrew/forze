"""A document's sealed field, read back by an in-place Mongo text search (real Mongo).

The document port binds each ciphertext to the document spec's name, so the search opens it
only when its spec's ``sealed_by`` names that document; without it the row is refused. The
search runs in its own context (its own keyring), so the read is cold.
"""

from uuid import uuid4

import pytest

from forze.application.contracts.crypto import FieldEncryption, KeyRef, StaticKeyDirectory
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import CryptoDepsModule, Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockKeyManagement
from forze_mongo.execution.deps import (
    ConfigurableMongoDocument,
    ConfigurableMongoSearch,
    MongoDocumentConfig,
)
from forze_mongo.execution.deps.configs import MongoSearchConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.integration

# ----------------------- #


class _Note(Document):
    title: str
    secret: str


class _NoteCreate(CreateDocumentCmd):
    title: str
    secret: str


class _NoteUpdate(BaseDTO):
    secret: str | None = None


class _NoteRead(ReadDocument):
    title: str
    secret: str


_POLICY = FieldEncryption(encrypted=frozenset({"secret"}))

_DOCUMENT = DocumentSpec(
    name="notes",
    read=_NoteRead,
    write={"domain": _Note, "create_cmd": _NoteCreate, "update_cmd": _NoteUpdate},  # type: ignore[arg-type]
    encryption=_POLICY,
)


def _ctx(mongo_client: MongoClient, db: str, collection: str) -> ExecutionContext:
    """A fresh context with its OWN keyring (a separate process, so reads start cold)."""

    document = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db, collection), write=(db, collection))
    )
    deps = Deps.merge(
        CryptoDepsModule(
            kms=MockKeyManagement(), directory=StaticKeyDirectory(KeyRef(key_id="notes-cmk"))
        )(),
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: document,
                DocumentCommandDepKey: document,
                SearchQueryDepKey: ConfigurableMongoSearch(
                    config=MongoSearchConfig(read=(db, collection), engine="text")
                ),
            }
        ),
    )
    return context_from_deps(deps)


# ....................... #


@pytest.mark.asyncio
async def test_text_search_opens_the_documents_rows_only_when_sealed_by_names_it(
    mongo_client: MongoClient,
) -> None:
    db = (await mongo_client.db()).name
    collection = f"notes_{uuid4().hex[:8]}"
    coll = await mongo_client.collection(collection, db_name=db)
    await coll.create_index([("title", "text")])

    writer = _ctx(mongo_client, db, collection).document.command(_DOCUMENT)
    await writer.create(_NoteCreate(title="ledger one", secret="plain-1"))

    def search(sealed_by: str | None) -> SearchSpec[_NoteRead]:
        return SearchSpec(
            name="notes",
            model_type=_NoteRead,
            fields=["title"],
            encryption=_POLICY,
            sealed_by=sealed_by,
        )

    named = _ctx(mongo_client, db, collection).search.query(search("notes"))
    page = await named.search("ledger")
    assert [hit.secret for hit in page.hits] == ["plain-1"]

    unnamed = _ctx(mongo_client, db, collection).search.query(search(None))

    with pytest.raises(CoreException) as refused:
        await unnamed.search("ledger")

    assert refused.value.code == "core.crypto.aead_auth_failed"

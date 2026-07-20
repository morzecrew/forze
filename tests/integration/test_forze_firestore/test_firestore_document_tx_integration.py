"""Document adapter operations inside Firestore transactions."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.transaction.deps import TransactionManagerDepKey
from forze.application.execution import Deps
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_firestore.execution.deps import (
    ConfigurableFirestoreDocument,
    firestore_txmanager,
)
from forze_firestore.execution.deps.configs import FirestoreDocumentConfig
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from tests.support.execution_context import (
    context_from_deps,
)


class TxDoc(Document):
    title: str


class TxCreate(CreateDocumentCmd):
    title: str


class TxUpdate(BaseDTO):
    title: str | None = None


class TxRead(ReadDocument):
    title: str


@pytest.mark.asyncio
async def test_document_create_commits_in_transaction(
    firestore_client: FirestoreClient,
) -> None:
    collection = f"tx_doc_{uuid4().hex[:8]}"
    spec = DocumentSpec(
        name="tx_doc",
        read=TxRead,
        write={
            "domain": TxDoc,
            "create_cmd": TxCreate,
            "update_cmd": TxUpdate,
        },
    )
    fac = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        ),
    )
    plain = Deps.plain(
        {
            FirestoreClientDepKey: firestore_client,
            DocumentQueryDepKey: fac,
            DocumentCommandDepKey: fac,
        }
    )
    routed = Deps.routed({TransactionManagerDepKey: {"firestore": firestore_txmanager}})
    ctx = context_from_deps(plain.merge(routed))

    async with ctx.tx_ctx.scope("firestore"):
        created = await ctx.document.command(spec).create(TxCreate(title="in-tx"))
        assert created.title == "in-tx"

    loaded = await ctx.document.query(spec).get(created.id)
    assert loaded.title == "in-tx"


@pytest.mark.asyncio
async def test_document_create_rolls_back_on_error(
    firestore_client: FirestoreClient,
) -> None:
    collection = f"tx_doc_rb_{uuid4().hex[:8]}"
    spec = DocumentSpec(
        name="tx_doc_rb",
        read=TxRead,
        write={
            "domain": TxDoc,
            "create_cmd": TxCreate,
            "update_cmd": TxUpdate,
        },
    )
    fac = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        ),
    )
    plain = Deps.plain(
        {
            FirestoreClientDepKey: firestore_client,
            DocumentQueryDepKey: fac,
            DocumentCommandDepKey: fac,
        }
    )
    routed = Deps.routed({TransactionManagerDepKey: {"firestore": firestore_txmanager}})
    ctx = context_from_deps(plain.merge(routed))

    with pytest.raises(RuntimeError, match="boom"):
        async with ctx.tx_ctx.scope("firestore"):
            await ctx.document.command(spec).create(TxCreate(title="lost"))
            raise RuntimeError("boom")

    assert await ctx.document.query(spec).count() == 0


@pytest.mark.asyncio
async def test_document_create_many_in_transaction(
    firestore_client: FirestoreClient,
) -> None:
    collection = f"tx_many_{uuid4().hex[:8]}"
    spec = DocumentSpec(
        name="tx_many",
        read=TxRead,
        write={
            "domain": TxDoc,
            "create_cmd": TxCreate,
            "update_cmd": TxUpdate,
        },
    )
    fac = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        ),
    )
    plain = Deps.plain(
        {
            FirestoreClientDepKey: firestore_client,
            DocumentQueryDepKey: fac,
            DocumentCommandDepKey: fac,
        }
    )
    routed = Deps.routed({TransactionManagerDepKey: {"firestore": firestore_txmanager}})
    ctx = context_from_deps(plain.merge(routed))

    async with ctx.tx_ctx.scope("firestore"):
        created = await ctx.document.command(spec).create_many(
            [
                TxCreate(title="one"),
                TxCreate(title="two"),
            ],
        )
        assert len(created) == 2

    page = await ctx.document.query(spec).find_many(None)
    titles = {row.title for row in page.hits}
    assert titles == {"one", "two"}

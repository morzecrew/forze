"""Unit tests for ``forze_firestore.adapters.document``."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.base.exceptions import CoreException

from forze.application.contracts.document import DocumentSpec
from forze.application.integrations.document import DocumentCache
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_firestore.adapters.document import FirestoreDocumentAdapter
from forze_firestore.kernel.gateways import FirestoreReadGateway, FirestoreWriteGateway


class MyDoc(Document):
    name: str


class MyCreateDoc(CreateDocumentCmd):
    name: str


class MyUpdateDoc(BaseDTO):
    name: str | None = None


class MyReadDoc(ReadDocument):
    name: str


def _doc_spec() -> DocumentSpec[MyReadDoc, MyDoc, MyCreateDoc, MyUpdateDoc]:
    return DocumentSpec(
        name="firestore-adapter-test",
        read=MyReadDoc,
        write={
            "domain": MyDoc,
            "create_cmd": MyCreateDoc,
            "update_cmd": MyUpdateDoc,
        },
    )


def test_write_gateway_requires_same_client() -> None:
    read_gw = MagicMock(spec=FirestoreReadGateway)
    read_gw.model_type = MyReadDoc
    read_gw.client = object()
    read_gw.tenant_aware = False

    write_gw = MagicMock(spec=FirestoreWriteGateway)
    write_gw.client = object()
    write_gw.tenant_aware = False

    spec = _doc_spec()
    cc = DocumentCache(
        read_model_type=MyReadDoc,
        document_name=spec.name,
        cache=None,
    )

    with pytest.raises(CoreException, match="same client"):
        FirestoreDocumentAdapter(
            spec=spec,
            read_gw=read_gw,
            write_gw=write_gw,
            document_cache=cc,
        )


def test_write_gateway_requires_matching_tenant_awareness() -> None:
    read_gw = MagicMock(spec=FirestoreReadGateway)
    read_gw.model_type = MyReadDoc
    client = object()
    read_gw.client = client
    read_gw.tenant_aware = False

    write_gw = MagicMock(spec=FirestoreWriteGateway)
    write_gw.client = client
    write_gw.tenant_aware = True

    spec = _doc_spec()
    cc = DocumentCache(
        read_model_type=MyReadDoc,
        document_name=spec.name,
        cache=None,
    )

    with pytest.raises(CoreException, match="tenant awareness"):
        FirestoreDocumentAdapter(
            spec=spec,
            read_gw=read_gw,
            write_gw=write_gw,
            document_cache=cc,
        )


def _domain_doc(name: str) -> MyDoc:
    now = datetime.now(tz=timezone.utc)
    return MyDoc(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        name=name,
    )


@pytest.mark.asyncio
async def test_create_in_transaction_uses_write_gateway_directly() -> None:
    read_gw = MagicMock(spec=FirestoreReadGateway)
    read_gw.model_type = MyReadDoc
    read_gw.client = MagicMock()
    read_gw.client.is_in_transaction.return_value = True
    read_gw.tenant_aware = False

    write_gw = MagicMock(spec=FirestoreWriteGateway)
    write_gw.client = read_gw.client
    write_gw.tenant_aware = False
    domain = _domain_doc("tx")
    write_gw.create = AsyncMock(return_value=domain)

    spec = _doc_spec()
    cache_coord = DocumentCache(
        read_model_type=MyReadDoc,
        document_name=spec.name,
        cache=None,
    )

    adapter = FirestoreDocumentAdapter(
        spec=spec,
        read_gw=read_gw,
        write_gw=write_gw,
        document_cache=cache_coord,
    )

    out = await adapter.create(MyCreateDoc(name="tx"))
    assert out is not None
    assert out.name == "tx"
    write_gw.create.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_many_in_transaction_returns_validated_reads() -> None:
    read_gw = MagicMock(spec=FirestoreReadGateway)
    read_gw.model_type = MyReadDoc
    read_gw.client = MagicMock()
    read_gw.client.is_in_transaction.return_value = True
    read_gw.tenant_aware = False

    write_gw = MagicMock(spec=FirestoreWriteGateway)
    write_gw.client = read_gw.client
    write_gw.tenant_aware = False
    domains = [_domain_doc("a"), _domain_doc("b")]
    write_gw.create_many = AsyncMock(return_value=domains)

    spec = _doc_spec()
    cache_coord = DocumentCache(
        read_model_type=MyReadDoc,
        document_name=spec.name,
        cache=None,
    )

    adapter = FirestoreDocumentAdapter(
        spec=spec,
        read_gw=read_gw,
        write_gw=write_gw,
        document_cache=cache_coord,
        batch_size=50,
    )

    out = await adapter.create_many(
        [MyCreateDoc(name="a"), MyCreateDoc(name="b")],
    )
    assert out is not None
    assert len(out) == 2
    write_gw.create_many.assert_awaited_once()

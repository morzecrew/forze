"""Unit tests for ``forze_firestore.adapters.document``."""

from unittest.mock import MagicMock

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.coordinators import DocumentCacheCoordinator
from forze.base.errors import CoreError
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
    cc = DocumentCacheCoordinator(
        read_model_type=MyReadDoc,
        document_name=spec.name,
        cache=None,
    )

    with pytest.raises(CoreError, match="same client"):
        FirestoreDocumentAdapter(
            spec=spec,
            read_gw=read_gw,
            write_gw=write_gw,
            cache_coord=cc,
        )

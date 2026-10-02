"""Firestore reads and deletes by document name check the tenant before any read.

A by-name read cannot carry a tenant filter, so the gateway checks each fetched row's tenant.
The collection of a static relation is resolved once and cached, so after the first call an
unbound caller no longer meets the tenant check that resolution makes: without its own check
first, the gateway read the document before refusing the caller.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import pytest

pytest.importorskip("google.cloud.firestore")

from forze.application.contracts.tenancy import TENANT_ID_FIELD, TenantIdentity
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_firestore.kernel.gateways import FirestoreReadGateway, FirestoreWriteGateway
from tests.unit._gateway_codec_helpers import write_codecs_for

pytestmark = pytest.mark.unit


class _Doc(Document):
    name: str


class _Create(CreateDocumentCmd):
    name: str


class _Update(BaseDTO):
    name: str | None = None


_CODEC, _CREATE, _UPDATE = write_codecs_for(
    domain_type=_Doc, create_type=_Create, update_type=_Update
)


class _CountingClient:
    """A Firestore client holding documents in memory and counting every read it serves."""

    def __init__(self, docs: dict[str, dict[str, Any]]) -> None:
        self.docs = docs
        self.reads = 0

    async def collection(self, name: str, *, database: str | None = None) -> str:
        return name

    async def get_document(self, coll: str, doc_id: str) -> dict[str, Any] | None:
        self.reads += 1
        return self.docs.get(doc_id)

    async def get_documents(self, coll: str, doc_ids: list[str]) -> dict[str, Any]:
        self.reads += 1
        return {i: self.docs[i] for i in doc_ids if i in self.docs}

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        yield

    async def delete_document(self, coll: str, doc_id: str) -> None:
        self.docs.pop(doc_id, None)


class TestAnUnboundCallerIsRefusedBeforeAnyRead:
    def _gateways(self) -> tuple[_CountingClient, Any, Any, UUID, dict[str, Any]]:
        tenant, pk = uuid4(), uuid4()
        doc = {"id": str(pk), "name": "n", "rev": 1, TENANT_ID_FIELD: str(tenant)}
        client = _CountingClient({str(pk): doc})
        bound: dict[str, Any] = {"tenant": TenantIdentity(tenant_id=tenant)}
        common: dict[str, Any] = {
            "relation": ("(default)", "docs"),
            "client": client,
            "model_type": _Doc,
            "codec": _CODEC,
            "tenant_aware": True,
            "tenant_provider": lambda: bound["tenant"],
        }
        read = FirestoreReadGateway(**common)
        write = FirestoreWriteGateway(
            **common,
            create_cmd_type=_Create,
            update_cmd_type=_Update,
            read_gw=read,
            create_codec=_CREATE,
            update_codec=_UPDATE,
        )

        return client, read, write, pk, bound

    @pytest.mark.parametrize("call", ["get", "get_many", "kill"])
    async def test_after_the_collection_is_cached(self, call: str) -> None:
        client, read, write, pk, bound = self._gateways()
        # Bound calls resolve and cache each gateway's static collection.
        await read.coll()
        await write.coll()
        bound["tenant"] = None
        operations = {
            "get": lambda: read.get(pk),
            "get_many": lambda: read.get_many([pk]),
            "kill": lambda: write.kill(pk),
        }

        with pytest.raises(CoreException) as refused:
            await operations[call]()

        assert (refused.value.code, client.reads) == ("tenant_required", 0)

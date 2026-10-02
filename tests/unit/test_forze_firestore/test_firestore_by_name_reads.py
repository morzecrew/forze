"""Firestore reads by document name: the tenant is checked before any read, and a batched get
goes out in bounded requests.

A by-name read cannot carry a tenant filter, so the gateway checks each fetched row's tenant.
The collection of a static relation is resolved once and cached, so after the first call an
unbound caller no longer meets the tenant check that resolution makes: without its own check
first, the gateway read the document before refusing the caller.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest

pytest.importorskip("google.cloud.firestore")

from forze.application.contracts.tenancy import TENANT_ID_FIELD, TenantIdentity
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_firestore.kernel.client import RoutedFirestoreClient
from forze_firestore.kernel.client import client as client_module
from forze_firestore.kernel.client.client import FirestoreClient
from forze_firestore.kernel.gateways import FirestoreReadGateway, FirestoreWriteGateway
from tests.unit._gateway_codec_helpers import write_codecs_for
from tests.unit.test_forze_firestore.test_routed_firestore_client import (
    _T1,
    _creds,
    _MemSecrets,
    _ref,
)

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
    """A Firestore client holding documents in memory, by collection and name, and counting
    every read it serves."""

    def __init__(self, docs: dict[tuple[str, str], dict[str, Any]]) -> None:
        self.docs = docs
        self.reads = 0

    async def collection(self, name: str, *, database: str | None = None) -> str:
        return name

    async def get_document(self, coll: str, doc_id: str) -> dict[str, Any] | None:
        self.reads += 1
        return self.docs.get((coll, doc_id))

    async def get_documents(self, coll: str, doc_ids: list[str]) -> dict[str, Any]:
        self.reads += 1
        return {i: self.docs[(coll, i)] for i in doc_ids if (coll, i) in self.docs}

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        yield

    async def delete_document(self, coll: str, doc_id: str) -> None:
        self.docs.pop((coll, doc_id), None)


def _gateways(client: _CountingClient, relation: Any, provider: Any) -> tuple[Any, Any]:
    common: dict[str, Any] = {
        "relation": relation,
        "client": client,
        "model_type": _Doc,
        "codec": _CODEC,
        "tenant_aware": True,
        "tenant_provider": provider,
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

    return read, write


def _by_id(read: Any, write: Any, pk: UUID) -> dict[str, Any]:
    return {
        "get": lambda: read.get(pk),
        "get_many": lambda: read.get_many([pk]),
        "kill": lambda: write.kill(pk),
    }


def _doc(pk: UUID, tenant: UUID) -> dict[str, Any]:
    return {"id": str(pk), "name": "n", "rev": 1, TENANT_ID_FIELD: str(tenant)}


class TestAnUnboundCallerIsRefusedBeforeAnyRead:
    @pytest.mark.parametrize("call", ["get", "get_many", "kill"])
    async def test_after_the_collection_is_cached(self, call: str) -> None:
        tenant, pk = uuid4(), uuid4()
        client = _CountingClient({("docs", str(pk)): _doc(pk, tenant)})
        bound: dict[str, Any] = {"tenant": TenantIdentity(tenant_id=tenant)}
        read, write = _gateways(client, ("(default)", "docs"), lambda: bound["tenant"])
        # Bound calls resolve and cache each gateway's static collection.
        await read.coll()
        await write.coll()
        bound["tenant"] = None

        with pytest.raises(CoreException) as refused:
            await _by_id(read, write, pk)[call]()

        assert (refused.value.code, client.reads) == ("tenant_required", 0)


class TestOneOperationAnswersForOneTenant:
    """The collection a by-id operation routes to and the rows it accepts come from one answer
    of the tenant provider, so a provider that answers differently each time cannot route an
    operation to one tenant's collection and accept the row as another's."""

    @pytest.mark.parametrize("call", ["get", "get_many", "kill"])
    async def test_a_provider_that_changes_its_answer(self, call: str) -> None:
        owner, other, pk = uuid4(), uuid4(), uuid4()
        client = _CountingClient({(f"docs_{owner}", str(pk)): _doc(pk, owner)})
        answers = [owner, other, owner, other]

        def provider() -> TenantIdentity:
            return TenantIdentity(tenant_id=answers.pop(0))

        read, write = _gateways(client, lambda tid: ("(default)", f"docs_{tid}"), provider)

        await _by_id(read, write, pk)[call]()

        # Asked once, and that answer both routed the read and accepted the row.
        assert len(answers) == 3
        assert (client.docs == {}) is (call == "kill")


# ----------------------- #


class _FakeAsyncClient:
    """Answers ``get_all`` for the names it holds, recording each request's size."""

    def __init__(self, names: set[str]) -> None:
        self.names = names
        self.requests: list[int] = []

    def collection(self, name: str) -> Any:
        return SimpleNamespace(document=lambda doc_id: SimpleNamespace(id=doc_id))

    async def get_all(self, refs: list[Any], transaction: Any = None) -> AsyncIterator[Any]:
        self.requests.append(len(refs))

        for ref in refs:
            exists = ref.id in self.names
            yield SimpleNamespace(
                id=ref.id,
                exists=exists,
                to_dict=lambda ref=ref: {"name": ref.id},
            )


def _client(names: set[str]) -> tuple[FirestoreClient, _FakeAsyncClient]:
    fake = _FakeAsyncClient(names)
    client = FirestoreClient()
    client._FirestoreClient__client = fake  # type: ignore[attr-defined]
    client._FirestoreClient__database_id = "(default)"  # type: ignore[attr-defined]
    client._FirestoreClient__lazy_tx = False  # type: ignore[attr-defined]

    return client, fake


class TestABatchedGetGoesOutInBoundedRequests:
    async def test_names_split_into_requests_of_the_batch_size(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(client_module, "_GET_ALL_BATCH", 2)
        client, fake = _client({"a", "b", "c", "e"})
        coll = await client.collection("docs")

        found = await client.get_documents(coll, ["e", "a", "b", "missing", "a", "c"])

        # Each distinct name is asked for once; one that names nothing is left out.
        assert fake.requests == [2, 2, 1]
        assert found == {n: {"name": n, "id": n} for n in ("a", "b", "c", "e")}

    async def test_no_names_send_no_request(self) -> None:
        client, fake = _client({"a"})

        assert await client.get_documents(await client.collection("docs"), []) == {}
        assert fake.requests == []


async def test_the_routed_client_asks_the_tenants_client() -> None:
    inner = MagicMock()
    inner.initialize = AsyncMock()
    inner.get_documents = AsyncMock(return_value={"a": {"id": "a"}})
    routed = RoutedFirestoreClient(
        secrets=_MemSecrets({_T1: _creds()}),
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: _T1,
        max_cached_tenants=1,
    )
    await routed.startup()

    with patch(
        "forze_firestore.kernel.client.routed_client.FirestoreClient", return_value=inner
    ):
        assert await routed.get_documents("coll", ["a"]) == {"a": {"id": "a"}}

    inner.get_documents.assert_awaited_once_with("coll", ["a"])

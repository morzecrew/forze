"""Unit tests for :mod:`forze_firestore.kernel.gateways.base.FirestoreGateway`."""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import UUID, uuid4

import attrs
import pytest
from google.cloud.firestore_v1.base_query import FieldFilter

from forze.base.exceptions import CoreException
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.domain.constants import ID_FIELD
from forze_firestore.kernel.gateways.base import FirestoreGateway
from tests.support.factories import IntegrationDocument
from tests.unit._gateway_codec_helpers import codec_for

_INTEGRATION_CODEC = codec_for(IntegrationDocument)


class _Gw(FirestoreGateway[IntegrationDocument]):
    pass


def test_static_relation_database_and_collection() -> None:
    gw = _Gw(
        client=MagicMock(),
        model_type=IntegrationDocument,
        codec=_INTEGRATION_CODEC,
        relation=("mydb", "mycoll"),
    )
    assert gw.database == "mydb"
    assert gw.collection == "mycoll"


def test_dynamic_relation_database_raises_before_resolve() -> None:
    async def resolver(_tid):
        return ("db", "coll")

    gw = _Gw(
        client=MagicMock(),
        model_type=IntegrationDocument,
        codec=_INTEGRATION_CODEC,
        relation=resolver,
    )
    with pytest.raises(CoreException, match="static relations"):
        _ = gw.database


@pytest.mark.asyncio
async def test_resolved_collection_caches_relation() -> None:
    client = MagicMock()
    gw = _Gw(
        client=client,
        model_type=IntegrationDocument,
        codec=_INTEGRATION_CODEC,
        relation=("db", "items"),
    )
    first = await gw._resolved_collection()
    second = await gw._resolved_collection()
    assert first == second == ("db", "items")
    client.collection.assert_not_called()


def test_coerce_query_value_uuid_and_nested() -> None:
    uid = uuid4()
    gw = _Gw(
        client=MagicMock(),
        model_type=IntegrationDocument,
        codec=_INTEGRATION_CODEC,
        relation=("db", "c"),
    )
    out = gw._coerce_query_value({"ids": [uid], "n": 1})
    assert out["ids"] == [str(uid)]
    assert out["n"] == 1


def test_from_storage_doc_maps_id_field() -> None:
    gw = _Gw(
        client=MagicMock(),
        model_type=IntegrationDocument,
        codec=_INTEGRATION_CODEC,
        relation=("db", "c"),
    )
    out = gw._from_storage_doc({"id": "doc-1", "name": "x"})
    assert out[ID_FIELD] == "doc-1"
    assert out["name"] == "x"


def test_add_tenant_filter_and_tenant_id() -> None:
    tid = uuid4()

    @attrs.define
    class _Tenant:
        tenant_id: UUID

    gw = _Gw(
        client=MagicMock(),
        model_type=IntegrationDocument,
        codec=_INTEGRATION_CODEC,
        relation=("db", "c"),
        tenant_aware=True,
        tenant_provider=lambda: _Tenant(tenant_id=tid),
    )
    base = FieldFilter("status", "==", "open")
    merged = gw._add_tenant_filter(base)
    assert merged is not None

    data = gw._add_tenant_id({"name": "a"})
    assert data[TENANT_ID_FIELD] == tid


def test_tenant_aware_requires_provider() -> None:
    gw = _Gw(
        client=MagicMock(),
        model_type=IntegrationDocument,
        codec=_INTEGRATION_CODEC,
        relation=("db", "c"),
        tenant_aware=True,
        tenant_provider=None,
    )
    with pytest.raises(CoreException, match="Tenant provider"):
        gw._add_tenant_filter(None)

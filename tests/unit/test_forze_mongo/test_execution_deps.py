"""Unit tests for ``forze_mongo.execution.deps`` (module, factories, utils)."""

from unittest.mock import MagicMock

import pytest

pytest.importorskip("pymongo")

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.tx import TxManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.adapters import MongoDocumentAdapter, MongoTxManagerAdapter
from forze_mongo.execution.deps import MongoClientDepKey, MongoDepsModule
from forze_mongo.execution.deps.deps import (
    ConfigurableMongoDocument,
    ConfigurableMongoReadOnlyDocument,
    mongo_txmanager,
)
from forze_mongo.execution.deps.utils import doc_write_gw, read_gw
from forze_mongo.kernel.gateways import MongoReadGateway, MongoWriteGateway
from forze_mongo.kernel.platform import MongoClient


class _R(ReadDocument):
    title: str


class _D(Document):
    title: str


class _C(CreateDocumentCmd):
    title: str


class _U(BaseDTO):
    title: str | None = None


def _rw_spec(*, history_enabled: bool = False) -> DocumentSpec:
    return DocumentSpec(
        name="mongo_dep",
        read=_R,
        write={"domain": _D, "create_cmd": _C, "update_cmd": _U},
        history_enabled=history_enabled,
    )


def _ctx() -> ExecutionContext:
    return ExecutionContext(deps=Deps.plain({MongoClientDepKey: MagicMock(spec=MongoClient)}))


def test_mongo_deps_module_registers_client_only() -> None:
    client = MagicMock(spec=MongoClient)
    module = MongoDepsModule(client=client)

    deps = module()

    assert isinstance(deps, Deps)
    assert deps.exists(MongoClientDepKey)


def test_mongo_deps_module_rw_registers_query_and_command() -> None:
    client = MagicMock(spec=MongoClient)
    module = MongoDepsModule(
        client=client,
        rw_documents={
            "doc": {
                "read": ("db", "col"),
                "write": ("db", "col"),
            },
        },
        tx={"session"},
    )

    deps = module()

    assert deps.exists(DocumentQueryDepKey, route="doc")
    assert deps.exists(DocumentCommandDepKey, route="doc")
    assert deps.exists(TxManagerDepKey, route="session")


def test_mongo_deps_module_ro_only() -> None:
    client = MagicMock(spec=MongoClient)
    module = MongoDepsModule(
        client=client,
        ro_documents={"view": {"read": ("db", "v")}},
    )

    deps = module()

    assert deps.exists(DocumentQueryDepKey, route="view")
    assert not deps.exists(DocumentCommandDepKey, route="view")


def test_configurable_mongo_read_only_builds_adapter() -> None:
    factory = ConfigurableMongoReadOnlyDocument(config={"read": ("db", "c")})
    ctx = _ctx()
    adapter = factory(ctx, DocumentSpec(name="x", read=_R))

    assert isinstance(adapter, MongoDocumentAdapter)
    assert adapter.write_gw is None


def test_configurable_mongo_document_requires_write_spec() -> None:
    factory = ConfigurableMongoDocument(
        config={"read": ("db", "c"), "write": ("db", "c")},
    )
    ctx = _ctx()

    with pytest.raises(CoreError, match="Write relation is required"):
        factory(ctx, DocumentSpec(name="n", read=_R))


def test_configurable_mongo_document_batch_size() -> None:
    factory = ConfigurableMongoDocument(
        config={
            "read": ("db", "c"),
            "write": ("db", "c"),
            "batch_size": 444,
        },
    )
    ctx = _ctx()
    adapter = factory(ctx, _rw_spec())

    assert isinstance(adapter, MongoDocumentAdapter)
    assert adapter.batch_size == 444


def test_configurable_mongo_read_only_document_batch_size() -> None:
    factory = ConfigurableMongoReadOnlyDocument(
        config={
            "read": ("db", "c"),
            "batch_size": 555,
        },
    )
    ctx = _ctx()
    adapter = factory(ctx, DocumentSpec(name="ro", read=_R))

    assert isinstance(adapter, MongoDocumentAdapter)
    assert adapter.batch_size == 555


def test_document_config_to_read_only_preserves_batch_size() -> None:
    from forze_mongo.execution.deps.module import _document_config_to_read_only

    rw: dict = {
        "read": ("db", "c"),
        "write": ("db", "c"),
        "batch_size": 999,
    }
    ro = _document_config_to_read_only(rw)  # type: ignore[arg-type]

    assert ro["read"] == ("db", "c")
    assert ro.get("batch_size") == 999


def test_mongo_txmanager() -> None:
    tx = mongo_txmanager(_ctx())

    assert isinstance(tx, MongoTxManagerAdapter)


def test_read_gw_factory() -> None:
    gw = read_gw(
        _ctx(),
        read_type=_R,
        read_relation=("db", "col"),
        tenant_aware=True,
    )

    assert isinstance(gw, MongoReadGateway)
    assert gw.database == "db"
    assert gw.collection == "col"


def test_doc_write_gw_without_history() -> None:
    gw = doc_write_gw(
        _ctx(),
        write_types=_rw_spec().write,  # type: ignore[arg-type]
        write_relation=("db", "w"),
        history_relation=None,
        history_enabled=False,
        tenant_aware=False,
    )

    assert isinstance(gw, MongoWriteGateway)
    assert gw.history_gw is None


def test_doc_write_gw_with_history() -> None:
    gw = doc_write_gw(
        _ctx(),
        write_types=_rw_spec().write,  # type: ignore[arg-type]
        write_relation=("db", "w"),
        history_relation=("db", "h"),
        history_enabled=True,
        tenant_aware=False,
    )

    assert gw.history_gw is not None
    assert gw.history_gw.collection == "h"

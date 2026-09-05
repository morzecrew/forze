"""Unit tests for ``forze_mongo.execution.deps`` (module, factories, utils)."""

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from forze.base.exceptions import CoreException, ExceptionKind

pytest.importorskip("pymongo")

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.durable.function import (
    DurableFunctionStepDepKey,
    DurableRunAdminDepKey,
    DurableRunStoreDepKey,
    DurableScheduleStoreDepKey,
)
from forze.application.contracts.idempotency import IdempotencyDepKey, IdempotencySpec
from forze.application.contracts.inbox import InboxDepKey, InboxSpec
from forze.application.contracts.transaction.deps import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.context.invocation import InvocationMetadata
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.adapters import MongoDocumentAdapter, MongoTxManagerAdapter
from forze_mongo.adapters.idempotency import MongoIdempotencyStore
from forze_mongo.adapters.inbox import MongoInboxStore
from forze_mongo.execution.deps import (
    ConfigurableMongoDocument,
    ConfigurableMongoDurableRun,
    ConfigurableMongoDurableStep,
    ConfigurableMongoIdempotency,
    ConfigurableMongoInbox,
    ConfigurableMongoReadOnlyDocument,
    ConfigurableMongoSearch,
    MongoClientDepKey,
    MongoDepsModule,
    MongoDocumentConfig,
    MongoDurableRunConfig,
    MongoDurableScheduleConfig,
    MongoDurableStepConfig,
    MongoIdempotencyConfig,
    MongoInboxConfig,
    MongoReadOnlyDocumentConfig,
    mongo_txmanager,
)
from forze_mongo.execution.deps.utils import doc_write_gw, read_gw
from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.gateways import MongoReadGateway, MongoWriteGateway
from tests.support.execution_context import context_from_deps


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
    return context_from_deps(Deps.plain({MongoClientDepKey: MagicMock(spec=MongoClient)})
    )


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
            "doc": MongoDocumentConfig(read=("db", "col"), write=("db", "col")),
        },
        tx={"session"},
    )

    deps = module()

    assert deps.exists(DocumentQueryDepKey, route="doc")
    assert deps.exists(DocumentCommandDepKey, route="doc")
    assert deps.exists(TransactionManagerDepKey, route="session")


def test_mongo_deps_module_registers_inbox() -> None:
    client = MagicMock(spec=MongoClient)
    module = MongoDepsModule(
        client=client,
        inboxes={"events": MongoInboxConfig(collection=("db", "inbox"))},
    )

    deps = module()

    assert deps.exists(InboxDepKey, route="events")
    assert not deps.exists(InboxDepKey, route="other")


def test_configurable_mongo_inbox_builds_store() -> None:
    factory = ConfigurableMongoInbox(
        config=MongoInboxConfig(collection=("db", "inbox")),
    )

    store = factory(_ctx(), InboxSpec(name="events"))

    assert isinstance(store, MongoInboxStore)


def test_mongo_deps_module_registers_idempotency() -> None:
    client = MagicMock(spec=MongoClient)
    module = MongoDepsModule(
        client=client,
        idempotencies={"orders": MongoIdempotencyConfig(collection=("db", "idem"))},
    )

    deps = module()

    assert deps.exists(IdempotencyDepKey, route="orders")
    assert not deps.exists(IdempotencyDepKey, route="other")


def test_configurable_mongo_idempotency_builds_store() -> None:
    factory = ConfigurableMongoIdempotency(
        config=MongoIdempotencyConfig(collection=("db", "idem")),
    )

    store = factory(_ctx(), IdempotencySpec(name="orders"))

    assert isinstance(store, MongoIdempotencyStore)


def test_configurable_mongo_idempotency_wires_the_claim_owner() -> None:
    # The fence degrades silently without a provider, so a factory that forgot to pass one
    # would leave every claim unowned and every store test still green.
    ctx = _ctx()
    factory = ConfigurableMongoIdempotency(
        config=MongoIdempotencyConfig(collection=("db", "idem")),
    )
    store = factory(ctx, IdempotencySpec(name="orders"))
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with ctx.inv_ctx.bind_metadata(metadata=metadata):
        assert store.claim_owner() == metadata.execution_id


def test_mongo_deps_module_ro_only() -> None:
    client = MagicMock(spec=MongoClient)
    module = MongoDepsModule(
        client=client,
        ro_documents={"view": MongoReadOnlyDocumentConfig(read=("db", "v"))},
    )

    deps = module()

    assert deps.exists(DocumentQueryDepKey, route="view")
    assert not deps.exists(DocumentCommandDepKey, route="view")


def test_configurable_mongo_read_only_builds_adapter() -> None:
    factory = ConfigurableMongoReadOnlyDocument(
        config=MongoReadOnlyDocumentConfig(read=("db", "c"))
    )
    ctx = _ctx()
    adapter = factory(ctx, DocumentSpec(name="x", read=_R))

    assert isinstance(adapter, MongoDocumentAdapter)
    assert adapter.write_gw is None


def test_configurable_mongo_document_requires_write_spec() -> None:
    factory = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=("db", "c"), write=("db", "c")),
    )
    ctx = _ctx()

    with pytest.raises(CoreException, match="Write relation is required"):
        factory(ctx, DocumentSpec(name="n", read=_R))


def test_configurable_mongo_document_batch_size() -> None:
    factory = ConfigurableMongoDocument(
        config=MongoDocumentConfig(
            read=("db", "c"),
            write=("db", "c"),
            batch_size=444,
        ),
    )
    ctx = _ctx()
    adapter = factory(ctx, _rw_spec())

    assert isinstance(adapter, MongoDocumentAdapter)
    assert adapter.batch_size == 444


def test_configurable_mongo_read_only_document_batch_size() -> None:
    factory = ConfigurableMongoReadOnlyDocument(
        config=MongoReadOnlyDocumentConfig(read=("db", "c"), batch_size=555),
    )
    ctx = _ctx()
    adapter = factory(ctx, DocumentSpec(name="ro", read=_R))

    assert isinstance(adapter, MongoDocumentAdapter)
    assert adapter.batch_size == 555


def test_document_config_to_read_only_preserves_batch_size() -> None:
    from forze.application.contracts.document.wiring import derive_read_only_document_config

    rw = MongoDocumentConfig(
        read=("db", "c"),
        write=("db", "c"),
        batch_size=999,
        tenant_aware=True,
    )
    ro = derive_read_only_document_config(
        rw,
        factory=MongoReadOnlyDocumentConfig,
    )

    assert ro.read == ("db", "c")
    assert ro.batch_size == 999
    assert ro.tenant_aware is True


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
    spec = _rw_spec()
    gw = doc_write_gw(
        _ctx(),
        write_types=spec.write,  # type: ignore[arg-type]
        codecs=spec.resolved_codecs,
        write_relation=("db", "w"),
        history_relation=None,
        history_enabled=False,
        tenant_aware=False,
    )

    assert isinstance(gw, MongoWriteGateway)
    assert gw.history_gw is None


def test_rejects_mapping_document_config() -> None:
    with pytest.raises(TypeError, match="MongoDocumentConfig"):
        ConfigurableMongoDocument(config={"read": ("db", "c"), "write": ("db", "c")})


def test_rejects_mapping_search_config() -> None:
    with pytest.raises(TypeError, match="MongoSearchConfig"):
        ConfigurableMongoSearch(
            config={"read": ("db", "c"), "engine": "text"},
        )


def test_doc_write_gw_with_history() -> None:
    spec = _rw_spec(history_enabled=True)
    gw = doc_write_gw(
        _ctx(),
        write_types=spec.write,  # type: ignore[arg-type]
        codecs=spec.resolved_codecs,
        write_relation=("db", "w"),
        history_relation=("db", "h"),
        history_enabled=True,
        tenant_aware=False,
    )

    assert gw.history_gw is not None
    assert gw.history_gw.collection == "h"


class TestMongoDurableWiring:
    """What the module registers for durable execution, and what it refuses to.

    Each of these is silent when wrong: a missing admin key looks like an unavailable ops
    plane rather than a wiring bug, and a route that asked for encryption and got none would
    journal business payloads in the clear while every functional test still passed.
    """

    def test_each_store_registers_under_its_own_key(self) -> None:
        module = MongoDepsModule(
            client=MagicMock(spec=MongoClient),
            durable_step=MongoDurableStepConfig(collection=("db", "steps")),
            durable_run=MongoDurableRunConfig(collection=("db", "runs")),
            durable_schedule=MongoDurableScheduleConfig(collection=("db", "schedules")),
        )

        deps = module()

        assert deps.exists(DurableFunctionStepDepKey)
        assert deps.exists(DurableRunStoreDepKey)
        assert deps.exists(DurableScheduleStoreDepKey)

    def test_nothing_is_registered_by_default(self) -> None:
        deps = MongoDepsModule(client=MagicMock(spec=MongoClient))()

        assert not deps.exists(DurableFunctionStepDepKey)
        assert not deps.exists(DurableRunStoreDepKey)
        assert not deps.exists(DurableScheduleStoreDepKey)

    def test_the_ops_plane_is_opt_in(self) -> None:
        # `admin` grants run *control*, not just visibility (request_cancel stops runs), so
        # it must never arrive with the store.
        client = MagicMock(spec=MongoClient)
        without = MongoDepsModule(
            client=client,
            durable_run=MongoDurableRunConfig(collection=("db", "runs")),
        )()
        with_admin = MongoDepsModule(
            client=client,
            durable_run=MongoDurableRunConfig(collection=("db", "runs"), admin=True),
        )()

        assert not without.exists(DurableRunAdminDepKey)
        assert with_admin.exists(DurableRunAdminDepKey)

    def test_encryption_without_a_keyring_fails_closed(self) -> None:
        ctx = context_from_deps(Deps.plain({MongoClientDepKey: MagicMock(spec=MongoClient)}))

        for factory in (
            ConfigurableMongoDurableStep(
                config=MongoDurableStepConfig(collection=("db", "steps"), encrypt=True)
            ),
            ConfigurableMongoDurableRun(
                config=MongoDurableRunConfig(collection=("db", "runs"), encrypt=True)
            ),
        ):
            with pytest.raises(CoreException) as ei:
                factory(ctx)

            assert ei.value.kind == ExceptionKind.CONFIGURATION

    def test_a_plain_route_builds_without_a_keyring(self) -> None:
        ctx = context_from_deps(Deps.plain({MongoClientDepKey: MagicMock(spec=MongoClient)}))
        store = ConfigurableMongoDurableRun(
            config=MongoDurableRunConfig(collection=("db", "runs"))
        )(ctx)

        assert store.cipher is None

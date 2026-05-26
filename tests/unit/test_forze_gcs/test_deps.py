from unittest.mock import Mock
from uuid import uuid4

from forze.application.contracts.storage import StorageDepKey, StorageSpec
from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze_gcs.adapters.storage import GCSStorageAdapter
from forze_gcs.execution.deps import GCSClientDepKey, GCSDepsModule
from forze_gcs.execution.deps.deps import ConfigurableGCSStorage
from forze_gcs.kernel.platform import GCSClient


def test_gcs_storage_factory_builds_adapter_without_tenant() -> None:
    gcs_mock = Mock(spec=GCSClient)
    deps = Deps.plain({GCSClientDepKey: gcs_mock})
    context = ExecutionContext(deps=deps)

    factory = ConfigurableGCSStorage(config={"bucket": "test-bucket"})
    storage = factory(context, StorageSpec(name="route"))

    assert isinstance(storage, GCSStorageAdapter)
    assert storage.client is gcs_mock
    assert storage.bucket == "test-bucket"
    assert storage.tenant_aware is False


def test_gcs_storage_factory_resolves_tenant_from_context() -> None:
    gcs_mock = Mock(spec=GCSClient)
    deps = Deps.plain({GCSClientDepKey: gcs_mock})
    context = ExecutionContext(deps=deps)
    tid = uuid4()

    factory = ConfigurableGCSStorage(
        config={"bucket": "tenant-bucket", "tenant_aware": True},
    )

    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    with context.inv.bind(
        metadata=metadata,
        authn=AuthnIdentity(principal_id=uuid4()),
        tenant=TenantIdentity(tenant_id=tid),
    ):
        storage = factory(context, StorageSpec(name="x"))
        assert storage.tenant_provider().tenant_id == tid


def test_gcs_deps_module_registers_expected_keys() -> None:
    gcs_mock = Mock(spec=GCSClient)
    module = GCSDepsModule(
        client=gcs_mock,
        storages={"module-bucket": {"bucket": "module-bucket"}},
    )

    deps = module()

    assert deps.exists(GCSClientDepKey)
    assert deps.exists(StorageDepKey, route="module-bucket")

    context = ExecutionContext(deps=deps)
    storage = context.storage(StorageSpec(name="module-bucket"))

    assert isinstance(storage, GCSStorageAdapter)
    assert storage.client is gcs_mock
    assert storage.bucket == "module-bucket"

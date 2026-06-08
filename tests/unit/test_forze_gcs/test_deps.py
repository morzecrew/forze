from unittest.mock import Mock
from uuid import uuid4

import pytest

from forze.application.contracts.storage import (
    StorageCommandDepKey,
    StorageQueryDepKey,
    StorageSpec,
)
from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze_gcs.adapters.storage import GCSStorageAdapter
from forze_gcs.execution.deps import (
    ConfigurableGCSStorageCommand,
    ConfigurableGCSStorageQuery,
    GCSClientDepKey,
    GCSDepsModule,
    GCSStorageConfig,
)
from forze_gcs.kernel.client import GCSClient


def test_rejects_mapping_config() -> None:
    with pytest.raises(TypeError, match="GCSStorageConfig"):
        ConfigurableGCSStorageQuery(config={"bucket": "b"})
    with pytest.raises(TypeError, match="GCSStorageConfig"):
        ConfigurableGCSStorageCommand(config={"bucket": "b"})


def test_gcs_storage_factory_builds_adapter_without_tenant() -> None:
    gcs_mock = Mock(spec=GCSClient)
    deps = Deps.plain({GCSClientDepKey: gcs_mock})
    context = context_from_deps(deps)

    query = ConfigurableGCSStorageQuery(config=GCSStorageConfig(bucket="test-bucket"))
    command = ConfigurableGCSStorageCommand(
        config=GCSStorageConfig(bucket="test-bucket")
    )

    for factory in (query, command):
        storage = factory(context, StorageSpec(name="route"))

        assert isinstance(storage, GCSStorageAdapter)
        assert storage.client is gcs_mock
        assert storage.bucket == "test-bucket"
        assert storage.tenant_aware is False


def test_gcs_storage_factory_resolves_tenant_from_context() -> None:
    gcs_mock = Mock(spec=GCSClient)
    deps = Deps.plain({GCSClientDepKey: gcs_mock})
    context = context_from_deps(deps)
    tid = uuid4()

    factory = ConfigurableGCSStorageQuery(
        config=GCSStorageConfig(bucket="tenant-bucket", tenant_aware=True),
    )

    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    with context.inv_ctx.bind(
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
        storages={
            "module-bucket": GCSStorageConfig(bucket="module-bucket"),
        },
    )

    deps = module()

    assert deps.exists(GCSClientDepKey)
    assert deps.exists(StorageQueryDepKey, route="module-bucket")
    assert deps.exists(StorageCommandDepKey, route="module-bucket")

    context = context_from_deps(deps)
    storage = context.storage.command(StorageSpec(name="module-bucket"))

    assert isinstance(storage, GCSStorageAdapter)
    assert storage.client is gcs_mock
    assert storage.bucket == "module-bucket"

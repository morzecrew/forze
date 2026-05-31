from unittest.mock import Mock
from uuid import uuid4

from forze.application.contracts.storage import StorageDepKey, StorageSpec
from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze_s3.adapters.storage import S3StorageAdapter
import pytest

from forze_s3.execution.deps import (
    ConfigurableS3Storage,
    S3ClientDepKey,
    S3DepsModule,
    S3StorageConfig,
)
from forze_s3.kernel.client import S3Client


def test_rejects_mapping_config() -> None:
    with pytest.raises(TypeError, match="S3StorageConfig"):
        ConfigurableS3Storage(config={"bucket": "test-bucket"})


def test_s3_storage_factory_builds_adapter_without_tenant() -> None:
    s3_mock = Mock(spec=S3Client)
    deps = Deps.plain({S3ClientDepKey: s3_mock})
    context = context_from_deps(deps)

    factory = ConfigurableS3Storage(config=S3StorageConfig(bucket="test-bucket"))
    storage = factory(context, StorageSpec(name="route"))

    assert isinstance(storage, S3StorageAdapter)
    assert storage.client is s3_mock
    assert storage.bucket == "test-bucket"
    assert storage.tenant_aware is False


def test_s3_storage_factory_resolves_tenant_from_context() -> None:
    s3_mock = Mock(spec=S3Client)
    deps = Deps.plain({S3ClientDepKey: s3_mock})
    context = context_from_deps(deps)
    tid = uuid4()

    factory = ConfigurableS3Storage(
        config=S3StorageConfig(bucket="tenant-bucket", tenant_aware=True),
    )

    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    with context.inv_ctx.bind(
        metadata=metadata,
        authn=AuthnIdentity(principal_id=uuid4()),
        tenant=TenantIdentity(tenant_id=tid),
    ):
        storage = factory(context, StorageSpec(name="x"))
        assert storage.tenant_provider().tenant_id == tid


def test_s3_deps_module_registers_expected_keys() -> None:
    s3_mock = Mock(spec=S3Client)
    module = S3DepsModule(
        client=s3_mock,
        storages={
            "module-bucket": S3StorageConfig(bucket="module-bucket"),
        },
    )

    deps = module()

    assert isinstance(deps, Deps)
    assert deps.exists(S3ClientDepKey)
    assert deps.exists(StorageDepKey, route="module-bucket")

    context = context_from_deps(deps)
    storage = context.storage(StorageSpec(name="module-bucket"))

    assert isinstance(storage, S3StorageAdapter)
    assert storage.client is s3_mock
    assert storage.bucket == "module-bucket"

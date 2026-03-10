from unittest.mock import Mock

from forze.application.contracts.storage import StorageDepKey
from forze.application.contracts.tenant import TenantContextDepKey, TenantContextPort
from forze.application.execution import Deps, ExecutionContext
from forze_s3.adapters.storage import S3StorageAdapter
from forze_s3.execution.deps import S3ClientDepKey, S3DepsModule
from forze_s3.execution.deps.deps import s3_storage
from forze_s3.kernel.platform import S3Client


def test_s3_storage_builds_adapter_without_tenant_context() -> None:
    s3_mock = Mock(spec=S3Client)
    deps = Deps(deps={S3ClientDepKey: s3_mock})
    context = ExecutionContext(deps=deps)

    storage = s3_storage(context, bucket="test-bucket")

    assert isinstance(storage, S3StorageAdapter)
    assert storage.client is s3_mock
    assert storage.bucket == "test-bucket"
    assert storage.tenant_context is None


def test_s3_storage_resolves_tenant_context_via_factory() -> None:
    s3_mock = Mock(spec=S3Client)
    tenant_context = Mock(spec=TenantContextPort)
    tenant_context_factory = Mock(return_value=tenant_context)

    deps = Deps(
        deps={
            S3ClientDepKey: s3_mock,
            TenantContextDepKey: tenant_context_factory,
        }
    )
    context = ExecutionContext(deps=deps)

    storage = s3_storage(context, bucket="tenant-bucket")

    assert isinstance(storage, S3StorageAdapter)
    tenant_context_factory.assert_called_once_with()
    assert storage.tenant_context is tenant_context


def test_s3_deps_module_registers_expected_keys() -> None:
    s3_mock = Mock(spec=S3Client)
    module = S3DepsModule(client=s3_mock)

    deps = module()

    assert isinstance(deps, Deps)
    assert deps.exists(S3ClientDepKey)
    assert deps.exists(StorageDepKey)

    context = ExecutionContext(deps=deps)
    storage = context.storage("module-bucket")

    assert isinstance(storage, S3StorageAdapter)
    assert storage.client is s3_mock
    assert storage.bucket == "module-bucket"

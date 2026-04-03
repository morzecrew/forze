from unittest.mock import Mock
from uuid import uuid4

from forze.application.contracts.storage import StorageDepKey, StorageSpec
from forze.application.execution import CallContext, Deps, ExecutionContext, PrincipalContext
from forze_s3.adapters.storage import S3StorageAdapter
from forze_s3.execution.deps import S3ClientDepKey, S3DepsModule
from forze_s3.execution.deps.deps import ConfigurableS3Storage
from forze_s3.kernel.platform import S3Client


def test_s3_storage_factory_builds_adapter_without_tenant() -> None:
    s3_mock = Mock(spec=S3Client)
    deps = Deps.plain({S3ClientDepKey: s3_mock})
    context = ExecutionContext(deps=deps)

    factory = ConfigurableS3Storage(config={"bucket": "test-bucket"})
    storage = factory(context, StorageSpec(name="route"))

    assert isinstance(storage, S3StorageAdapter)
    assert storage.client is s3_mock
    assert storage.bucket == "test-bucket"
    assert storage.tenant_aware is False


def test_s3_storage_factory_resolves_tenant_from_context() -> None:
    s3_mock = Mock(spec=S3Client)
    deps = Deps.plain({S3ClientDepKey: s3_mock})
    context = ExecutionContext(deps=deps)
    tid = uuid4()

    factory = ConfigurableS3Storage(
        config={"bucket": "tenant-bucket", "tenant_aware": True},
    )

    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())
    principal = PrincipalContext(tenant_id=tid)

    with context.bind_call(call=call, principal=principal):
        storage = factory(context, StorageSpec(name="x"))
        assert storage.tenant_provider() == tid


def test_s3_deps_module_registers_expected_keys() -> None:
    s3_mock = Mock(spec=S3Client)
    module = S3DepsModule(
        client=s3_mock,
        storages={"module-bucket": {"bucket": "module-bucket"}},
    )

    deps = module()

    assert isinstance(deps, Deps)
    assert deps.exists(S3ClientDepKey)
    assert deps.exists(StorageDepKey, route="module-bucket")

    context = ExecutionContext(deps=deps)
    storage = context.storage(StorageSpec(name="module-bucket"))

    assert isinstance(storage, S3StorageAdapter)
    assert storage.client is s3_mock
    assert storage.bucket == "module-bucket"

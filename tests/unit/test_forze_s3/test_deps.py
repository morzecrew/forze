from unittest.mock import Mock
from uuid import uuid4

from forze.application.contracts.storage import (
    StorageCommandDepKey,
    StorageQueryDepKey,
    StorageSpec,
)
from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze_s3.adapters.storage import S3StorageAdapter
import pytest

from forze_s3.execution.deps import (
    ConfigurableS3StorageCommand,
    ConfigurableS3StorageQuery,
    S3ClientDepKey,
    S3DepsModule,
    S3StorageConfig,
)
from forze_s3.kernel.client import S3Client


def test_rejects_mapping_config() -> None:
    with pytest.raises(TypeError, match="S3StorageConfig"):
        ConfigurableS3StorageQuery(config={"bucket": "test-bucket"})
    with pytest.raises(TypeError, match="S3StorageConfig"):
        ConfigurableS3StorageCommand(config={"bucket": "test-bucket"})


def test_s3_storage_factory_builds_adapter_without_tenant() -> None:
    s3_mock = Mock(spec=S3Client)
    deps = Deps.plain({S3ClientDepKey: s3_mock})
    context = context_from_deps(deps)

    query = ConfigurableS3StorageQuery(config=S3StorageConfig(bucket="test-bucket"))
    command = ConfigurableS3StorageCommand(config=S3StorageConfig(bucket="test-bucket"))

    for factory in (query, command):
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

    factory = ConfigurableS3StorageQuery(
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
    assert deps.exists(StorageQueryDepKey, route="module-bucket")
    assert deps.exists(StorageCommandDepKey, route="module-bucket")

    context = context_from_deps(deps)
    storage = context.storage.command(StorageSpec(name="module-bucket"))

    assert isinstance(storage, S3StorageAdapter)
    assert storage.client is s3_mock
    assert storage.bucket == "module-bucket"


# ----------------------- #
# tenant-isolation tier validation


def _shared_client() -> object:
    # A plain S3Client mock is not a RoutedS3Client → derived tier comes from config.
    return Mock(spec=S3Client)


def test_required_dedicated_isolation_rejects_shared_client() -> None:
    from forze.base.exceptions import CoreException

    with pytest.raises(CoreException, match="s3_storage_tenancy_validation_failed"):
        S3DepsModule(
            client=_shared_client(),
            required_tenant_isolation="dedicated",
            storages={"r": S3StorageConfig(bucket="b", tenant_aware=True)},
        )


def test_namespace_floor_satisfied_by_per_tenant_bucket_resolver() -> None:
    # A dynamic (per-tenant) bucket resolver derives the "namespace" tier.
    S3DepsModule(
        client=_shared_client(),
        required_tenant_isolation="namespace",
        storages={"r": S3StorageConfig(bucket=lambda t: f"tenant-{t}")},
    )


def test_namespace_floor_rejects_static_bucket() -> None:
    from forze.base.exceptions import CoreException

    # Static bucket + tenant_aware path prefix is only "tagged" — below a "namespace" floor.
    with pytest.raises(CoreException, match="s3_storage_tenancy_validation_failed"):
        S3DepsModule(
            client=_shared_client(),
            required_tenant_isolation="namespace",
            storages={"r": S3StorageConfig(bucket="static", tenant_aware=True)},
        )


def test_no_isolation_floor_allows_any_wiring() -> None:
    S3DepsModule(
        client=_shared_client(),
        storages={"r": S3StorageConfig(bucket="static", tenant_aware=True)},
    )

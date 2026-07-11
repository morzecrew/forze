"""Unit tests for the cloud KMS tenant provisioners (mocked clients).

Each provisioner resolves the tenant through the **same** directory the keyring encrypts
through, so a provisioned key and the encrypt-path key can never drift. Teardown is opt-in
everywhere: destroying a tenant's KEK is unrecoverable.
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("aioboto3")
pytest.importorskip("google.cloud.kms")
pytest.importorskip("yandexcloud")

from forze.application.contracts.crypto import (
    KeyRef,
    StaticKeyDirectory,
    TenantTemplateKeyDirectory,
)
from forze.application.contracts.tenancy import TenantIdentity, TenantProvisionerPort
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kms.aws import AwsKmsClientPort, AwsKmsTenantProvisioner
from forze_kms.gcp import GcpKmsClientPort, GcpKmsTenantProvisioner
from forze_kms.yc import YcKmsClientPort, YcKmsKeyDirectory, YcKmsTenantProvisioner

# ----------------------- #

_TENANT = TenantIdentity(tenant_id=uuid4())
_GCP_RING = "projects/p/locations/global/keyRings/app"


# ----------------------- #
# AWS — CMK addressed by a caller-chosen alias


def _aws(**kwargs: object) -> tuple[MagicMock, AwsKmsTenantProvisioner]:
    client = MagicMock(spec=AwsKmsClientPort)
    client.find_key_id_by_alias = AsyncMock(return_value=None)
    client.create_key_with_alias = AsyncMock(return_value="cmk-123")
    client.delete_alias = AsyncMock()
    client.schedule_key_deletion = AsyncMock()

    provisioner = AwsKmsTenantProvisioner(
        client=client,
        directory=TenantTemplateKeyDirectory(
            template="alias/tenant-{tenant_id}", default_key_id="alias/shared"
        ),
        **kwargs,  # type: ignore[arg-type]
    )

    return client, provisioner


async def test_aws_provision_creates_the_cmk_behind_the_alias() -> None:
    client, provisioner = _aws()

    await provisioner.provision(_TENANT)

    client.create_key_with_alias.assert_awaited_once()
    alias = client.create_key_with_alias.await_args[0][0]
    assert alias == f"alias/tenant-{_TENANT.tenant_id}"


async def test_aws_provision_is_idempotent() -> None:
    client, provisioner = _aws()
    client.find_key_id_by_alias = AsyncMock(return_value="cmk-123")  # already there

    await provisioner.provision(_TENANT)

    client.create_key_with_alias.assert_not_awaited()


async def test_aws_deprovision_is_off_by_default() -> None:
    client, provisioner = _aws()
    client.find_key_id_by_alias = AsyncMock(return_value="cmk-123")

    await provisioner.deprovision(_TENANT)

    client.delete_alias.assert_not_awaited()
    client.schedule_key_deletion.assert_not_awaited()


async def test_aws_deprovision_drops_alias_then_schedules_deletion() -> None:
    client, provisioner = _aws(allow_deletion=True, pending_window_days=7)
    client.find_key_id_by_alias = AsyncMock(return_value="cmk-123")

    await provisioner.deprovision(_TENANT)

    client.delete_alias.assert_awaited_once()
    client.schedule_key_deletion.assert_awaited_once_with(
        "cmk-123", pending_window_days=7
    )


async def test_aws_deprovision_of_an_already_gone_tenant_is_a_no_op() -> None:
    """Teardown is retried after a partial offboarding, so it must tolerate absence."""

    client, provisioner = _aws(allow_deletion=True)
    client.find_key_id_by_alias = AsyncMock(return_value=None)

    await provisioner.deprovision(_TENANT)

    client.delete_alias.assert_not_awaited()
    client.schedule_key_deletion.assert_not_awaited()


async def test_yc_deprovision_of_an_already_gone_tenant_is_a_no_op() -> None:
    client, _, provisioner = _yc(allow_deletion=True)
    client.find_key_id_by_name = AsyncMock(return_value=None)

    await provisioner.deprovision(_TENANT)

    client.delete_key.assert_not_awaited()


async def test_aws_rejects_a_directory_that_is_not_an_alias() -> None:
    """A CMK id is minted by KMS, so a directory must resolve to an alias."""

    client = MagicMock(spec=AwsKmsClientPort)
    provisioner = AwsKmsTenantProvisioner(
        client=client,
        directory=StaticKeyDirectory(KeyRef(key_id="cmk-123")),  # not an alias
    )

    with pytest.raises(CoreException) as ei:
        await provisioner.provision(_TENANT)

    assert ei.value.code == "core.crypto.key_id_not_an_alias"


def test_aws_rejects_an_out_of_range_deletion_window() -> None:
    with pytest.raises(CoreException) as ei:
        _aws(pending_window_days=1)

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.crypto.pending_window_invalid"


# ----------------------- #
# GCP — CryptoKey addressed by a caller-chosen id


def _gcp(**kwargs: object) -> tuple[MagicMock, GcpKmsTenantProvisioner]:
    client = MagicMock(spec=GcpKmsClientPort)
    client.ensure_crypto_key = AsyncMock(return_value="…")
    client.destroy_crypto_key_versions = AsyncMock(return_value=1)

    provisioner = GcpKmsTenantProvisioner(
        client=client,
        directory=TenantTemplateKeyDirectory(
            template=f"{_GCP_RING}/cryptoKeys/tenant-{{tenant_id}}",
            default_key_id=f"{_GCP_RING}/cryptoKeys/shared",
        ),
        **kwargs,  # type: ignore[arg-type]
    )

    return client, provisioner


async def test_gcp_provision_splits_the_resource_name_and_creates_the_key() -> None:
    client, provisioner = _gcp()

    await provisioner.provision(_TENANT)

    client.ensure_crypto_key.assert_awaited_once_with(
        _GCP_RING, f"tenant-{_TENANT.tenant_id}"
    )


async def test_gcp_deprovision_is_off_by_default() -> None:
    client, provisioner = _gcp()

    await provisioner.deprovision(_TENANT)

    client.destroy_crypto_key_versions.assert_not_awaited()


async def test_gcp_deprovision_destroys_the_versions() -> None:
    """GCP cannot delete a CryptoKey — destroying its versions is the strongest teardown."""

    client, provisioner = _gcp(allow_deletion=True)

    await provisioner.deprovision(_TENANT)

    client.destroy_crypto_key_versions.assert_awaited_once_with(
        f"{_GCP_RING}/cryptoKeys/tenant-{_TENANT.tenant_id}"
    )


async def test_gcp_rejects_a_directory_that_is_not_a_crypto_key() -> None:
    client = MagicMock(spec=GcpKmsClientPort)
    provisioner = GcpKmsTenantProvisioner(
        client=client,
        directory=StaticKeyDirectory(KeyRef(key_id="just-a-name")),
    )

    with pytest.raises(CoreException) as ei:
        await provisioner.provision(_TENANT)

    assert ei.value.code == "core.crypto.key_id_not_a_crypto_key"


# ----------------------- #
# Yandex Cloud — key id is minted by the service, so the directory looks it up by name


def _yc(**kwargs: object) -> tuple[MagicMock, YcKmsKeyDirectory, YcKmsTenantProvisioner]:
    client = MagicMock(spec=YcKmsClientPort)
    client.find_key_id_by_name = AsyncMock(return_value=None)
    client.create_key = AsyncMock(return_value="abj-new")
    client.delete_key = AsyncMock()

    directory = YcKmsKeyDirectory(client=client, folder_id="fldr-1")
    provisioner = YcKmsTenantProvisioner(
        client=client,
        directory=directory,
        **kwargs,  # type: ignore[arg-type]
    )

    return client, directory, provisioner


async def test_yc_provision_creates_the_key_under_the_directory_name() -> None:
    client, _, provisioner = _yc()

    await provisioner.provision(_TENANT)

    client.create_key.assert_awaited_once_with(
        "fldr-1",
        f"tenant-{_TENANT.tenant_id}",
        algorithm="AES_256",
        description=None,
    )


async def test_yc_provision_is_idempotent() -> None:
    client, _, provisioner = _yc()
    client.find_key_id_by_name = AsyncMock(return_value="abj-existing")

    await provisioner.provision(_TENANT)

    client.create_key.assert_not_awaited()


async def test_yc_deprovision_is_off_by_default() -> None:
    client, _, provisioner = _yc()
    client.find_key_id_by_name = AsyncMock(return_value="abj-existing")

    await provisioner.deprovision(_TENANT)

    client.delete_key.assert_not_awaited()


async def test_yc_deprovision_deletes_the_looked_up_key() -> None:
    client, _, provisioner = _yc(allow_deletion=True)
    client.find_key_id_by_name = AsyncMock(return_value="abj-existing")

    await provisioner.deprovision(_TENANT)

    client.delete_key.assert_awaited_once_with("abj-existing")


async def test_yc_directory_resolves_a_tenant_to_the_minted_key_id() -> None:
    client, directory, _ = _yc()
    client.find_key_id_by_name = AsyncMock(return_value="abj-existing")

    key_ref = await directory.resolve(_TENANT)

    assert key_ref.key_id == "abj-existing"
    client.find_key_id_by_name.assert_awaited_once_with(
        "fldr-1", f"tenant-{_TENANT.tenant_id}"
    )


async def test_yc_directory_rejects_an_unprovisioned_tenant() -> None:
    _, directory, _ = _yc()  # find_key_id_by_name returns None

    with pytest.raises(CoreException) as ei:
        await directory.resolve(_TENANT)

    assert ei.value.code == "core.crypto.tenant_key_not_provisioned"


async def test_yc_directory_rejects_an_unbound_tenant_without_a_default() -> None:
    _, directory, _ = _yc()

    with pytest.raises(CoreException) as ei:
        await directory.resolve(None)

    assert ei.value.code == "core.crypto.default_key_missing"


# ----------------------- #


def test_all_three_satisfy_the_provisioner_port() -> None:
    _, aws = _aws()
    _, gcp = _gcp()
    _, _, yc = _yc()

    for provisioner in (aws, gcp, yc):
        assert isinstance(provisioner, TenantProvisionerPort)

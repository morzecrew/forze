"""Integration tests for AWS KMS envelope key management (LocalStack)."""

from uuid import uuid4

import pytest

pytest.importorskip("aioboto3")

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
    TenantTemplateKeyDirectory,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze.base.exceptions import CoreException
from forze_kms.aws import AwsKmsClient, AwsKmsKeyManagement

# ----------------------- #


@pytest.fixture
def kms(kms_client: AwsKmsClient) -> AwsKmsKeyManagement:
    return AwsKmsKeyManagement(client=kms_client)


# ----------------------- #


@pytest.mark.integration
async def test_generate_then_unwrap_round_trip(
    kms: AwsKmsKeyManagement, cmk_id: str
) -> None:
    data_key = await kms.generate_data_key(KeyRef(key_id=cmk_id))

    assert len(data_key.plaintext) == 32  # AES-256 data key
    assert data_key.wrapped  # opaque KMS ciphertext blob
    assert data_key.key_version is None  # KMS rotation is transparent

    recovered = await kms.unwrap_data_key(
        wrapped=data_key.wrapped,
        key_ref=KeyRef(key_id=cmk_id),
    )

    assert recovered == data_key.plaintext


# ....................... #


@pytest.mark.integration
async def test_full_keyring_round_trip_against_kms(
    kms: AwsKmsKeyManagement, cmk_id: str
) -> None:
    keyring = Keyring(
        kms=kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id=cmk_id)),
    )

    blob = await keyring.encrypt(b"sensitive payload", tenant=None, aad=b"ctx")

    assert await keyring.decrypt(blob, aad=b"ctx") == b"sensitive payload"
    # A mismatched AAD must not authenticate.
    with pytest.raises(CoreException):
        await keyring.decrypt(blob, aad=b"other")


# ....................... #


@pytest.mark.integration
async def test_per_tenant_keys_isolate_and_confused_deputy_is_rejected(
    kms_client: AwsKmsClient,
) -> None:
    """Two tenants get two CMKs; a cross-tenant key_id is rejected before KMS unwrap."""

    tenant_a = TenantIdentity(tenant_id=uuid4())
    tenant_b = TenantIdentity(tenant_id=uuid4())

    # Each tenant's CMK is addressed by a per-tenant KMS alias, so the directory's
    # ``alias/{tenant_id}`` template resolves each tenant to its own key.
    async with kms_client.client() as c:
        cmk_a = (await c.create_key(Description="tenant-a"))["KeyMetadata"]["KeyId"]
        cmk_b = (await c.create_key(Description="tenant-b"))["KeyMetadata"]["KeyId"]
        await c.create_alias(
            AliasName=f"alias/{tenant_a.tenant_id}", TargetKeyId=cmk_a
        )
        await c.create_alias(
            AliasName=f"alias/{tenant_b.tenant_id}", TargetKeyId=cmk_b
        )

    directory = TenantTemplateKeyDirectory(
        template="alias/{tenant_id}",
        default_key_id=f"alias/{tenant_a.tenant_id}",
    )
    keyring = Keyring(
        kms=AwsKmsKeyManagement(client=kms_client),
        aead=AesGcmAead(),
        directory=directory,
    )

    blob_a = await keyring.encrypt(b"a-secret", tenant=tenant_a)

    # Tenant A reads its own ciphertext.
    assert await keyring.decrypt(blob_a, tenant=tenant_a) == b"a-secret"

    # Tenant B cannot decrypt tenant A's ciphertext: the keyring's confused-deputy
    # guard rejects the envelope's key_id (tenant A's CMK) before any KMS unwrap.
    with pytest.raises(CoreException) as ei:
        await keyring.decrypt(blob_a, tenant=tenant_b)

    assert ei.value.code == "core.crypto.key_id_unauthorized"

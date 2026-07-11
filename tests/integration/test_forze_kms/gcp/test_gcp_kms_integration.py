"""Integration tests for GCP KMS envelope key management (fake-cloud-kms emulator)."""

from uuid import uuid4

import pytest

pytest.importorskip("google.cloud.kms")

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
    TenantTemplateKeyDirectory,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze.base.exceptions import CoreException
from forze_kms.gcp import GcpKmsClient, GcpKmsKeyManagement, GcpKmsTenantProvisioner

# ----------------------- #

_PROJECT = "forze-test"
_LOCATION = "global"


@pytest.fixture
def kms(gcp_kms_client: GcpKmsClient) -> GcpKmsKeyManagement:
    return GcpKmsKeyManagement(client=gcp_kms_client)


# ----------------------- #


@pytest.mark.integration
async def test_generate_then_unwrap_round_trip(
    kms: GcpKmsKeyManagement, cmk_name: str
) -> None:
    data_key = await kms.generate_data_key(KeyRef(key_id=cmk_name))

    assert len(data_key.plaintext) == 32  # AES-256 data key, minted client-side
    assert data_key.wrapped  # opaque KMS ciphertext
    assert data_key.key_version is None  # KMS rotation is transparent

    recovered = await kms.unwrap_data_key(
        wrapped=data_key.wrapped,
        key_ref=KeyRef(key_id=cmk_name),
    )

    assert recovered == data_key.plaintext


# ....................... #


@pytest.mark.integration
async def test_full_keyring_round_trip_against_kms(
    kms: GcpKmsKeyManagement, cmk_name: str
) -> None:
    keyring = Keyring(
        kms=kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id=cmk_name)),
    )

    blob = await keyring.encrypt(b"sensitive payload", tenant=None, aad=b"ctx")

    assert await keyring.decrypt(blob, aad=b"ctx") == b"sensitive payload"
    # A mismatched AAD must not authenticate.
    with pytest.raises(CoreException):
        await keyring.decrypt(blob, aad=b"other")


# ....................... #


@pytest.mark.integration
async def test_kek_rotation_is_transparent_through_the_keyring(
    gcp_kms_client: GcpKmsClient, cmk_name: str
) -> None:
    """A new CryptoKey version becomes primary while envelopes written under the old
    version still decrypt — rotation never orphans data."""

    from google.cloud.kms_v1.types import CryptoKeyVersion

    kms = GcpKmsKeyManagement(client=gcp_kms_client)
    directory = StaticKeyDirectory(KeyRef(key_id=cmk_name))
    # max_dek_messages=1 forces a fresh (re-wrapped) data key on the post-rotation
    # write, so its envelope is wrapped under the new primary version.
    writer = Keyring(kms=kms, aead=AesGcmAead(), directory=directory, max_dek_messages=1)

    env_before = await writer.encrypt(b"before-rotation", tenant=None)

    # Rotate: add a version and make it the primary the next Encrypt uses.
    async with gcp_kms_client.client() as c:
        version = await c.create_crypto_key_version(
            parent=cmk_name, crypto_key_version=CryptoKeyVersion()
        )
        await c.update_crypto_key_primary_version(
            name=cmk_name, crypto_key_version_id=version.name.split("/")[-1]
        )

    env_after = await writer.encrypt(b"after-rotation", tenant=None)

    # A cold keyring (no warmed caches) must unwrap both envelopes through KMS —
    # the pre-rotation one under the old version, the post-rotation one under the new.
    reader = Keyring(kms=kms, aead=AesGcmAead(), directory=directory)
    assert await reader.decrypt(env_before) == b"before-rotation"
    assert await reader.decrypt(env_after) == b"after-rotation"


# ....................... #


@pytest.mark.integration
async def test_per_tenant_keys_isolate_and_confused_deputy_is_rejected(
    gcp_kms_client: GcpKmsClient,
) -> None:
    """Two tenants get two CryptoKeys; a cross-tenant key_id is rejected pre-unwrap."""

    from google.cloud.kms_v1.types import CryptoKey

    tenant_a = TenantIdentity(tenant_id=uuid4())
    tenant_b = TenantIdentity(tenant_id=uuid4())

    location = f"projects/{_PROJECT}/locations/{_LOCATION}"
    ring_id = f"ring-{uuid4().hex[:8]}"
    ring = f"{location}/keyRings/{ring_id}"

    # One ring, a CryptoKey per tenant named by tenant id, so the directory's
    # ``.../cryptoKeys/{tenant_id}`` template resolves each tenant to its own key.
    async with gcp_kms_client.client() as c:
        await c.create_key_ring(parent=location, key_ring_id=ring_id, key_ring={})
        for tenant in (tenant_a, tenant_b):
            await c.create_crypto_key(
                parent=ring,
                crypto_key_id=str(tenant.tenant_id),
                crypto_key=CryptoKey(
                    purpose=CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT
                ),
            )

    directory = TenantTemplateKeyDirectory(
        template=f"{ring}/cryptoKeys/{{tenant_id}}",
        default_key_id=f"{ring}/cryptoKeys/{tenant_a.tenant_id}",
    )
    keyring = Keyring(
        kms=GcpKmsKeyManagement(client=gcp_kms_client),
        aead=AesGcmAead(),
        directory=directory,
    )

    blob_a = await keyring.encrypt(b"a-secret", tenant=tenant_a)

    assert await keyring.decrypt(blob_a, tenant=tenant_a) == b"a-secret"

    # Tenant B cannot decrypt tenant A's ciphertext: the keyring's confused-deputy
    # guard rejects the envelope's key_id before any KMS unwrap.
    with pytest.raises(CoreException) as ei:
        await keyring.decrypt(blob_a, tenant=tenant_b)

    assert ei.value.code == "core.crypto.key_id_unauthorized"


# ....................... #


@pytest.mark.integration
async def test_provisioning_a_tenant_makes_its_key_usable(
    gcp_kms_client: GcpKmsClient,
) -> None:
    """Onboarding a tenant creates its CryptoKey, so the keyring can encrypt for it."""

    tenant = TenantIdentity(tenant_id=uuid4())
    location = f"projects/{_PROJECT}/locations/{_LOCATION}"
    ring_id = f"ring-{uuid4().hex[:8]}"
    ring = f"{location}/keyRings/{ring_id}"

    # The key ring is a shared, long-lived resource — not per-tenant.
    async with gcp_kms_client.client() as c:
        await c.create_key_ring(parent=location, key_ring_id=ring_id, key_ring={})

    directory = TenantTemplateKeyDirectory(
        template=f"{ring}/cryptoKeys/tenant-{{tenant_id}}",
        default_key_id=f"{ring}/cryptoKeys/shared",
    )
    keyring = Keyring(
        kms=GcpKmsKeyManagement(client=gcp_kms_client),
        aead=AesGcmAead(),
        directory=directory,
    )
    provisioner = GcpKmsTenantProvisioner(client=gcp_kms_client, directory=directory)

    # Before onboarding, the tenant has no key at all.
    with pytest.raises(CoreException):
        await keyring.encrypt(b"secret", tenant=tenant)

    await provisioner.provision(tenant)

    blob = await keyring.encrypt(b"secret", tenant=tenant)
    assert await keyring.decrypt(blob, tenant=tenant) == b"secret"

    # Re-provisioning an existing key is a no-op (AlreadyExists is tolerated).
    await provisioner.provision(tenant)
    assert await keyring.decrypt(blob, tenant=tenant) == b"secret"

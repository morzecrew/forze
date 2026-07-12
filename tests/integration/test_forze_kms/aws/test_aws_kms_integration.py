"""Integration tests for AWS KMS envelope key management (floci emulator).

AWS KMS rotation keeps the same CMK id and rotates the backing material
transparently (no version in the wrapped blob), so rotation transparency is
asserted through ``RotateKeyOnDemand``: envelopes wrapped before a rotation
must still decrypt after it. Vault (``v1``→``v2``) and GCP (primary CryptoKey
version) cover the *versioned* rotation shape.
"""

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
from forze_kms.aws import AwsKmsClient, AwsKmsKeyManagement, AwsKmsTenantProvisioner

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
async def test_kek_rotation_is_transparent_through_the_keyring(
    kms_client: AwsKmsClient, cmk_id: str
) -> None:
    """On-demand rotation swaps the CMK's backing material while envelopes wrapped
    before it still decrypt — rotation never orphans data, and the key id never
    changes (AWS rotation is fully transparent; nothing versions the envelope)."""

    kms = AwsKmsKeyManagement(client=kms_client)
    directory = StaticKeyDirectory(KeyRef(key_id=cmk_id))
    # max_dek_messages=1 forces a fresh (re-wrapped) data key on the post-rotation
    # write, so its envelope is wrapped under the rotated material.
    writer = Keyring(kms=kms, aead=AesGcmAead(), directory=directory, max_dek_messages=1)

    env_before = await writer.encrypt(b"before-rotation", tenant=None)

    async with kms_client.client() as c:
        await c.rotate_key_on_demand(KeyId=cmk_id)

    env_after = await writer.encrypt(b"after-rotation", tenant=None)

    # A cold keyring (no warmed caches) must unwrap both envelopes through KMS —
    # the pre-rotation one under the old material, the post-rotation one under
    # the new, both addressed by the same CMK id.
    reader = Keyring(kms=kms, aead=AesGcmAead(), directory=directory)
    assert await reader.decrypt(env_before) == b"before-rotation"
    assert await reader.decrypt(env_after) == b"after-rotation"


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


# ....................... #


@pytest.mark.integration
async def test_provisioning_a_tenant_makes_its_key_usable(
    kms_client: AwsKmsClient,
) -> None:
    """Onboarding a tenant creates the CMK behind its alias, so the keyring can encrypt."""

    tenant = TenantIdentity(tenant_id=uuid4())
    directory = TenantTemplateKeyDirectory(
        template="alias/tenant-{tenant_id}",
        default_key_id="alias/shared",
    )
    keyring = Keyring(
        kms=AwsKmsKeyManagement(client=kms_client),
        aead=AesGcmAead(),
        directory=directory,
    )
    provisioner = AwsKmsTenantProvisioner(client=kms_client, directory=directory)

    # Before onboarding, the tenant has no key at all.
    with pytest.raises(CoreException):
        await keyring.encrypt(b"secret", tenant=tenant)

    await provisioner.provision(tenant)

    blob = await keyring.encrypt(b"secret", tenant=tenant)
    assert await keyring.decrypt(blob, tenant=tenant) == b"secret"

    # Re-provisioning is a no-op: the alias already resolves to the same CMK.
    alias = f"alias/tenant-{tenant.tenant_id}"
    cmk = await kms_client.find_key_id_by_alias(alias)
    await provisioner.provision(tenant)
    assert await kms_client.find_key_id_by_alias(alias) == cmk


# ....................... #


@pytest.mark.integration
async def test_deprovision_is_opt_in_and_retires_the_alias(
    kms_client: AwsKmsClient,
) -> None:
    tenant = TenantIdentity(tenant_id=uuid4())
    alias = f"alias/tenant-{tenant.tenant_id}"
    directory = TenantTemplateKeyDirectory(
        template="alias/tenant-{tenant_id}",
        default_key_id="alias/shared",
    )

    guarded = AwsKmsTenantProvisioner(client=kms_client, directory=directory)
    await guarded.provision(tenant)

    # Teardown is off by default — a tenant's KEK is never destroyed implicitly.
    await guarded.deprovision(tenant)
    assert await kms_client.find_key_id_by_alias(alias) is not None

    destructive = AwsKmsTenantProvisioner(
        client=kms_client,
        directory=directory,
        allow_deletion=True,
        pending_window_days=7,
    )
    await destructive.deprovision(tenant)

    # The alias is gone; the CMK itself serves out its deletion window.
    assert await kms_client.find_key_id_by_alias(alias) is None


# ....................... #


@pytest.mark.integration
async def test_migrating_a_tenant_to_a_new_kek(kms_client: AwsKmsClient) -> None:
    """Replace a tenant's KEK end to end against real KMS: overlap → re-encrypt → drop.

    Without the overlap this is impossible — repointing the tenant at a new CMK strands
    everything under the old one, because the keyring refuses an envelope whose key id is
    not the tenant's current key (it cannot even be read back to migrate it).
    """

    from forze.base.crypto import unpack_envelope

    tenant = TenantIdentity(tenant_id=uuid4())
    old_template = "alias/v1-{tenant_id}"
    new_template = "alias/v2-{tenant_id}"

    def _keyring(directory: TenantTemplateKeyDirectory) -> Keyring:
        return Keyring(
            kms=AwsKmsKeyManagement(client=kms_client),
            aead=AesGcmAead(),
            directory=directory,
        )

    # 1. The tenant is provisioned on its v1 CMK and writes data.
    old_dir = TenantTemplateKeyDirectory(
        template=old_template, default_key_id="alias/shared"
    )
    await AwsKmsTenantProvisioner(client=kms_client, directory=old_dir).provision(tenant)

    written = await _keyring(old_dir).encrypt(b"tenant payload", tenant=tenant)

    # 2. A new CMK is provisioned and the directory opens a read overlap.
    overlap_dir = TenantTemplateKeyDirectory(
        template=new_template,
        default_key_id="alias/shared",
        previous_template=old_template,
    )
    await AwsKmsTenantProvisioner(
        client=kms_client, directory=overlap_dir
    ).provision(tenant)

    migrating = _keyring(overlap_dir)

    # Data written under the old CMK is still readable — this is what makes the
    # migration possible at all.
    assert await migrating.decrypt(written, tenant=tenant) == b"tenant payload"

    # 3. The sweep's read→write round-trip re-seals it under the new CMK.
    rewritten = await migrating.encrypt(
        await migrating.decrypt(written, tenant=tenant), tenant=tenant
    )
    assert unpack_envelope(rewritten).key_id == f"alias/v2-{tenant.tenant_id}"

    # 4. Overlap dropped. The migrated value reads; the old CMK is now free to retire.
    migrated = _keyring(
        TenantTemplateKeyDirectory(template=new_template, default_key_id="alias/shared")
    )
    assert await migrated.decrypt(rewritten, tenant=tenant) == b"tenant payload"

    with pytest.raises(CoreException) as ei:
        await migrated.decrypt(written, tenant=tenant)

    assert ei.value.code == "core.crypto.key_id_unauthorized"

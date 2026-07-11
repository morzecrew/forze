"""Integration tests for Vault Transit envelope key management (real Vault)."""

import pytest

pytest.importorskip("hvac")

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.integrations.crypto import Keyring
from forze_vault import VaultConfig, VaultTransitKeyManagement
from forze_vault.kernel.client import VaultClient

# ----------------------- #

_KEY = "app-cmk"


@pytest.fixture
async def transit_kms(vault_container):
    container, hvac_client = vault_container

    # Create the Transit key the adapter will wrap data keys under.
    hvac_client.secrets.transit.create_key(name=_KEY, mount_point="transit")

    config = VaultConfig(
        url=container.get_connection_url(),
        token=container.root_token,
        transit_mount="transit",
        verify=False,
    )
    client = VaultClient(config=config)
    await client.initialize()

    try:
        yield VaultTransitKeyManagement(client=client)
    finally:
        await client.close()


# ----------------------- #


@pytest.mark.integration
async def test_generate_then_unwrap_round_trip(
    transit_kms: VaultTransitKeyManagement,
) -> None:
    data_key = await transit_kms.generate_data_key(KeyRef(key_id=_KEY))

    assert len(data_key.plaintext) == 32  # 256-bit data key
    assert data_key.wrapped.startswith(b"vault:")
    assert data_key.key_version is not None

    recovered = await transit_kms.unwrap_data_key(
        wrapped=data_key.wrapped,
        key_ref=KeyRef(key_id=_KEY),
    )

    assert recovered == data_key.plaintext


# ....................... #


@pytest.mark.integration
async def test_full_keyring_round_trip_against_vault(
    transit_kms: VaultTransitKeyManagement,
) -> None:
    keyring = Keyring(
        kms=transit_kms,
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id=_KEY)),
    )

    blob = await keyring.encrypt(b"sensitive payload", tenant=None, aad=b"ctx")

    assert await keyring.decrypt(blob, aad=b"ctx") == b"sensitive payload"


# ....................... #


@pytest.mark.integration
async def test_kek_rotation_bumps_version_and_stays_transparent(
    transit_kms: VaultTransitKeyManagement,
    vault_container,
) -> None:
    """Rotating the Transit key wraps new data keys under a new version while old
    wrapped keys still unwrap — rotation never orphans data."""

    _, hvac_client = vault_container

    dk_v1 = await transit_kms.generate_data_key(KeyRef(key_id=_KEY))
    assert dk_v1.key_version == "v1"

    # Rotate the KEK (Transit key) to a new version.
    hvac_client.secrets.transit.rotate_key(name=_KEY, mount_point="transit")

    # New data keys are wrapped under the new version...
    dk_v2 = await transit_kms.generate_data_key(KeyRef(key_id=_KEY))
    assert dk_v2.key_version == "v2"

    # ...and the pre-rotation wrapped key still unwraps (self-describing envelope).
    recovered = await transit_kms.unwrap_data_key(
        wrapped=dk_v1.wrapped, key_ref=KeyRef(key_id=_KEY)
    )
    assert recovered == dk_v1.plaintext
